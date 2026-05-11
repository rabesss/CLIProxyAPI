package executor

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

const (
	devinProvider          = "devin"
	windsurfProvider       = "windsurf"
	defaultDevinCommand    = "devin"
	devinACPAuthMethod     = "windsurf-api-key"
	devinDefaultMode       = "ask"
	devinCredentialsAPIKey = "windsurf_api_key"
)

// DevinExecutor talks to Devin for Terminal through its ACP stdio server.
// The same executor is used for Devin and Windsurf auth entries because Devin's
// ACP server accepts Windsurf API keys and also backs Windsurf-authenticated
// Devin for Terminal sessions.
type DevinExecutor struct {
	cfg      *config.Config
	provider string
}

// NewDevinExecutor creates a Devin/Windsurf ACP executor for provider.
func NewDevinExecutor(cfg *config.Config, provider string) *DevinExecutor {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if provider == "" {
		provider = devinProvider
	}
	return &DevinExecutor{cfg: cfg, provider: provider}
}

// Identifier returns the provider identifier handled by this executor.
func (e *DevinExecutor) Identifier() string { return e.provider }

// Execute runs a non-streaming request through Devin ACP and returns a response
// in the caller's original API format.
func (e *DevinExecutor) Execute(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (resp cliproxyexecutor.Response, err error) {
	prompt, errPrompt := devinPromptFromRequest(req, opts)
	if errPrompt != nil {
		return resp, errPrompt
	}
	var usage devinUsage
	var text bytes.Buffer
	err = e.runACP(ctx, auth, req.Model, prompt, func(chunk string) bool {
		text.WriteString(chunk)
		return true
	}, &usage)
	if err != nil {
		return resp, err
	}
	payload, errBuild := devinBuildNonStreamResponse(opts.SourceFormat, req.Model, text.String(), usage)
	if errBuild != nil {
		return resp, errBuild
	}
	return cliproxyexecutor.Response{Payload: payload}, nil
}

// ExecuteStream runs a streaming request through Devin ACP. Chunks are emitted
// already shaped for the inbound API handler.
func (e *DevinExecutor) ExecuteStream(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	prompt, errPrompt := devinPromptFromRequest(req, opts)
	if errPrompt != nil {
		return nil, errPrompt
	}
	chunks := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		var usage devinUsage
		created := time.Now().Unix()
		id := "devin-" + randomHex(8)
		send := func(payload []byte) bool {
			if len(payload) == 0 {
				return true
			}
			select {
			case <-ctx.Done():
				return false
			case chunks <- cliproxyexecutor.StreamChunk{Payload: payload}:
				return true
			}
		}
		errRun := e.runACP(ctx, auth, req.Model, prompt, func(text string) bool {
			payload, errChunk := devinBuildStreamDelta(opts.SourceFormat, id, req.Model, text, created)
			if errChunk != nil {
				select {
				case <-ctx.Done():
				case chunks <- cliproxyexecutor.StreamChunk{Err: errChunk}:
				}
				return false
			}
			return send(payload)
		}, &usage)
		if errRun != nil {
			select {
			case <-ctx.Done():
			case chunks <- cliproxyexecutor.StreamChunk{Err: errRun}:
			}
			return
		}
		finalPayload, errFinal := devinBuildStreamFinal(opts.SourceFormat, id, req.Model, created, usage)
		if errFinal != nil {
			select {
			case <-ctx.Done():
			case chunks <- cliproxyexecutor.StreamChunk{Err: errFinal}:
			}
			return
		}
		_ = send(finalPayload)
	}()
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

// CountTokens returns a lightweight local estimate. Devin ACP does not expose a
// token-count-only method, so avoid burning a model request for counting.
func (e *DevinExecutor) CountTokens(ctx context.Context, auth *cliproxyauth.Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	_ = ctx
	_ = auth
	prompt, err := devinPromptFromRequest(req, opts)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	tokens := estimateDevinPromptTokens(prompt)
	payload, errMarshal := json.Marshal(map[string]any{"totalTokens": tokens, "estimated": true})
	if errMarshal != nil {
		return cliproxyexecutor.Response{}, errMarshal
	}
	return cliproxyexecutor.Response{Payload: payload}, nil
}

// Refresh is a no-op for Devin ACP; credentials are supplied from the auth
// entry or Devin's credentials.toml on every process start.
func (e *DevinExecutor) Refresh(ctx context.Context, auth *cliproxyauth.Auth) (*cliproxyauth.Auth, error) {
	_ = ctx
	return auth, nil
}

// HttpRequest is not supported because Devin exposes ACP, not a raw HTTP model API.
func (e *DevinExecutor) HttpRequest(ctx context.Context, auth *cliproxyauth.Auth, req *http.Request) (*http.Response, error) {
	_ = ctx
	_ = auth
	_ = req
	return nil, statusErr{code: http.StatusNotImplemented, msg: "devin executor does not support raw HTTP requests"}
}

func (e *DevinExecutor) runACP(ctx context.Context, auth *cliproxyauth.Auth, model string, prompt []devinContentBlock, onText func(string) bool, usage *devinUsage) error {
	if ctx == nil {
		ctx = context.Background()
	}
	client, errStart := newDevinACPClient(ctx, auth)
	if errStart != nil {
		return errStart
	}
	defer client.Close()

	if _, errInit := client.Call(ctx, "initialize", map[string]any{
		"protocolVersion": 1,
		"clientCapabilities": map[string]any{
			"terminal": false,
			"fs": map[string]bool{
				"readTextFile":  false,
				"writeTextFile": false,
			},
		},
		"clientInfo": map[string]string{
			"name":    "cliproxyapi",
			"title":   "CLIProxyAPI",
			"version": "devin-acp",
		},
	}); errInit != nil {
		return errInit
	}

	apiKey, errKey := resolveDevinAPIKey(auth)
	if errKey != nil {
		return errKey
	}
	if apiKey != "" {
		if _, errAuth := client.Call(ctx, "authenticate", map[string]any{
			"methodId": devinACPAuthMethod,
			"meta": map[string]string{
				"api_key": apiKey,
			},
		}); errAuth != nil {
			return errAuth
		}
	}

	cwd := resolveDevinCWD(auth)
	result, errSession := client.Call(ctx, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}})
	if errSession != nil {
		return errSession
	}
	sessionID := strings.TrimSpace(gjson.GetBytes(result, "sessionId").String())
	if sessionID == "" {
		return fmt.Errorf("devin acp: session/new response missing sessionId")
	}

	_ = client.CallNoResult(ctx, "session/set_mode", map[string]string{"sessionId": sessionID, "modeId": devinDefaultMode})
	model = strings.TrimSpace(model)
	if model != "" {
		if _, errModel := client.Call(ctx, "session/set_config_option", map[string]string{"sessionId": sessionID, "configId": "model", "value": model}); errModel != nil {
			return fmt.Errorf("devin acp: model %q is not available: %w", model, errModel)
		}
	}

	_, errPrompt := client.CallWithNotifications(ctx, "session/prompt", map[string]any{"sessionId": sessionID, "prompt": prompt}, func(msg json.RawMessage) bool {
		update := gjson.GetBytes(msg, "params.update")
		switch update.Get("sessionUpdate").String() {
		case "agent_message_chunk":
			if text := update.Get("content.text").String(); text != "" {
				return onText(text)
			}
		case "usage_update":
			if usage != nil {
				usage.InputTokens = int(update.Get("_meta.cognition\\.ai/inputTokens").Int())
				usage.OutputTokens = int(update.Get("_meta.cognition\\.ai/outputTokens").Int())
				usage.TotalTokens = int(update.Get("used").Int())
			}
		}
		return true
	})
	if errPrompt != nil {
		return errPrompt
	}
	return nil
}

type devinACPClient struct {
	cmd      *exec.Cmd
	stdin    io.WriteCloser
	stdoutCh chan []byte
	errCh    chan error
	nextID   atomic.Int64
	writeMu  sync.Mutex
}

func newDevinACPClient(ctx context.Context, auth *cliproxyauth.Auth) (*devinACPClient, error) {
	command := strings.TrimSpace(authString(auth, "command"))
	if command == "" {
		command = defaultDevinCommand
	}
	cmd := exec.CommandContext(ctx, command, "acp")
	if configPath := strings.TrimSpace(authString(auth, "config_path")); configPath != "" {
		cmd.Args = append(cmd.Args, "--config", expandPath(configPath))
	}
	cmd.Dir = resolveDevinCWD(auth)
	cmd.Env = append(os.Environ(), "NO_COLOR=1")
	stdin, errStdin := cmd.StdinPipe()
	if errStdin != nil {
		return nil, errStdin
	}
	stdout, errStdout := cmd.StdoutPipe()
	if errStdout != nil {
		return nil, errStdout
	}
	stderr, errStderr := cmd.StderrPipe()
	if errStderr != nil {
		return nil, errStderr
	}
	if errStart := cmd.Start(); errStart != nil {
		return nil, errStart
	}
	c := &devinACPClient{cmd: cmd, stdin: stdin, stdoutCh: make(chan []byte, 32), errCh: make(chan error, 1)}
	go c.readStdout(stdout)
	go drainDevinACPStderr(stderr)
	go func() {
		errWait := cmd.Wait()
		if errWait != nil && ctx.Err() == nil {
			select {
			case c.errCh <- errWait:
			default:
			}
		}
	}()
	return c, nil
}

func (c *devinACPClient) Close() {
	if c == nil || c.cmd == nil || c.cmd.Process == nil {
		return
	}
	_ = c.stdin.Close()
	_ = c.cmd.Process.Kill()
}

func (c *devinACPClient) readStdout(r io.Reader) {
	reader := bufio.NewReader(r)
	for {
		line, err := reader.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			c.stdoutCh <- bytes.TrimSpace(line)
		}
		if err != nil {
			close(c.stdoutCh)
			return
		}
	}
}

func drainDevinACPStderr(r io.Reader) {
	_, _ = io.Copy(io.Discard, r)
}

func (c *devinACPClient) Call(ctx context.Context, method string, params any) (json.RawMessage, error) {
	return c.CallWithNotifications(ctx, method, params, nil)
}

func (c *devinACPClient) CallNoResult(ctx context.Context, method string, params any) error {
	_, err := c.Call(ctx, method, params)
	return err
}

func (c *devinACPClient) CallWithNotifications(ctx context.Context, method string, params any, onNotification func(json.RawMessage) bool) (json.RawMessage, error) {
	id := c.nextID.Add(1)
	if err := c.write(map[string]any{"jsonrpc": "2.0", "id": id, "method": method, "params": params}); err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-c.errCh:
			if err != nil {
				return nil, fmt.Errorf("devin acp exited: %w", err)
			}
		case line, ok := <-c.stdoutCh:
			if !ok {
				return nil, io.ErrUnexpectedEOF
			}
			var envelope struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      *int64          `json:"id,omitempty"`
				Method  string          `json:"method,omitempty"`
				Result  json.RawMessage `json:"result,omitempty"`
				Error   *jsonRPCError   `json:"error,omitempty"`
			}
			if errJSON := json.Unmarshal(line, &envelope); errJSON != nil {
				return nil, fmt.Errorf("devin acp: invalid json-rpc line: %w", errJSON)
			}
			if envelope.ID != nil && envelope.Method != "" {
				_ = c.respondCancelled(*envelope.ID)
				continue
			}
			if envelope.ID != nil && *envelope.ID == id {
				if envelope.Error != nil {
					return nil, envelope.Error
				}
				return envelope.Result, nil
			}
			if envelope.ID == nil && envelope.Method != "" && onNotification != nil {
				if !onNotification(append(json.RawMessage(nil), line...)) {
					return nil, context.Canceled
				}
			}
		}
	}
}

func (c *devinACPClient) respondCancelled(id int64) error {
	return c.write(map[string]any{"jsonrpc": "2.0", "id": id, "result": map[string]any{"outcome": map[string]string{"outcome": "cancelled"}}})
}

func (c *devinACPClient) write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, errWrite := c.stdin.Write(append(data, '\n')); errWrite != nil {
		return errWrite
	}
	return nil
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *jsonRPCError) Error() string {
	if e == nil {
		return "devin acp error"
	}
	if e.Message != "" {
		return e.Message
	}
	return fmt.Sprintf("devin acp error %d", e.Code)
}

type devinUsage struct {
	InputTokens  int
	OutputTokens int
	TotalTokens  int
}

type devinContentBlock struct {
	Type string `json:"type"`
	Text string `json:"text,omitempty"`
}

func devinPromptFromRequest(req cliproxyexecutor.Request, opts cliproxyexecutor.Options) ([]devinContentBlock, error) {
	raw := opts.OriginalRequest
	if len(raw) == 0 {
		raw = req.Payload
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, statusErr{code: http.StatusBadRequest, msg: "empty request payload"}
	}
	format := opts.SourceFormat
	if format == "" {
		format = req.Format
	}
	text := ""
	switch format {
	case sdktranslator.FormatOpenAI:
		text = promptFromOpenAIChat(raw)
	case sdktranslator.FormatOpenAIResponse:
		text = promptFromOpenAIResponses(raw)
	case sdktranslator.FormatClaude:
		text = promptFromClaude(raw)
	case sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		text = promptFromGemini(raw)
	default:
		text = promptFromOpenAIChat(raw)
		if strings.TrimSpace(text) == "" {
			text = string(raw)
		}
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return nil, statusErr{code: http.StatusBadRequest, msg: "request payload does not contain prompt text"}
	}
	return []devinContentBlock{{Type: "text", Text: text}}, nil
}

func promptFromOpenAIChat(raw []byte) string {
	root := gjson.ParseBytes(raw)
	var b strings.Builder
	if instruction := strings.TrimSpace(root.Get("instructions").String()); instruction != "" {
		appendTranscriptLine(&b, "instructions", instruction)
	}
	root.Get("messages").ForEach(func(_, msg gjson.Result) bool {
		role := strings.TrimSpace(msg.Get("role").String())
		if role == "" {
			role = "message"
		}
		content := contentText(msg.Get("content"))
		appendTranscriptLine(&b, role, content)
		return true
	})
	if b.Len() == 0 {
		if prompt := root.Get("prompt"); prompt.Exists() {
			appendTranscriptLine(&b, "user", valueText(prompt))
		}
	}
	return b.String()
}

func promptFromOpenAIResponses(raw []byte) string {
	root := gjson.ParseBytes(raw)
	var b strings.Builder
	if instruction := strings.TrimSpace(root.Get("instructions").String()); instruction != "" {
		appendTranscriptLine(&b, "instructions", instruction)
	}
	input := root.Get("input")
	if input.IsArray() {
		input.ForEach(func(_, item gjson.Result) bool {
			role := strings.TrimSpace(item.Get("role").String())
			if role == "" {
				role = "user"
			}
			appendTranscriptLine(&b, role, contentText(item.Get("content")))
			return true
		})
	} else {
		appendTranscriptLine(&b, "user", valueText(input))
	}
	return b.String()
}

func promptFromClaude(raw []byte) string {
	root := gjson.ParseBytes(raw)
	var b strings.Builder
	if system := root.Get("system"); system.Exists() {
		appendTranscriptLine(&b, "system", contentText(system))
	}
	root.Get("messages").ForEach(func(_, msg gjson.Result) bool {
		role := strings.TrimSpace(msg.Get("role").String())
		if role == "" {
			role = "message"
		}
		appendTranscriptLine(&b, role, contentText(msg.Get("content")))
		return true
	})
	return b.String()
}

func promptFromGemini(raw []byte) string {
	root := gjson.ParseBytes(raw)
	var b strings.Builder
	if sys := root.Get("systemInstruction.parts"); sys.Exists() {
		appendTranscriptLine(&b, "system", contentText(sys))
	}
	root.Get("contents").ForEach(func(_, msg gjson.Result) bool {
		role := strings.TrimSpace(msg.Get("role").String())
		if role == "" {
			role = "user"
		}
		appendTranscriptLine(&b, role, contentText(msg.Get("parts")))
		return true
	})
	return b.String()
}

func contentText(v gjson.Result) string {
	if !v.Exists() {
		return ""
	}
	if v.IsArray() {
		parts := make([]string, 0)
		v.ForEach(func(_, part gjson.Result) bool {
			if part.Type == gjson.String {
				parts = appendNonEmpty(parts, part.String())
				return true
			}
			for _, key := range []string{"text", "input_text", "output_text", "content"} {
				if s := strings.TrimSpace(part.Get(key).String()); s != "" {
					parts = append(parts, s)
					return true
				}
			}
			return true
		})
		return strings.Join(parts, "\n")
	}
	if v.IsObject() {
		for _, key := range []string{"text", "input_text", "output_text", "content"} {
			if s := strings.TrimSpace(v.Get(key).String()); s != "" {
				return s
			}
		}
	}
	return valueText(v)
}

func valueText(v gjson.Result) string {
	if !v.Exists() {
		return ""
	}
	if v.Type == gjson.String {
		return v.String()
	}
	return v.Raw
}

func appendTranscriptLine(b *strings.Builder, role, content string) {
	content = strings.TrimSpace(content)
	if content == "" {
		return
	}
	if b.Len() > 0 {
		b.WriteString("\n\n")
	}
	b.WriteString(strings.TrimSpace(role))
	b.WriteString(": ")
	b.WriteString(content)
}

func appendNonEmpty(parts []string, text string) []string {
	text = strings.TrimSpace(text)
	if text == "" {
		return parts
	}
	return append(parts, text)
}

func devinBuildNonStreamResponse(format sdktranslator.Format, model, text string, usage devinUsage) ([]byte, error) {
	created := time.Now().Unix()
	switch format {
	case sdktranslator.FormatOpenAIResponse:
		return json.Marshal(map[string]any{
			"id":                  "resp_" + randomHex(12),
			"object":              "response",
			"created_at":          created,
			"status":              "completed",
			"model":               model,
			"output":              []any{map[string]any{"id": "msg_" + randomHex(8), "type": "message", "status": "completed", "role": "assistant", "content": []any{map[string]any{"type": "output_text", "text": text, "annotations": []any{}}}}},
			"parallel_tool_calls": false,
			"usage":               usage.toOpenAIResponsesUsage(),
		})
	case sdktranslator.FormatClaude:
		return json.Marshal(map[string]any{
			"id":            "msg_" + randomHex(12),
			"type":          "message",
			"role":          "assistant",
			"model":         model,
			"content":       []any{map[string]any{"type": "text", "text": text}},
			"stop_reason":   "end_turn",
			"stop_sequence": nil,
			"usage":         usage.toClaudeUsage(),
		})
	case sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		return json.Marshal(map[string]any{
			"candidates":    []any{map[string]any{"content": map[string]any{"role": "model", "parts": []any{map[string]any{"text": text}}}, "finishReason": "STOP", "index": 0}},
			"usageMetadata": usage.toGeminiUsage(),
		})
	default:
		return json.Marshal(map[string]any{
			"id":      "chatcmpl-" + randomHex(12),
			"object":  "chat.completion",
			"created": created,
			"model":   model,
			"choices": []any{map[string]any{"index": 0, "message": map[string]any{"role": "assistant", "content": text}, "finish_reason": "stop"}},
			"usage":   usage.toOpenAIUsage(),
		})
	}
}

func devinBuildStreamDelta(format sdktranslator.Format, id, model, text string, created int64) ([]byte, error) {
	switch format {
	case sdktranslator.FormatOpenAIResponse:
		payload, err := json.Marshal(map[string]any{"type": "response.output_text.delta", "delta": text, "item_id": id, "output_index": 0, "content_index": 0})
		if err != nil {
			return nil, err
		}
		return appendSSE(nil, "response.output_text.delta", payload), nil
	case sdktranslator.FormatClaude:
		return json.Marshal(map[string]any{"type": "content_block_delta", "index": 0, "delta": map[string]any{"type": "text_delta", "text": text}})
	case sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		return json.Marshal(map[string]any{"candidates": []any{map[string]any{"content": map[string]any{"role": "model", "parts": []any{map[string]any{"text": text}}}, "index": 0}}})
	default:
		return json.Marshal(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{"content": text}, "finish_reason": nil}}})
	}
}

func devinBuildStreamFinal(format sdktranslator.Format, id, model string, created int64, usage devinUsage) ([]byte, error) {
	switch format {
	case sdktranslator.FormatOpenAIResponse:
		completed, err := json.Marshal(map[string]any{"type": "response.completed", "response": map[string]any{"id": id, "object": "response", "created_at": created, "status": "completed", "model": model, "output": []any{}, "usage": usage.toOpenAIResponsesUsage()}})
		if err != nil {
			return nil, err
		}
		return appendSSE(nil, "response.completed", completed), nil
	case sdktranslator.FormatClaude:
		return json.Marshal(map[string]any{"type": "message_stop"})
	case sdktranslator.FormatGemini, sdktranslator.FormatGeminiCLI:
		return json.Marshal(map[string]any{"candidates": []any{map[string]any{"finishReason": "STOP", "index": 0}}, "usageMetadata": usage.toGeminiUsage()})
	default:
		return json.Marshal(map[string]any{"id": id, "object": "chat.completion.chunk", "created": created, "model": model, "choices": []any{map[string]any{"index": 0, "delta": map[string]any{}, "finish_reason": "stop"}}, "usage": usage.toOpenAIUsage()})
	}
}

func appendSSE(dst []byte, event string, payload []byte) []byte {
	if event != "" {
		dst = append(dst, "event: "...)
		dst = append(dst, event...)
		dst = append(dst, '\n')
	}
	dst = append(dst, "data: "...)
	dst = append(dst, payload...)
	dst = append(dst, '\n', '\n')
	return dst
}

func (u devinUsage) toOpenAIUsage() map[string]int {
	return map[string]int{"prompt_tokens": u.InputTokens, "completion_tokens": u.OutputTokens, "total_tokens": u.total()}
}

func (u devinUsage) toOpenAIResponsesUsage() map[string]int {
	return map[string]int{"input_tokens": u.InputTokens, "output_tokens": u.OutputTokens, "total_tokens": u.total()}
}

func (u devinUsage) toClaudeUsage() map[string]int {
	return map[string]int{"input_tokens": u.InputTokens, "output_tokens": u.OutputTokens}
}

func (u devinUsage) toGeminiUsage() map[string]int {
	return map[string]int{"promptTokenCount": u.InputTokens, "candidatesTokenCount": u.OutputTokens, "totalTokenCount": u.total()}
}

func (u devinUsage) total() int {
	if u.TotalTokens > 0 {
		return u.TotalTokens
	}
	return u.InputTokens + u.OutputTokens
}

func resolveDevinAPIKey(auth *cliproxyauth.Auth) (string, error) {
	if key := strings.TrimSpace(authString(auth, "api_key")); key != "" {
		return key, nil
	}
	credentialsPath := strings.TrimSpace(authString(auth, "credentials_path"))
	if credentialsPath == "" {
		credentialsPath = defaultDevinCredentialsPath()
	}
	key, err := readSimpleTOMLString(expandPath(credentialsPath), devinCredentialsAPIKey)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", statusErr{code: http.StatusUnauthorized, msg: "devin credentials not found; run devin auth login or set api_key"}
		}
		return "", err
	}
	if strings.TrimSpace(key) == "" {
		return "", statusErr{code: http.StatusUnauthorized, msg: "devin credentials missing windsurf_api_key"}
	}
	return key, nil
}

func authString(auth *cliproxyauth.Auth, key string) string {
	if auth == nil {
		return ""
	}
	if auth.Attributes != nil {
		if v := strings.TrimSpace(auth.Attributes[key]); v != "" {
			return v
		}
	}
	if auth.Metadata != nil {
		if v, ok := auth.Metadata[key]; ok {
			switch typed := v.(type) {
			case string:
				return strings.TrimSpace(typed)
			case fmt.Stringer:
				return strings.TrimSpace(typed.String())
			}
		}
	}
	return ""
}

func defaultDevinCredentialsPath() string {
	if xdg := strings.TrimSpace(os.Getenv("XDG_DATA_HOME")); xdg != "" {
		return filepath.Join(xdg, "devin", "credentials.toml")
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".local", "share", "devin", "credentials.toml")
	}
	return "credentials.toml"
}

func resolveDevinCWD(auth *cliproxyauth.Auth) string {
	if cwd := strings.TrimSpace(authString(auth, "cwd")); cwd != "" {
		if filepath.IsAbs(cwd) {
			return cwd
		}
		if abs, err := filepath.Abs(cwd); err == nil {
			return abs
		}
	}
	if cwd, err := os.Getwd(); err == nil && cwd != "" {
		return cwd
	}
	return "."
}

func expandPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" || path[0] != '~' {
		return path
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return path
	}
	if path == "~" {
		return home
	}
	if len(path) > 1 && (path[1] == '/' || path[1] == filepath.Separator) {
		return filepath.Join(home, path[2:])
	}
	return path
}

func readSimpleTOMLString(path, key string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	prefix := key + " ="
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "#") || !strings.HasPrefix(line, prefix) {
			continue
		}
		value := strings.TrimSpace(strings.TrimPrefix(line, prefix))
		if value == "" {
			return "", nil
		}
		if unquoted, errUnquote := strconv.Unquote(value); errUnquote == nil {
			return unquoted, nil
		}
		return strings.Trim(value, `"'`), nil
	}
	if errScan := scanner.Err(); errScan != nil {
		return "", errScan
	}
	return "", nil
}

func estimateDevinTokens(text string) int {
	text = strings.TrimSpace(text)
	if text == "" {
		return 0
	}
	words := len(strings.Fields(text))
	chars := len([]rune(text)) / 4
	if chars > words {
		return chars
	}
	return words
}

func estimateDevinPromptTokens(prompt []devinContentBlock) int {
	if len(prompt) == 0 {
		return 0
	}
	total := 0
	for _, block := range prompt {
		total += estimateDevinTokens(block.Text)
	}
	return total
}

func randomHex(n int) string {
	if n <= 0 {
		return ""
	}
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf)
}
