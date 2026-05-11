package executor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestDevinPromptFromRequest_OpenAIChat(t *testing.T) {
	raw := []byte(`{"instructions":"be precise","messages":[{"role":"system","content":"rules"},{"role":"user","content":[{"type":"text","text":"hello"},{"type":"image_url","image_url":{"url":"ignored"}}]}]}`)
	blocks, err := devinPromptFromRequest(cliproxyexecutor.Request{Payload: raw, Format: sdktranslator.FormatOpenAI}, cliproxyexecutor.Options{})
	if err != nil {
		t.Fatalf("prompt from request: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
	want := "instructions: be precise\n\nsystem: rules\n\nuser: hello"
	if blocks[0].Type != "text" || blocks[0].Text != want {
		t.Fatalf("unexpected prompt block: %#v", blocks[0])
	}
}

func TestDevinPromptFromRequest_OpenAIResponsesUsesOriginalRequest(t *testing.T) {
	translated := []byte(`{"messages":[{"role":"user","content":"translated"}]}`)
	original := []byte(`{"instructions":"follow","input":[{"role":"user","content":[{"type":"input_text","text":"original"}]}]}`)
	blocks, err := devinPromptFromRequest(
		cliproxyexecutor.Request{Payload: translated, Format: sdktranslator.FormatOpenAI},
		cliproxyexecutor.Options{OriginalRequest: original, SourceFormat: sdktranslator.FormatOpenAIResponse},
	)
	if err != nil {
		t.Fatalf("prompt from request: %v", err)
	}
	want := "instructions: follow\n\nuser: original"
	if len(blocks) != 1 || blocks[0].Text != want {
		t.Fatalf("unexpected prompt blocks: %#v", blocks)
	}
}

func TestResolveDevinAPIKeyReadsCredentialsPath(t *testing.T) {
	tmp := t.TempDir()
	credentialsPath := filepath.Join(tmp, "credentials.toml")
	if err := os.WriteFile(credentialsPath, []byte("# comment\nwindsurf_api_key = \"ws-test-key\"\n"), 0o600); err != nil {
		t.Fatalf("write credentials: %v", err)
	}
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"credentials_path": credentialsPath}}
	key, err := resolveDevinAPIKey(auth)
	if err != nil {
		t.Fatalf("resolve api key: %v", err)
	}
	if key != "ws-test-key" {
		t.Fatalf("expected key from credentials, got %q", key)
	}
}

func TestResolveDevinAPIKeyPrefersAuthAttribute(t *testing.T) {
	auth := &cliproxyauth.Auth{Attributes: map[string]string{"api_key": "direct-key", "credentials_path": filepath.Join(t.TempDir(), "missing.toml")}}
	key, err := resolveDevinAPIKey(auth)
	if err != nil {
		t.Fatalf("resolve api key: %v", err)
	}
	if key != "direct-key" {
		t.Fatalf("expected direct api key, got %q", key)
	}
}

func TestDevinBuildNonStreamResponseFormats(t *testing.T) {
	usage := devinUsage{InputTokens: 2, OutputTokens: 3}

	openAI, err := devinBuildNonStreamResponse(sdktranslator.FormatOpenAI, "swe-1-6-fast", "done", usage)
	if err != nil {
		t.Fatalf("build openai response: %v", err)
	}
	if got := gjson.GetBytes(openAI, "choices.0.message.content").String(); got != "done" {
		t.Fatalf("expected OpenAI content done, got %q", got)
	}
	if got := gjson.GetBytes(openAI, "usage.total_tokens").Int(); got != 5 {
		t.Fatalf("expected total tokens 5, got %d", got)
	}

	responses, err := devinBuildNonStreamResponse(sdktranslator.FormatOpenAIResponse, "swe-1-6-fast", "done", usage)
	if err != nil {
		t.Fatalf("build responses response: %v", err)
	}
	if got := gjson.GetBytes(responses, "output.0.content.0.text").String(); got != "done" {
		t.Fatalf("expected Responses content done, got %q", got)
	}
	if got := gjson.GetBytes(responses, "usage.total_tokens").Int(); got != 5 {
		t.Fatalf("expected Responses total tokens 5, got %d", got)
	}
}

func TestDevinBuildOpenAIResponsesStreamFrames(t *testing.T) {
	delta, err := devinBuildStreamDelta(sdktranslator.FormatOpenAIResponse, "resp-test", "swe-1-6-fast", "hi", 123)
	if err != nil {
		t.Fatalf("build stream delta: %v", err)
	}
	if !strings.Contains(string(delta), "event: response.output_text.delta") {
		t.Fatalf("expected response delta SSE event, got %q", string(delta))
	}
	payload := strings.TrimSpace(strings.TrimPrefix(strings.Split(string(delta), "\n")[1], "data:"))
	if !json.Valid([]byte(payload)) {
		t.Fatalf("expected valid JSON payload, got %q", payload)
	}
	if got := gjson.Get(payload, "delta").String(); got != "hi" {
		t.Fatalf("expected delta hi, got %q", got)
	}
}
