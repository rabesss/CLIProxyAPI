package auth

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v7/internal/config"
)

func TestExecutionModelCandidates_DevinCLIMapsAliasToUpstream(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{
		DevinCLI: []internalconfig.DevinCLI{
			{
				Name:     "devin-local",
				Provider: "devin",
				Models: []internalconfig.DevinModel{
					{Name: "swe-1-6-fast", Alias: "devin-fast"},
				},
			},
		},
	})
	auth := &Auth{ID: "auth-devin", Provider: "devin", Label: "devin-local", Attributes: map[string]string{"auth_kind": "cli"}}

	got := manager.executionModelCandidates(auth, "devin-fast(8192)")
	want := []string{"swe-1-6-fast(8192)"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("execution candidates = %#v, want %#v", got, want)
	}
}

func TestExecutionModelCandidates_WindsurfCLIMapsAliasByCredential(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	manager.SetConfig(&internalconfig.Config{
		DevinCLI: []internalconfig.DevinCLI{
			{
				Name:     "windsurf-local",
				Provider: "windsurf",
				APIKey:   "ws-key",
				Models: []internalconfig.DevinModel{
					{Name: "MODEL_GPT_5_2_LOW", Alias: "windsurf-gpt-low"},
				},
			},
		},
	})
	auth := &Auth{ID: "auth-windsurf", Provider: "windsurf", Attributes: map[string]string{"auth_kind": "cli", "api_key": "ws-key"}}

	got := manager.executionModelCandidates(auth, "windsurf-gpt-low")
	want := []string{"MODEL_GPT_5_2_LOW"}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("execution candidates = %#v, want %#v", got, want)
	}
}
