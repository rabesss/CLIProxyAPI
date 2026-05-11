package cliproxy

import (
	"strings"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestRegisterModelsForAuth_DevinCLIConfigModels(t *testing.T) {
	service := &Service{
		cfg: &config.Config{
			DevinCLI: []config.DevinCLI{
				{
					Name:     "devin-local",
					Provider: "devin",
					Models: []config.DevinModel{
						{Name: "swe-1-6-fast", Alias: "devin-fast", DisplayName: "Devin Fast"},
						{Name: "MODEL_GPT_5_2_LOW", Alias: "devin-gpt-low"},
					},
					ExcludedModels: []string{"devin-gpt-low"},
				},
			},
		},
	}
	auth := &coreauth.Auth{
		ID:         "auth-devin-cli",
		Provider:   "devin",
		Label:      "devin-local",
		Status:     coreauth.StatusActive,
		Attributes: map[string]string{"auth_kind": "cli", "excluded_models": "devin-gpt-low"},
	}
	registry := GlobalModelRegistry()
	registry.UnregisterClient(auth.ID)
	t.Cleanup(func() { registry.UnregisterClient(auth.ID) })

	service.registerModelsForAuth(auth)

	models := registry.GetAvailableModelsByProvider("devin")
	if len(models) != 1 {
		t.Fatalf("expected 1 devin model after exclusion, got %d", len(models))
	}
	model := models[0]
	if model == nil {
		t.Fatal("expected non-nil model")
	}
	if model.ID != "devin-fast" || model.DisplayName != "Devin Fast" || model.OwnedBy != "cognition" || model.Type != "devin" {
		t.Fatalf("unexpected devin model: %#v", model)
	}
}

func TestRegisterModelsForAuth_WindsurfDefaults(t *testing.T) {
	service := &Service{cfg: &config.Config{}}
	auth := &coreauth.Auth{ID: "auth-windsurf", Provider: "windsurf", Status: coreauth.StatusActive, Attributes: map[string]string{"auth_kind": "cli"}}
	registry := GlobalModelRegistry()
	registry.UnregisterClient(auth.ID)
	t.Cleanup(func() { registry.UnregisterClient(auth.ID) })

	service.registerModelsForAuth(auth)

	models := registry.GetAvailableModelsByProvider("windsurf")
	if len(models) == 0 {
		t.Fatal("expected windsurf models to be registered")
	}
	for _, model := range models {
		if model == nil {
			continue
		}
		if !strings.EqualFold(model.Type, "windsurf") || !strings.EqualFold(model.OwnedBy, "windsurf") {
			t.Fatalf("expected windsurf model ownership/type, got %#v", model)
		}
	}
}
