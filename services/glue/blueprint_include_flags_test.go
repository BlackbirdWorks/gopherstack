package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestGetBlueprint_IncludeFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		includeBP    *bool
		includeSpec  *bool
		name         string
		action       string
		wantLocation bool
		wantSpec     bool
	}{
		{name: "unset", action: "GetBlueprint", wantLocation: true, wantSpec: true},
		{name: "both_false", action: "GetBlueprint", includeBP: new(false), includeSpec: new(false)},
		{
			name: "blueprint_only", action: "GetBlueprint", includeBP: new(true), includeSpec: new(false),
			wantLocation: true,
		},
		{
			name: "spec_only", action: "GetBlueprint", includeBP: new(false), includeSpec: new(true),
			wantSpec: true,
		},
		{name: "batch_both_false", action: "BatchGetBlueprints", includeBP: new(false), includeSpec: new(false)},
		{name: "batch_unset", action: "BatchGetBlueprints", wantLocation: true, wantSpec: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := glue.NewInMemoryBackend(testAccountID, testRegion)
			backend.AddBlueprintInternal(&glue.Blueprint{
				Name: "bp", BlueprintLocation: "s3://bucket/bp.zip", ParameterSpec: `{"p":{}}`, Status: "ACTIVE",
			})
			h := glue.NewHandler(backend)

			body := map[string]any{}
			if tt.includeBP != nil {
				body["IncludeBlueprint"] = *tt.includeBP
			}

			if tt.includeSpec != nil {
				body["IncludeParameterSpec"] = *tt.includeSpec
			}

			if tt.action == "GetBlueprint" {
				body["Name"] = "bp"
			} else {
				body["Names"] = []string{"bp"}
			}

			rec := doGlueRequest(t, h, tt.action, body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			bp, _ := out["Blueprint"].(map[string]any)
			if tt.action == "BatchGetBlueprints" {
				list, _ := out["Blueprints"].([]any)
				require.Len(t, list, 1)
				bp, _ = list[0].(map[string]any)
			}

			assert.Equal(t, tt.wantLocation, bp["BlueprintLocation"] != nil)
			assert.Equal(t, tt.wantSpec, bp["ParameterSpec"] != nil)
		})
	}
}
