package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateGlossary_ClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		token1   string
		token2   string
		wantSame bool
	}{
		{name: "same_token", token1: "t", token2: "t", wantSame: true},
		{name: "different_token", token1: "t1", token2: "t2"},
		{name: "no_token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createGlossary := func(token string) string {
				rec := doGlueRequest(t, h, "CreateGlossary", map[string]any{"Name": "g", "ClientToken": token})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out["Id"].(string)
			}

			createTerm := func(glossary, token string) string {
				rec := doGlueRequest(t, h, "CreateGlossaryTerm", map[string]any{
					"GlossaryIdentifier": glossary, "Name": "term", "ClientToken": token,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return out["Id"].(string)
			}

			g1, g2 := createGlossary(tt.token1), createGlossary(tt.token2)
			assert.Equal(t, tt.wantSame, g1 == g2)

			t1, t2 := createTerm(g1, tt.token1), createTerm(g1, tt.token2)
			assert.Equal(t, tt.wantSame, t1 == t2)
		})
	}
}
