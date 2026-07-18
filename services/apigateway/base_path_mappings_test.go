package apigateway_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestUpdateBasePathMapping tests UpdateBasePathMapping.
func TestUpdateBasePathMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newStage string
		wantCode int
		useValid bool
	}{
		{
			name:     "update_stage",
			wantCode: http.StatusOK,
			useValid: true,
			newStage: "v2",
		},
		{
			name:     "not_found",
			wantCode: http.StatusNotFound,
			useValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			handler, e := boostSetup()

			// Create base path mapping first
			postWithHandler(t, handler, e, "CreateBasePathMapping",
				`{"domainName":"api.example.com","basePath":"v1","restApiId":"abc123","stage":"prod"}`)

			domainName := "api.example.com"
			basePath := "v1"
			if !tt.useValid {
				domainName = "notexist.example.com"
			}

			rec := postWithHandler(t, handler, e, "UpdateBasePathMapping",
				fmt.Sprintf(`{"domainName":%q,"basePath":%q,"stage":%q}`, domainName, basePath, tt.newStage))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
