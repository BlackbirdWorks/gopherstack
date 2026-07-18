package apigatewayv2_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tagsToSet  map[string]string
		wantTags   map[string]string
		name       string
		keysToRM   []string
		wantStatus int
	}{
		{
			name:       "get_tags_empty",
			wantTags:   map[string]string{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag_and_get",
			tagsToSet:  map[string]string{"env": "prod", "team": "platform"},
			wantTags:   map[string]string{"env": "prod", "team": "platform"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "tag_then_untag",
			tagsToSet:  map[string]string{"env": "prod", "team": "platform"},
			keysToRM:   []string{"team"},
			wantTags:   map[string]string{"env": "prod"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			apiID := createAPI(t, h, "tagged-api")

			arn := "arn:aws:apigateway:us-east-1::/apis/" + apiID

			if len(tt.tagsToSet) > 0 {
				rr := doRequest(t, h, http.MethodPost, "/v2/tags/"+arn, map[string]any{
					"tags": tt.tagsToSet,
				})
				assert.Equal(t, http.StatusCreated, rr.Code)
			}

			if len(tt.keysToRM) > 0 {
				tagKeysParam := strings.Join(tt.keysToRM, ",")
				req := httptest.NewRequest(http.MethodDelete, "/v2/tags/"+arn+"?tagKeys="+tagKeysParam, nil)
				rr := httptest.NewRecorder()
				e := echo.New()
				c := e.NewContext(req, rr)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusNoContent, rr.Code)
			}

			rr := doRequest(t, h, http.MethodGet, "/v2/tags/"+arn, nil)
			assert.Equal(t, tt.wantStatus, rr.Code)

			if tt.wantStatus == http.StatusOK {
				var out struct {
					Tags map[string]string `json:"tags"`
				}
				require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &out))
				assert.Equal(t, tt.wantTags, out.Tags)
			}
		})
	}
}

func TestHandler_Tags_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantStatus int
	}{
		{
			name:       "get_not_found",
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "post_not_found",
			method:     http.MethodPost,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_not_found",
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			arn := "arn:aws:apigateway:us-east-1::/apis/nonexistent"

			var rr *httptest.ResponseRecorder
			if tt.method == http.MethodPost {
				rr = doRequest(t, h, tt.method, "/v2/tags/"+arn, map[string]any{"tags": map[string]string{}})
			} else {
				rr = doRequest(t, h, tt.method, "/v2/tags/"+arn, nil)
			}

			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}
