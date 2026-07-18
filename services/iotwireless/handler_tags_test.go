package iotwireless_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListTagsTagUntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		addTags    string
		wantKey    string
		wantValue  string
		wantGone   string
		removeTags []string
	}{
		{
			name:      "tag_and_list",
			addTags:   `{"Tags":[{"Key":"env","Value":"prod"},{"Key":"team","Value":"platform"}]}`,
			wantKey:   "env",
			wantValue: "prod",
		},
		{
			name:       "tag_then_untag",
			addTags:    `{"Tags":[{"Key":"env","Value":"staging"},{"Key":"team","Value":"infra"}]}`,
			removeTags: []string{"team"},
			wantKey:    "env",
			wantValue:  "staging",
			wantGone:   "team",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandlerHTTP()

			// Create a service profile to get a real ARN.
			rec := doIoTWRequest(t, h, http.MethodPost, "/service-profiles", `{"Name":"tag-test-profile"}`)
			require.Equal(t, http.StatusCreated, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			arn := createResp["Arn"].(string)
			require.NotEmpty(t, arn)

			// Real AWS binds all three tag ops to the bare "/tags" path with
			// the resource ARN as the "resourceArn" query parameter (never a
			// path segment).
			encodedARN := url.QueryEscape(arn)

			// TagResource
			rec = doIoTWRequest(t, h, http.MethodPost, "/tags?resourceArn="+encodedARN, tt.addTags)
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// Untag if specified.
			if len(tt.removeTags) > 0 {
				var queryStr strings.Builder
				queryStr.WriteString("resourceArn=" + encodedARN)
				for _, k := range tt.removeTags {
					queryStr.WriteString("&tagKeys=" + k)
				}

				e := echo.New()
				req := httptest.NewRequest(http.MethodDelete, "/tags?"+queryStr.String(), http.NoBody)
				recDel := httptest.NewRecorder()
				c := e.NewContext(req, recDel)
				require.NoError(t, h.Handler()(c))
				assert.Equal(t, http.StatusNoContent, recDel.Code)
			}

			// ListTags
			rec = doIoTWRequest(t, h, http.MethodGet, "/tags?resourceArn="+encodedARN, "")
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
			tagList, ok := tagsResp["Tags"].([]any)
			require.True(t, ok)

			got := make(map[string]string, len(tagList))

			for _, kv := range tagList {
				m, kvOK := kv.(map[string]any)
				require.True(t, kvOK)
				got[m["Key"].(string)] = m["Value"].(string)
			}

			assert.Equal(t, tt.wantValue, got[tt.wantKey])

			if tt.wantGone != "" {
				_, present := got[tt.wantGone]
				assert.False(t, present, "tag %q should be removed", tt.wantGone)
			}
		})
	}
}
