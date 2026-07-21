package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerPromptCRUD(t *testing.T) {
	t.Parallel()

	type tc struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}

	h, e := setupHandler(t)

	createBody := map[string]any{
		"name":           "test-prompt",
		"defaultVariant": "v1",
		"variants": []any{
			map[string]any{
				"name":         "v1",
				"templateType": "TEXT",
			},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/prompts", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create prompt: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	promptID, _ := createResp["id"].(string)

	if promptID == "" {
		t.Fatal("no id in prompt response")
	}

	// Create a version
	rVersion := doRequest(
		t,
		h,
		e,
		http.MethodPost,
		"/prompts/"+promptID+"/versions",
		map[string]any{"description": "v1"},
	)
	var verResp map[string]any
	_ = json.Unmarshal(rVersion.Body.Bytes(), &verResp)
	verID := verResp["version"].(string)

	cases := []tc{
		{
			name:       "ListPrompts",
			method:     http.MethodGet,
			path:       "/prompts",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetPrompt",
			method:     http.MethodGet,
			path:       "/prompts/" + promptID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetPrompt_NotFound",
			method:     http.MethodGet,
			path:       "/prompts/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "UpdatePrompt",
			method:     http.MethodPut,
			path:       "/prompts/" + promptID,
			body:       createBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdatePrompt_NotFound",
			method:     http.MethodPut,
			path:       "/prompts/notfound",
			body:       createBody,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "DeletePrompt",
			method:     http.MethodDelete,
			path:       "/prompts/" + promptID,
			wantStatus: http.StatusOK, // deletes return 200 with id
		},
		{
			name:       "DeletePrompt_NotFound",
			method:     http.MethodDelete,
			path:       "/prompts/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "GetPromptVersion",
			method:     http.MethodGet,
			path:       "/prompts/" + promptID + "/versions/" + verID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "DeletePromptVersion",
			method:     http.MethodDelete,
			path:       "/prompts/" + promptID + "/versions/" + verID,
			wantStatus: http.StatusOK, // returns 200 with id and version
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)

			rP := doRequest(t, hLocal, eLocal, http.MethodPost, "/prompts", createBody)
			var pResp map[string]any
			_ = json.Unmarshal(rP.Body.Bytes(), &pResp)
			pID := pResp["id"].(string)

			rV := doRequest(
				t,
				hLocal,
				eLocal,
				http.MethodPost,
				"/prompts/"+pID+"/versions",
				map[string]any{"description": "v1"},
			)
			var vResp map[string]any
			_ = json.Unmarshal(rV.Body.Bytes(), &vResp)
			vID := vResp["version"].(string)

			path := tt.path
			if promptID != "" && pID != "" {
				switch path {
				case "/prompts/" + promptID:
					path = "/prompts/" + pID
				case "/prompts/" + promptID + "/versions/" + verID:
					path = "/prompts/" + pID + "/versions/" + vID
				}
			}
			r := doRequest(t, hLocal, eLocal, tt.method, path, tt.body)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
