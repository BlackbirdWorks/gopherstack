package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerFlowCRUD(t *testing.T) {
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
		"name":             "test-flow",
		"executionRoleArn": "arn:aws:iam::123456789012:role/FlowRole",
		"definition": map[string]any{
			"nodes":       []any{},
			"connections": []any{},
		},
	}

	rec := doRequest(t, h, e, http.MethodPost, "/flows", createBody)
	if rec.Code != http.StatusCreated {
		t.Fatalf("create flow: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	flowID, _ := createResp["id"].(string)

	if flowID == "" {
		t.Fatal("no id in flow response")
	}

	// Prepare it
	doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/prepare", nil)

	// Create a version
	rVersion := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/versions", map[string]any{})
	var verResp map[string]any
	_ = json.Unmarshal(rVersion.Body.Bytes(), &verResp)
	verID := verResp["version"].(string)

	// Create an alias
	aliasBody := map[string]any{
		"name": "prod",
		"routingConfiguration": []map[string]any{
			{"flowVersion": verID},
		},
	}
	rAlias := doRequest(t, h, e, http.MethodPost, "/flows/"+flowID+"/aliases", aliasBody)
	var alResp map[string]any
	_ = json.Unmarshal(rAlias.Body.Bytes(), &alResp)
	aliasID := alResp["id"].(string)

	cases := []tc{
		{
			name:       "ListFlows",
			method:     http.MethodGet,
			path:       "/flows",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetFlow",
			method:     http.MethodGet,
			path:       "/flows/" + flowID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateFlow",
			method:     http.MethodPut,
			path:       "/flows/" + flowID,
			body:       createBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "PrepareFlow",
			method:     http.MethodPost,
			path:       "/flows/" + flowID + "/prepare",
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "DeleteFlow",
			method:     http.MethodDelete,
			path:       "/flows/" + flowID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ValidateFlowDefinition",
			method:     http.MethodPost,
			path:       "/flows/validate-definition",
			body:       createBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ListFlowVersions",
			method:     http.MethodGet,
			path:       "/flows/" + flowID + "/versions",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetFlowVersion",
			method:     http.MethodGet,
			path:       "/flows/" + flowID + "/versions/" + verID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "DeleteFlowVersion",
			method:     http.MethodDelete,
			path:       "/flows/" + flowID + "/versions/" + verID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "ListFlowAliases",
			method:     http.MethodGet,
			path:       "/flows/" + flowID + "/aliases",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetFlowAlias",
			method:     http.MethodGet,
			path:       "/flows/" + flowID + "/aliases/" + aliasID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateFlowAlias",
			method:     http.MethodPut,
			path:       "/flows/" + flowID + "/aliases/" + aliasID,
			body:       aliasBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "DeleteFlowAlias",
			method:     http.MethodDelete,
			path:       "/flows/" + flowID + "/aliases/" + aliasID,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)

			rF := doRequest(t, hLocal, eLocal, http.MethodPost, "/flows", createBody)
			var fResp map[string]any
			_ = json.Unmarshal(rF.Body.Bytes(), &fResp)
			fID := fResp["id"].(string)

			doRequest(t, hLocal, eLocal, http.MethodPost, "/flows/"+fID+"/prepare", nil)

			rV := doRequest(t, hLocal, eLocal, http.MethodPost, "/flows/"+fID+"/versions", map[string]any{})
			var vResp map[string]any
			_ = json.Unmarshal(rV.Body.Bytes(), &vResp)
			vID := vResp["version"].(string)

			aBody := map[string]any{
				"name": "prod",
				"routingConfiguration": []map[string]any{
					{"flowVersion": vID},
				},
			}
			rA := doRequest(t, hLocal, eLocal, http.MethodPost, "/flows/"+fID+"/aliases", aBody)
			var aResp map[string]any
			_ = json.Unmarshal(rA.Body.Bytes(), &aResp)
			aID := aResp["id"].(string)

			// Rewrite path to use local IDs
			path := tt.path
			if flowID != "" && fID != "" {
				switch path {
				case "/flows/" + flowID:
					path = "/flows/" + fID
				case "/flows/" + flowID + "/prepare":
					path = "/flows/" + fID + "/prepare"
				case "/flows/" + flowID + "/versions":
					path = "/flows/" + fID + "/versions"
				case "/flows/" + flowID + "/versions/" + verID:
					path = "/flows/" + fID + "/versions/" + vID
				case "/flows/" + flowID + "/aliases":
					path = "/flows/" + fID + "/aliases"
				case "/flows/" + flowID + "/aliases/" + aliasID:
					path = "/flows/" + fID + "/aliases/" + aID
				}
			}
			// for alias update we need to inject the local version id
			reqBody := tt.body
			if reqBody != nil && reqBody["name"] == "prod" {
				reqBody = aBody
			}

			r := doRequest(t, hLocal, eLocal, tt.method, path, reqBody)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
