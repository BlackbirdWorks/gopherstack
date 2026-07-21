package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerKnowledgeBaseCRUD(t *testing.T) {
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
		"name":    "test-kb",
		"roleArn": "arn:aws:iam::123456789012:role/KBRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
		},
	}

	rec := doRequest(t, h, e, http.MethodPut, "/knowledgebases", createBody)
	if rec.Code != http.StatusOK {
		t.Fatalf("create kb: %d %s", rec.Code, rec.Body.String())
	}

	var createResp map[string]map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &createResp)
	kbID := createResp["knowledgeBase"]["knowledgeBaseId"].(string)

	// Create Data Source for docs
	dsBody := map[string]any{
		"name": "doc-ds",
		"dataSourceConfiguration": map[string]any{
			"type": "S3",
			"s3Configuration": map[string]any{
				"bucketArn": "arn:aws:s3:::my-bucket",
			},
		},
	}
	rDS := doRequest(t, h, e, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", dsBody)
	var dsResp map[string]map[string]any
	_ = json.Unmarshal(rDS.Body.Bytes(), &dsResp)
	dsID := dsResp["dataSource"]["dataSourceId"].(string)

	docsBasePath := "/knowledgebases/" + kbID + "/datasources/" + dsID + "/documents"

	cases := []tc{
		{
			name:       "ListKBs",
			method:     http.MethodGet,
			path:       "/knowledgebases",
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetKB",
			method:     http.MethodGet,
			path:       "/knowledgebases/" + kbID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "UpdateKB",
			method:     http.MethodPut,
			path:       "/knowledgebases/" + kbID,
			body:       createBody,
			wantStatus: http.StatusOK,
		},
		{
			name:       "DeleteKB",
			method:     http.MethodDelete,
			path:       "/knowledgebases/" + kbID,
			wantStatus: http.StatusOK,
		},
		{
			name:   "IngestKBDocs",
			method: http.MethodPost,
			path:   docsBasePath,
			body: map[string]any{
				"documents": []map[string]any{
					{
						"documentId": "doc1",
						"content": map[string]any{
							"dataSourceType": "CUSTOM",
							"custom": map[string]any{
								"customContent": map[string]any{
									"text": "hello",
								},
							},
						},
					},
				},
			},
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "ListKBDocs",
			method:     http.MethodGet,
			path:       docsBasePath,
			wantStatus: http.StatusOK,
		},
		{
			name:   "GetKBDocs",
			method: http.MethodPost,
			path:   docsBasePath + "/getDocuments",
			body: map[string]any{
				"documentIds": []string{"doc1"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "DeleteKBDocs",
			method: http.MethodPost,
			path:   docsBasePath + "/deleteDocuments",
			body: map[string]any{
				"documentIds": []string{"doc1"},
			},
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)

			rK := doRequest(t, hLocal, eLocal, http.MethodPut, "/knowledgebases", createBody)
			var kResp map[string]map[string]any
			_ = json.Unmarshal(rK.Body.Bytes(), &kResp)
			kID := kResp["knowledgeBase"]["knowledgeBaseId"].(string)

			rD := doRequest(t, hLocal, eLocal, http.MethodPut, "/knowledgebases/"+kID+"/datasources", dsBody)
			var dResp map[string]map[string]any
			_ = json.Unmarshal(rD.Body.Bytes(), &dResp)
			dID := dResp["dataSource"]["dataSourceId"].(string)

			// For GetKBDocs and DeleteKBDocs, we need to ingest the doc first
			if tt.name == "GetKBDocs" || tt.name == "DeleteKBDocs" {
				doRequest(
					t,
					hLocal,
					eLocal,
					http.MethodPost,
					"/knowledgebases/"+kID+"/datasources/"+dID+"/documents",
					map[string]any{
						"documents": []map[string]any{
							{
								"documentId": "doc1",
								"content": map[string]any{
									"dataSourceType": "CUSTOM",
									"custom": map[string]any{
										"customContent": map[string]any{"text": "hello"},
									},
								},
							},
						},
					},
				)
			}

			path := tt.path
			if kbID != "" && kID != "" {
				switch path {
				case "/knowledgebases/" + kbID:
					path = "/knowledgebases/" + kID
				case docsBasePath:
					path = "/knowledgebases/" + kID + "/datasources/" + dID + "/documents"
				case docsBasePath + "/getDocuments":
					path = "/knowledgebases/" + kID + "/datasources/" + dID + "/documents/getDocuments"
				case docsBasePath + "/deleteDocuments":
					path = "/knowledgebases/" + kID + "/datasources/" + dID + "/documents/deleteDocuments"
				}
			}
			r := doRequest(t, hLocal, eLocal, tt.method, path, tt.body)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
