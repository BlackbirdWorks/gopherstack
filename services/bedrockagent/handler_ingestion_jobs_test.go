package bedrockagent_test

import (
	"encoding/json"
	"net/http"
	"testing"
)

func TestHandlerIngestionJobs(t *testing.T) {
	t.Parallel()

	type tc struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}

	h, e := setupHandler(t)

	// Create KB
	kbBody := map[string]any{
		"name":    "job-kb",
		"roleArn": "arn:aws:iam::123456789012:role/AmazonBedrockRole",
		"knowledgeBaseConfiguration": map[string]any{
			"type": "VECTOR",
			"vectorKnowledgeBaseConfiguration": map[string]any{
				"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
			},
		},
		"storageConfiguration": map[string]any{
			"type": "OPENSEARCH_SERVERLESS",
			"opensearchServerlessConfiguration": map[string]any{
				"collectionArn":   "arn:aws:aoss:us-east-1:123456789012:collection/xyz123",
				"vectorIndexName": "idx",
				"fieldMapping": map[string]any{
					"vectorField":   "vec",
					"textField":     "txt",
					"metadataField": "meta",
				},
			},
		},
	}
	rKB := doRequest(t, h, e, http.MethodPut, "/knowledgebases", kbBody)
	var kbResp map[string]map[string]any
	_ = json.Unmarshal(rKB.Body.Bytes(), &kbResp)
	kbID := kbResp["knowledgeBase"]["knowledgeBaseId"].(string)

	// Create Data Source
	dsBody := map[string]any{
		"name": "job-ds",
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

	basePath := "/knowledgebases/" + kbID + "/datasources/" + dsID + "/ingestionjobs"

	// Start Job
	jobBody := map[string]any{
		"description": "test ingestion",
	}
	rJob := doRequest(t, h, e, http.MethodPut, basePath, jobBody)
	var jobResp map[string]map[string]any
	_ = json.Unmarshal(rJob.Body.Bytes(), &jobResp)
	jobID := jobResp["ingestionJob"]["ingestionJobId"].(string)

	cases := []tc{
		{
			name:       "ListIngestionJobs",
			method:     http.MethodGet,
			path:       basePath,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetIngestionJob",
			method:     http.MethodGet,
			path:       basePath + "/" + jobID,
			wantStatus: http.StatusOK,
		},
		{
			name:       "GetIngestionJob_NotFound",
			method:     http.MethodGet,
			path:       basePath + "/notfound",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "StopIngestionJob",
			method:     http.MethodPost,
			path:       basePath + "/" + jobID + "/stop",
			wantStatus: http.StatusOK,
		},
		{
			name:       "StopIngestionJob_NotFound",
			method:     http.MethodPost,
			path:       basePath + "/notfound/stop",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			hLocal, eLocal := setupHandler(t)

			rK := doRequest(t, hLocal, eLocal, http.MethodPut, "/knowledgebases", kbBody)
			var kResp map[string]map[string]any
			_ = json.Unmarshal(rK.Body.Bytes(), &kResp)
			kID := kResp["knowledgeBase"]["knowledgeBaseId"].(string)

			rD := doRequest(t, hLocal, eLocal, http.MethodPut, "/knowledgebases/"+kID+"/datasources", dsBody)
			var dResp map[string]map[string]any
			_ = json.Unmarshal(rD.Body.Bytes(), &dResp)
			dID := dResp["dataSource"]["dataSourceId"].(string)

			bP := "/knowledgebases/" + kID + "/datasources/" + dID + "/ingestionjobs"

			rJ := doRequest(t, hLocal, eLocal, http.MethodPut, bP, jobBody)
			var jResp map[string]map[string]any
			_ = json.Unmarshal(rJ.Body.Bytes(), &jResp)
			jID := jResp["ingestionJob"]["ingestionJobId"].(string)

			path := tt.path
			if jobID != "" && jID != "" {
				switch path {
				case basePath:
					path = bP
				case basePath + "/" + jobID:
					path = bP + "/" + jID
				case basePath + "/notfound":
					path = bP + "/notfound"
				case basePath + "/" + jobID + "/stop":
					path = bP + "/" + jID + "/stop"
				case basePath + "/notfound/stop":
					path = bP + "/notfound/stop"
				}
			}
			r := doRequest(t, hLocal, eLocal, tt.method, path, tt.body)
			if r.Code != tt.wantStatus {
				t.Errorf("got %d want %d", r.Code, tt.wantStatus)
			}
		})
	}
}
