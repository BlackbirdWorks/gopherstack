package omics_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultipartUploadClientToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		token     string
		wantEqual bool
	}{
		{name: "same token replays", token: "tok", wantEqual: true},
		{name: "no token creates new", token: "", wantEqual: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
			storeID := decodeBody(t, rec.Body.Bytes())["id"].(string)

			body := map[string]any{
				"name": "up", "sourceFileType": "FASTQ", "sampleId": "s", "subjectId": "j",
				"generatedFrom": "gen",
				"referenceArn":  "arn:aws:omics:us-east-1:000000000000:referenceStore/1/reference/2",
			}
			if tt.token != "" {
				body["clientToken"] = tt.token
			}

			path := fmt.Sprintf("/sequencestore/%s/upload", storeID)
			first := decodeBody(t, doRequest(t, h, http.MethodPost, path, body).Body.Bytes())
			second := decodeBody(t, doRequest(t, h, http.MethodPost, path, body).Body.Bytes())

			assert.Equal(t, tt.wantEqual, first["uploadId"] == second["uploadId"])
		})
	}
}

func TestReadSetCreationTypeAndWorkflowAccess(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/sequencestore", map[string]any{"name": "s"})
	storeID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/sequencestore/%s/importjob", storeID), map[string]any{
		"roleArn": testRunBatchRoleArn,
		"sources": []map[string]any{{
			"sourceFileType": "FASTQ", "sourceFiles": map[string]any{"source1": "s3://b/k.fq"},
			"name": "rs", "sampleId": "s", "subjectId": "j", "generatedFrom": "gen",
			"referenceArn": "arn:aws:omics:us-east-1:000000000000:referenceStore/1/reference/2",
		}},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/sequencestore/%s/readsets", storeID), map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	sets, _ := decodeBody(t, rec.Body.Bytes())["readSets"].([]any)
	require.Len(t, sets, 1)

	set := sets[0].(map[string]any)
	assert.Equal(t, "IMPORT", set["creationType"])
	seq, _ := set["sequenceInformation"].(map[string]any)
	assert.Equal(t, "gen", seq["generatedFrom"])

	rec = doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name": "wf", "engine": "WDL", "definitionUri": "s3://b/wf.wdl", "requestId": "r",
	})
	require.Equal(t, http.StatusCreated, rec.Code)
	wfID := decodeBody(t, rec.Body.Bytes())["id"].(string)

	for _, tt := range []struct {
		query string
		want  int
	}{
		{"", http.StatusOK},
		{"?workflowOwnerId=000000000000&type=PRIVATE", http.StatusOK},
		{"?workflowOwnerId=111111111111", http.StatusNotFound},
		{"?type=READY2RUN", http.StatusNotFound},
	} {
		assert.Equal(t, tt.want, doRequest(t, h, http.MethodGet, "/workflow/"+wfID+tt.query, nil).Code, tt.query)
	}
}
