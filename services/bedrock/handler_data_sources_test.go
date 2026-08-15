package bedrock_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_DataSourceLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ds-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	kbID := kb.KnowledgeBaseID

	// Create data source.
	rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/"+kbID+"/datasources", map[string]any{
		"name":        "my-ds",
		"description": "test data source",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	ds := createOut["dataSource"].(map[string]any)
	dsID := ds["dataSourceId"].(string)
	assert.NotEmpty(t, dsID)

	// List data sources.
	rec2 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID+"/datasources", nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &listOut))
	assert.Len(t, listOut["dataSourceSummaries"].([]any), 1)

	// Get data source.
	rec3 := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID+"/datasources/"+dsID, nil)
	assert.Equal(t, http.StatusOK, rec3.Code)

	// Update data source.
	rec4 := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/"+kbID+"/datasources/"+dsID, map[string]any{
		"description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Delete data source.
	rec5 := doAgentRequest(t, h, http.MethodDelete, "/knowledgebases/"+kbID+"/datasources/"+dsID, nil)
	assert.Equal(t, http.StatusOK, rec5.Code)
}

func TestAgentsHandler_DataSource_KBNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPut, "/knowledgebases/nonexistent/datasources", map[string]any{
		"name": "ds",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAgentsHandler_DataSource_NotFound(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("notfound-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	for _, tc := range []struct {
		path   string
		method string
	}{
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodGet},
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodPut},
		{"/knowledgebases/" + kb.KnowledgeBaseID + "/datasources/nonexistent", http.MethodDelete},
	} {
		rec := doAgentRequest(t, h, tc.method, tc.path, map[string]any{"name": "test"})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	}
}

func TestAgentsHandler_DataSourceIngestionConfigurations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sourceType       string
		chunkingStrategy string
		parsingStrategy  string
	}{
		{sourceType: "S3", chunkingStrategy: "FIXED_SIZE", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
		{sourceType: "WEB", chunkingStrategy: "HIERARCHICAL", parsingStrategy: "BEDROCK_DATA_AUTOMATION"},
		{sourceType: "CONFLUENCE", chunkingStrategy: "SEMANTIC", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
		{sourceType: "SALESFORCE", chunkingStrategy: "FIXED_SIZE", parsingStrategy: "BEDROCK_DATA_AUTOMATION"},
		{sourceType: "SHAREPOINT", chunkingStrategy: "HIERARCHICAL", parsingStrategy: "BEDROCK_FOUNDATION_MODEL"},
	}

	for _, tt := range tests {
		t.Run(tt.sourceType, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			kb, err := b.CreateKnowledgeBase("kb-"+tt.sourceType, "", "", nil, nil, nil)
			require.NoError(t, err)
			rec := doAgentRequest(
				t, h, http.MethodPut,
				fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
				map[string]any{
					"name":                    "source",
					"dataSourceConfiguration": map[string]any{"type": tt.sourceType},
					"vectorIngestionConfiguration": map[string]any{
						"chunkingConfiguration": map[string]any{"chunkingStrategy": tt.chunkingStrategy},
						"parsingConfiguration":  map[string]any{"parsingStrategy": tt.parsingStrategy},
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ds := body["dataSource"].(map[string]any)
			assert.Equal(t, tt.sourceType, ds["dataSourceConfiguration"].(map[string]any)["type"])
			vector := ds["vectorIngestionConfiguration"].(map[string]any)
			assert.Equal(t, tt.chunkingStrategy, vector["chunkingConfiguration"].(map[string]any)["chunkingStrategy"])
			assert.Equal(t, tt.parsingStrategy, vector["parsingConfiguration"].(map[string]any)["parsingStrategy"])
		})
	}
}

func TestAccuracy_DataSource_S3VectorIngestionConfigPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		chunkingStrategy string
		chunkingConfig   map[string]any
		parsingStrategy  string
	}{
		{
			name:             "fixed-size chunking with BDA parsing",
			chunkingStrategy: "FIXED_SIZE",
			chunkingConfig: map[string]any{
				"fixedSizeChunkingConfiguration": map[string]any{
					"maxTokens":         512,
					"overlapPercentage": 20,
				},
			},
			parsingStrategy: "BEDROCK_DATA_AUTOMATION",
		},
		{
			name:             "hierarchical chunking with foundation model parsing",
			chunkingStrategy: "HIERARCHICAL",
			chunkingConfig: map[string]any{
				"hierarchicalChunkingConfiguration": map[string]any{
					"levelConfigurations": []map[string]any{
						{"maxTokens": 2048},
						{"maxTokens": 512},
					},
					"overlapTokens": 64,
				},
			},
			parsingStrategy: "BEDROCK_FOUNDATION_MODEL",
		},
		{
			name:             "semantic chunking",
			chunkingStrategy: "SEMANTIC",
			chunkingConfig: map[string]any{
				"semanticChunkingConfiguration": map[string]any{
					"maxTokens":                     300,
					"bufferSize":                    0,
					"breakpointPercentileThreshold": 95,
				},
			},
			parsingStrategy: "BEDROCK_FOUNDATION_MODEL",
		},
		{
			name:             "no chunking with BDA",
			chunkingStrategy: "NONE",
			parsingStrategy:  "BEDROCK_DATA_AUTOMATION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			kb, err := b.CreateKnowledgeBase("kb-"+tt.name, "", "", nil, nil, nil)
			require.NoError(t, err)

			chunkConfig := map[string]any{"chunkingStrategy": tt.chunkingStrategy}
			if tt.chunkingConfig != nil {
				maps.Copy(chunkConfig, tt.chunkingConfig)
			}

			vectorConfig := map[string]any{
				"chunkingConfiguration": chunkConfig,
				"parsingConfiguration": map[string]any{
					"parsingStrategy": tt.parsingStrategy,
				},
			}

			rec := doAgentRequest(
				t, h, http.MethodPut,
				fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
				map[string]any{
					"name":                         tt.name,
					"dataSourceConfiguration":      map[string]any{"type": "S3"},
					"vectorIngestionConfiguration": vectorConfig,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ds := body["dataSource"].(map[string]any)
			dsID := ds["dataSourceId"].(string)

			getRec := doAgentRequest(t, h, http.MethodGet,
				fmt.Sprintf("/knowledgebases/%s/datasources/%s", kb.KnowledgeBaseID, dsID), nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getBody map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
			gotDS := getBody["dataSource"].(map[string]any)

			vic := gotDS["vectorIngestionConfiguration"].(map[string]any)
			chunking := vic["chunkingConfiguration"].(map[string]any)
			parsing := vic["parsingConfiguration"].(map[string]any)

			assert.Equal(t, tt.chunkingStrategy, chunking["chunkingStrategy"])
			assert.Equal(t, tt.parsingStrategy, parsing["parsingStrategy"])
		})
	}
}

func TestAccuracy_DataSource_S3BucketConfigPreserved(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("s3-bucket-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	dsConfig := map[string]any{
		"type": "S3",
		"s3Configuration": map[string]any{
			"bucketArn":         "arn:aws:s3:::my-kb-bucket",
			"inclusionPrefixes": []string{"documents/", "reports/"},
		},
	}

	rec := doAgentRequest(
		t, h, http.MethodPut,
		fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
		map[string]any{
			"name":                    "s3-source",
			"dataSourceConfiguration": dsConfig,
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ds := body["dataSource"].(map[string]any)
	dsID := ds["dataSourceId"].(string)

	getRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s", kb.KnowledgeBaseID, dsID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
	gotDS := getBody["dataSource"].(map[string]any)
	gotDSConfig := gotDS["dataSourceConfiguration"].(map[string]any)

	assert.Equal(t, "S3", gotDSConfig["type"])
	s3Conf := gotDSConfig["s3Configuration"].(map[string]any)
	assert.Equal(t, "arn:aws:s3:::my-kb-bucket", s3Conf["bucketArn"])
}

func TestAccuracy_DataSource_DeletionPolicyPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		deletionPolicy string
	}{
		{name: "retain policy", deletionPolicy: "RETAIN"},
		{name: "delete policy", deletionPolicy: "DELETE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			kb, err := b.CreateKnowledgeBase("del-policy-kb-"+tt.name, "", "", nil, nil, nil)
			require.NoError(t, err)

			rec := doAgentRequest(
				t, h, http.MethodPut,
				fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
				map[string]any{
					"name":               "source-with-deletion",
					"dataDeletionPolicy": tt.deletionPolicy,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ds := body["dataSource"].(map[string]any)

			assert.Equal(t, tt.deletionPolicy, ds["dataDeletionPolicy"])
		})
	}
}
