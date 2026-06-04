package bedrock_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// ─────────────────────────────────────────────────────────────
// Knowledge base — vector store configuration accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_KnowledgeBase_VectorStoreConfigPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kbConfig    map[string]any
		storageConf map[string]any
		name        string
	}{
		{
			name: "VECTOR type with openSearch",
			kbConfig: map[string]any{
				"type": "VECTOR",
				"vectorKnowledgeBaseConfiguration": map[string]any{
					"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v1",
				},
			},
			storageConf: map[string]any{
				"type": "OPENSEARCH_SERVERLESS",
				"opensearchServerlessConfiguration": map[string]any{
					"collectionArn":   "arn:aws:aoss:us-east-1:000000000000:collection/kb-coll",
					"vectorIndexName": "kb-index",
					"fieldMapping": map[string]any{
						"vectorField":   "embedding",
						"textField":     "text",
						"metadataField": "metadata",
					},
				},
			},
		},
		{
			name: "VECTOR type with pinecone",
			kbConfig: map[string]any{
				"type": "VECTOR",
				"vectorKnowledgeBaseConfiguration": map[string]any{
					"embeddingModelArn": "arn:aws:bedrock:us-east-1::foundation-model/amazon.titan-embed-text-v2:0",
				},
			},
			storageConf: map[string]any{
				"type": "PINECONE",
				"pineconeConfiguration": map[string]any{
					"connectionString":     "https://kb.svc.pinecone.io",
					"credentialsSecretArn": "arn:aws:secretsmanager:us-east-1:000000000000:secret/pinecone",
					"namespace":            "kb-ns",
					"fieldMapping": map[string]any{
						"textField":     "chunk",
						"metadataField": "meta",
					},
				},
			},
		},
		{
			name: "KENDRA type",
			kbConfig: map[string]any{
				"type": "KENDRA",
				"kendraKnowledgeBaseConfiguration": map[string]any{
					"kendraIndexArn": "arn:aws:kendra:us-east-1:000000000000:index/12345",
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestAgentsHandler(t)

			body := map[string]any{
				"name":                       tt.name,
				"roleArn":                    "arn:aws:iam::000000000000:role/kb-role",
				"knowledgeBaseConfiguration": tt.kbConfig,
			}

			if tt.storageConf != nil {
				body["storageConfiguration"] = tt.storageConf
			}

			rec := doAgentRequest(t, h, http.MethodPost, "/knowledgebases", body)
			require.Equal(t, http.StatusAccepted, rec.Code)

			var created map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
			kbID := created["knowledgeBase"].(map[string]any)["knowledgeBaseId"].(string)

			getRec := doAgentRequest(t, h, http.MethodGet, "/knowledgebases/"+kbID, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var got map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &got))
			kb := got["knowledgeBase"].(map[string]any)

			kbCfg := kb["knowledgeBaseConfiguration"].(map[string]any)
			assert.Equal(t, tt.kbConfig["type"], kbCfg["type"])
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Data source — S3 vector ingestion + chunking strategies
// ─────────────────────────────────────────────────────────────

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

			rec := doAgentRequest(t, h, http.MethodPost,
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

	rec := doAgentRequest(t, h, http.MethodPost,
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

			rec := doAgentRequest(t, h, http.MethodPost,
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

// ─────────────────────────────────────────────────────────────
// Ingestion job — lifecycle and status transitions
// ─────────────────────────────────────────────────────────────

func TestAccuracy_IngestionJob_StatusTransitions(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "test ingestion"},
	)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	job := body["ingestionJob"].(map[string]any)
	jobID := job["ingestionJobId"].(string)
	assert.NotEmpty(t, jobID)

	// Status starts as STARTING
	assert.Equal(t, "STARTING", job["status"])
	assert.Equal(t, kbID, job["knowledgeBaseId"])
	assert.Equal(t, dsID, job["dataSourceId"])

	// Get returns the job
	getRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s", kbID, dsID, jobID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
	gotJob := getBody["ingestionJob"].(map[string]any)
	assert.Equal(t, jobID, gotJob["ingestionJobId"])
}

func TestAccuracy_IngestionJob_ListContainsStartedJob(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	// AWS only allows one running job per data source; stop the first before starting the second.
	rec1 := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "first job"},
	)
	require.Equal(t, http.StatusAccepted, rec1.Code)

	var body1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &body1))
	job1ID := body1["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	stopRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop", kbID, dsID, job1ID), nil)
	require.Equal(t, http.StatusOK, stopRec.Code)

	doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "second job"},
	)

	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	jobs := listBody["ingestionJobSummaries"].([]any)
	assert.GreaterOrEqual(t, len(jobs), 2)
}

func TestAccuracy_IngestionJob_DescriptionPreserved(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	const desc = "my important ingestion job"

	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": desc},
	)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	job := body["ingestionJob"].(map[string]any)
	assert.Equal(t, desc, job["description"])
}

// ─────────────────────────────────────────────────────────────
// Agent action group — return-control executor type
// ─────────────────────────────────────────────────────────────

func TestAccuracy_ActionGroup_ReturnControlExecutorPreserved(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("rc-agent", "amazon.titan-text-express-v1", "", "", nil)
	require.NoError(t, err)

	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
		map[string]any{
			"actionGroupName": "return-control-group",
			"actionGroupExecutor": map[string]any{
				"returnControl": map[string]any{},
			},
			"functionSchema": map[string]any{
				"functions": []map[string]any{
					{
						"name":        "get_order_status",
						"description": "Get the status of an order",
						"parameters": map[string]any{
							"order_id": map[string]any{
								"type":     "string",
								"required": true,
							},
						},
					},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ag := body["agentActionGroup"].(map[string]any)
	agID := ag["actionGroupId"].(string)
	assert.NotEmpty(t, agID)

	executor := ag["actionGroupExecutor"].(map[string]any)
	assert.Contains(t, executor, "returnControl", "executor should contain returnControl key")
}

func TestAccuracy_ActionGroup_LambdaExecutorPreserved(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("lambda-agent", "amazon.titan-text-express-v1", "", "", nil)
	require.NoError(t, err)

	const lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:my-action-handler"

	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
		map[string]any{
			"actionGroupName": "lambda-group",
			"actionGroupExecutor": map[string]any{
				"lambda": lambdaARN,
			},
			"apiSchema": map[string]any{
				"payload": "openapi: 3.0.0\ninfo:\n  title: My API\n",
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	ag := body["agentActionGroup"].(map[string]any)
	executor := ag["actionGroupExecutor"].(map[string]any)

	assert.Equal(t, lambdaARN, executor["lambda"])
}

func TestAccuracy_ActionGroup_ReturnControlVsLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		executorKey   string
		executorValue any
		name          string
	}{
		{
			name:          "return-control executor",
			executorKey:   "returnControl",
			executorValue: map[string]any{},
		},
		{
			name:          "lambda executor",
			executorKey:   "lambda",
			executorValue: "arn:aws:lambda:us-east-1:000000000000:function:handler",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			agent, err := b.CreateAgent("exec-test-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)

			rec := doAgentRequest(t, h, http.MethodPost,
				fmt.Sprintf("/agents/%s/action-groups", agent.AgentID),
				map[string]any{
					"actionGroupName": "exec-group",
					"actionGroupExecutor": map[string]any{
						tt.executorKey: tt.executorValue,
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			ag := body["agentActionGroup"].(map[string]any)
			executor := ag["actionGroupExecutor"].(map[string]any)
			assert.Contains(t, executor, tt.executorKey)
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Agent collaborator — relay conversation history accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_AgentCollaborator_RelayConversationHistoryPreserved(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		relayHistory string
	}{
		{name: "relay enabled", relayHistory: "TO_COLLABORATOR"},
		{name: "relay disabled", relayHistory: "DISABLED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			supervisor, err := b.CreateAgent("supervisor-"+tt.name, "model", "", "", nil)
			require.NoError(t, err)

			rec := doAgentRequest(t, h, http.MethodPost,
				fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
				map[string]any{
					"collaboratorArn":          "arn:aws:bedrock:us-east-1:000000000000:agent/collab-agent",
					"agentVersion":             "DRAFT",
					"relayConversationHistory": tt.relayHistory,
				},
			)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			collab := body["agentCollaborator"].(map[string]any)
			collabID := collab["collaboratorId"].(string)
			assert.NotEmpty(t, collabID)
			assert.Equal(t, tt.relayHistory, collab["relayConversationHistory"])

			// Verify GET preserves the relay setting
			getRec := doAgentRequest(t, h, http.MethodGet,
				fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", supervisor.AgentID, collabID), nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getBody map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
			gotCollab := getBody["agentCollaborator"].(map[string]any)
			assert.Equal(t, tt.relayHistory, gotCollab["relayConversationHistory"])
		})
	}
}

func TestAccuracy_AgentCollaborator_UpdateRelayHistory(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	supervisor, err := b.CreateAgent("relay-update-agent", "model", "", "", nil)
	require.NoError(t, err)

	// Associate with relay disabled
	assocRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
		map[string]any{
			"collaboratorArn":          "arn:aws:bedrock:us-east-1:000000000000:agent/subagent",
			"agentVersion":             "DRAFT",
			"relayConversationHistory": "DISABLED",
		},
	)
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocBody map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocBody))
	collabID := assocBody["agentCollaborator"].(map[string]any)["collaboratorId"].(string)

	// Update relay to enabled
	updateRec := doAgentRequest(t, h, http.MethodPut,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", supervisor.AgentID, collabID),
		map[string]any{"relayConversationHistory": "TO_COLLABORATOR"},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateBody map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateBody))
	updatedCollab := updateBody["agentCollaborator"].(map[string]any)
	assert.Equal(t, "TO_COLLABORATOR", updatedCollab["relayConversationHistory"])
}

func TestAccuracy_AgentCollaborator_SupervisorPattern(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)

	// Create supervisor agent with SUPERVISOR collaboration
	supervisor, err := b.CreateAgentWithConfiguration(bedrock.AgentConfiguration{
		AgentName:          "supervisor",
		FoundationModel:    "model",
		AgentCollaboration: "SUPERVISOR",
	})
	require.NoError(t, err)
	assert.Equal(t, "SUPERVISOR", supervisor.AgentCollaboration)

	// Associate two subagents
	for i := range 2 {
		rec := doAgentRequest(t, h, http.MethodPost,
			fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID),
			map[string]any{
				"collaboratorArn": fmt.Sprintf("arn:aws:bedrock:us-east-1:000000000000:agent/subagent-%d", i),
				"agentVersion":    "DRAFT",
			},
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	// List should show 2 collaborators
	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", supervisor.AgentID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	assert.Len(t, listBody["agentCollaboratorSummaries"], 2)
}

func TestAccuracy_AgentCollaborator_DisassociateRemovesFromList(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("disassoc-agent", "model", "", "", nil)
	require.NoError(t, err)

	// Associate
	rec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", agent.AgentID),
		map[string]any{
			"collaboratorArn": "arn:aws:bedrock:us-east-1:000000000000:agent/temp-collab",
			"agentVersion":    "DRAFT",
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	collabID := body["agentCollaborator"].(map[string]any)["collaboratorId"].(string)

	// Disassociate
	deleteRec := doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators/%s", agent.AgentID, collabID), nil)
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	// List should be empty
	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s/agentversions/DRAFT/agentcollaborators", agent.AgentID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	assert.Empty(t, listBody["agentCollaboratorSummaries"])
}

// ─────────────────────────────────────────────────────────────
// Prompt management — variant and version accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Prompt_VariantPreserved(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name":        "variant-prompt",
		"description": "a prompt with variants",
		"variants": []map[string]any{
			{
				"name":         "default",
				"templateType": "TEXT",
				"modelId":      "amazon.titan-text-express-v1",
				"templateConfiguration": map[string]any{
					"text": map[string]any{
						"text": "You are a helpful assistant. {{user_input}}",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	prompt := createBody["prompt"].(map[string]any)
	promptID := prompt["promptId"].(string)
	assert.NotEmpty(t, promptID)

	// GET preserves name and description
	getRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
	gotPrompt := getBody["prompt"].(map[string]any)
	assert.Equal(t, "variant-prompt", gotPrompt["name"])
	assert.Equal(t, "a prompt with variants", gotPrompt["description"])
}

func TestAccuracy_Prompt_VersionPreservesContent(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create prompt
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{
		"name": "versioned-prompt",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID := createBody["prompt"].(map[string]any)["promptId"].(string)

	// Create version
	verRec := doAgentRequest(t, h, http.MethodPost, "/prompts/"+promptID+"/versions", nil)
	require.Equal(t, http.StatusCreated, verRec.Code)

	var verBody map[string]any
	require.NoError(t, json.Unmarshal(verRec.Body.Bytes(), &verBody))
	pv := verBody["promptVersion"].(map[string]any)
	version := pv["version"].(string)
	assert.NotEmpty(t, version)

	// GET version preserves prompt ID
	getVerRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID+"/versions/"+version, nil)
	require.Equal(t, http.StatusOK, getVerRec.Code)

	var getVerBody map[string]any
	require.NoError(t, json.Unmarshal(getVerRec.Body.Bytes(), &getVerBody))
	gotVer := getVerBody["promptVersion"].(map[string]any)
	assert.Equal(t, promptID, gotVer["promptId"])
}

func TestAccuracy_Prompt_UpdateNameAndDescription(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	// Create
	rec := doAgentRequest(t, h, http.MethodPost, "/prompts",
		map[string]any{"name": "original-name", "description": "original desc"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID := createBody["prompt"].(map[string]any)["promptId"].(string)

	// Update
	updateRec := doAgentRequest(t, h, http.MethodPut, "/prompts/"+promptID,
		map[string]any{"name": "updated-name", "description": "updated desc"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Verify
	getRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
	p := getBody["prompt"].(map[string]any)
	assert.Equal(t, "updated-name", p["name"])
	assert.Equal(t, "updated desc", p["description"])
}

func TestAccuracy_Prompt_ListVersionsReturnsCreatedVersions(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	rec := doAgentRequest(t, h, http.MethodPost, "/prompts", map[string]any{"name": "multi-version-prompt"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createBody map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createBody))
	promptID := createBody["prompt"].(map[string]any)["promptId"].(string)

	for range 3 {
		vRec := doAgentRequest(t, h, http.MethodPost, "/prompts/"+promptID+"/versions", nil)
		require.Equal(t, http.StatusCreated, vRec.Code)
	}

	listRec := doAgentRequest(t, h, http.MethodGet, "/prompts/"+promptID+"/versions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	versions := listBody["promptVersionSummaries"].([]any)
	assert.Len(t, versions, 3)
}

// ─────────────────────────────────────────────────────────────
// KB document ingestion with BDA parsing strategy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_KBDocuments_IngestWithBDAParsingStrategy(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("bda-kb", "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create data source with BDA parsing
	dsRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources", kb.KnowledgeBaseID),
		map[string]any{
			"name": "bda-source",
			"vectorIngestionConfiguration": map[string]any{
				"parsingConfiguration": map[string]any{
					"parsingStrategy": "BEDROCK_DATA_AUTOMATION",
					"bedrockDataAutomationConfiguration": map[string]any{
						"parsingModality": "MULTIMODAL",
					},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, dsRec.Code)

	var dsBody map[string]any
	require.NoError(t, json.Unmarshal(dsRec.Body.Bytes(), &dsBody))
	dsID := dsBody["dataSource"].(map[string]any)["dataSourceId"].(string)

	// Ingest documents
	ingestRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kb.KnowledgeBaseID, dsID),
		map[string]any{"documentIds": []string{"doc-1", "doc-2", "doc-3"}},
	)
	require.Equal(t, http.StatusOK, ingestRec.Code)

	var ingestBody map[string]any
	require.NoError(t, json.Unmarshal(ingestRec.Body.Bytes(), &ingestBody))
	docs := ingestBody["documents"].([]any)
	assert.Len(t, docs, 3)

	// Verify documents are active
	for _, doc := range docs {
		d := doc.(map[string]any)
		assert.Equal(t, "ACTIVE", d["status"])
	}
}

func TestAccuracy_KBDocuments_GetSpecificDocuments(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	// Ingest 5 documents
	ingestRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents", kbID, dsID),
		map[string]any{"documentIds": []string{"d1", "d2", "d3", "d4", "d5"}},
	)
	require.Equal(t, http.StatusOK, ingestRec.Code)

	// Get specific 2
	getDocRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/documents/getDocuments", kbID, dsID),
		map[string]any{"documentIds": []string{"d2", "d4"}},
	)
	require.Equal(t, http.StatusOK, getDocRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getDocRec.Body.Bytes(), &getBody))
	docs := getBody["documentDetails"].([]any)
	assert.Len(t, docs, 2)
}
