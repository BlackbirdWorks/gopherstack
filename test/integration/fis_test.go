package integration_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------
// FIS HTTP helpers
// ----------------------------------------

// fisRequest sends an HTTP request to the FIS endpoint and returns the response body.
func fisRequest(
	t *testing.T,
	method, path string,
	body any,
) *http.Response {
	t.Helper()

	var bodyReader *bytes.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)
		bodyReader = bytes.NewReader(b)
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bodyReader)
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func fisBody(t *testing.T, resp *http.Response, out any) {
	t.Helper()

	defer func() { _ = resp.Body.Close() }()

	require.NoError(t, json.NewDecoder(resp.Body).Decode(out))
}

// ----------------------------------------
// Integration tests
// ----------------------------------------

// TestIntegration_FIS_ExperimentTemplateLifecycle tests the full experiment template CRUD.
func TestIntegration_FIS_ExperimentTemplateLifecycle(t *testing.T) {
	t.Parallel()

	// --- Create ---
	createBody := map[string]any{
		"description": "integration test template",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{},
		"tags":    map[string]string{"env": "integration-test"},
	}

	createResp := fisRequest(t, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	var createResult struct {
		ExperimentTemplate struct {
			Tags        map[string]string `json:"tags"`
			ID          string            `json:"id"`
			Arn         string            `json:"arn"`
			Description string            `json:"description"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, createResp, &createResult)
	require.NotEmpty(t, createResult.ExperimentTemplate.ID)
	assert.Equal(t, "integration test template", createResult.ExperimentTemplate.Description)
	assert.Equal(t, "integration-test", createResult.ExperimentTemplate.Tags["env"])

	id := createResult.ExperimentTemplate.ID
	arnStr := createResult.ExperimentTemplate.Arn

	// --- Get ---
	getResp := fisRequest(t, http.MethodGet, "/experimentTemplates/"+id, nil)
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var getResult struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, getResp, &getResult)
	assert.Equal(t, id, getResult.ExperimentTemplate.ID)

	// --- Update ---
	updateResp := fisRequest(t, http.MethodPatch, "/experimentTemplates/"+id, map[string]any{
		"description": "updated description",
	})
	require.Equal(t, http.StatusOK, updateResp.StatusCode)

	var updateResult struct {
		ExperimentTemplate struct {
			Description string `json:"description"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, updateResp, &updateResult)
	assert.Equal(t, "updated description", updateResult.ExperimentTemplate.Description)

	// --- List ---
	listResp := fisRequest(t, http.MethodGet, "/experimentTemplates", nil)
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	var listResult struct {
		ExperimentTemplates []struct {
			ID string `json:"id"`
		} `json:"experimentTemplates"`
	}

	fisBody(t, listResp, &listResult)
	found := false

	for _, tpl := range listResult.ExperimentTemplates {
		if tpl.ID == id {
			found = true

			break
		}
	}

	assert.True(t, found, "created template should appear in list")

	// --- Tag ---
	tagPath := fmt.Sprintf("/tags/%s", arnStr)
	tagResp := fisRequest(t, http.MethodPost, tagPath, map[string]any{
		"tags": map[string]string{"team": "platform"},
	})
	assert.Equal(t, http.StatusNoContent, tagResp.StatusCode)

	_ = tagResp.Body.Close()

	// --- ListTags ---
	tagsResp := fisRequest(t, http.MethodGet, tagPath, nil)
	require.Equal(t, http.StatusOK, tagsResp.StatusCode)

	var tagsResult struct {
		Tags map[string]string `json:"tags"`
	}

	fisBody(t, tagsResp, &tagsResult)
	assert.Equal(t, "platform", tagsResult.Tags["team"])

	// --- Untag ---
	untagResp := fisRequest(t, http.MethodDelete, tagPath+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, untagResp.StatusCode)

	_ = untagResp.Body.Close()

	// --- Delete ---
	delResp := fisRequest(t, http.MethodDelete, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	_ = delResp.Body.Close()

	// Verify deleted.
	getAfterDel := fisRequest(t, http.MethodGet, "/experimentTemplates/"+id, nil)
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode)

	_ = getAfterDel.Body.Close()
}

// TestIntegration_FIS_ExperimentLifecycle tests starting, monitoring, and stopping experiments.
func TestIntegration_FIS_ExperimentLifecycle(t *testing.T) {
	t.Parallel()

	// Create a template with a short wait action.
	createBody := map[string]any{
		"description": "lifecycle test",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{},
		"actions": map[string]any{
			"wait": map[string]any{
				"actionId": "aws:fis:wait",
				"parameters": map[string]string{
					// PT60S gives enough headroom for slow CI environments.
					"duration": "PT60S",
				},
			},
		},
	}

	createResp := fisRequest(t, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	var createResult struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, createResp, &createResult)
	templateID := createResult.ExperimentTemplate.ID

	// Start experiment.
	startResp := fisRequest(t, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, startResp.StatusCode)

	var startResult struct {
		Experiment struct {
			ID     string `json:"id"`
			Status struct {
				Status string `json:"status"`
			} `json:"status"`
		} `json:"experiment"`
	}

	fisBody(t, startResp, &startResult)
	expID := startResult.Experiment.ID
	require.NotEmpty(t, expID)

	// Verify initial status is pending or running.
	initStatus := startResult.Experiment.Status.Status
	assert.True(t, initStatus == "pending" || initStatus == "running",
		"expected pending or running, got %s", initStatus)

	// List experiments and verify ours is present.
	listResp := fisRequest(t, http.MethodGet, "/experiments", nil)
	require.Equal(t, http.StatusOK, listResp.StatusCode)

	var listResult struct {
		Experiments []struct {
			ID string `json:"id"`
		} `json:"experiments"`
	}

	fisBody(t, listResp, &listResult)

	found := false

	for _, exp := range listResult.Experiments {
		if exp.ID == expID {
			found = true

			break
		}
	}

	assert.True(t, found, "started experiment should appear in list")

	// Stop the experiment.
	stopResp := fisRequest(t, http.MethodDelete, "/experiments/"+expID, nil)
	assert.Equal(t, http.StatusOK, stopResp.StatusCode)

	var stopResult struct {
		Experiment struct {
			Status struct {
				Status string `json:"status"`
			} `json:"status"`
		} `json:"experiment"`
	}

	fisBody(t, stopResp, &stopResult)

	// Wait for the experiment to reach a terminal state.
	require.Eventually(t, func() bool {
		getResp := fisRequest(t, http.MethodGet, "/experiments/"+expID, nil)
		if getResp.StatusCode != http.StatusOK {
			_ = getResp.Body.Close()

			return false
		}

		var getResult struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		defer func() { _ = getResp.Body.Close() }()

		if err := json.NewDecoder(getResp.Body).Decode(&getResult); err != nil {
			return false
		}

		s := getResult.Experiment.Status.Status

		return s == "stopped" || s == "completed"
	}, 10*time.Second, 200*time.Millisecond)
}

// TestIntegration_FIS_DiscoveryEndpoints tests the action and target-resource-type discovery.
func TestIntegration_FIS_DiscoveryEndpoints(t *testing.T) {
	t.Parallel()

	// ListActions.
	actionsResp := fisRequest(t, http.MethodGet, "/actions", nil)
	require.Equal(t, http.StatusOK, actionsResp.StatusCode)

	var actionsResult struct {
		Actions []struct {
			ID string `json:"id"`
		} `json:"actions"`
	}

	fisBody(t, actionsResp, &actionsResult)
	require.NotEmpty(t, actionsResult.Actions)

	// Verify well-known built-in actions are present.
	actionMap := make(map[string]bool, len(actionsResult.Actions))

	for _, a := range actionsResult.Actions {
		actionMap[a.ID] = true
	}

	assert.True(t, actionMap["aws:fis:wait"], "aws:fis:wait should be listed")
	assert.True(t, actionMap["aws:fis:inject-api-internal-error"], "inject-api-internal-error should be listed")

	// GetAction.
	getActionResp := fisRequest(t, http.MethodGet, "/actions/aws:fis:wait", nil)
	require.Equal(t, http.StatusOK, getActionResp.StatusCode)

	var getActionResult struct {
		Action struct {
			ID string `json:"id"`
		} `json:"action"`
	}

	fisBody(t, getActionResp, &getActionResult)
	assert.Equal(t, "aws:fis:wait", getActionResult.Action.ID)

	// GetAction not found.
	notFoundResp := fisRequest(t, http.MethodGet, "/actions/aws:notreal:action", nil)
	assert.Equal(t, http.StatusNotFound, notFoundResp.StatusCode)

	_ = notFoundResp.Body.Close()

	// ListTargetResourceTypes.
	typesResp := fisRequest(t, http.MethodGet, "/targetResourceTypes", nil)
	require.Equal(t, http.StatusOK, typesResp.StatusCode)

	var typesResult struct {
		TargetResourceTypes []struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceTypes"`
	}

	fisBody(t, typesResp, &typesResult)
	require.NotEmpty(t, typesResult.TargetResourceTypes)

	typeMap := make(map[string]bool, len(typesResult.TargetResourceTypes))

	for _, rt := range typesResult.TargetResourceTypes {
		typeMap[rt.ResourceType] = true
	}

	assert.True(t, typeMap["aws:ec2:instance"])
	assert.True(t, typeMap["aws:lambda:function"])

	// GetTargetResourceType.
	getTypeResp := fisRequest(t, http.MethodGet, "/targetResourceTypes/aws:ec2:instance", nil)
	require.Equal(t, http.StatusOK, getTypeResp.StatusCode)

	var getTypeResult struct {
		TargetResourceType struct {
			ResourceType string `json:"resourceType"`
		} `json:"targetResourceType"`
	}

	fisBody(t, getTypeResp, &getTypeResult)
	assert.Equal(t, "aws:ec2:instance", getTypeResult.TargetResourceType.ResourceType)
}

// TestIntegration_FIS_InjectAPIErrorViaExperiment verifies that an aws:fis:inject-api-*
// experiment template can be created, started, and stopped via the FIS API.
func TestIntegration_FIS_InjectAPIErrorViaExperiment(t *testing.T) {
	t.Parallel()

	// Create a template that injects 503 errors into DynamoDB ListTables.
	createBody := map[string]any{
		"description": "inject 503 into DynamoDB",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{
			"FISRole": map[string]any{
				"resourceType":  "aws:iam:role",
				"selectionMode": "ALL",
				"resourceArns": []string{
					"arn:aws:iam::000000000000:role/FISTestRole",
				},
			},
		},
		"actions": map[string]any{
			"injectError": map[string]any{
				"actionId": "aws:fis:inject-api-unavailable-error",
				"parameters": map[string]string{
					"service":    "dynamodb",
					"operations": "ListTables",
					"percentage": "100",
					// PT60S gives enough headroom for slow CI environments.
					"duration": "PT60S",
				},
				"targets": map[string]string{"Roles": "FISRole"},
			},
		},
	}

	createResp := fisRequest(t, http.MethodPost, "/experimentTemplates", createBody)
	require.Equal(t, http.StatusCreated, createResp.StatusCode)

	var createResult struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, createResp, &createResult)
	templateID := createResult.ExperimentTemplate.ID

	// Start experiment.
	startResp := fisRequest(t, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, startResp.StatusCode)

	var startResult struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	fisBody(t, startResp, &startResult)
	expID := startResult.Experiment.ID
	require.NotEmpty(t, expID)

	// Stop the experiment.
	stopResp := fisRequest(t, http.MethodDelete, "/experiments/"+expID, nil)
	assert.Equal(t, http.StatusOK, stopResp.StatusCode)

	_ = stopResp.Body.Close()
}

// TestIntegration_FIS_TagResource_NotFound verifies that tagging a non-existent resource returns 404.
func TestIntegration_FIS_TagResource_NotFound(t *testing.T) {
	t.Parallel()

	unknownARN := "arn:aws:fis:us-east-1:000000000000:experiment-template/EXTdoesnotexist00000000"
	tagPath := "/tags/" + unknownARN

	tagResp := fisRequest(t, http.MethodPost, tagPath, map[string]any{
		"tags": map[string]string{"key": "value"},
	})

	assert.Equal(t, http.StatusNotFound, tagResp.StatusCode)

	_ = tagResp.Body.Close()
}

// TestIntegration_FIS_KinesisThroughputException verifies the end-to-end flow of the
// aws:kinesis:stream-provisioned-throughput-exception FIS action via the FIS HTTP API.
// It creates a stream, starts an experiment targeting that stream, confirms the fault is
// active (PutRecord returns a throttle error), stops the experiment, and confirms the
// fault is cleared.
func TestIntegration_FIS_KinesisThroughputException(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()
	kinesisClient := createKinesisClient(t)
	streamName := "fis-test-stream-" + t.Name()

	// Create a Kinesis stream for use as a target.
	_, err := kinesisClient.CreateStream(ctx, &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Obtain the stream ARN so we can reference it in the FIS target.
	descOut, err := kinesisClient.DescribeStream(ctx, &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	streamARN := aws.ToString(descOut.StreamDescription.StreamARN)
	require.NotEmpty(t, streamARN)

	// Build a FIS experiment template that uses the throughput-exception action.
	createTemplateBody := map[string]any{
		"description": "Kinesis FIS integration test",
		"stopConditions": []map[string]any{
			{"source": "none"},
		},
		"targets": map[string]any{
			"MyStream": map[string]any{
				"resourceType":  "aws:kinesis:stream",
				"selectionMode": "ALL",
				"resourceArns":  []string{streamARN},
			},
		},
		"actions": map[string]any{
			"throttle": map[string]any{
				"actionId": "aws:kinesis:stream-provisioned-throughput-exception",
				"parameters": map[string]string{
					"duration":   "PT60S",
					"percentage": "100",
				},
				"targets": map[string]string{"Streams": "MyStream"},
			},
		},
	}

	templateResp := fisRequest(t, http.MethodPost, "/experimentTemplates", createTemplateBody)
	require.Equal(t, http.StatusCreated, templateResp.StatusCode)

	var templateResult struct {
		ExperimentTemplate struct {
			ID string `json:"id"`
		} `json:"experimentTemplate"`
	}

	fisBody(t, templateResp, &templateResult)
	templateID := templateResult.ExperimentTemplate.ID
	require.NotEmpty(t, templateID)

	// Start the experiment.
	startResp := fisRequest(t, http.MethodPost, "/experiments", map[string]any{
		"experimentTemplateId": templateID,
	})
	require.Equal(t, http.StatusCreated, startResp.StatusCode)

	var startResult struct {
		Experiment struct {
			ID string `json:"id"`
		} `json:"experiment"`
	}

	fisBody(t, startResp, &startResult)
	expID := startResult.Experiment.ID
	require.NotEmpty(t, expID)

	// Wait for the experiment to reach "running" status before probing.
	require.Eventually(t, func() bool {
		getResp := fisRequest(t, http.MethodGet, "/experiments/"+expID, nil)

		var getResult struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		defer func() { _ = getResp.Body.Close() }()

		if getResp.StatusCode != http.StatusOK {
			return false
		}

		if decodeErr := json.NewDecoder(getResp.Body).Decode(&getResult); decodeErr != nil {
			return false
		}

		return getResult.Experiment.Status.Status == "running"
	}, 5*time.Second, 100*time.Millisecond, "experiment should reach running state")

	// With the fault active, PutRecord should return a throttle error.
	iterOut, err := kinesisClient.GetShardIterator(ctx, &kinesissdk.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String("shardId-000000000000"),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	_, putErr := kinesisClient.PutRecord(ctx, &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("test-key"),
		Data:         []byte("test-data"),
	})
	require.Error(t, putErr, "PutRecord should be throttled while experiment is running")

	_, getErr := kinesisClient.GetRecords(ctx, &kinesissdk.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.Error(t, getErr, "GetRecords should be throttled while experiment is running")

	// Stop the experiment.
	stopResp := fisRequest(t, http.MethodDelete, "/experiments/"+expID, nil)
	assert.Equal(t, http.StatusOK, stopResp.StatusCode)

	_ = stopResp.Body.Close()

	// Wait for the experiment to reach a terminal state.
	require.Eventually(t, func() bool {
		getResp := fisRequest(t, http.MethodGet, "/experiments/"+expID, nil)

		var getResult struct {
			Experiment struct {
				Status struct {
					Status string `json:"status"`
				} `json:"status"`
			} `json:"experiment"`
		}

		defer func() { _ = getResp.Body.Close() }()

		if getResp.StatusCode != http.StatusOK {
			return false
		}

		if decodeErr := json.NewDecoder(getResp.Body).Decode(&getResult); decodeErr != nil {
			return false
		}

		s := getResult.Experiment.Status.Status

		return s == "stopped" || s == "completed"
	}, 10*time.Second, 200*time.Millisecond, "experiment should reach terminal state")

	// After StopExperiment the fault should be cleared.
	require.Eventually(t, func() bool {
		_, putAfterErr := kinesisClient.PutRecord(ctx, &kinesissdk.PutRecordInput{
			StreamName:   aws.String(streamName),
			PartitionKey: aws.String("test-key"),
			Data:         []byte("test-data"),
		})

		return putAfterErr == nil
	}, 3*time.Second, 100*time.Millisecond, "PutRecord should succeed after experiment is stopped")
}
