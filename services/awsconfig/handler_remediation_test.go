package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

// TestAWSConfigHandler_StartAndDescribeRemediationExecution round-trips a
// remediation execution through the wire.
func TestAWSConfigHandler_StartAndDescribeRemediationExecution(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)

	rec := doAWSConfigRequest(t, h, "PutRemediationConfigurations", map[string]any{
		"RemediationConfigurations": []map[string]any{
			{"ConfigRuleName": "rule1", "TargetType": "SSM_DOCUMENT", "TargetId": "AWS-RunShellScript"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	startRec := doAWSConfigRequest(t, h, "StartRemediationExecution", map[string]any{
		"ConfigRuleName": "rule1",
		"ResourceKeys": []map[string]any{
			{"resourceType": "AWS::S3::Bucket", "resourceId": "b1"},
		},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	describeRec := doAWSConfigRequest(t, h, "DescribeRemediationExecutionStatus", map[string]any{
		"ConfigRuleName": "rule1",
	})
	require.Equal(t, http.StatusOK, describeRec.Code)

	var out struct {
		RemediationExecutionStatuses []struct {
			State string `json:"State"`
		} `json:"RemediationExecutionStatuses"`
	}
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &out))
	require.Len(t, out.RemediationExecutionStatuses, 1)
	assert.Equal(t, "SUCCEEDED", out.RemediationExecutionStatuses[0].State)
}

// TestPutRemediationExceptionsWireShape asserts on the raw HTTP response for
// PutRemediationExceptions' real request shape: a ResourceKeys list of
// {ResourceType, ResourceId} objects (PascalCase), not the flat top-level
// ResourceType/ResourceId strings this handler used to expect. A body
// serializing ResourceKeys as an empty/absent field parses identically either
// way, which is how gopherstack silently accepted the wrong shape before;
// asserting the 400 and its message is what catches a regression to that
// shape.
func TestPutRemediationExceptionsWireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "nested resource keys",
			body: map[string]any{
				"ConfigRuleName": "rule1",
				"ResourceKeys": []map[string]any{
					{"ResourceType": "AWS::S3::Bucket", "ResourceId": "my-bucket"},
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "old flat invented ResourceType/ResourceId fields are rejected",
			body: map[string]any{
				"ConfigRuleName": "rule1",
				"ResourceType":   "AWS::S3::Bucket",
				"ResourceId":     "my-bucket",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing config rule name",
			body: map[string]any{
				"ResourceKeys": []map[string]any{{"ResourceType": "AWS::S3::Bucket", "ResourceId": "b1"}},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing resource keys",
			body:       map[string]any{"ConfigRuleName": "rule1"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestAWSConfigHandler(t)
			rec := doAWSConfigRequest(t, h, "PutRemediationExceptions", tt.body)
			require.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				assert.Contains(t, rec.Body.String(), "InvalidParameterValueException")

				return
			}

			exs := h.Backend.DescribeRemediationExceptions("rule1")
			require.Len(t, exs, 1)
			assert.Equal(t, "AWS::S3::Bucket", exs[0].ResourceType)
			assert.Equal(t, "my-bucket", exs[0].ResourceID)
		})
	}
}

// TestDeleteRemediationExceptionsWireShape asserts on the raw HTTP response
// for DeleteRemediationExceptions' real request shape: a ResourceKeys list,
// not the invented top-level ResourceGroupName field this handler used to
// read (which doesn't exist on the real API at all, so a real client's
// request never populated it and nothing was ever actually deleted).
func TestDeleteRemediationExceptionsWireShape(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	require.NoError(t, h.Backend.PutRemediationExceptions("rule1", []awsconfig.RemediationExceptionResourceKey{
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket1"},
		{ResourceType: "AWS::S3::Bucket", ResourceID: "bucket2"},
	}))

	// The old invented ResourceGroupName field must have no effect now that
	// it isn't decoded into anything the handler reads.
	rec := doAWSConfigRequest(t, h, "DeleteRemediationExceptions", map[string]any{
		"ConfigRuleName":    "rule1",
		"ResourceGroupName": "bucket1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Len(
		t, h.Backend.DescribeRemediationExceptions("rule1"), 2,
		"invented ResourceGroupName field must not delete anything",
	)

	rec = doAWSConfigRequest(t, h, "DeleteRemediationExceptions", map[string]any{
		"ConfigRuleName": "rule1",
		"ResourceKeys": []map[string]any{
			{"ResourceType": "AWS::S3::Bucket", "ResourceId": "bucket1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	exs := h.Backend.DescribeRemediationExceptions("rule1")
	require.Len(t, exs, 1)
	assert.Equal(t, "bucket2", exs[0].ResourceID)
}

// TestAWSConfigHandler_StartRemediationExecution_NoConfiguration verifies the
// wire error type for a rule with no remediation configuration.
func TestAWSConfigHandler_StartRemediationExecution_NoConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestAWSConfigHandler(t)
	rec := doAWSConfigRequest(t, h, "StartRemediationExecution", map[string]any{
		"ConfigRuleName": "no-such-rule",
		"ResourceKeys": []map[string]any{
			{"resourceType": "AWS::S3::Bucket", "resourceId": "b1"},
		},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "NoSuchRemediationConfigurationException")
}
