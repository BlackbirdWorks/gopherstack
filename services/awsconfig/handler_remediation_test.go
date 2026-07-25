package awsconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
