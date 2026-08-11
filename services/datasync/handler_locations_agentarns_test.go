package datasync_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// agentArnsLocationCase covers one of the five location types with a real
// top-level AgentArns member on Create/UpdateLocation*Request (botocore
// datasync 2018-11-09 model: CreateLocationS3Request,
// CreateLocationAzureBlobRequest, CreateLocationHdfsRequest,
// CreateLocationObjectStorageRequest, CreateLocationSmbRequest).
// createBody/updateBody build a request with the given AgentArns value
// substituted in, so the same case data drives both the reject-phantom-agent
// and accept-real-agent assertions. updateBody is nil for S3, whose
// UpdateLocationS3Request has no AgentArns member.
type agentArnsLocationCase struct {
	createBody     func(agentArn string) map[string]any
	updateBody     func(locationArn, agentArn string) map[string]any
	name           string
	createAction   string
	describeAction string
	updateAction   string
}

func agentArnsLocationCases() []agentArnsLocationCase {
	return []agentArnsLocationCase{
		{
			name:           "s3",
			createAction:   "CreateLocationS3",
			describeAction: "DescribeLocationS3",
			createBody: func(agentArn string) map[string]any {
				return map[string]any{
					"S3BucketArn": "arn:aws:s3:::my-bucket",
					"S3Config":    map[string]any{"BucketAccessRoleArn": "arn:aws:iam::000000000000:role/r"},
					"AgentArns":   []string{agentArn},
				}
			},
		},
		{
			name:           "azureblob",
			createAction:   "CreateLocationAzureBlob",
			describeAction: "DescribeLocationAzureBlob",
			updateAction:   "UpdateLocationAzureBlob",
			createBody: func(agentArn string) map[string]any {
				return map[string]any{
					"ContainerUrl":       "https://myaccount.blob.core.windows.net/mycontainer",
					"AuthenticationType": "NONE",
					"AgentArns":          []string{agentArn},
				}
			},
			updateBody: func(locationArn, agentArn string) map[string]any {
				return map[string]any{"LocationArn": locationArn, "AgentArns": []string{agentArn}}
			},
		},
		{
			name:           "hdfs",
			createAction:   "CreateLocationHdfs",
			describeAction: "DescribeLocationHdfs",
			updateAction:   "UpdateLocationHdfs",
			createBody: func(agentArn string) map[string]any {
				return map[string]any{
					"NameNodes": []any{
						map[string]any{"Hostname": "namenode1.example.com", "Port": 8020},
					},
					"AuthenticationType": "SIMPLE",
					"SimpleUser":         "hadoop",
					"AgentArns":          []string{agentArn},
				}
			},
			updateBody: func(locationArn, agentArn string) map[string]any {
				return map[string]any{"LocationArn": locationArn, "AgentArns": []string{agentArn}}
			},
		},
		{
			name:           "objectstorage",
			createAction:   "CreateLocationObjectStorage",
			describeAction: "DescribeLocationObjectStorage",
			updateAction:   "UpdateLocationObjectStorage",
			createBody: func(agentArn string) map[string]any {
				return map[string]any{
					"ServerHostname": "objstore.example.com",
					"BucketName":     "my-bucket",
					"AgentArns":      []string{agentArn},
				}
			},
			updateBody: func(locationArn, agentArn string) map[string]any {
				return map[string]any{"LocationArn": locationArn, "AgentArns": []string{agentArn}}
			},
		},
		{
			name:           "smb",
			createAction:   "CreateLocationSmb",
			describeAction: "DescribeLocationSmb",
			updateAction:   "UpdateLocationSmb",
			createBody: func(agentArn string) map[string]any {
				return map[string]any{
					"ServerHostname": "smb.example.com",
					"Subdirectory":   "/share",
					"User":           "smbuser",
					"Password":       "smbpass",
					"AgentArns":      []string{agentArn},
				}
			},
			updateBody: func(locationArn, agentArn string) map[string]any {
				return map[string]any{"LocationArn": locationArn, "AgentArns": []string{agentArn}}
			},
		},
	}
}

const phantomAgentArn = "arn:aws:datasync:us-east-1:000000000000:agent/does-not-exist"

// TestDataSync_AgentArns_RejectsPhantomAgent verifies CreateLocation* rejects
// an AgentArns entry that was never created, with the same error shape AWS
// uses for a malformed request: 400 InvalidRequestException. Before this
// sweep's validateAgentArns fix, none of these five operations checked
// AgentArns against known agents at all.
func TestDataSync_AgentArns_RejectsPhantomAgent(t *testing.T) {
	t.Parallel()

	for _, tc := range agentArnsLocationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, tc.createAction, tc.createBody(phantomAgentArn))
			require.Equal(t, http.StatusBadRequest, rec.Code, "body: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidRequestException", resp["__type"])
		})
	}
}

// TestDataSync_AgentArns_UpdateRejectsPhantomAgent mirrors the Create test
// for the four UpdateLocation* operations that also accept AgentArns
// (UpdateLocationS3Request has no AgentArns member, so S3 is skipped).
func TestDataSync_AgentArns_UpdateRejectsPhantomAgent(t *testing.T) {
	t.Parallel()

	for _, tc := range agentArnsLocationCases() {
		if tc.updateBody == nil {
			continue
		}

		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			realAgentArn := createTestAgent(t, h)

			rec := doRequest(t, h, tc.createAction, tc.createBody(realAgentArn))
			require.Equal(t, http.StatusOK, rec.Code, "create: %s", rec.Body.String())

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			rec = doRequest(t, h, tc.updateAction, tc.updateBody(locArn, phantomAgentArn))
			require.Equal(t, http.StatusBadRequest, rec.Code, "update: %s", rec.Body.String())

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidRequestException", resp["__type"])

			// The rejected update must not have applied: AgentArns on the
			// location stays the one real agent from Create.
			rec = doRequest(t, h, tc.describeAction, map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			agentArns, ok := descResp["AgentArns"].([]any)
			require.True(t, ok)
			assert.ElementsMatch(t, []any{realAgentArn}, agentArns)
		})
	}
}

// TestDataSync_AgentArns_AcceptsRealAgent is the positive control: without
// it, a validateAgentArns that rejected every request (or every call site
// silently unwired) would also pass the two tests above.
func TestDataSync_AgentArns_AcceptsRealAgent(t *testing.T) {
	t.Parallel()

	for _, tc := range agentArnsLocationCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			agentArn := createTestAgent(t, h)

			rec := doRequest(t, h, tc.createAction, tc.createBody(agentArn))
			require.Equal(t, http.StatusOK, rec.Code, "body: %s", rec.Body.String())

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			locArn, ok := createResp["LocationArn"].(string)
			require.True(t, ok)

			rec = doRequest(t, h, tc.describeAction, map[string]any{"LocationArn": locArn})
			require.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			agentArns, ok := descResp["AgentArns"].([]any)
			require.True(t, ok)
			assert.ElementsMatch(t, []any{agentArn}, agentArns)
		})
	}
}
