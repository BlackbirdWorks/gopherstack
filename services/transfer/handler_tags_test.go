package transfer_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

// TestHandler_ListTagsForResourceCreationTagsVisible verifies that tags specified at
// resource creation time are visible via ListTagsForResource. Real AWS returns creation-time
// tags from ListTagsForResource without a separate TagResource call.
func TestHandler_ListTagsForResourceCreationTagsVisible(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doTransferRequest(t, h, "CreateServer", map[string]any{
		"Tags": []map[string]any{
			{"Key": "Environment", "Value": "production"},
			{"Key": "Owner", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code, "CreateServer failed: %s", createRec.Body.String())

	var createOut struct {
		ServerID string `json:"ServerId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	serverARN := "arn:aws:transfer:us-east-1:123456789012:server/" + createOut.ServerID
	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": serverARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code, "ListTagsForResource failed: %s", listRec.Body.String())

	var listOut struct {
		Tags []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))

	tagMap := make(map[string]string, len(listOut.Tags))

	for _, tag := range listOut.Tags {
		tagMap[tag.Key] = tag.Value
	}

	assert.Equal(t, "production", tagMap["Environment"],
		"creation-time tags must be visible via ListTagsForResource")
	assert.Equal(t, "platform", tagMap["Owner"])
}

// TestHandler_ListTagsForResourceCreationTagsVisibleAcrossResources extends the
// Server-only coverage above to every other taggable Transfer resource type.
// Real AWS returns creation-time tags from ListTagsForResource for Agreement,
// Profile, User, WebApp, Certificate, and HostKey without a separate TagResource
// call, exactly as it does for Server.
func TestHandler_ListTagsForResourceCreationTagsVisibleAcrossResources(t *testing.T) {
	t.Parallel()

	creationTags := []map[string]any{{"Key": "Env", "Value": "test"}}

	tests := []struct {
		setup func(t *testing.T, h *transfer.Handler) string // returns resource ARN
		name  string
	}{
		{
			name: "Agreement",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "CreateAgreement", map[string]any{
					"ServerId": serverID,
					"Tags":     creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					AgreementID string `json:"AgreementId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:server/" + serverID + "/agreement/" + out.AgreementID
			},
		},
		{
			name: "Profile",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "CreateProfile", map[string]any{
					"ProfileType": "LOCAL",
					"Tags":        creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					ProfileID string `json:"ProfileId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:profile/" + out.ProfileID
			},
		},
		{
			name: "User",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "CreateUser", map[string]any{
					"ServerId":      serverID,
					"UserName":      "alice",
					"Role":          "arn:aws:iam::123456789012:role/test",
					"HomeDirectory": "/alice",
					"Tags":          creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return "arn:aws:transfer:us-east-1:123456789012:user/" + serverID + "/alice"
			},
		},
		{
			name: "WebApp",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "CreateWebApp", map[string]any{
					"Tags": creationTags,
					"IdentityProviderDetails": map[string]any{
						"IdentityCenterConfig": map[string]any{
							"InstanceArn": "arn:aws:sso:::instance/ssoins-tagtest",
							"Role":        "arn:aws:iam::123456789012:role/webapp-idp",
						},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					WebAppID string `json:"WebAppId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:webapp/" + out.WebAppID
			},
		},
		{
			name: "Certificate",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				rec := doTransferRequest(t, h, "ImportCertificate", map[string]any{
					"Usage": "SIGNING",
					"Tags":  creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					CertificateID string `json:"CertificateId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:certificate/" + out.CertificateID
			},
		},
		{
			name: "HostKey",
			setup: func(t *testing.T, h *transfer.Handler) string {
				t.Helper()

				serverID := mustCreateServer(t, h)

				rec := doTransferRequest(t, h, "ImportHostKey", map[string]any{
					"ServerId":    serverID,
					"HostKeyBody": testSSHHostKeyBody,
					"Tags":        creationTags,
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				var out struct {
					HostKeyID string `json:"HostKeyId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

				return "arn:aws:transfer:us-east-1:123456789012:host-key/" + serverID + "/" + out.HostKeyID
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(t, h)

			listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{"Arn": resourceARN})
			require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())

			var listOut struct {
				Tags []struct {
					Key   string `json:"Key"`
					Value string `json:"Value"`
				} `json:"Tags"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))

			tagMap := make(map[string]string, len(listOut.Tags))
			for _, tag := range listOut.Tags {
				tagMap[tag.Key] = tag.Value
			}

			assert.Equal(t, "test", tagMap["Env"],
				"creation-time tags must be visible via ListTagsForResource for "+tt.name)
		})
	}
}

// TestHandler_TagResourceListTagsRoundTripServerARN verifies TagResource and
// ListTagsForResource work with server ARNs.
func TestHandler_TagResourceListTagsRoundTripServerARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	// Build the server ARN as the AWS SDK would.
	arnStr := fmt.Sprintf("arn:aws:transfer:us-east-1:000000000000:server/%s", s.ServerID)

	tagRec := doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn": arnStr,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "infra"},
		},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": arnStr,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	tags := listResp["Tags"].([]any)
	tagMap := make(map[string]string)
	for _, t := range tags {
		entry := t.(map[string]any)
		tagMap[entry["Key"].(string)] = entry["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["Env"])
	assert.Equal(t, "infra", tagMap["Team"])
}

// TestHandler_UntagResourceRoundTrip verifies UntagResource removes specific tags.
func TestHandler_UntagResourceRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arnStr := fmt.Sprintf("arn:aws:transfer:us-east-1:000000000000:server/%s", s.ServerID)

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn": arnStr,
		"Tags": []map[string]any{
			{"Key": "Env", "Value": "prod"},
			{"Key": "Team", "Value": "infra"},
		},
	})

	untagRec := doTransferRequest(t, h, "UntagResource", map[string]any{
		"Arn":     arnStr,
		"TagKeys": []string{"Team"},
	})
	require.Equal(t, http.StatusOK, untagRec.Code)

	listRec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{"Arn": arnStr})
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

	tags := listResp["Tags"].([]any)
	tagMap := make(map[string]string)
	for _, tg := range tags {
		entry := tg.(map[string]any)
		tagMap[entry["Key"].(string)] = entry["Value"].(string)
	}

	assert.Equal(t, "prod", tagMap["Env"])
	assert.NotContains(t, tagMap, "Team")
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	rec := doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	rec := doTransferRequest(t, h, "ListTagsForResource", map[string]any{
		"Arn": arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Tags"])
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	s, err := h.Backend.CreateServer(nil, nil)
	require.NoError(t, err)

	arn := "arn:aws:transfer:us-east-1:123456789012:server/" + s.ServerID

	doTransferRequest(t, h, "TagResource", map[string]any{
		"Arn":  arn,
		"Tags": []map[string]any{{"Key": "Env", "Value": "test"}},
	})

	rec := doTransferRequest(t, h, "UntagResource", map[string]any{
		"Arn":     arn,
		"TagKeys": []string{"Env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
