package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CreateNetwork_FrameworkConfiguration verifies CreateNetwork's
// optional FrameworkConfiguration.Fabric.Edition wire shape: when supplied,
// it is validated and surfaced on GetNetwork's FrameworkAttributes.Fabric;
// when omitted, no Fabric attributes are invented, but
// VpcEndpointServiceName is still always populated (real AWS assigns every
// AVAILABLE network a VPC PrivateLink endpoint service name regardless of
// framework configuration).
func TestHandler_CreateNetwork_FrameworkConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		frameworkConfiguration map[string]any
		name                   string
		wantEdition            string
		wantCreateStatus       int
		wantFrameworkAttrsNil  bool
	}{
		{
			name: "edition supplied is surfaced on GetNetwork",
			frameworkConfiguration: map[string]any{
				"Fabric": map[string]any{"Edition": "STARTER"},
			},
			wantCreateStatus: http.StatusOK,
			wantEdition:      "STARTER",
		},
		{
			name:                  "omitted FrameworkConfiguration invents no Fabric attributes",
			wantCreateStatus:      http.StatusOK,
			wantFrameworkAttrsNil: true,
		},
		{
			name: "Fabric object present but Edition missing is rejected",
			frameworkConfiguration: map[string]any{
				"Fabric": map[string]any{},
			},
			wantCreateStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"Name":                "net-" + tt.name,
				"MemberConfiguration": testMemberConfiguration("m1"),
			}
			if tt.frameworkConfiguration != nil {
				body["FrameworkConfiguration"] = tt.frameworkConfiguration
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", body)
			require.Equal(t, tt.wantCreateStatus, rec.Code)

			if tt.wantCreateStatus != http.StatusOK {
				return
			}

			var createResp struct {
				NetworkID string `json:"NetworkId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			rec = doRequest(t, h, http.MethodGet, "/networks/"+createResp.NetworkID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp struct {
				Network struct {
					FrameworkAttributes *struct {
						Fabric *struct {
							Edition                 string `json:"Edition"`
							OrderingServiceEndpoint string `json:"OrderingServiceEndpoint"`
						} `json:"Fabric"`
					} `json:"FrameworkAttributes"`
					VpcEndpointServiceName string `json:"VpcEndpointServiceName"`
				} `json:"Network"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			assert.NotEmpty(t, getResp.Network.VpcEndpointServiceName,
				"VpcEndpointServiceName must be populated regardless of FrameworkConfiguration")

			if tt.wantFrameworkAttrsNil {
				assert.Nil(t, getResp.Network.FrameworkAttributes)

				return
			}

			require.NotNil(t, getResp.Network.FrameworkAttributes)
			require.NotNil(t, getResp.Network.FrameworkAttributes.Fabric)
			assert.Equal(t, tt.wantEdition, getResp.Network.FrameworkAttributes.Fabric.Edition)
			assert.NotEmpty(t, getResp.Network.FrameworkAttributes.Fabric.OrderingServiceEndpoint)
		})
	}
}

// TestHandler_CreateNetwork_UnsupportedFramework verifies real AWS's
// documented CreateNetwork restriction ("Applies only to Hyperledger
// Fabric") is enforced: a Framework other than HYPERLEDGER_FABRIC is
// rejected with InvalidRequestException, even though ETHEREUM remains a
// valid Framework enum member elsewhere (e.g. accessors).
func TestHandler_CreateNetwork_UnsupportedFramework(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		framework  string
		wantStatus int
	}{
		{name: "HYPERLEDGER_FABRIC accepted", framework: "HYPERLEDGER_FABRIC", wantStatus: http.StatusOK},
		{name: "empty framework defaults to Fabric", framework: "", wantStatus: http.StatusOK},
		{name: "ETHEREUM rejected", framework: "ETHEREUM", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"Name":                "net-" + tt.name,
				"MemberConfiguration": testMemberConfiguration("m1"),
			}
			if tt.framework != "" {
				body["Framework"] = tt.framework
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateMember_FrameworkConfigurationValidation locks in the
// server-side mirror of the real aws-sdk-go-v2 client-side validators
// (validateMemberConfiguration/validateMemberFrameworkConfiguration/
// validateMemberFabricConfiguration) for MemberConfiguration.
// FrameworkConfiguration -- required end to end, unlike a raw HTTP client
// that bypasses SDK-side validation and would otherwise sail through
// gopherstack with a member missing all Fabric identity.
func TestHandler_CreateMember_FrameworkConfigurationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		memberConfiguration map[string]any
		name                string
		wantStatus          int
	}{
		{
			name: "missing FrameworkConfiguration entirely",
			memberConfiguration: map[string]any{
				"Name": "m1",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "FrameworkConfiguration present but Fabric missing",
			memberConfiguration: map[string]any{
				"Name":                   "m1",
				"FrameworkConfiguration": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Fabric present but AdminUsername missing",
			memberConfiguration: map[string]any{
				"Name": "m1",
				"FrameworkConfiguration": map[string]any{
					"Fabric": map[string]any{"AdminPassword": "Passw0rd!"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "Fabric present but AdminPassword missing",
			memberConfiguration: map[string]any{
				"Name": "m1",
				"FrameworkConfiguration": map[string]any{
					"Fabric": map[string]any{"AdminUsername": "admin"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "AdminPassword too short",
			memberConfiguration: map[string]any{
				"Name": "m1",
				"FrameworkConfiguration": map[string]any{
					"Fabric": map[string]any{"AdminUsername": "admin", "AdminPassword": "short"},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "AdminPassword too long",
			memberConfiguration: map[string]any{
				"Name": "m1",
				"FrameworkConfiguration": map[string]any{
					"Fabric": map[string]any{
						"AdminUsername": "admin",
						"AdminPassword": "this-password-is-way-too-long-to-be-valid-1234",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:                "fully populated is accepted",
			memberConfiguration: testMemberConfiguration("m1"),
			wantStatus:          http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			networkID, _ := createTestNetwork(t, h)

			rec := doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/members",
				map[string]any{"MemberConfiguration": tt.memberConfiguration})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_CreateMember_FrameworkAttributesRoundTrip verifies GetMember
// surfaces FrameworkAttributes.Fabric.AdminUsername/CaEndpoint (synthesized
// from the CreateMember request, since gopherstack has no real Fabric CA to
// query) and KmsKeyArn (the real API's documented "AWS Owned KMS Key"
// sentinel by default, or the caller's own ARN when supplied).
func TestHandler_CreateMember_FrameworkAttributesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kmsKeyArn     string
		name          string
		wantKmsKeyArn string
	}{
		{
			name:          "default KmsKeyArn is the AWS-owned-key sentinel",
			wantKmsKeyArn: "AWS Owned KMS Key",
		},
		{
			name:          "custom KmsKeyArn is echoed verbatim",
			kmsKeyArn:     "arn:aws:kms:us-east-1:000000000000:key/test-key",
			wantKmsKeyArn: "arn:aws:kms:us-east-1:000000000000:key/test-key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			networkID, _ := createTestNetwork(t, h)

			memberConfig := testMemberConfiguration("fabric-member")
			if tt.kmsKeyArn != "" {
				memberConfig["KmsKeyArn"] = tt.kmsKeyArn
			}

			rec := doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/members",
				map[string]any{"MemberConfiguration": memberConfig})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp struct {
				MemberID string `json:"MemberId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID+"/members/"+createResp.MemberID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp struct {
				Member struct {
					KmsKeyArn           string `json:"KmsKeyArn"`
					FrameworkAttributes struct {
						Fabric struct {
							AdminUsername string `json:"AdminUsername"`
							CaEndpoint    string `json:"CaEndpoint"`
						} `json:"Fabric"`
					} `json:"FrameworkAttributes"`
				} `json:"Member"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			assert.Equal(t, "admin", getResp.Member.FrameworkAttributes.Fabric.AdminUsername)
			assert.NotEmpty(t, getResp.Member.FrameworkAttributes.Fabric.CaEndpoint)
			assert.Equal(t, tt.wantKmsKeyArn, getResp.Member.KmsKeyArn)
		})
	}
}

// TestHandler_CreateNode_FrameworkAttributesRoundTrip verifies GetNode
// surfaces FrameworkAttributes.Fabric.PeerEndpoint/PeerEventEndpoint
// (synthesized, since gopherstack has no real Fabric peer to query),
// StateDB (the real API's documented CouchDB default for Fabric 1.4+ when
// the caller omits it, or the caller's own value), and KmsKeyArn inherited
// from the owning member ("The node inherits this parameter from the
// member that it belongs to.").
func TestHandler_CreateNode_FrameworkAttributesRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stateDB     string
		name        string
		wantStateDB string
	}{
		{name: "default StateDB is CouchDB", wantStateDB: "CouchDB"},
		{name: "custom StateDB is echoed verbatim", stateDB: "LevelDB", wantStateDB: "LevelDB"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			networkID, memberID := createTestNetwork(t, h)

			nodeConfig := map[string]any{
				"InstanceType":     "bc.t3.small",
				"AvailabilityZone": "us-east-1a",
			}
			if tt.stateDB != "" {
				nodeConfig["StateDB"] = tt.stateDB
			}

			rec := doRequest(t, h, http.MethodPost, "/networks/"+networkID+"/nodes", map[string]any{
				"MemberId":          memberID,
				"NodeConfiguration": nodeConfig,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp struct {
				NodeID string `json:"NodeId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			rec = doRequest(t, h, http.MethodGet,
				"/networks/"+networkID+"/nodes/"+createResp.NodeID+"?memberId="+memberID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var getResp struct {
				Node struct {
					StateDB             string `json:"StateDB"`
					KmsKeyArn           string `json:"KmsKeyArn"`
					FrameworkAttributes struct {
						Fabric struct {
							PeerEndpoint      string `json:"PeerEndpoint"`
							PeerEventEndpoint string `json:"PeerEventEndpoint"`
						} `json:"Fabric"`
					} `json:"FrameworkAttributes"`
				} `json:"Node"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))

			assert.Equal(t, tt.wantStateDB, getResp.Node.StateDB)
			assert.Equal(t, "AWS Owned KMS Key", getResp.Node.KmsKeyArn,
				"node KmsKeyArn must be inherited from its owning member's default")
			assert.NotEmpty(t, getResp.Node.FrameworkAttributes.Fabric.PeerEndpoint)
			assert.NotEmpty(t, getResp.Node.FrameworkAttributes.Fabric.PeerEventEndpoint)
			assert.NotEqual(t,
				getResp.Node.FrameworkAttributes.Fabric.PeerEndpoint,
				getResp.Node.FrameworkAttributes.Fabric.PeerEventEndpoint,
				"peer and peer-event endpoints must be distinct")
		})
	}
}

// TestInMemoryBackend_CloneFrameworkAttributesDoesNotMutate verifies
// cloneNetwork/cloneMember/cloneNode deep-copy their new FrameworkAttributes
// pointer fields, matching the same shallow-copy-leak bug class the
// existing TestInMemoryBackend_CloneVotingPolicyDoesNotMutate test in
// networks_test.go guards against for VotingPolicy.
func TestInMemoryBackend_CloneFrameworkAttributesDoesNotMutate(t *testing.T) {
	t.Parallel()

	b := newBackend()

	network, member, err := b.CreateNetwork(
		testRegion, testAccountID, "clone-net", "", "", "", "clone-member", "",
		nil, nil, nil, "STARTER", "admin", "")
	require.NoError(t, err)

	node, err := b.CreateNode(testRegion, testAccountID, network.ID, member.ID, "bc.t3.small", "us-east-1a", "", nil)
	require.NoError(t, err)

	// Mutate the returned clones' nested pointers.
	network.FrameworkAttributes.Fabric.Edition = "MUTATED"
	member.FrameworkAttributes.Fabric.AdminUsername = "MUTATED"
	node.FrameworkAttributes.Fabric.PeerEndpoint = "MUTATED"

	// Re-fetch fresh clones and verify the backend's own state was untouched.
	gotNetwork, err := b.GetNetwork(network.ID)
	require.NoError(t, err)
	assert.Equal(t, "STARTER", gotNetwork.FrameworkAttributes.Fabric.Edition)

	gotMember, err := b.GetMember(network.ID, member.ID)
	require.NoError(t, err)
	assert.Equal(t, "admin", gotMember.FrameworkAttributes.Fabric.AdminUsername)

	gotNode, err := b.GetNode(network.ID, member.ID, node.ID)
	require.NoError(t, err)
	assert.NotEqual(t, "MUTATED", gotNode.FrameworkAttributes.Fabric.PeerEndpoint)
}
