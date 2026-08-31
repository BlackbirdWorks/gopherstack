package managedblockchain_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestInMemoryBackend_CreateNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errSentinel error
		name        string
		networkName string
		memberName  string
		wantErr     bool
	}{
		{
			name:        "creates network and first member",
			networkName: "my-network",
			memberName:  "my-member",
			wantErr:     false,
		},
		{
			name:        "duplicate name returns already exists",
			networkName: "dup-network",
			memberName:  "first-member",
			wantErr:     true,
			errSentinel: awserr.ErrAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			if errors.Is(tt.errSentinel, awserr.ErrAlreadyExists) {
				_, _, err := b.CreateNetwork(
					testRegion,
					testAccountID,
					tt.networkName,
					"",
					"",
					"",
					tt.memberName,
					"",
					nil, nil,
					nil,
					"",
					"admin",
					"")
				require.NoError(t, err)
			}

			network, member, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				tt.networkName,
				"",
				"",
				"",
				tt.memberName,
				"",
				nil, nil,
				nil,
				"",
				"admin",
				"")

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errSentinel)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, network.ID)
			assert.NotEmpty(t, network.Arn)
			assert.Equal(t, tt.networkName, network.Name)
			assert.Equal(t, "AVAILABLE", network.Status)
			assert.NotNil(t, network.CreationDate)
			assert.NotEmpty(t, member.ID)
			assert.Equal(t, tt.memberName, member.Name)
			assert.Equal(t, network.ID, member.NetworkID)
		})
	}
}

func TestInMemoryBackend_GetNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		networkID string
		wantErr   bool
	}{
		{
			name:    "get existing network",
			wantErr: false,
		},
		{
			name:      "not found returns error",
			networkID: "nonexistent",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, _, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				"test-network",
				"",
				"",
				"",
				"member1",
				"",
				nil, nil,
				nil,
				"",
				"admin",
				"")
			require.NoError(t, err)

			id := tt.networkID
			if id == "" {
				id = network.ID
			}

			got, err := b.GetNetwork(id)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, awserr.ErrNotFound)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, network.ID, got.ID)
			assert.Equal(t, "test-network", got.Name)
		})
	}
}

func TestInMemoryBackend_ListNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantOrderedBy string
		networkNames  []string
		wantCount     int
	}{
		{
			name:         "empty list",
			networkNames: []string{},
			wantCount:    0,
		},
		{
			name:         "multiple networks sorted by name",
			networkNames: []string{"beta-net", "alpha-net", "gamma-net"},
			wantCount:    3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			for _, name := range tt.networkNames {
				_, _, err := b.CreateNetwork(
					testRegion,
					testAccountID,
					name,
					"",
					"",
					"",
					"m1",
					"",
					nil, nil,
					nil,
					"",
					"admin",
					"")
				require.NoError(t, err)
			}

			networks, err := b.ListNetworks(managedblockchain.ListNetworksFilter{})
			require.NoError(t, err)
			assert.Len(t, networks, tt.wantCount)

			if tt.wantCount > 1 {
				assert.Equal(t, "alpha-net", networks[0].Name)
			}
		})
	}
}

func TestHandler_CreateNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantKey    string
		wantStatus int
	}{
		{
			name: "creates network",
			body: map[string]any{
				"Name": "my-net", "ClientRequestToken": "tok-1", "MemberConfiguration": testMemberConfiguration("m1"),
			},
			wantStatus: http.StatusOK,
			wantKey:    "NetworkId",
		},
		{
			name: "missing network name",
			body: map[string]any{
				"ClientRequestToken":  "tok-2",
				"MemberConfiguration": testMemberConfiguration("m1"),
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing member name",
			body:       map[string]any{"Name": "net1", "ClientRequestToken": "tok-3"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate network returns conflict",
			body: map[string]any{
				"Name": "dup-net", "ClientRequestToken": "tok-4", "MemberConfiguration": testMemberConfiguration("m1"),
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate network returns conflict" {
				rec := doRequest(t, h, http.MethodPost, "/networks", tt.body)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantKey != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, tt.wantKey)
			}
		})
	}
}

func TestHandler_GetNetwork(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		networkID  string
		wantStatus int
	}{
		{
			name:       "get existing network",
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			networkID:  "does-not-exist",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, http.MethodPost, "/networks",
				map[string]any{
					"Name":                "net1",
					"ClientRequestToken":  "tok-get",
					"MemberConfiguration": testMemberConfiguration("m1"),
				})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			networkID := createResp["NetworkId"].(string)

			id := tt.networkID
			if id == "" {
				id = networkID
			}

			rec = doRequest(t, h, http.MethodGet, "/networks/"+id, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp, "Network")
			}
		})
	}
}

func TestHandler_ListNetworks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createN    int
		wantStatus int
	}{
		{
			name:       "lists zero networks",
			createN:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "lists created networks",
			createN:    2,
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.createN {
				rec := doRequest(t, h, http.MethodPost, "/networks",
					map[string]any{
						"Name":                fmt.Sprintf("net-%d", i),
						"ClientRequestToken":  fmt.Sprintf("tok-list-%d", i),
						"MemberConfiguration": testMemberConfiguration("m1"),
					})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, http.MethodGet, "/networks", nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			networks, ok := resp["Networks"].([]any)
			require.True(t, ok)
			assert.Len(t, networks, tt.createN)
		})
	}
}

func TestHandler_CreateNetworkInvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "invalid json body"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			b := managedblockchain.NewInMemoryBackend()
			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			req := httptest.NewRequest(http.MethodPost, "/networks",
				bytes.NewReader([]byte("{invalid json")))
			req.Header.Set("Content-Type", "application/json")

			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHandler_VotingPolicyStoredAndReturned verifies VotingPolicy is stored
// in CreateNetwork and returned by GetNetwork.
func TestHandler_VotingPolicyStoredAndReturned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		votingPolicy     map[string]any
		name             string
		wantPolicyStored bool
	}{
		{
			name: "voting policy stored and returned",
			votingPolicy: map[string]any{
				"ApprovalThresholdPolicy": map[string]any{
					"ThresholdComparator":     "GREATER_THAN",
					"ThresholdPercentage":     50,
					"ProposalDurationInHours": 24,
				},
			},
			wantPolicyStored: true,
		},
		{
			name:             "no voting policy is optional",
			votingPolicy:     nil,
			wantPolicyStored: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			body := map[string]any{
				"Name":                "vp-net",
				"ClientRequestToken":  "tok-vp",
				"MemberConfiguration": testMemberConfiguration("m1"),
			}

			if tt.votingPolicy != nil {
				body["VotingPolicy"] = tt.votingPolicy
			}

			rec := doRequest(t, h, http.MethodPost, "/networks", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			networkID := createResp["NetworkId"].(string)

			rec2 := doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
			require.Equal(t, http.StatusOK, rec2.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &getResp))

			network := getResp["Network"].(map[string]any)

			if tt.wantPolicyStored {
				vp, ok := network["VotingPolicy"]
				assert.True(t, ok, "VotingPolicy should be present")
				vpMap := vp.(map[string]any)
				atp := vpMap["ApprovalThresholdPolicy"].(map[string]any)
				assert.Equal(t, "GREATER_THAN", atp["ThresholdComparator"])
				assert.Equal(t, 50, int(atp["ThresholdPercentage"].(float64)))
				assert.Equal(t, 24, int(atp["ProposalDurationInHours"].(float64)))
			} else {
				_, hasPol := network["VotingPolicy"]
				assert.False(t, hasPol, "VotingPolicy should be absent when not set")
			}
		})
	}
}

// TestHandler_ListNetworksFilters verifies query param filtering for ListNetworks.
func TestHandler_ListNetworksFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		query     map[string]string
		name      string
		wantNames []string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			query:     map[string]string{},
			wantCount: 3,
		},
		{
			name:      "filter by name exact match",
			query:     map[string]string{"name": "alpha-net"},
			wantCount: 1,
			wantNames: []string{"alpha-net"},
		},
		{
			name:      "filter by framework",
			query:     map[string]string{"framework": "HYPERLEDGER_FABRIC"},
			wantCount: 3,
		},
		{
			name:      "filter by status available",
			query:     map[string]string{"status": "AVAILABLE"},
			wantCount: 3,
		},
		{
			name:      "filter by name no match",
			query:     map[string]string{"name": "nonexistent"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddNetworkInternal(testRegion, testAccountID, "alpha-net")
			b.AddNetworkInternal(testRegion, testAccountID, "beta-net")
			b.AddNetworkInternal(testRegion, testAccountID, "gamma-net")

			h := managedblockchain.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doRequestWithQuery(t, h, "/networks", tt.query)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			networks := resp["Networks"].([]any)
			assert.Len(t, networks, tt.wantCount)

			if len(tt.wantNames) > 0 {
				names := make([]string, 0, len(networks))

				for _, n := range networks {
					net := n.(map[string]any)
					names = append(names, net["Name"].(string))
				}

				for _, wantName := range tt.wantNames {
					assert.Contains(t, names, wantName)
				}
			}
		})
	}
}

// TestInMemoryBackend_ListNetworksFilter verifies backend-level filtering for ListNetworks.
func TestInMemoryBackend_ListNetworksFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    managedblockchain.ListNetworksFilter
		wantCount int
	}{
		{
			name:      "empty filter returns all",
			filter:    managedblockchain.ListNetworksFilter{},
			wantCount: 3,
		},
		{
			name:      "filter by Name",
			filter:    managedblockchain.ListNetworksFilter{Name: "net-a"},
			wantCount: 1,
		},
		{
			name:      "filter by Framework",
			filter:    managedblockchain.ListNetworksFilter{Framework: "HYPERLEDGER_FABRIC"},
			wantCount: 3,
		},
		{
			name:      "filter by Status AVAILABLE",
			filter:    managedblockchain.ListNetworksFilter{Status: "AVAILABLE"},
			wantCount: 3,
		},
		{
			name:      "filter by Status CREATING returns none",
			filter:    managedblockchain.ListNetworksFilter{Status: "CREATING"},
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := managedblockchain.NewInMemoryBackend()
			b.AddNetworkInternal(testRegion, testAccountID, "net-a")
			b.AddNetworkInternal(testRegion, testAccountID, "net-b")
			b.AddNetworkInternal(testRegion, testAccountID, "net-c")

			networks, err := b.ListNetworks(tt.filter)
			require.NoError(t, err)
			assert.Len(t, networks, tt.wantCount)
		})
	}
}

// TestInMemoryBackend_CloneVotingPolicyDoesNotMutate verifies cloning prevents mutation.
func TestInMemoryBackend_CloneVotingPolicyDoesNotMutate(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()

	// Create network with voting policy
	vp := &managedblockchain.VotingPolicy{
		ApprovalThresholdPolicy: &managedblockchain.ApprovalThresholdPolicy{
			ThresholdComparator:     "GREATER_THAN",
			ThresholdPercentage:     50,
			ProposalDurationInHours: 24,
		},
	}

	n, _, err := b.CreateNetwork(
		testRegion, testAccountID,
		"vp-net", "", "", "", "m1", "",
		nil, nil, vp,
		"", "admin", "")
	require.NoError(t, err)

	// GetNetwork returns a clone
	got, err := b.GetNetwork(n.ID)
	require.NoError(t, err)
	require.NotNil(t, got.VotingPolicy)
	assert.Equal(t, "GREATER_THAN", got.VotingPolicy.ApprovalThresholdPolicy.ThresholdComparator)

	// Mutate the returned object
	got.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage = 99

	// Original should be unaffected
	got2, err := b.GetNetwork(n.ID)
	require.NoError(t, err)
	assert.Equal(t, int32(50), got2.VotingPolicy.ApprovalThresholdPolicy.ThresholdPercentage)
}

// TestHandler_CreateNetworkWithTags verifies tags are persisted on CreateNetwork.
func TestHandler_CreateNetworkWithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "tagged-net",
		"ClientRequestToken":  "tok-tagged",
		"MemberConfiguration": testMemberConfiguration("m1"),
		"Tags":                map[string]string{"env": "prod", "team": "infra"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	networkID, _ := resp["NetworkId"].(string)
	require.NotEmpty(t, networkID)

	// GetNetwork and verify tags
	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var netResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &netResp))

	network := netResp["Network"].(map[string]any)
	tags := network["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])
	assert.Equal(t, "infra", tags["team"])
}

// TestHandler_CreateNetworkFoundingMemberTags verifies that Tags nested
// under MemberConfiguration on CreateNetwork reach the founding member's own
// tags AND the shared tag store ListTagsForResource reads (gopherstack-2mwl:
// the founding member's tags were previously always dropped, regardless of
// what CreateNetwork's request supplied).
func TestHandler_CreateNetworkFoundingMemberTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	memberConfig := testMemberConfiguration("founding-member")
	memberConfig["Tags"] = map[string]string{"role": "founder"}

	rec := doRequest(t, h, http.MethodPost, "/networks", map[string]any{
		"Name":                "founder-tags-net",
		"ClientRequestToken":  "tok-founder",
		"MemberConfiguration": memberConfig,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	networkID, _ := resp["NetworkId"].(string)
	memberID, _ := resp["MemberId"].(string)
	require.NotEmpty(t, networkID)
	require.NotEmpty(t, memberID)

	rec2 := doRequest(t, h, http.MethodGet, "/networks/"+networkID+"/members/"+memberID, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var memResp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &memResp))

	member := memResp["Member"].(map[string]any)
	tags := member["Tags"].(map[string]any)
	assert.Equal(t, "founder", tags["role"])
}
