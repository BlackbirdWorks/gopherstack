package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func networkBody(mutate func(body, member, fabric map[string]any)) map[string]any {
	fabric := map[string]any{"AdminUsername": "admin", "AdminPassword": "Passw0rdAbc"}
	member := map[string]any{
		"Name":                   "mem",
		"FrameworkConfiguration": map[string]any{"Fabric": fabric},
	}
	body := map[string]any{
		"ClientRequestToken":  "tok",
		"Name":                "net",
		"Framework":           "HYPERLEDGER_FABRIC",
		"FrameworkVersion":    "2.2",
		"MemberConfiguration": member,
	}

	if mutate != nil {
		mutate(body, member, fabric)
	}

	return body
}

func TestCreateNetwork_InputValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(body, member, fabric map[string]any)
		name    string
		wantErr bool
	}{
		{name: "valid"},
		{name: "member name digit first", wantErr: true, mutate: func(_, m, _ map[string]any) { m["Name"] = "9mem" }},
		{name: "member name double hyphen", wantErr: true, mutate: func(_, m, _ map[string]any) { m["Name"] = "a--b" }},
		{
			name:    "member name trailing hyphen",
			wantErr: true,
			mutate:  func(_, m, _ map[string]any) { m["Name"] = "ab-" },
		},
		{name: "member name too long", wantErr: true, mutate: func(_, m, _ map[string]any) {
			m["Name"] = strings.Repeat("a", 65)
		}},
		{
			name:    "username underscore",
			wantErr: true,
			mutate:  func(_, _, f map[string]any) { f["AdminUsername"] = "bad_user" },
		},
		{name: "username too long", wantErr: true, mutate: func(_, _, f map[string]any) {
			f["AdminUsername"] = strings.Repeat("a", 17)
		}},
		{
			name:    "password no digit",
			wantErr: true,
			mutate:  func(_, _, f map[string]any) { f["AdminPassword"] = "Passwordabc" },
		},
		{
			name:    "password no upper",
			wantErr: true,
			mutate:  func(_, _, f map[string]any) { f["AdminPassword"] = "passw0rdabc" },
		},
		{
			name:    "password forbidden char",
			wantErr: true,
			mutate:  func(_, _, f map[string]any) { f["AdminPassword"] = "Passw0rd@bc" },
		},
		{name: "blank network name", wantErr: true, mutate: func(b, _, _ map[string]any) { b["Name"] = "   " }},
		{name: "description too long", wantErr: true, mutate: func(b, _, _ map[string]any) {
			b["Description"] = strings.Repeat("d", 129)
		}},
		{name: "duration too long", wantErr: true, mutate: func(b, _, _ map[string]any) {
			b["VotingPolicy"] = map[string]any{"ApprovalThresholdPolicy": map[string]any{
				"ThresholdPercentage": 50, "ProposalDurationInHours": 169, "ThresholdComparator": "GREATER_THAN",
			}}
		}},
		{name: "percentage too high", wantErr: true, mutate: func(b, _, _ map[string]any) {
			b["VotingPolicy"] = map[string]any{"ApprovalThresholdPolicy": map[string]any{
				"ThresholdPercentage": 101, "ProposalDurationInHours": 24, "ThresholdComparator": "GREATER_THAN",
			}}
		}},
		{name: "bad comparator", wantErr: true, mutate: func(b, _, _ map[string]any) {
			b["VotingPolicy"] = map[string]any{"ApprovalThresholdPolicy": map[string]any{
				"ThresholdPercentage": 50, "ProposalDurationInHours": 24, "ThresholdComparator": "EQUAL",
			}}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, newTestHandler(t), http.MethodPost, "/networks", networkBody(tt.mutate))
			if !tt.wantErr {
				assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				return
			}

			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

			var resp map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, "InvalidRequestException", resp["Code"])
			assert.NotContains(t, resp["message"], "InvalidRequestException")
		})
	}
}

func TestCreateNetwork_RealisticIDs(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), http.MethodPost, "/networks", networkBody(nil))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Regexp(t, `^n-[A-Z2-7]{26}$`, resp["NetworkId"])
	assert.Regexp(t, `^m-[A-Z2-7]{26}$`, resp["MemberId"])
}

func TestErrorMessages_NoCodePrefix(t *testing.T) {
	t.Parallel()

	rec := doRequest(t, newTestHandler(t), http.MethodGet, "/networks/n-MISSING", nil)
	require.Equal(t, http.StatusNotFound, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp["Code"])
	assert.NotContains(t, resp["message"], "ResourceNotFoundException")
}
