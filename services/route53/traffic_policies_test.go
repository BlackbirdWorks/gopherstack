package route53_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/route53"
)

// TestRoute53_TrafficPolicyGetDelete covers GetTrafficPolicy and
// DeleteTrafficPolicy.
func TestRoute53_TrafficPolicyGetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *route53.Handler) (id string, version int)
		name         string
		method       string
		wantContains []string
		wantCode     int
	}{
		{
			name: "get_traffic_policy_success",
			setup: func(t *testing.T, h *route53.Handler) (string, int) {
				t.Helper()

				return createTPForOpsTest(t, h, "my-policy"), 1
			},
			method:       http.MethodGet,
			wantCode:     http.StatusOK,
			wantContains: []string{"TrafficPolicy", "my-policy"},
		},
		{
			name: "get_traffic_policy_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, int) {
				t.Helper()

				return "nonexistent-id", 1
			},
			method:   http.MethodGet,
			wantCode: http.StatusNotFound,
		},
		{
			name: "delete_traffic_policy_success",
			setup: func(t *testing.T, h *route53.Handler) (string, int) {
				t.Helper()

				return createTPForOpsTest(t, h, "del-policy"), 1
			},
			method:       http.MethodDelete,
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteTrafficPolicyResponse"},
		},
		{
			name: "delete_traffic_policy_not_found",
			setup: func(t *testing.T, _ *route53.Handler) (string, int) {
				t.Helper()

				return "nonexistent-id", 1
			},
			method:   http.MethodDelete,
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			id, version := tt.setup(t, h)
			path := fmt.Sprintf("/2013-04-01/trafficpolicy/%s/%d", id, version)
			rec := send(t, h, tt.method, path, "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListTrafficPolicies covers ListTrafficPolicies.
func TestRoute53_ListTrafficPolicies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains []string
		setupCount   int
		wantCode     int
	}{
		{
			name:         "list_empty",
			setupCount:   0,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPoliciesResponse"},
		},
		{
			name:         "list_with_policies",
			setupCount:   2,
			wantCode:     http.StatusOK,
			wantContains: []string{"ListTrafficPoliciesResponse", "policy-0", "policy-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			for i := range tt.setupCount {
				createTPForOpsTest(t, h, fmt.Sprintf("policy-%d", i))
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicies", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRoute53_ListTrafficPolicyVersions covers ListTrafficPolicyVersions.
func TestRoute53_ListTrafficPolicyVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		useID         string
		wantContains  []string
		setupVersions int
		wantCode      int
	}{
		{
			name:          "list_versions_success",
			setupVersions: 2,
			wantCode:      http.StatusOK,
			wantContains:  []string{"ListTrafficPolicyVersionsResponse", "TrafficPolicy"},
		},
		{
			name:     "list_versions_not_found",
			useID:    "nonexistent-id",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			id := tt.useID
			if id == "" {
				id = createTPForOpsTest(t, h, "versioned-policy")
				// Create additional versions
				for range tt.setupVersions - 1 {
					body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
  <Comment>v2</Comment>
</CreateTrafficPolicyVersionRequest>`
					rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+id, body)
					require.Equal(t, http.StatusCreated, rec.Code)
				}
			}

			rec := send(t, h, http.MethodGet, "/2013-04-01/trafficpolicies/"+id+"/versions", "")

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestExtractOperation_CreateTrafficPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_traffic_policy",
			path:   "/2013-04-01/trafficpolicy",
			method: http.MethodPost,
			wantOp: "CreateTrafficPolicy",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestExtractOperation_CreateTrafficPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		method string
		wantOp string
	}{
		{
			name:   "create_traffic_policy_version",
			path:   "/2013-04-01/trafficpolicy/some-policy-id",
			method: http.MethodPost,
			wantOp: "CreateTrafficPolicyVersion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			op := extractOpFromPath(t, h, tt.method, tt.path)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestTrafficPolicyVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		extraVersions   int
		wantPolicyCount int
	}{
		{
			name:            "single_version",
			extraVersions:   0,
			wantPolicyCount: 1,
		},
		{
			name:            "two_versions_same_policy_id",
			extraVersions:   1,
			wantPolicyCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)

			body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
  <Comment>test policy</Comment>
</CreateTrafficPolicyRequest>`
			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			tpID := extractTrafficPolicyID(t, rec.Body.String())

			for range tt.extraVersions {
				vBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
  <Comment>version 2</Comment>
</CreateTrafficPolicyVersionRequest>`
				vRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+tpID, vBody)
				require.Equal(t, http.StatusCreated, vRec.Code)
			}

			// All versions are under the same policy ID.
			assert.Equal(t, tt.wantPolicyCount, route53.TrafficPolicyCount(h.Backend.(*route53.InMemoryBackend)))
		})
	}
}

func TestCreateTrafficPolicy_NameUniqueness(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>unique-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
</CreateTrafficPolicyRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Second attempt with same name must fail. Real Route 53 returns
	// TrafficPolicyAlreadyExists with httpStatusCode 409 (confirmed against
	// the botocore api-2.json route53 model), not 400.
	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
	assert.Contains(t, rec.Body.String(), "TrafficPolicyAlreadyExists")
}

func TestDeleteTrafficPolicy_InUse(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create zone, traffic policy, and instance.
	rec := send(t, h, http.MethodPost, "/2013-04-01/hostedzone", createZoneXML)
	require.Equal(t, http.StatusCreated, rec.Code)
	zoneID := extractZoneID(t, rec.Body.String())

	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>inuse-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
</CreateTrafficPolicyRequest>`

	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	type tpResp struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	var tpr tpResp
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &tpr))
	tpID := tpr.TrafficPolicy.ID

	instBody := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyInstanceRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <HostedZoneId>%s</HostedZoneId>
  <Name>www.example.com</Name>
  <TrafficPolicyId>%s</TrafficPolicyId>
  <TrafficPolicyVersion>1</TrafficPolicyVersion>
  <TTL>60</TTL>
</CreateTrafficPolicyInstanceRequest>`, zoneID, tpID)

	rec = send(t, h, http.MethodPost, "/2013-04-01/trafficpolicyinstance", instBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Now attempt to delete the policy version that is in use.
	rec = send(t, h, http.MethodDelete,
		fmt.Sprintf("/2013-04-01/trafficpolicy/%s/1", tpID), "")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TrafficPolicyInUse")
}

func TestUpdateTrafficPolicyComment(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>comment-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
  <Comment>original comment</Comment>
</CreateTrafficPolicyRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tpr struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &tpr))
	tpID := tpr.TrafficPolicy.ID

	// Update the comment.
	updateBody := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateTrafficPolicyCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Comment>updated comment</Comment>
</UpdateTrafficPolicyCommentRequest>`

	rec = send(t, h, http.MethodPost,
		fmt.Sprintf("/2013-04-01/trafficpolicy/%s/1", tpID), updateBody)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "UpdateTrafficPolicyCommentResponse")
	assert.Contains(t, rec.Body.String(), "updated comment")

	// Verify persisted by getting the policy.
	rec = send(t, h, http.MethodGet,
		fmt.Sprintf("/2013-04-01/trafficpolicy/%s/1", tpID), "")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "updated comment")
}

func TestUpdateTrafficPolicyComment_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	body := `<?xml version="1.0" encoding="UTF-8"?>
<UpdateTrafficPolicyCommentRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Comment>nope</Comment>
</UpdateTrafficPolicyCommentRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/NONEXISTENT/1", body)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListTrafficPolicies_VersionCount(t *testing.T) {
	t.Parallel()

	h := newHandler(t)

	// Create a policy and add two more versions.
	tpBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>versioned-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
</CreateTrafficPolicyRequest>`

	rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tpBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var tpr struct {
		TrafficPolicy struct {
			ID string `xml:"Id"`
		} `xml:"TrafficPolicy"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &tpr))
	tpID := tpr.TrafficPolicy.ID

	// Create version 2.
	v2Body := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","RecordType":"A","Endpoints":{},"Rules":{}}</Document>
</CreateTrafficPolicyVersionRequest>`

	rec = send(t, h, http.MethodPost,
		fmt.Sprintf("/2013-04-01/trafficpolicy/%s", tpID), v2Body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// Create version 3.
	rec = send(t, h, http.MethodPost,
		fmt.Sprintf("/2013-04-01/trafficpolicy/%s", tpID), v2Body)
	require.Equal(t, http.StatusCreated, rec.Code)

	// ListTrafficPolicies should show LatestVersion=3 and TrafficPolicyCount=3.
	rec = send(t, h, http.MethodGet, "/2013-04-01/trafficpolicies", "")
	require.Equal(t, http.StatusOK, rec.Code)
	body2 := rec.Body.String()

	assert.Contains(t, body2, "<LatestVersion>3</LatestVersion>")
	assert.Contains(t, body2, "<TrafficPolicyCount>3</TrafficPolicyCount>")
}

func TestRoute53_CreateTrafficPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_tp_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
  <Comment>test policy</Comment>
</CreateTrafficPolicyRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateTrafficPolicyResponse", "my-policy", "test policy"},
		},
		{
			name: "create_tp_missing_name",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name: "create_tp_missing_document",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>my-policy</Name>
</CreateTrafficPolicyRequest>`,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "create_tp_invalid_xml",
			body:     "not-xml",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestRoute53_CreateTrafficPolicyVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		policyID     string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "create_version_success",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"AWSPolicyFormatVersion":"2015-10-01","v2":true}</Document>
  <Comment>version 2</Comment>
</CreateTrafficPolicyVersionRequest>`,
			wantCode:     http.StatusCreated,
			wantContains: []string{"CreateTrafficPolicyVersionResponse", "version 2"},
		},
		{
			name:     "create_version_not_found",
			policyID: "NONEXISTENT-POLICY-ID",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Document>{"v":"2"}</Document>
</CreateTrafficPolicyVersionRequest>`,
			wantCode: http.StatusNotFound,
		},
		{
			name: "create_version_missing_document",
			body: `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyVersionRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
</CreateTrafficPolicyVersionRequest>`,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler(t)
			policyID := tt.policyID

			if policyID == "" && tt.wantCode != http.StatusNotFound {
				// Create initial policy.
				createBody := `<?xml version="1.0" encoding="UTF-8"?>
<CreateTrafficPolicyRequest xmlns="https://route53.amazonaws.com/doc/2013-04-01/">
  <Name>versioned-policy</Name>
  <Document>{"AWSPolicyFormatVersion":"2015-10-01"}</Document>
</CreateTrafficPolicyRequest>`
				createRec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy", createBody)
				require.Equal(t, http.StatusCreated, createRec.Code)
				policyID = extractTrafficPolicyID(t, createRec.Body.String())
			} else if policyID == "" {
				policyID = "NONEXISTENT-POLICY-ID"
			}

			rec := send(t, h, http.MethodPost, "/2013-04-01/trafficpolicy/"+policyID, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}
