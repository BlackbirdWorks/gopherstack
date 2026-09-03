package shield_test

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

func TestHandler_CreateProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler)
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) {
				_ = h.Backend.CreateSubscription()
			},
			body: map[string]any{
				"Name":        "my-protection",
				"ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-12345678",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "duplicate name returns conflict",
			setup: func(h *shield.Handler) {
				_ = h.Backend.CreateSubscription()
				_, _ = h.Backend.CreateProtection("my-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
			},
			body: map[string]any{
				"Name":        "my-protection",
				"ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-99",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing name returns error",
			body: map[string]any{
				"ResourceArn": "arn:aws:ec2:us-east-1:123456789012:eip-allocation/eipalloc-12345678",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resource arn returns error",
			body: map[string]any{
				"Name": "my-protection",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doShieldRequest(t, h, "CreateProtection", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.NotEmpty(t, result["ProtectionId"])
			}
		})
	}
}

func TestHandler_DescribeProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "by protection id",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				p, _ := h.Backend.CreateProtection("my-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionId": id}
			},
			wantStatus: http.StatusOK,
			wantName:   "my-protection",
		},
		{
			name: "by resource arn",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				_, _ = h.Backend.CreateProtection("arn-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2", nil)

				return "arn:aws:ec2:us-east-1:123:eip/eipalloc-2"
			},
			body: func(id string) map[string]any {
				return map[string]any{"ResourceArn": id}
			},
			wantStatus: http.StatusOK,
			wantName:   "arn-protection",
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"ProtectionId": "nonexistent"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing id and arn returns error",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DescribeProtection", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				prot, ok := result["Protection"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, prot["Name"])
			}
		})
	}
}

func TestHandler_DeleteProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*shield.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *shield.Handler) string {
				_ = h.Backend.CreateSubscription()
				p, _ := h.Backend.CreateProtection("my-protection", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)

				return p.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"ProtectionId": id}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"ProtectionId": "nonexistent"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing id returns error",
			setup: func(_ *shield.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doShieldRequest(t, h, "DeleteProtection", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*shield.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty list",
			setup:     func(_ *shield.Handler) {},
			wantCount: 0,
		},
		{
			name: "two protections",
			setup: func(h *shield.Handler) {
				_ = h.Backend.CreateSubscription()
				_, _ = h.Backend.CreateProtection("p1", "arn:aws:ec2:us-east-1:123:eip/eipalloc-1", nil)
				_, _ = h.Backend.CreateProtection("p2", "arn:aws:ec2:us-east-1:123:eip/eipalloc-2", nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doShieldRequest(t, h, "ListProtections", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			protections, ok := result["Protections"].([]any)
			require.True(t, ok)
			assert.Len(t, protections, tt.wantCount)
		})
	}
}

// TestParity_OpaquePageToken_ListProtections verifies pagination emits opaque JSON-wrapped tokens.
func TestHandler_ListProtectionsOpaquePageToken(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	for i := range 5 {
		b.AddProtectionInternal(
			"prot-"+string(rune('a'+i)),
			"arn:aws:ec2:us-east-1:123456789012:eip/eipalloc-0"+string(rune('a'+i)),
		)
	}

	rec := doShieldRequest(t, h, "ListProtections", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		NextToken string `json:"NextToken"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.NextToken)

	offset := opaqueOffset(t, out.NextToken)
	assert.Equal(t, 3, offset)

	// Old-style plain-integer token must be rejected on next page request.
	badToken := base64.StdEncoding.EncodeToString([]byte("3"))
	rec2 := doShieldRequest(t, h, "ListProtections", map[string]any{"NextToken": badToken})
	assert.NotEqual(t, http.StatusOK, rec2.Code)
}

// TestParity_ApplyProtectionFilters_NoBacking verifies filtered list doesn't corrupt the original.
func TestHandler_ApplyProtectionFiltersNoBacking(t *testing.T) {
	t.Parallel()

	h, b := newSubscribedHandler(t)

	b.AddProtectionInternal("prot-cf", "arn:aws:cloudfront::123456789012:distribution/dist1")
	b.AddProtectionInternal("prot-alb", "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/alb1/abc")
	b.AddProtectionInternal("prot-r53", "arn:aws:route53:::hostedzone/Z1")

	// Filter to CLOUDFRONT_DISTRIBUTION only.
	rec := doShieldRequest(t, h, "ListProtections", map[string]any{
		"InclusionFilters": map[string]any{
			"ResourceTypes": []string{"CLOUDFRONT_DISTRIBUTION"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var filtered struct {
		Protections []map[string]any `json:"Protections"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &filtered))
	assert.Len(t, filtered.Protections, 1)

	// Unfiltered list must still return all three.
	rec2 := doShieldRequest(t, h, "ListProtections", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var all struct {
		Protections []map[string]any `json:"Protections"`
	}

	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &all))
	assert.Len(t, all.Protections, 3)
}

// TestAudit_Gap4_ProtectionResponseIncludesALAR verifies ALAR config in protection response.
func TestHandler_ProtectionResponseIncludesALAR(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)
	require.NoError(t, b.EnableApplicationLayerAutomaticResponse(eipARN("1"), "BLOCK"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeProtection", map[string]any{
		"ProtectionId": p.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prot := resp["Protection"].(map[string]any)
	alarRaw, ok := prot["ApplicationLayerAutomaticResponseConfiguration"]
	assert.True(t, ok, "Protection response must include ALAR config")
	alarcfg := alarRaw.(map[string]any)
	assert.Equal(t, "ENABLED", alarcfg["Status"])
	action := alarcfg["Action"].(map[string]any)
	assert.NotNil(t, action["Block"])
}

// TestAudit_Gap4_ProtectionWithoutALARHasNoConfig verifies omitted ALAR when not set.
func TestHandler_ProtectionWithoutALARHasNoConfig(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("2"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeProtection", map[string]any{
		"ProtectionId": p.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prot := resp["Protection"].(map[string]any)
	_, hasALAR := prot["ApplicationLayerAutomaticResponseConfiguration"]
	assert.False(t, hasALAR, "Protection without ALAR should not include the field")
}

// --- Gap 5: Attack model includes rich fields ---

// TestAudit_Gap7_ListProtectionsPagination verifies pagination with MaxResults.
func TestHandler_ListProtectionsPagination(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	for i := range 5 {
		_, err := b.CreateProtection(
			"prot-"+string(rune('a'+i)),
			eipARN(string(rune('0'+i))),
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtections", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prots := resp["Protections"].([]any)
	assert.Len(t, prots, 2)

	nextToken, hasNext := resp["NextToken"]
	assert.True(t, hasNext, "NextToken should be present when more results exist")
	assert.NotEmpty(t, nextToken)
}

// TestHandler_ListProtectionsDefaultMaxResults verifies that omitting
// MaxResults pages at the documented default of 20
// (api_op_ListProtections.go: "The default setting is 20."), not at the
// handler's internal cap.
func TestHandler_ListProtectionsDefaultMaxResults(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	const numProtections = 25
	for i := range numProtections {
		_, err := b.CreateProtection(
			fmt.Sprintf("prot-%02d", i),
			eipARN(fmt.Sprintf("%02d", i)),
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prots := resp["Protections"].([]any)
	assert.Len(t, prots, 20, "omitted MaxResults must default to 20 per the documented default")
	assert.NotEmpty(t, resp["NextToken"], "25 protections at a default page size of 20 must continue")
}

// TestAudit_Gap7_ListProtectionsNextPage verifies continuation token retrieves next page.
func TestHandler_ListProtectionsNextPage(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	for i := range 4 {
		_, err := b.CreateProtection(
			"prot-"+string(rune('a'+i)),
			eipARN(string(rune('0'+i))),
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)

	// First page.
	rec1 := doShieldRequest(t, h, "ListProtections", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	token := resp1["NextToken"].(string)

	// Second page.
	rec2 := doShieldRequest(t, h, "ListProtections", map[string]any{
		"MaxResults": 2,
		"NextToken":  token,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	prots2 := resp2["Protections"].([]any)
	assert.Len(t, prots2, 2)
	_, hasNext := resp2["NextToken"]
	assert.False(t, hasNext, "Second page should have no NextToken")
}

// TestAudit_Gap8_ListProtectionsInclusionFilterByARN verifies ResourceArns filter.
func TestHandler_ListProtectionsInclusionFilterByARN(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())

	arns := []string{eipARN("1"), eipARN("2"), eipARN("3")}
	for i, arn := range arns {
		_, err := b.CreateProtection("prot-"+string(rune('a'+i)), arn, nil)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtections", map[string]any{
		"InclusionFilters": map[string]any{
			"ResourceArns": []string{eipARN("1"), eipARN("3")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prots := resp["Protections"].([]any)
	assert.Len(t, prots, 2)
}

// TestAudit_Gap8_ListProtectionsInclusionFilterByName verifies ProtectionNames filter.
func TestHandler_ListProtectionsInclusionFilterByName(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("alpha", eipARN("1"), nil)
	require.NoError(t, err)
	_, err = b.CreateProtection("beta", eipARN("2"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtections", map[string]any{
		"InclusionFilters": map[string]any{
			"ProtectionNames": []string{"alpha"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prots := resp["Protections"].([]any)
	assert.Len(t, prots, 1)

	p := prots[0].(map[string]any)
	assert.Equal(t, "alpha", p["Name"])
}

// --- Gap 9: ListProtectionGroups InclusionFilters ---

// TestHandler_Protection_NoCreationTimeKey covers gopherstack-y1zn.
// protectionToMap emitted "CreationTime"; types.Protection (shield@v1.37.4
// types/types.go) has no such member -- only
// ApplicationLayerAutomaticResponseConfiguration/HealthCheckIds/Id/Name/
// ProtectionArn/ResourceArn.
func TestHandler_Protection_NoCreationTimeKey(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeProtection", map[string]any{
		"ProtectionId": p.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"CreationTime"`,
		"types.Protection has no CreationTime member")
}

// TestAudit_Gap14_CreateProtectionValidResourceTypes verifies valid resource type ARNs.
func TestHandler_CreateProtectionValidResourceTypes(t *testing.T) {
	t.Parallel()

	validARNs := []struct {
		name string
		arn  string
	}{
		{"EIP", eipARN("123")},
		{"ALB", albARN("myapp")},
		{"CloudFront", cfARN("E1234")},
		{"Route53", r53ARN("ABCDEF")},
		{"GlobalAccelerator", "arn:aws:globalaccelerator::000000000000:accelerator/abc"},
		{"CLB", "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/myclb"},
	}

	for _, tc := range validARNs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := shield.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b.CreateSubscription())
			h := shield.NewHandler(b)

			rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
				"Name":        "prot-" + tc.name,
				"ResourceArn": tc.arn,
			})
			assert.Equal(t, http.StatusOK, rec.Code, "ARN %q should be accepted", tc.arn)
		})
	}
}

// TestAudit_Gap14_CreateProtectionInvalidResourceType verifies invalid ARNs are rejected.
func TestHandler_CreateProtectionInvalidResourceType(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	// EC2 instance ARNs (not EIPs) should be rejected.
	rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
		"Name":        "prot",
		"ResourceArn": "arn:aws:ec2:us-east-1::instance/i-12345678",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "EC2 instance ARNs should be rejected")
}

// --- Gap 15: EnableProactiveEngagement requires emergency contacts ---

// TestAudit_ALARInListProtections verifies ALAR config appears in ListProtections.
func TestHandler_ALARInListProtections(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)
	require.NoError(t, b.EnableApplicationLayerAutomaticResponse(eipARN("1"), "COUNT"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtections", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	prots := resp["Protections"].([]any)
	require.Len(t, prots, 1)

	prot := prots[0].(map[string]any)
	alarcfg, ok := prot["ApplicationLayerAutomaticResponseConfiguration"]
	assert.True(t, ok)
	cfg := alarcfg.(map[string]any)
	assert.Equal(t, "ENABLED", cfg["Status"])
	action := cfg["Action"].(map[string]any)
	assert.NotNil(t, action["Count"])
}

// TestRefinement1_HTTPCreateProtection tests HTTP create protection.
func TestHandler_CreateProtectionBasic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "CreateProtection", map[string]any{
		"Name":        "test-prot",
		"ResourceArn": "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-123",
	})
	assert.Equal(t, 200, rec.Code)
}

// TestRefinement1_HTTPListProtections tests HTTP list protections.
func TestHandler_ListProtectionsBasic(t *testing.T) {
	t.Parallel()

	b := shield.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddProtectionInternal("p1", "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-1")
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "ListProtections", nil)
	assert.Equal(t, 200, rec.Code)
}
