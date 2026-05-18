package shield_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/shield"
)

// eipARN returns a test EIP allocation ARN.
func eipARN(id string) string {
	return "arn:aws:ec2:us-east-1::eip-allocation/eipalloc-" + id
}

// albARN returns a test Application Load Balancer ARN.
func albARN(id string) string {
	return "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/" + id + "/abc123"
}

// cfARN returns a test CloudFront distribution ARN.
func cfARN(id string) string {
	return "arn:aws:cloudfront::000000000000:distribution/" + id
}

// r53ARN returns a test Route 53 hosted zone ARN.
func r53ARN(id string) string {
	return "arn:aws:route53:::hostedzone/" + id
}

func newAuditBackend() *shield.InMemoryBackend {
	return shield.NewInMemoryBackend("000000000000", "us-east-1")
}

// --- Gap 1: TagResource/ListTagsForResource/UntagResource resolve by Shield ARN ---

// TestAudit_Gap1_TagResourceByShieldARN verifies TagResource accepts Shield protection ARNs.
func TestAudit_Gap1_TagResourceByShieldARN(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	err := b.TagResource(p.ProtectionArn, map[string]string{"key": "val"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.Equal(t, "val", tags["key"])
}

// TestAudit_Gap1_TagResourceByResourceARN verifies TagResource accepts resource ARNs.
func TestAudit_Gap1_TagResourceByResourceARN(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	resourceARN := eipARN("99")
	b.AddProtectionInternal("prot", resourceARN)

	err := b.TagResource(resourceARN, map[string]string{"env": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(resourceARN)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["env"])
}

// TestAudit_Gap1_UntagResourceByShieldARN verifies UntagResource resolves Shield ARN.
func TestAudit_Gap1_UntagResourceByShieldARN(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("2"))
	require.NoError(t, b.TagResource(p.ProtectionArn, map[string]string{"k": "v"}))

	err := b.UntagResource(p.ProtectionArn, []string{"k"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(p.ProtectionArn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// --- Gap 2: DescribeSubscription includes ProactiveEngagementStatus, Limits, SubscriptionLimits ---

// TestAudit_Gap2_DescribeSubscriptionFields verifies extended subscription response fields.
func TestAudit_Gap2_DescribeSubscriptionFields(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	sub := resp["Subscription"].(map[string]any)
	assert.NotNil(t, sub["ProactiveEngagementStatus"])
	assert.NotNil(t, sub["Limits"])
	assert.NotNil(t, sub["SubscriptionLimits"])
	assert.NotNil(t, sub["SubscriptionArn"])

	// Gap 22: SubscriptionArn must NOT have trailing path.
	subArn, _ := sub["SubscriptionArn"].(string)
	assert.True(
		t,
		strings.HasSuffix(subArn, ":subscription"),
		"SubscriptionArn %q should end with :subscription",
		subArn,
	)
	assert.NotContains(t, subArn, "subscription/", "SubscriptionArn %q should not have trailing path", subArn)
}

// TestAudit_Gap2_SubscriptionLimitsStructure verifies SubscriptionLimits structure.
func TestAudit_Gap2_SubscriptionLimitsStructure(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	sub := resp["Subscription"].(map[string]any)
	limits := sub["SubscriptionLimits"].(map[string]any)
	assert.NotNil(t, limits["ProtectionLimits"])
	assert.NotNil(t, limits["ProtectionGroupLimits"])
}

// --- Gap 3: CreateSubscription sets ProactiveEngagementStatus to DISABLED ---

// TestAudit_Gap3_CreateSubscriptionDefaultsProactiveDisabled verifies DISABLED default.
func TestAudit_Gap3_CreateSubscriptionDefaultsProactiveDisabled(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	assert.Equal(t, shield.ProactiveEngagementDisabled, shield.GetProactiveEngagementStatus(b))
}

// --- Gap 4: Protection response includes ApplicationLayerAutomaticResponseConfiguration ---

// TestAudit_Gap4_ProtectionResponseIncludesALAR verifies ALAR config in protection response.
func TestAudit_Gap4_ProtectionResponseIncludesALAR(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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
func TestAudit_Gap4_ProtectionWithoutALARHasNoConfig(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_Gap5_AttackRichFields verifies DescribeAttack returns rich fields.
func TestAudit_Gap5_AttackRichFields(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	a := b.AddAttackInternal("atk-001", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeAttack", map[string]any{
		"AttackId": a.AttackID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	atk := resp["Attack"].(map[string]any)
	assert.NotNil(t, atk["AttackVectors"])
	assert.NotNil(t, atk["AttackCounters"])
	assert.NotNil(t, atk["Mitigations"])
}

// --- Gap 6: ListAttacks supports multiple ResourceArns ---

// TestAudit_Gap6_ListAttacksMultipleARNs verifies multiple ARN filtering.
func TestAudit_Gap6_ListAttacksMultipleARNs(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	b.AddAttackInternal("atk-1", eipARN("1"))
	b.AddAttackInternal("atk-2", eipARN("2"))
	b.AddAttackInternal("atk-3", eipARN("3"))

	attacks := b.ListAttacks([]string{eipARN("1"), eipARN("2")}, 0, 0)
	assert.Len(t, attacks, 2)

	ids := make(map[string]bool)
	for _, a := range attacks {
		ids[a.AttackID] = true
	}

	assert.True(t, ids["atk-1"])
	assert.True(t, ids["atk-2"])
	assert.False(t, ids["atk-3"])
}

// TestAudit_Gap6_ListAttacksHTTPMultipleARNs verifies HTTP multiple ARN filtering.
func TestAudit_Gap6_ListAttacksHTTPMultipleARNs(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	b.AddAttackInternal("atk-1", eipARN("1"))
	b.AddAttackInternal("atk-2", eipARN("2"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"ResourceArns": []string{eipARN("1"), eipARN("2")},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 2)
}

// --- Gap 7: Pagination for ListAttacks, ListProtections, ListProtectionGroups ---

// TestAudit_Gap7_ListProtectionsPagination verifies pagination with MaxResults.
func TestAudit_Gap7_ListProtectionsPagination(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_Gap7_ListProtectionsNextPage verifies continuation token retrieves next page.
func TestAudit_Gap7_ListProtectionsNextPage(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_Gap7_ListProtectionGroupsPagination verifies pagination for protection groups.
func TestAudit_Gap7_ListProtectionGroupsPagination(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())

	for i := range 3 {
		_, err := b.CreateProtectionGroup(
			"grp-"+string(rune('a'+i)),
			shield.AggregationSum,
			shield.PatternAll,
			"",
			nil,
		)
		require.NoError(t, err)
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

// TestAudit_Gap7_ListAttacksPagination verifies pagination for attacks.
func TestAudit_Gap7_ListAttacksPagination(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()

	for i := range 5 {
		b.AddAttackInternal("atk-"+string(rune('a'+i)), eipARN("1"))
	}

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	assert.Len(t, summaries, 2)
	assert.NotEmpty(t, resp["NextToken"])
}

// --- Gap 8: ListProtections InclusionFilters ---

// TestAudit_Gap8_ListProtectionsInclusionFilterByARN verifies ResourceArns filter.
func TestAudit_Gap8_ListProtectionsInclusionFilterByARN(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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
func TestAudit_Gap8_ListProtectionsInclusionFilterByName(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_Gap9_ListProtectionGroupsInclusionFilterByPattern verifies Patterns filter.
func TestAudit_Gap9_ListProtectionGroupsInclusionFilterByPattern(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp-all", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)
	_, err = b.CreateProtectionGroup(
		"grp-arb",
		shield.AggregationMax,
		shield.PatternArbitrary,
		"",
		[]string{eipARN("1")},
	)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"InclusionFilters": map[string]any{
			"Patterns": []string{shield.PatternAll},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 1)

	g := groups[0].(map[string]any)
	assert.Equal(t, "grp-all", g["ProtectionGroupId"])
}

// TestAudit_Gap9_ListProtectionGroupsInclusionFilterByAggregation verifies Aggregations filter.
func TestAudit_Gap9_ListProtectionGroupsInclusionFilterByAggregation(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())

	_, err := b.CreateProtectionGroup("grp-sum", shield.AggregationSum, shield.PatternAll, "", nil)
	require.NoError(t, err)
	_, err = b.CreateProtectionGroup("grp-max", shield.AggregationMax, shield.PatternAll, "", nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListProtectionGroups", map[string]any{
		"InclusionFilters": map[string]any{
			"Aggregations": []string{shield.AggregationMax},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	groups := resp["ProtectionGroups"].([]any)
	assert.Len(t, groups, 1)
}

// --- Gap 11: GetAttackVectorDefinitionVersion ---

// TestAudit_Gap11_GetAttackVectorDefinitionVersion verifies the operation exists and returns version.
func TestAudit_Gap11_GetAttackVectorDefinitionVersion(t *testing.T) {
	t.Parallel()

	h := shield.NewHandler(newAuditBackend())
	rec := doShieldRequest(t, h, "GetAttackVectorDefinitionVersion", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	ver, ok := resp["AttackVectorDefinitionVersion"]
	assert.True(t, ok, "Response must include AttackVectorDefinitionVersion")
	assert.NotEmpty(t, ver)
}

// --- Gap 12: DescribeAttackStatistics per-bucket with AttackVolume ---

// TestAudit_Gap12_DescribeAttackStatisticsWithVolume verifies AttackVolume in statistics.
func TestAudit_Gap12_DescribeAttackStatisticsWithVolume(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	b.AddAttackInternal("atk-1", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stats := resp["AttackStatistics"].(map[string]any)
	assert.NotNil(t, stats["TimeRange"])
	assert.NotNil(t, stats["DataItems"])

	items := stats["DataItems"].([]any)
	assert.NotEmpty(t, items)
}

// TestAudit_Gap12_DescribeAttackStatisticsEmptyWhenNoAttacks verifies empty stats.
func TestAudit_Gap12_DescribeAttackStatisticsEmptyWhenNoAttacks(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	stats := resp["AttackStatistics"].(map[string]any)
	items := stats["DataItems"].([]any)
	assert.Len(t, items, 1)
}

// --- Gap 13: Timestamps emitted as float seconds (not int) ---

// TestAudit_Gap13_ProtectionTimestampIsFloat verifies protection timestamps are floats.
func TestAudit_Gap13_ProtectionTimestampIsFloat(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "DescribeProtection", map[string]any{
		"ProtectionId": p.ID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Raw JSON must decode CreationTime as a float64, not string.
	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	prot := raw["Protection"].(map[string]any)
	ct, ok := prot["CreationTime"].(float64)
	assert.True(t, ok, "CreationTime should be float64")
	assert.Greater(t, ct, float64(1e9), "CreationTime should be a real epoch second, not sub-1970")
}

// TestAudit_Gap13_SubscriptionTimestampIsFloat verifies subscription timestamps are floats.
func TestAudit_Gap13_SubscriptionTimestampIsFloat(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	sub := raw["Subscription"].(map[string]any)
	st, ok := sub["StartTime"].(float64)
	assert.True(t, ok, "StartTime should be float64")
	assert.Greater(t, st, float64(1e9), "StartTime should be a real epoch second")
}

// TestAudit_Gap13_AttackTimestampIsFloat verifies attack timestamps are floats.
func TestAudit_Gap13_AttackTimestampIsFloat(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	a := b.AddAttackInternal("atk-1", eipARN("1"))
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeAttack", map[string]any{"AttackId": a.AttackID})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	atk := raw["Attack"].(map[string]any)
	st, ok := atk["StartTime"].(float64)
	assert.True(t, ok, "StartTime should be float64")
	assert.Greater(t, st, float64(1e9))
}

// --- Gap 14: CreateProtection ResourceType validation ---

// TestAudit_Gap14_CreateProtectionValidResourceTypes verifies valid resource type ARNs.
func TestAudit_Gap14_CreateProtectionValidResourceTypes(t *testing.T) {
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

			b := newAuditBackend()
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
func TestAudit_Gap14_CreateProtectionInvalidResourceType(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_Gap15_EnableProactiveEngagementRequiresContacts verifies contact requirement.
func TestAudit_Gap15_EnableProactiveEngagementRequiresContacts(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())

	err := b.EnableProactiveEngagement()
	require.Error(t, err, "EnableProactiveEngagement must fail when no contacts configured")
}

// TestAudit_Gap15_EnableProactiveEngagementSucceedsWithContacts verifies success with contacts.
func TestAudit_Gap15_EnableProactiveEngagementSucceedsWithContacts(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "cirt@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))
}

// --- Gap 16: UpdateEmergencyContactSettings validation ---

// TestAudit_Gap16_UpdateEmergencyContactExceeds10ContactsRejected verifies cap.
func TestAudit_Gap16_UpdateEmergencyContactExceeds10ContactsRejected(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	contacts := make([]shield.EmergencyContact, 11)

	for i := range contacts {
		contacts[i] = shield.EmergencyContact{EmailAddress: "user@example.com"}
	}

	err := b.UpdateEmergencyContactSettings(contacts)
	require.Error(t, err, "More than 10 contacts should be rejected")
}

// TestAudit_Gap16_UpdateEmergencyContactMissingEmailRejected verifies email required.
func TestAudit_Gap16_UpdateEmergencyContactMissingEmailRejected(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	err := b.UpdateEmergencyContactSettings([]shield.EmergencyContact{
		{PhoneNumber: "+1-555-0100"},
	})
	require.Error(t, err, "Contact without EmailAddress should be rejected")
}

// TestAudit_Gap16_UpdateEmergencyContactTenContactsAccepted verifies exactly 10 allowed.
func TestAudit_Gap16_UpdateEmergencyContactTenContactsAccepted(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	contacts := make([]shield.EmergencyContact, 10)

	for i := range contacts {
		contacts[i] = shield.EmergencyContact{EmailAddress: "user@example.com"}
	}

	require.NoError(t, b.UpdateEmergencyContactSettings(contacts))
}

// --- Gap 17: DisassociateDRTRole is idempotent ---

// TestAudit_Gap17_DisassociateDRTRoleIdempotent verifies no error when no role.
func TestAudit_Gap17_DisassociateDRTRoleIdempotent(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	err := b.DisassociateDRTRole()
	require.NoError(t, err, "DisassociateDRTRole must be idempotent when no role is set")
}

// TestAudit_Gap17_DisassociateDRTRoleAfterAssociate verifies correct role removal.
func TestAudit_Gap17_DisassociateDRTRoleAfterAssociate(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateDRTRole("arn:aws:iam::123:role/DRTRole"))
	require.NoError(t, b.DisassociateDRTRole())
	assert.Empty(t, b.DescribeDRTAccess().RoleArn)
}

// --- Gap 18: AssociateProactiveEngagementDetails sets PENDING status ---

// TestAudit_Gap18_AssociateProactiveEngagementSetsPending verifies PENDING transition.
func TestAudit_Gap18_AssociateProactiveEngagementSetsPending(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	// Starts DISABLED after CreateSubscription.
	assert.Equal(t, shield.ProactiveEngagementDisabled, shield.GetProactiveEngagementStatus(b))

	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	assert.Equal(t, shield.ProactiveEngagementPending, shield.GetProactiveEngagementStatus(b))
}

// TestAudit_Gap18_AssociateProactiveEngagementDoesNotOverwriteEnabled verifies no regression.
func TestAudit_Gap18_AssociateProactiveEngagementDoesNotOverwriteEnabled(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "sec@example.com"},
	}))
	require.NoError(t, b.EnableProactiveEngagement())
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))

	// Re-associate should not revert ENABLED back to PENDING.
	require.NoError(t, b.AssociateProactiveEngagementDetails([]shield.EmergencyContact{
		{EmailAddress: "other@example.com"},
	}))
	assert.Equal(t, shield.ProactiveEngagementEnabled, shield.GetProactiveEngagementStatus(b))
}

// --- Gap 19: AssociateHealthCheck validates Route53 ARN ---

// TestAudit_Gap19_AssociateHealthCheckValidARN verifies valid Route53 ARN accepted.
func TestAudit_Gap19_AssociateHealthCheckValidARN(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "AssociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:route53:::healthcheck/abc12345",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestAudit_Gap19_AssociateHealthCheckInvalidARNRejected verifies non-Route53 ARN rejected.
func TestAudit_Gap19_AssociateHealthCheckInvalidARNRejected(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "AssociateHealthCheck", map[string]any{
		"ProtectionId":   p.ID,
		"HealthCheckArn": "arn:aws:ec2:us-east-1::instance/i-1",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Gap 20: TagResource subscription gate and limits ---

// TestAudit_Gap20_TagResourceRequiresSubscription verifies subscription gate.
func TestAudit_Gap20_TagResourceRequiresSubscription(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	p := b.AddProtectionInternal("prot", eipARN("1"))

	err := b.TagResource(p.ProtectionArn, map[string]string{"k": "v"})
	require.Error(t, err, "TagResource without subscription should fail")
}

// TestAudit_Gap20_TagResourceKeyLengthLimit verifies 128-char key limit.
func TestAudit_Gap20_TagResourceKeyLengthLimit(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	longKey := strings.Repeat("k", 129)
	err := b.TagResource(p.ProtectionArn, map[string]string{longKey: "val"})
	require.Error(t, err, "Tag key > 128 chars should be rejected")
}

// TestAudit_Gap20_TagResourceValueLengthLimit verifies 256-char value limit.
func TestAudit_Gap20_TagResourceValueLengthLimit(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	longVal := strings.Repeat("v", 257)
	err := b.TagResource(p.ProtectionArn, map[string]string{"key": longVal})
	require.Error(t, err, "Tag value > 256 chars should be rejected")
}

// TestAudit_Gap20_TagResourceCap50Tags verifies 50-tag limit.
func TestAudit_Gap20_TagResourceCap50Tags(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	p := b.AddProtectionInternal("prot", eipARN("1"))

	// Add 50 tags.
	const maxTags = 50
	tags := make(map[string]string, maxTags)

	for i := range maxTags {
		tags["key"+string(rune('a'+i%26))+string(rune('0'+i/26))] = "val"
	}

	require.NoError(t, b.TagResource(p.ProtectionArn, tags))

	// Attempt to add one more.
	err := b.TagResource(p.ProtectionArn, map[string]string{"extra": "tag"})
	require.Error(t, err, "Adding beyond 50-tag limit should fail")
}

// --- Gap 22: SubscriptionArn format ---

// TestAudit_Gap22_SubscriptionArnFormat verifies correct ARN format (no trailing path).
func TestAudit_Gap22_SubscriptionArnFormat(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "DescribeSubscription", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	sub := resp["Subscription"].(map[string]any)
	arn := sub["SubscriptionArn"].(string)

	assert.Equal(t, "arn:aws:shield::000000000000:subscription", arn)
}

// --- Gap 23: Reset is atomic ---

// TestAudit_Gap23_ResetClearsAllState verifies Reset clears all fields.
func TestAudit_Gap23_ResetClearsAllState(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	b.AddProtectionInternal("prot", eipARN("1"))
	b.AddAttackInternal("atk-1", eipARN("1"))

	b.Reset()

	assert.Equal(t, 0, shield.ProtectionCount(b))
	assert.Equal(t, 0, shield.AttackCount(b))
	assert.Equal(t, "INACTIVE", b.GetSubscriptionState())
}

// --- Gap 24: ListResourcesInProtectionGroup BY_RESOURCE_TYPE ---

// TestAudit_Gap24_ListResourcesByResourceType verifies BY_RESOURCE_TYPE pattern.
func TestAudit_Gap24_ListResourcesByResourceType(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())

	// Add an EIP and an ALB protection.
	_, err := b.CreateProtection("eip-prot", eipARN("1"), nil)
	require.NoError(t, err)
	_, err = b.CreateProtection("alb-prot", albARN("myapp"), nil)
	require.NoError(t, err)

	// Create group with BY_RESOURCE_TYPE for EIP.
	_, err = b.CreateProtectionGroup(
		"eip-group",
		shield.AggregationMax,
		shield.PatternByResourceType,
		"ELASTIC_IP_ALLOCATION",
		nil,
	)
	require.NoError(t, err)

	arns, err := b.ListResourcesInProtectionGroup("eip-group")
	require.NoError(t, err)
	assert.Len(t, arns, 1)
	assert.Contains(t, arns[0], "eip")
}

// --- Gap 25: SimulateAttack endpoint ---

// TestAudit_Gap25_SimulateAttackCreatesAttack verifies SimulateAttack backend method.
func TestAudit_Gap25_SimulateAttackCreatesAttack(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	atk, err := b.SimulateAttack(eipARN("1"), []string{"SYN_FLOOD", "UDP_TRAFFIC"})
	require.NoError(t, err)
	require.NotNil(t, atk)
	assert.NotEmpty(t, atk.AttackID)
	assert.Equal(t, eipARN("1"), atk.ResourceARN)
	assert.Len(t, atk.AttackVectors, 2)
	assert.Equal(t, 1, shield.AttackCount(b))
}

// TestAudit_Gap25_SimulateAttackHTTPEndpoint verifies __SimulateAttack HTTP endpoint.
func TestAudit_Gap25_SimulateAttackHTTPEndpoint(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("1"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn":       eipARN("1"),
		"AttackVectorTypes": []string{"SYN_FLOOD"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp["AttackId"])
	assert.Equal(t, 1, shield.AttackCount(b))
}

// TestAudit_Gap25_SimulateAttackNoProtectionFails verifies protection required.
func TestAudit_Gap25_SimulateAttackNoProtectionFails(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	h := shield.NewHandler(b)

	rec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn": eipARN("notprotected"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- Combined scenario: attack simulation visible in statistics and list ---

// TestAudit_AttackSimulationEndToEnd verifies simulated attacks flow through all endpoints.
func TestAudit_AttackSimulationEndToEnd(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	require.NoError(t, b.CreateSubscription())
	_, err := b.CreateProtection("prot", eipARN("42"), nil)
	require.NoError(t, err)

	h := shield.NewHandler(b)

	// Simulate an attack.
	simRec := doShieldRequest(t, h, "__SimulateAttack", map[string]any{
		"ResourceArn": eipARN("42"),
	})
	require.Equal(t, http.StatusOK, simRec.Code)

	var simResp map[string]any
	require.NoError(t, json.Unmarshal(simRec.Body.Bytes(), &simResp))
	attackID := simResp["AttackId"].(string)

	// ListAttacks must show it.
	listRec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	summaries := listResp["AttackSummaries"].([]any)
	require.Len(t, summaries, 1)

	// DescribeAttack must show it with rich fields.
	descRec := doShieldRequest(t, h, "DescribeAttack", map[string]any{"AttackId": attackID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	atk := descResp["Attack"].(map[string]any)
	assert.NotEmpty(t, atk["AttackVectors"])
	assert.NotEmpty(t, atk["AttackCounters"])
	assert.NotEmpty(t, atk["Mitigations"])

	// Statistics must reflect the attack.
	statsRec := doShieldRequest(t, h, "DescribeAttackStatistics", nil)
	require.Equal(t, http.StatusOK, statsRec.Code)

	var statsResp map[string]any
	require.NoError(t, json.Unmarshal(statsRec.Body.Bytes(), &statsResp))
	stats := statsResp["AttackStatistics"].(map[string]any)
	items := stats["DataItems"].([]any)
	require.NotEmpty(t, items)

	item0 := items[0].(map[string]any)
	count, _ := item0["AttackCount"].(float64)
	assert.InDelta(t, 1.0, count, 0)
}

// --- ListProtections includes ALAR in response ---

// TestAudit_ALARInListProtections verifies ALAR config appears in ListProtections.
func TestAudit_ALARInListProtections(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
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

// TestAudit_ListAttacksIncludesVectors verifies AttackVectors in ListAttacks response.
func TestAudit_ListAttacksIncludesVectors(t *testing.T) {
	t.Parallel()

	b := newAuditBackend()
	b.AddAttackInternal("atk-1", eipARN("1"))

	h := shield.NewHandler(b)
	rec := doShieldRequest(t, h, "ListAttacks", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	summaries := resp["AttackSummaries"].([]any)
	require.Len(t, summaries, 1)

	atk := summaries[0].(map[string]any)
	vectors, ok := atk["AttackVectors"]
	assert.True(t, ok, "AttackSummaries items should include AttackVectors")
	assert.NotEmpty(t, vectors)
}
