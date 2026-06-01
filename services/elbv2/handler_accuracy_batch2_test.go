package elbv2_test

// handler_accuracy_batch2_test.go — table-driven tests for the second batch of
// AWS-accuracy audit fixes: tag key/value validation, DescribeTargetGroups
// not-found on unknown ARN, and DescribeRules not-found on unknown ARN.

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func newBatch2Handler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("000000000000", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

// ─── Issue: Tag key/value length validation in AddTags ───────────────────────

// TestBatch2_AddTags_KeyValueValidation verifies that AddTags enforces AWS tag limits:
// key 1-128 chars, value 0-256 chars, max 50 tags per resource.
func TestBatch2_AddTags_KeyValueValidation(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	lbArn := mustCreateLB(t, h, "tag-val-lb")

	tests := []struct {
		name       string
		tagKey     string
		tagValue   string
		wantStatus int
	}{
		{
			name:       "valid_key_and_value",
			tagKey:     "env",
			tagValue:   "prod",
			wantStatus: http.StatusOK,
		},
		{
			name:       "key_exactly_128_chars_accepted",
			tagKey:     strings.Repeat("k", 128),
			tagValue:   "v",
			wantStatus: http.StatusOK,
		},
		{
			name:       "key_129_chars_rejected",
			tagKey:     strings.Repeat("k", 129),
			tagValue:   "v",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "value_empty_accepted",
			tagKey:     "mykey",
			tagValue:   "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "value_exactly_256_chars_accepted",
			tagKey:     "mykey2",
			tagValue:   strings.Repeat("v", 256),
			wantStatus: http.StatusOK,
		},
		{
			name:       "value_257_chars_rejected",
			tagKey:     "mykey3",
			tagValue:   strings.Repeat("v", 257),
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doELBv2(t, h, url.Values{
				"Action":                   {"AddTags"},
				"Version":                  {"2015-12-01"},
				"ResourceArns.member.1":    {lbArn},
				"Tags.member.1.Key":        {tc.tagKey},
				"Tags.member.1.Value":      {tc.tagValue},
			})
			assert.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
		})
	}
}

// TestBatch2_AddTags_MaxTagsPerResource verifies that adding tags beyond the 50-tag limit
// returns a ValidationError.
func TestBatch2_AddTags_MaxTagsPerResource(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	lbArn := mustCreateLB(t, h, "max-tags-lb")

	// Add 50 tags one at a time.
	for i := 0; i < 50; i++ {
		rec := doELBv2(t, h, url.Values{
			"Action":                {"AddTags"},
			"Version":               {"2015-12-01"},
			"ResourceArns.member.1": {lbArn},
			"Tags.member.1.Key":     {strings.Repeat("a", 1) + strings.Repeat("0", i%9) + string(rune('a'+i%26))},
			"Tags.member.1.Value":   {"v"},
		})
		require.Equal(t, http.StatusOK, rec.Code, "tag %d: %s", i, rec.Body.String())
	}

	// 51st distinct key should be rejected.
	rec := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"overflow-key-that-is-definitely-new"},
		"Tags.member.1.Value":   {"v"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

// TestBatch2_AddTags_UpdateExistingKeyDoesNotCount verifies that updating an existing tag
// key does not count toward the 50-tag limit.
func TestBatch2_AddTags_UpdateExistingKeyDoesNotCount(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	lbArn := mustCreateLB(t, h, "tag-update-lb")

	// Add exactly 50 tags.
	vals := url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
	}
	for i := 0; i < 50; i++ {
		vals.Set("Tags.member."+itoa(i+1)+".Key", "key-"+itoa(i))
		vals.Set("Tags.member."+itoa(i+1)+".Value", "v")
	}
	rec := doELBv2(t, h, vals)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	// Updating an existing key should succeed (not a new tag).
	rec2 := doELBv2(t, h, url.Values{
		"Action":                {"AddTags"},
		"Version":               {"2015-12-01"},
		"ResourceArns.member.1": {lbArn},
		"Tags.member.1.Key":     {"key-0"},
		"Tags.member.1.Value":   {"updated"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())
}

// ─── Issue: DescribeTargetGroups returns TargetGroupNotFound for unknown ARN ──

// TestBatch2_DescribeTargetGroups_UnknownArnReturnsNotFound verifies that querying
// a non-existent target group ARN returns TargetGroupNotFound (404), matching AWS.
func TestBatch2_DescribeTargetGroups_UnknownArnReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arns []string
	}{
		{
			name: "single_unknown_arn",
			arns: []string{"arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/no-such-tg/0000000000000000"},
		},
		{
			name: "mix_of_known_and_unknown",
			arns: nil, // set in test body after creating a real TG
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler()

			arns := tc.arns
			if tc.name == "mix_of_known_and_unknown" {
				realArn := mustCreateTG(t, h, "real-tg-"+tc.name)
				arns = []string{
					realArn,
					"arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/ghost/0000000000000000",
				}
			}

			vals := url.Values{
				"Action":  {"DescribeTargetGroups"},
				"Version": {"2015-12-01"},
			}
			for i, a := range arns {
				vals.Set("TargetGroupArns.member."+itoa(i+1), a)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
		})
	}
}

// TestBatch2_DescribeTargetGroups_AllKnownArnsSucceeds verifies that querying
// target groups by ARN continues to work when all ARNs exist.
func TestBatch2_DescribeTargetGroups_AllKnownArnsSucceeds(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	arn1 := mustCreateTG(t, h, "known-tg-1")
	arn2 := mustCreateTG(t, h, "known-tg-2")

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {arn1},
		"TargetGroupArns.member.2": {arn2},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.TargetGroups.Members, 2)
}

// ─── Issue: DescribeRules returns RuleNotFound for unknown ARN ────────────────

// TestBatch2_DescribeRules_UnknownArnReturnsNotFound verifies that querying a
// non-existent rule ARN (without listenerArn) returns RuleNotFound (404).
func TestBatch2_DescribeRules_UnknownArnReturnsNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "single_unknown_rule_arn"},
		{name: "mix_known_and_unknown_rule_arns"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newBatch2Handler()
			ghostArn := "arn:aws:elasticloadbalancing:us-east-1:000000000000:listener-rule/app/lb/0123456789abcdef/0000000000000000/ghost"

			vals := url.Values{
				"Action":  {"DescribeRules"},
				"Version": {"2015-12-01"},
			}

			if tc.name == "mix_known_and_unknown_rule_arns" {
				lbArn := mustCreateLB(t, h, "rule-lb-mix")
				tgArn := mustCreateTG(t, h, "rule-tg-mix")
				listenerArn := mustCreateListener(t, h, lbArn, tgArn)
				realRuleArn := mustCreateRule(t, h, listenerArn, tgArn, "10")
				vals.Set("RuleArns.member.1", realRuleArn)
				vals.Set("RuleArns.member.2", ghostArn)
			} else {
				vals.Set("RuleArns.member.1", ghostArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
		})
	}
}

// TestBatch2_DescribeRules_AllKnownArnsSucceeds verifies that DescribeRules
// without listenerArn still works when all rule ARNs exist.
func TestBatch2_DescribeRules_AllKnownArnsSucceeds(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()
	lbArn := mustCreateLB(t, h, "rule-lb-known")
	tgArn := mustCreateTG(t, h, "rule-tg-known")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)
	ruleArn1 := mustCreateRule(t, h, listenerArn, tgArn, "10")
	ruleArn2 := mustCreateRule(t, h, listenerArn, tgArn, "20")

	rec := doELBv2(t, h, url.Values{
		"Action":              {"DescribeRules"},
		"Version":             {"2015-12-01"},
		"RuleArns.member.1":   {ruleArn1},
		"RuleArns.member.2":   {ruleArn2},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"DescribeRulesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Rules.Members, 2)
}

// ─── helpers ─────────────────────────────────────────────────────────────────

// mustCreateRule creates a forward rule on the given listener and returns its ARN.
func mustCreateRule(t *testing.T, h *elbv2.Handler, listenerArn, tgArn, priority string) string {
	t.Helper()

	rec := doELBv2(t, h, url.Values{
		"Action":                                  {"CreateRule"},
		"Version":                                 {"2015-12-01"},
		"ListenerArn":                             {listenerArn},
		"Priority":                                {priority},
		"Conditions.member.1.Field":               {"path-pattern"},
		"Conditions.member.1.Values.member.1":     {"/p-" + priority + "/*"},
		"Actions.member.1.Type":                   {"forward"},
		"Actions.member.1.TargetGroupArn":         {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)

	return resp.Result.Rules.Members[0].RuleArn
}

// itoa converts an int to its decimal string representation.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	b := make([]byte, 0, 10)
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}

	return string(b)
}
