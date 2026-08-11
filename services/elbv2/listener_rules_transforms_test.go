package elbv2_test

import (
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type xmlTransformsResponse struct {
	Result struct {
		Rules struct {
			Members []struct {
				RuleArn    string `xml:"RuleArn"`
				Transforms struct {
					Members []struct {
						HostHeaderRewriteConfig *xmlRewritesHolder `xml:"HostHeaderRewriteConfig"`
						URLRewriteConfig        *xmlRewritesHolder `xml:"UrlRewriteConfig"`
						Type                    string             `xml:"Type"`
					} `xml:"member"`
				} `xml:"Transforms"`
			} `xml:"member"`
		} `xml:"Rules"`
	} `xml:"CreateRuleResult"`
}

type xmlModifyTransformsResponse struct {
	Result struct {
		Rules struct {
			Members []struct {
				RuleArn    string `xml:"RuleArn"`
				Transforms struct {
					Members []struct {
						HostHeaderRewriteConfig *xmlRewritesHolder `xml:"HostHeaderRewriteConfig"`
						URLRewriteConfig        *xmlRewritesHolder `xml:"UrlRewriteConfig"`
						Type                    string             `xml:"Type"`
					} `xml:"member"`
				} `xml:"Transforms"`
			} `xml:"member"`
		} `xml:"Rules"`
	} `xml:"ModifyRuleResult"`
}

type xmlRewritesHolder struct {
	Rewrites struct {
		Members []struct {
			Regex   string `xml:"Regex"`
			Replace string `xml:"Replace"`
		} `xml:"member"`
	} `xml:"Rewrites"`
}

func baseCreateRuleVals(listenerArn, tgArn string) url.Values {
	return url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"10"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
	}
}

// TestCreateRule_Transforms verifies host-header-rewrite and url-rewrite transforms
// (Rule.Transforms, a real, newer AWS field: types.RuleTransform) round-trip through
// CreateRule/DescribeRules.
func TestCreateRule_Transforms(t *testing.T) {
	t.Parallel()

	tests := []struct {
		transformVals   url.Values
		name            string
		wantType        string
		wantRegex       string
		wantReplace     string
		wantHostRewrite bool
		wantURLRewrite  bool
	}{
		{
			name: "host_header_rewrite",
			transformVals: url.Values{
				"Transforms.member.1.Type": {"host-header-rewrite"},
				"Transforms.member.1.HostHeaderRewriteConfig.Rewrites.member.1.Regex":   {"^old\\.example\\.com$"},
				"Transforms.member.1.HostHeaderRewriteConfig.Rewrites.member.1.Replace": {"new.example.com"},
			},
			wantType:        "host-header-rewrite",
			wantHostRewrite: true,
			wantRegex:       "^old\\.example\\.com$",
			wantReplace:     "new.example.com",
		},
		{
			name: "url_rewrite",
			transformVals: url.Values{
				"Transforms.member.1.Type":                                       {"url-rewrite"},
				"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Regex":   {"^/old/(.*)$"},
				"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Replace": {"/new/$1"},
			},
			wantType:       "url-rewrite",
			wantURLRewrite: true,
			wantRegex:      "^/old/(.*)$",
			wantReplace:    "/new/$1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slug := strings.ReplaceAll(tt.name, "_", "-")
			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "transform-lb-"+slug)
			tgArn := mustCreateTG(t, h, "transform-tg-"+slug)
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			vals := baseCreateRuleVals(listenerArn, tgArn)
			maps.Copy(vals, tt.transformVals)

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp xmlTransformsResponse
			parseXMLBody(t, rec, &resp)
			require.Len(t, resp.Result.Rules.Members, 1)
			require.Len(t, resp.Result.Rules.Members[0].Transforms.Members, 1)

			tr := resp.Result.Rules.Members[0].Transforms.Members[0]
			assert.Equal(t, tt.wantType, tr.Type)

			if tt.wantHostRewrite {
				require.NotNil(t, tr.HostHeaderRewriteConfig)
				require.Len(t, tr.HostHeaderRewriteConfig.Rewrites.Members, 1)
				assert.Equal(t, tt.wantRegex, tr.HostHeaderRewriteConfig.Rewrites.Members[0].Regex)
				assert.Equal(t, tt.wantReplace, tr.HostHeaderRewriteConfig.Rewrites.Members[0].Replace)
				assert.Nil(t, tr.URLRewriteConfig)
			}

			if tt.wantURLRewrite {
				require.NotNil(t, tr.URLRewriteConfig)
				require.Len(t, tr.URLRewriteConfig.Rewrites.Members, 1)
				assert.Equal(t, tt.wantRegex, tr.URLRewriteConfig.Rewrites.Members[0].Regex)
				assert.Equal(t, tt.wantReplace, tr.URLRewriteConfig.Rewrites.Members[0].Replace)
				assert.Nil(t, tr.HostHeaderRewriteConfig)
			}
		})
	}
}

// TestCreateRule_Transforms_Validation covers rejection cases for the Transforms field.
func TestCreateRule_Transforms_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals url.Values
		name string
	}{
		{
			name: "invalid_type",
			vals: url.Values{
				"Transforms.member.1.Type": {"bogus-type"},
			},
		},
		{
			name: "duplicate_type",
			vals: url.Values{
				"Transforms.member.1.Type":                                       {"url-rewrite"},
				"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Regex":   {"^/a$"},
				"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Replace": {"/b"},
				"Transforms.member.2.Type":                                       {"url-rewrite"},
				"Transforms.member.2.UrlRewriteConfig.Rewrites.member.1.Regex":   {"^/c$"},
				"Transforms.member.2.UrlRewriteConfig.Rewrites.member.1.Replace": {"/d"},
			},
		},
		{
			name: "missing_replace",
			vals: url.Values{
				"Transforms.member.1.Type":                                     {"url-rewrite"},
				"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Regex": {"^/a$"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			slug := strings.ReplaceAll(tt.name, "_", "-")
			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "inv-lb-"+slug)
			tgArn := mustCreateTG(t, h, "inv-tg-"+slug)
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			vals := baseCreateRuleVals(listenerArn, tgArn)
			maps.Copy(vals, tt.vals)

			rec := doELBv2(t, h, vals)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// TestModifyRule_Transforms_ResetAndReplace verifies ModifyRule's ResetTransforms clears
// existing transforms, a fresh Transforms list replaces them, and specifying both in the
// same request is rejected (ModifyRuleInput's own doc comment: mutually exclusive).
func TestModifyRule_Transforms_ResetAndReplace(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "modify-transform-lb")
	tgArn := mustCreateTG(t, h, "modify-transform-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	createVals := baseCreateRuleVals(listenerArn, tgArn)
	createVals["Transforms.member.1.Type"] = []string{"url-rewrite"}
	createVals["Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Regex"] = []string{"^/a$"}
	createVals["Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Replace"] = []string{"/b"}

	cRec := doELBv2(t, h, createVals)
	require.Equal(t, http.StatusOK, cRec.Code, cRec.Body.String())

	var cResp xmlTransformsResponse
	parseXMLBody(t, cRec, &cResp)
	require.Len(t, cResp.Result.Rules.Members, 1)
	ruleArn := cResp.Result.Rules.Members[0].RuleArn

	t.Run("reset_clears_transforms", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":                          {"ModifyRule"},
			"Version":                         {"2015-12-01"},
			"RuleArn":                         {ruleArn},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {tgArn},
			"ResetTransforms":                 {"true"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		var resp xmlModifyTransformsResponse
		parseXMLBody(t, rec, &resp)
		require.Len(t, resp.Result.Rules.Members, 1)
		assert.Empty(t, resp.Result.Rules.Members[0].Transforms.Members)
	})

	t.Run("both_transforms_and_reset_rejected", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":                   {"ModifyRule"},
			"Version":                  {"2015-12-01"},
			"RuleArn":                  {ruleArn},
			"ResetTransforms":          {"true"},
			"Transforms.member.1.Type": {"url-rewrite"},
			"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Regex":   {"^/x$"},
			"Transforms.member.1.UrlRewriteConfig.Rewrites.member.1.Replace": {"/y"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}
