package elbv2_test

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDescribeRules_AllListenersPaginationDoesNotDropRules exercises
// DescribeRules with neither ListenerArn nor RuleArns set, which lists rules
// across every listener sorted by Priority. Priority is only required to be
// unique per-listener (CreateRule checks b.rulesByListener), so this listing
// commonly contains many rules sharing the same Priority across different
// listeners -- exactly the tie-admitting sort key the RuleArn marker must
// stay correct across.
func TestDescribeRules_AllListenersPaginationDoesNotDropRules(t *testing.T) {
	t.Parallel()

	const numListeners = 8

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "pag-tg")

	want := make(map[string]bool, numListeners)

	for i := range numListeners {
		lbArn := mustCreateLB(t, h, "pag-lb-"+strconv.Itoa(i))
		listenerArn := mustCreateListener(t, h, lbArn, tgArn)
		ruleArn := mustCreateRule(t, h, listenerArn, tgArn, "5")
		want[ruleArn] = true
	}

	// Go re-randomizes map iteration order on every range, so whether any
	// given walk hits the reordering is probabilistic -- repeat the walk to
	// make a real regression fail reliably instead of flaking green.
	const trials = 30

	for trial := range trials {
		seen := map[string]bool{}
		marker := ""

		for range numListeners + 1 {
			vals := url.Values{
				"Action":   {"DescribeRules"},
				"Version":  {"2015-12-01"},
				"PageSize": {"1"},
			}
			if marker != "" {
				vals.Set("Marker", marker)
			}

			rec := doELBv2(t, h, vals)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			var resp struct {
				Result struct {
					NextMarker string `xml:"NextMarker"`
					Rules      struct {
						Members []struct {
							RuleArn string `xml:"RuleArn"`
						} `xml:"member"`
					} `xml:"Rules"`
				} `xml:"DescribeRulesResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			for _, m := range resp.Result.Rules.Members {
				seen[m.RuleArn] = true
			}

			if resp.Result.NextMarker == "" {
				break
			}

			marker = resp.Result.NextMarker
		}

		for arn := range want {
			require.True(t, seen[arn], "trial %d: rule %s dropped from paginated DescribeRules walk", trial, arn)
		}
	}
}
