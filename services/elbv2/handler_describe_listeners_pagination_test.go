package elbv2_test

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDescribeListeners_AllLoadBalancersPaginationDoesNotDropListeners exercises
// DescribeListeners with neither LoadBalancerArn nor ListenerArns set, which
// lists listeners across every load balancer sorted by Port. Port is only
// required to be unique per-load-balancer (CreateListener checks
// checkDuplicateListenerPort against b.listenersByLB), so this listing
// commonly contains many listeners sharing the same Port across different
// load balancers -- exactly the tie-admitting sort key the ListenerArn
// marker must stay correct across.
func TestDescribeListeners_AllLoadBalancersPaginationDoesNotDropListeners(t *testing.T) {
	t.Parallel()

	const numLBs = 8

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "pag-tg")

	want := make(map[string]bool, numLBs)

	for i := range numLBs {
		lbArn := mustCreateLB(t, h, "pag-lb-"+strconv.Itoa(i))
		listenerArn := mustCreateListener(t, h, lbArn, tgArn)
		want[listenerArn] = true
	}

	const trials = 30

	for trial := range trials {
		seen := map[string]bool{}
		marker := ""

		for range numLBs + 1 {
			vals := url.Values{
				"Action":   {"DescribeListeners"},
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
					Listeners  struct {
						Members []struct {
							ListenerArn string `xml:"ListenerArn"`
						} `xml:"member"`
					} `xml:"Listeners"`
				} `xml:"DescribeListenersResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			for _, m := range resp.Result.Listeners.Members {
				seen[m.ListenerArn] = true
			}

			if resp.Result.NextMarker == "" {
				break
			}

			marker = resp.Result.NextMarker
		}

		for arn := range want {
			require.True(
				t,
				seen[arn],
				"trial %d: listener %s dropped from paginated DescribeListeners walk",
				trial,
				arn,
			)
		}
	}
}
