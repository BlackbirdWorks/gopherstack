package elbv2_test

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDescribeTrustStoreAssociations_PaginationDoesNotDropAssociations exercises
// DescribeTrustStoreAssociations pagination. The backend builds the association
// list by scanning every listener (a randomized map walk) and never sorts the
// result before applyMarkerPage runs, so even though each ListenerArn marker is
// unique, the relative order of associations is not stable across the call
// that issues a marker and the call that resumes from it.
func TestDescribeTrustStoreAssociations_PaginationDoesNotDropAssociations(t *testing.T) {
	t.Parallel()

	const numListeners = 8

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tsa-tg")

	tsRec := doELBv2(t, h, url.Values{
		"Action":  {"CreateTrustStore"},
		"Version": {"2015-12-01"},
		"Name":    {"tsa-store"},
	})
	require.Equal(t, 200, tsRec.Code, tsRec.Body.String())

	var tsResp struct {
		Result struct {
			TrustStores struct {
				Members []struct {
					TrustStoreArn string `xml:"TrustStoreArn"`
				} `xml:"member"`
			} `xml:"TrustStores"`
		} `xml:"CreateTrustStoreResult"`
	}
	require.NoError(t, xml.Unmarshal(tsRec.Body.Bytes(), &tsResp))
	tsArn := tsResp.Result.TrustStores.Members[0].TrustStoreArn

	want := make(map[string]bool, numListeners)

	for i := range numListeners {
		lbArn := mustCreateLB(t, h, "tsa-lb-"+strconv.Itoa(i))

		listRec := doELBv2(t, h, url.Values{
			"Action":                                 {"CreateListener"},
			"Version":                                {"2015-12-01"},
			"LoadBalancerArn":                        {lbArn},
			"Protocol":                               {"HTTPS"},
			"Port":                                   {"443"},
			"DefaultActions.member.1.Type":           {"forward"},
			"DefaultActions.member.1.TargetGroupArn": {tgArn},
			"Certificates.member.1.CertificateArn":   {"arn:aws:acm:us-east-1:000000000000:certificate/tsa"},
			"MutualAuthentication.Mode":              {"verify"},
			"MutualAuthentication.TrustStoreArn":     {tsArn},
		})
		require.Equal(t, 200, listRec.Code, listRec.Body.String())

		var listResp struct {
			Result struct {
				Listeners struct {
					Members []struct {
						ListenerArn string `xml:"ListenerArn"`
					} `xml:"member"`
				} `xml:"Listeners"`
			} `xml:"CreateListenerResult"`
		}
		require.NoError(t, xml.Unmarshal(listRec.Body.Bytes(), &listResp))
		want[listResp.Result.Listeners.Members[0].ListenerArn] = true
	}

	const trials = 30

	for trial := range trials {
		seen := map[string]bool{}
		marker := ""

		for range numListeners + 1 {
			vals := url.Values{
				"Action":        {"DescribeTrustStoreAssociations"},
				"Version":       {"2015-12-01"},
				"TrustStoreArn": {tsArn},
				"PageSize":      {"1"},
			}
			if marker != "" {
				vals.Set("Marker", marker)
			}

			rec := doELBv2(t, h, vals)
			require.Equal(t, 200, rec.Code, rec.Body.String())

			var resp struct {
				Result struct {
					NextMarker             string `xml:"NextMarker"`
					TrustStoreAssociations struct {
						Members []struct {
							ResourceArn string `xml:"ResourceArn"`
						} `xml:"member"`
					} `xml:"TrustStoreAssociations"`
				} `xml:"DescribeTrustStoreAssociationsResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			for _, m := range resp.Result.TrustStoreAssociations.Members {
				seen[m.ResourceArn] = true
			}

			if resp.Result.NextMarker == "" {
				break
			}

			marker = resp.Result.NextMarker
		}

		for arn := range want {
			require.True(t, seen[arn],
				"trial %d: association %s dropped from paginated DescribeTrustStoreAssociations walk", trial, arn)
		}
	}
}
