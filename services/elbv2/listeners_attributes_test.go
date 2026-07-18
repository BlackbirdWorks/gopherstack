package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestModifyListenerAttributes tests listener attribute modification.
func TestModifyListenerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "ml-attrs-lb")
	tgArn := mustCreateTG(t, h, "ml-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":      {"ModifyListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {listenerArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"ModifyListenerAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":      {"ModifyListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no-such/0/80"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeListenerAttributes tests listener attribute retrieval.
func TestDescribeListenerAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "dl-attrs-lb")
	tgArn := mustCreateTG(t, h, "dl-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":      {"DescribeListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {listenerArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"DescribeListenerAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":      {"DescribeListenerAttributes"},
				"Version":     {"2015-12-01"},
				"ListenerArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:listener/app/no-such/0/80"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := doELBv2(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestModifyListenerAttributesPersists verifies listener attributes are stored.
func TestModifyListenerAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "listener-attrs-lb")
	tgArn := mustCreateTG(t, h, "listener-attrs-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyListenerAttributes"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {listenerArn},
		"Attributes.member.1.Key":   {"routing.http2.enabled"},
		"Attributes.member.1.Value": {"false"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeListenerAttributes.
	descRec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeListenerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "false", attrMap["routing.http2.enabled"])
}

// TestListenerAttributeDefaults tests that HTTP/HTTPS listeners get default attributes.
func TestListenerAttributeDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "attr-defaults-lb")
	tgArn := mustCreateTG(t, h, "attr-defaults-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {listenerArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Attributes struct {
				Members []struct {
					Key   string `xml:"Key"`
					Value string `xml:"Value"`
				} `xml:"member"`
			} `xml:"Attributes"`
		} `xml:"DescribeListenerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}

	assert.Equal(t, "true", attrMap["routing.http2.enabled"])
	assert.Equal(t, "60", attrMap["idle_timeout.timeout_seconds"])
	assert.Equal(t, "defensive", attrMap["routing.http.desync_mitigation_mode"])
}

func TestListenerAttributes_DefaultRouting(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "listener-attr-lb")
	tgArn := b1CreateTG(t, h, "listener-attr-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "routing.http")
}

func TestModifyListenerAttributes_Smoke(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-listener-attr-lb")
	tgArn := b1CreateTG(t, h, "mod-listener-attr-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyListenerAttributes"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Attributes.member.1.Key":   {"routing.http.response.server.enabled"},
		"Attributes.member.1.Value": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":      {"DescribeListenerAttributes"},
		"Version":     {"2015-12-01"},
		"ListenerArn": {lArn},
	})
	assert.Contains(t, rec2.Body.String(), "routing.http.response.server.enabled")
}

func TestForwardWeightedTGs(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "weighted-lb")
	tgArn1 := b1CreateTG(t, h, "weighted-tg1")
	tgArn2 := b1CreateTG(t, h, "weighted-tg2")

	rec := doELBv2(t, h, url.Values{
		"Action":                       {"CreateListener"},
		"Version":                      {"2015-12-01"},
		"LoadBalancerArn":              {lbArn},
		"Protocol":                     {"HTTP"},
		"Port":                         {"80"},
		"DefaultActions.member.1.Type": {"forward"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn": {tgArn1},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.Weight":         {"80"},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.TargetGroupArn": {tgArn2},
		"DefaultActions.member.1.ForwardConfig.TargetGroups.member.2.Weight":         {"20"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, tgArn1)
	assert.Contains(t, body, tgArn2)
}

// TestForwardAction_ForwardConfigNormalization verifies that a simple
// forward action (TargetGroupArn only, no ForwardConfig) is serialized with a
// ForwardConfig containing the single TG in DescribeListeners output.
func TestForwardAction_ForwardConfigNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		useForwardCfg bool
	}{
		{
			name:          "simple_forward_action_gets_forwardconfig",
			useForwardCfg: false,
		},
		{
			name:          "explicit_forwardconfig_preserved",
			useForwardCfg: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			lbArn := pbCreateLB(t, h, "fc-lb")
			tgArn := pbCreateTG(t, h, "fc-tg")

			vals := url.Values{
				"Action":          {"CreateListener"},
				"Version":         {"2015-12-01"},
				"LoadBalancerArn": {lbArn},
				"Protocol":        {"HTTP"},
				"Port":            {"80"},
			}

			if tc.useForwardCfg {
				vals.Set("DefaultActions.member.1.Type", "forward")
				vals.Set("DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn", tgArn)
				vals.Set("DefaultActions.member.1.ForwardConfig.TargetGroups.member.1.Weight", "1")
			} else {
				vals.Set("DefaultActions.member.1.Type", "forward")
				vals.Set("DefaultActions.member.1.TargetGroupArn", tgArn)
			}

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp struct {
				Result struct {
					Listeners struct {
						Members []struct {
							ListenerArn string `xml:"ListenerArn"`
						} `xml:"member"`
					} `xml:"Listeners"`
				} `xml:"CreateListenerResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &createResp))
			require.Len(t, createResp.Result.Listeners.Members, 1)

			lArn := createResp.Result.Listeners.Members[0].ListenerArn

			// DescribeListeners and verify ForwardConfig is present.
			descRec := doELBv2(t, h, url.Values{
				"Action":                {"DescribeListeners"},
				"Version":               {"2015-12-01"},
				"ListenerArns.member.1": {lArn},
			})
			require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

			var descResp struct {
				Result struct {
					Listeners struct {
						Members []struct {
							DefaultActions struct {
								Members []struct {
									Type           string `xml:"Type"`
									TargetGroupArn string `xml:"TargetGroupArn"`
									ForwardConfig  struct {
										TargetGroups struct {
											Members []struct {
												TargetGroupArn string `xml:"TargetGroupArn"`
												Weight         int    `xml:"Weight"`
											} `xml:"member"`
										} `xml:"TargetGroups"`
									} `xml:"ForwardConfig"`
								} `xml:"member"`
							} `xml:"DefaultActions"`
						} `xml:"member"`
					} `xml:"Listeners"`
				} `xml:"DescribeListenersResult"`
			}
			require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &descResp))
			require.Len(t, descResp.Result.Listeners.Members, 1)

			actions := descResp.Result.Listeners.Members[0].DefaultActions.Members
			require.Len(t, actions, 1)
			assert.Equal(t, "forward", actions[0].Type)

			if !tc.useForwardCfg {
				assert.Equal(t, tgArn, actions[0].TargetGroupArn)
			}

			tgMembers := actions[0].ForwardConfig.TargetGroups.Members
			require.Len(t, tgMembers, 1, "ForwardConfig.TargetGroups must contain exactly one member")
			assert.Equal(t, tgArn, tgMembers[0].TargetGroupArn)
			assert.Equal(t, 1, tgMembers[0].Weight)
		})
	}
}
