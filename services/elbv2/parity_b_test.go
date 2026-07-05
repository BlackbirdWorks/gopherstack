package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

func newParityBHandler() *elbv2.Handler {
	b := elbv2.NewInMemoryBackend("123456789012", config.DefaultRegion)

	return elbv2.NewHandler(b)
}

// pbCreateLB creates a load balancer for parity_b tests.
func pbCreateLB(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":  {"CreateLoadBalancer"},
		"Version": {"2015-12-01"},
		"Name":    {name},
		"Type":    {"application"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			LoadBalancers struct {
				Members []struct {
					LoadBalancerArn string `xml:"LoadBalancerArn"`
				} `xml:"member"`
			} `xml:"LoadBalancers"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancers.Members, 1)

	return resp.Result.LoadBalancers.Members[0].LoadBalancerArn
}

// pbCreateTG creates a target group for parity_b tests.
func pbCreateTG(t *testing.T, h *elbv2.Handler, name string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {name},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	return resp.Result.TargetGroups.Members[0].TargetGroupArn
}

// pbCreateListener creates a listener for parity_b tests.
func pbCreateListener(t *testing.T, h *elbv2.Handler, lbArn, tgArn string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":                                 {"CreateListener"},
		"Version":                                {"2015-12-01"},
		"LoadBalancerArn":                        {lbArn},
		"Protocol":                               {"HTTP"},
		"Port":                                   {"80"},
		"DefaultActions.member.1.Type":           {"forward"},
		"DefaultActions.member.1.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp struct {
		Result struct {
			Listeners struct {
				Members []struct {
					ListenerArn string `xml:"ListenerArn"`
				} `xml:"member"`
			} `xml:"Listeners"`
		} `xml:"CreateListenerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Listeners.Members, 1)

	return resp.Result.Listeners.Members[0].ListenerArn
}

// pbCreateRule creates a rule on a listener with the given priority.
func pbCreateRule(t *testing.T, h *elbv2.Handler, listenerArn, tgArn, priority string) string {
	t.Helper()
	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {listenerArn},
		"Priority":                  {priority},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/p" + priority + "/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
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

// TestParity_DeleteTargetGroup_ErrorCode verifies that deleting a TG still
// attached to a listener returns the AWS-accurate error code "ResourceInUse".
func TestParity_DeleteTargetGroup_ErrorCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantErrCode  string
		wantCode     int
		detachBefore bool
	}{
		{
			name:         "in_use_returns_ResourceInUse",
			detachBefore: false,
			wantCode:     http.StatusBadRequest,
			wantErrCode:  "ResourceInUse",
		},
		{
			name:         "after_detach_returns_ok",
			detachBefore: true,
			wantCode:     http.StatusOK,
			wantErrCode:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			lbArn := pbCreateLB(t, h, "dtg-lb")
			tgArn := pbCreateTG(t, h, "dtg-tg")
			lArn := pbCreateListener(t, h, lbArn, tgArn)

			if tc.detachBefore {
				tgArn2 := pbCreateTG(t, h, "dtg-tg2")
				rec := doELBv2(t, h, url.Values{
					"Action":                                 {"ModifyListener"},
					"Version":                                {"2015-12-01"},
					"ListenerArn":                            {lArn},
					"DefaultActions.member.1.Type":           {"forward"},
					"DefaultActions.member.1.TargetGroupArn": {tgArn2},
				})
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			}

			rec := doELBv2(t, h, url.Values{
				"Action":         {"DeleteTargetGroup"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantErrCode != "" {
				var errResp struct {
					Error struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tc.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

// TestParity_DescribeListeners_UnknownLB verifies that describing listeners for
// a nonexistent LB ARN returns LoadBalancerNotFound.
func TestParity_DescribeListeners_UnknownLB(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		lbArn       string
		wantErrCode string
		wantCode    int
	}{
		{
			name:        "unknown_lb_arn_returns_not_found",
			lbArn:       "arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/nonexistent/abcdef123456",
			wantCode:    http.StatusBadRequest,
			wantErrCode: "LoadBalancerNotFound",
		},
		{
			name:     "empty_lb_arn_returns_all",
			lbArn:    "",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			vals := url.Values{
				"Action":  {"DescribeListeners"},
				"Version": {"2015-12-01"},
			}

			if tc.lbArn != "" {
				vals.Set("LoadBalancerArn", tc.lbArn)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantErrCode != "" {
				var errResp struct {
					Error struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tc.wantErrCode, errResp.Error.Code)
			}
		})
	}
}

// TestParity_SetRulePriorities_CollisionWithExisting verifies that
// SetRulePriorities rejects a priority that collides with a non-batch rule.
func TestParity_SetRulePriorities_CollisionWithExisting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		newPrio1     string // new priority for rule1 (batch)
		newPrio2     string // new priority for rule2 (batch), empty = omit rule2 from batch
		existingPrio string // priority of an additional non-batch rule
		wantCode     int
	}{
		{
			name:         "collision_with_existing_rule_rejected",
			newPrio1:     "20",
			existingPrio: "20",
			wantCode:     http.StatusBadRequest,
		},
		{
			name:         "no_collision_accepted",
			newPrio1:     "30",
			existingPrio: "20",
			wantCode:     http.StatusOK,
		},
		{
			name:     "swap_within_batch_accepted",
			newPrio1: "50",
			newPrio2: "10",
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			lbArn := pbCreateLB(t, h, "srp-lb")
			tgArn := pbCreateTG(t, h, "srp-tg")
			lArn := pbCreateListener(t, h, lbArn, tgArn)

			// rule1 at 10, rule2 at 50.
			rule1Arn := pbCreateRule(t, h, lArn, tgArn, "10")
			rule2Arn := pbCreateRule(t, h, lArn, tgArn, "50")

			if tc.existingPrio != "" {
				pbCreateRule(t, h, lArn, tgArn, tc.existingPrio)
			}

			vals := url.Values{
				"Action":                           {"SetRulePriorities"},
				"Version":                          {"2015-12-01"},
				"RulePriorities.member.1.RuleArn":  {rule1Arn},
				"RulePriorities.member.1.Priority": {tc.newPrio1},
			}

			if tc.newPrio2 != "" {
				vals.Set("RulePriorities.member.2.RuleArn", rule2Arn)
				vals.Set("RulePriorities.member.2.Priority", tc.newPrio2)
			}

			rec := doELBv2(t, h, vals)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestParity_ForwardAction_ForwardConfigNormalization verifies that a simple
// forward action (TargetGroupArn only, no ForwardConfig) is serialized with a
// ForwardConfig containing the single TG in DescribeListeners output.
func TestParity_ForwardAction_ForwardConfigNormalization(t *testing.T) {
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

// TestParity_LBName_Validation verifies that LB names reject underscores and
// require at least 2 characters, unlike target group names.
func TestParity_LBName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		lbName   string
		wantCode int
	}{
		{
			name:     "valid_name_accepted",
			lbName:   "my-lb",
			wantCode: http.StatusOK,
		},
		{
			name:     "underscore_rejected",
			lbName:   "my_lb",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "single_char_rejected",
			lbName:   "x",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "two_chars_accepted",
			lbName:   "lb",
			wantCode: http.StatusOK,
		},
		{
			name:     "leading_hyphen_rejected",
			lbName:   "-lb",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			rec := doELBv2(t, h, url.Values{
				"Action":  {"CreateLoadBalancer"},
				"Version": {"2015-12-01"},
				"Name":    {tc.lbName},
				"Type":    {"application"},
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
		})
	}
}

// TestParity_LambdaTG_HealthCheckEnabled_Default verifies that creating a
// lambda target group without HealthCheckEnabled defaults to false, whereas
// non-lambda TGs default to true.
func TestParity_LambdaTG_HealthCheckEnabled_Default(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		targetType        string
		explicitHCEnabled string
		wantHCEnabled     bool
	}{
		{
			name:          "lambda_default_disabled",
			targetType:    "lambda",
			wantHCEnabled: false,
		},
		{
			name:          "instance_default_enabled",
			targetType:    "instance",
			wantHCEnabled: true,
		},
		{
			name:          "ip_default_enabled",
			targetType:    "ip",
			wantHCEnabled: true,
		},
		{
			name:              "lambda_explicit_true_respected",
			targetType:        "lambda",
			explicitHCEnabled: "true",
			wantHCEnabled:     true,
		},
		{
			name:              "instance_explicit_false_respected",
			targetType:        "instance",
			explicitHCEnabled: "false",
			wantHCEnabled:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newParityBHandler()
			vals := url.Values{
				"Action":     {"CreateTargetGroup"},
				"Version":    {"2015-12-01"},
				"Name":       {"hc-tg"},
				"TargetType": {tc.targetType},
				"VpcId":      {"vpc-00000000"},
			}

			if tc.targetType != "lambda" {
				vals.Set("Protocol", "HTTP")
				vals.Set("Port", "80")
			}

			if tc.explicitHCEnabled != "" {
				vals.Set("HealthCheckEnabled", tc.explicitHCEnabled)
			}

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var resp struct {
				Result struct {
					TargetGroups struct {
						Members []struct {
							HealthCheckEnabled bool `xml:"HealthCheckEnabled"`
						} `xml:"member"`
					} `xml:"TargetGroups"`
				} `xml:"CreateTargetGroupResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.TargetGroups.Members, 1)
			assert.Equal(t, tc.wantHCEnabled, resp.Result.TargetGroups.Members[0].HealthCheckEnabled)
		})
	}
}
