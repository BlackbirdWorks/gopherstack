package elbv2_test

import (
	"encoding/xml"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateRuleWithConditions tests that rule conditions are stored and returned.
func TestCreateRuleWithConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		conditionVals url.Values
		wantField     string
	}{
		{
			name: "host_header",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                            {"host-header"},
				"Conditions.member.1.HostHeaderConfig.Values.member.1": {"example.com"},
				"Conditions.member.1.HostHeaderConfig.Values.member.2": {"*.example.com"},
			},
			wantField: "host-header",
		},
		{
			name: "path_pattern",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                             {"path-pattern"},
				"Conditions.member.1.PathPatternConfig.Values.member.1": {"/api/*"},
			},
			wantField: "path-pattern",
		},
		{
			name: "http_header",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                            {"http-header"},
				"Conditions.member.1.HttpHeaderConfig.HttpHeaderName":  {"X-Custom"},
				"Conditions.member.1.HttpHeaderConfig.Values.member.1": {"value1"},
			},
			wantField: "http-header",
		},
		{
			name: "http_request_method",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                                   {"http-request-method"},
				"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {"GET"},
			},
			wantField: "http-request-method",
		},
		{
			name: "source_ip",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                          {"source-ip"},
				"Conditions.member.1.SourceIpConfig.Values.member.1": {"10.0.0.0/8"},
			},
			wantField: "source-ip",
		},
		{
			name: "query_string",
			conditionVals: url.Values{
				"Conditions.member.1.Field":                                   {"query-string"},
				"Conditions.member.1.QueryStringConfig.Values.member.1.Key":   {"version"},
				"Conditions.member.1.QueryStringConfig.Values.member.1.Value": {"v2"},
			},
			wantField: "query-string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbArn := mustCreateLB(t, h, "cond-lb")
			tgArn := mustCreateTG(t, h, "cond-tg")
			listenerArn := mustCreateListener(t, h, lbArn, tgArn)

			vals := url.Values{
				"Action":                          {"CreateRule"},
				"Version":                         {"2015-12-01"},
				"ListenerArn":                     {listenerArn},
				"Priority":                        {"10"},
				"Actions.member.1.Type":           {"forward"},
				"Actions.member.1.TargetGroupArn": {tgArn},
			}
			maps.Copy(vals, tt.conditionVals)

			rec := doELBv2(t, h, vals)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Result struct {
					Rules struct {
						Members []struct {
							RuleArn    string `xml:"RuleArn"`
							Conditions struct {
								Members []struct {
									Field string `xml:"Field"`
								} `xml:"member"`
							} `xml:"Conditions"`
						} `xml:"member"`
					} `xml:"Rules"`
				} `xml:"CreateRuleResult"`
			}
			parseXMLBody(t, rec, &resp)
			require.Len(t, resp.Result.Rules.Members, 1)
			require.Len(t, resp.Result.Rules.Members[0].Conditions.Members, 1)
			assert.Equal(t, tt.wantField, resp.Result.Rules.Members[0].Conditions.Members[0].Field)
		})
	}
}

// TestModifyRuleWithConditions tests that ModifyRule updates conditions.
func TestModifyRuleWithConditions(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "modrule-cond-lb")
	tgArn := mustCreateTG(t, h, "modrule-cond-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	createRec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"5"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"host-header"},
		"Conditions.member.1.HostHeaderConfig.Values.member.1": {"old.example.com"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	parseXMLBody(t, createRec, &createResp)
	require.Len(t, createResp.Result.Rules.Members, 1)
	ruleArn := createResp.Result.Rules.Members[0].RuleArn

	modRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyRule"},
		"Version":                   {"2015-12-01"},
		"RuleArn":                   {ruleArn},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/v2/*"},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Conditions struct {
						Members []struct {
							Field             string `xml:"Field"`
							PathPatternConfig struct {
								Values struct {
									Members []struct {
										Value string `xml:",chardata"`
									} `xml:"member"`
								} `xml:"Values"`
							} `xml:"PathPatternConfig"`
						} `xml:"member"`
					} `xml:"Conditions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"ModifyRuleResult"`
	}
	parseXMLBody(t, modRec, &modResp)
	require.Len(t, modResp.Result.Rules.Members, 1)
	require.Len(t, modResp.Result.Rules.Members[0].Conditions.Members, 1)
	assert.Equal(t, "path-pattern", modResp.Result.Rules.Members[0].Conditions.Members[0].Field)
	require.Len(t, modResp.Result.Rules.Members[0].Conditions.Members[0].PathPatternConfig.Values.Members, 1)
	assert.Equal(
		t,
		"/v2/*",
		modResp.Result.Rules.Members[0].Conditions.Members[0].PathPatternConfig.Values.Members[0].Value,
	)
}

// TestHTTPRequestMethodConditionWhitelist verifies invalid HTTP methods are rejected.
func TestHTTPRequestMethodConditionWhitelist(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "method-whitelist-lb")
	tgArn := mustCreateTG(t, h, "method-whitelist-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	// Valid methods should work.
	for _, method := range []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS", "HEAD"} {
		rec := doELBv2(t, h, url.Values{
			"Action":                          {"CreateRule"},
			"Version":                         {"2015-12-01"},
			"ListenerArn":                     {listenerArn},
			"Priority":                        {"1"},
			"Actions.member.1.Type":           {"forward"},
			"Actions.member.1.TargetGroupArn": {tgArn},
			"Conditions.member.1.Field":       {"http-request-method"},
			"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {method},
		})
		assert.Equal(t, http.StatusOK, rec.Code, "method %q should be valid", method)

		// Delete the rule so we can reuse priority 1.
		var ruleResp struct {
			Result struct {
				Rules struct {
					Members []struct {
						RuleArn string `xml:"RuleArn"`
					} `xml:"member"`
				} `xml:"Rules"`
			} `xml:"CreateRuleResult"`
		}
		if err := xml.Unmarshal(rec.Body.Bytes(), &ruleResp); err == nil && len(ruleResp.Result.Rules.Members) > 0 {
			doELBv2(t, h, url.Values{
				"Action":  {"DeleteRule"},
				"Version": {"2015-12-01"},
				"RuleArn": {ruleResp.Result.Rules.Members[0].RuleArn},
			})
		}
	}

	// Invalid method should fail.
	rec := doELBv2(t, h, url.Values{
		"Action":                          {"CreateRule"},
		"Version":                         {"2015-12-01"},
		"ListenerArn":                     {listenerArn},
		"Priority":                        {"1"},
		"Actions.member.1.Type":           {"forward"},
		"Actions.member.1.TargetGroupArn": {tgArn},
		"Conditions.member.1.Field":       {"http-request-method"},
		"Conditions.member.1.HttpRequestMethodConfig.Values.member.1": {"INVALID"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRedirectActionParsed verifies redirect actions are stored and returned.
func TestRedirectActionParsed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "redirect-lb")
	tgArn := mustCreateTG(t, h, "redirect-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"10"},
		"Actions.member.1.Type": {"redirect"},
		"Actions.member.1.RedirectConfig.Protocol":              {"HTTPS"},
		"Actions.member.1.RedirectConfig.Port":                  {"443"},
		"Actions.member.1.RedirectConfig.StatusCode":            {"HTTP_301"},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type           string `xml:"Type"`
							RedirectConfig struct {
								Protocol   string `xml:"Protocol"`
								Port       string `xml:"Port"`
								StatusCode string `xml:"StatusCode"`
							} `xml:"RedirectConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	require.Len(t, resp.Result.Rules.Members[0].Actions.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "redirect", action.Type)
	assert.Equal(t, "HTTPS", action.RedirectConfig.Protocol)
	assert.Equal(t, "443", action.RedirectConfig.Port)
	assert.Equal(t, "HTTP_301", action.RedirectConfig.StatusCode)
}

// TestFixedResponseActionParsed verifies fixed-response actions are stored and returned.
func TestFixedResponseActionParsed(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "fixed-response-lb")
	tgArn := mustCreateTG(t, h, "fixed-response-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"20"},
		"Actions.member.1.Type": {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode":       {"404"},
		"Actions.member.1.FixedResponseConfig.MessageBody":      {"Not Found"},
		"Actions.member.1.FixedResponseConfig.ContentType":      {"text/plain"},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/missing"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                string `xml:"Type"`
							FixedResponseConfig struct {
								StatusCode  string `xml:"StatusCode"`
								MessageBody string `xml:"MessageBody"`
								ContentType string `xml:"ContentType"`
							} `xml:"FixedResponseConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "fixed-response", action.Type)
	assert.Equal(t, "404", action.FixedResponseConfig.StatusCode)
	assert.Equal(t, "Not Found", action.FixedResponseConfig.MessageBody)
	assert.Equal(t, "text/plain", action.FixedResponseConfig.ContentType)
}

// TestForwardWeightedTargetGroups verifies ForwardConfig with weighted target groups.
func TestForwardWeightedTargetGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "weighted-lb")
	tg1Arn := mustCreateTG(t, h, "weighted-tg1")
	tg2Arn := mustCreateTG(t, h, "weighted-tg2")
	listenerArn := mustCreateListener(t, h, lbArn, tg1Arn)

	rec := doELBv2(t, h, url.Values{
		"Action":                {"CreateRule"},
		"Version":               {"2015-12-01"},
		"ListenerArn":           {listenerArn},
		"Priority":              {"30"},
		"Actions.member.1.Type": {"forward"},
		"Actions.member.1.ForwardConfig.TargetGroups.member.1.TargetGroupArn": {tg1Arn},
		"Actions.member.1.ForwardConfig.TargetGroups.member.1.Weight":         {"80"},
		"Actions.member.1.ForwardConfig.TargetGroups.member.2.TargetGroupArn": {tg2Arn},
		"Actions.member.1.ForwardConfig.TargetGroups.member.2.Weight":         {"20"},
		"Conditions.member.1.Field":                                           {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":               {"/weighted"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type          string `xml:"Type"`
							ForwardConfig struct {
								TargetGroups struct {
									Members []struct {
										TargetGroupArn string `xml:"TargetGroupArn"`
										Weight         int32  `xml:"Weight"`
									} `xml:"member"`
								} `xml:"TargetGroups"`
							} `xml:"ForwardConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	action := resp.Result.Rules.Members[0].Actions.Members[0]
	assert.Equal(t, "forward", action.Type)
	require.Len(t, action.ForwardConfig.TargetGroups.Members, 2)
	assert.Equal(t, int32(80), action.ForwardConfig.TargetGroups.Members[0].Weight)
	assert.Equal(t, int32(20), action.ForwardConfig.TargetGroups.Members[1].Weight)
}

// TestAuthenticateCognitoAction verifies authenticate-cognito actions are stored and returned.
func TestAuthenticateCognitoAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "cognito-lb")
	tgArn := mustCreateTG(t, h, "cognito-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"CreateRule"},
		"Version":                {"2015-12-01"},
		"ListenerArn":            {listenerArn},
		"Priority":               {"40"},
		"Actions.member.1.Type":  {"authenticate-cognito"},
		"Actions.member.1.Order": {"1"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolArn": {
			"arn:aws:cognito-idp:us-east-1:123:userpool/us-east-1_abc",
		},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolClientId":         {"client123"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolDomain":           {"auth.example.com"},
		"Actions.member.1.AuthenticateCognitoConfig.OnUnauthenticatedRequest": {"deny"},
		"Actions.member.2.Type":                                 {"forward"},
		"Actions.member.2.Order":                                {"2"},
		"Actions.member.2.TargetGroupArn":                       {tgArn},
		"Conditions.member.1.Field":                             {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/auth"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                      string `xml:"Type"`
							AuthenticateCognitoConfig struct {
								UserPoolArn              string `xml:"UserPoolArn"`
								UserPoolClientID         string `xml:"UserPoolClientId"`
								UserPoolDomain           string `xml:"UserPoolDomain"`
								OnUnauthenticatedRequest string `xml:"OnUnauthenticatedRequest"`
							} `xml:"AuthenticateCognitoConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	actions := resp.Result.Rules.Members[0].Actions.Members
	require.Len(t, actions, 2)

	cognitoAction := actions[0]
	assert.Equal(t, "authenticate-cognito", cognitoAction.Type)
	assert.Equal(
		t,
		"arn:aws:cognito-idp:us-east-1:123:userpool/us-east-1_abc",
		cognitoAction.AuthenticateCognitoConfig.UserPoolArn,
	)
	assert.Equal(t, "client123", cognitoAction.AuthenticateCognitoConfig.UserPoolClientID)
	assert.Equal(t, "deny", cognitoAction.AuthenticateCognitoConfig.OnUnauthenticatedRequest)
}

// TestAuthenticateOidcAction verifies authenticate-oidc actions are stored and returned.
func TestAuthenticateOidcAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	lbArn := mustCreateLB(t, h, "oidc-lb")
	tgArn := mustCreateTG(t, h, "oidc-tg")
	listenerArn := mustCreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                 {"CreateRule"},
		"Version":                {"2015-12-01"},
		"ListenerArn":            {listenerArn},
		"Priority":               {"50"},
		"Actions.member.1.Type":  {"authenticate-oidc"},
		"Actions.member.1.Order": {"1"},
		"Actions.member.1.AuthenticateOidcConfig.Issuer":                {"https://idp.example.com"},
		"Actions.member.1.AuthenticateOidcConfig.AuthorizationEndpoint": {"https://idp.example.com/auth"},
		"Actions.member.1.AuthenticateOidcConfig.TokenEndpoint":         {"https://idp.example.com/token"},
		"Actions.member.1.AuthenticateOidcConfig.UserInfoEndpoint":      {"https://idp.example.com/userinfo"},
		"Actions.member.1.AuthenticateOidcConfig.ClientId":              {"myapp"},
		"Actions.member.1.AuthenticateOidcConfig.ClientSecret":          {"secret"},
		"Actions.member.2.Type":                                         {"forward"},
		"Actions.member.2.Order":                                        {"2"},
		"Actions.member.2.TargetGroupArn":                               {tgArn},
		"Conditions.member.1.Field":                                     {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":         {"/oidc"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			Rules struct {
				Members []struct {
					Actions struct {
						Members []struct {
							Type                   string `xml:"Type"`
							AuthenticateOidcConfig struct {
								Issuer                string `xml:"Issuer"`
								AuthorizationEndpoint string `xml:"AuthorizationEndpoint"`
								ClientID              string `xml:"ClientId"`
							} `xml:"AuthenticateOidcConfig"`
						} `xml:"member"`
					} `xml:"Actions"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Rules.Members, 1)
	actions := resp.Result.Rules.Members[0].Actions.Members
	require.Len(t, actions, 2)
	assert.Equal(t, "authenticate-oidc", actions[0].Type)
	assert.Equal(t, "https://idp.example.com", actions[0].AuthenticateOidcConfig.Issuer)
	assert.Equal(t, "myapp", actions[0].AuthenticateOidcConfig.ClientID)
}

func TestCreateRule_PathPattern(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-path-lb")
	tgArn := b1CreateTG(t, h, "rule-path-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"10"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/api/*"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "/api/*")
}

func TestCreateRule_HostHeader(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-host-lb")
	tgArn := b1CreateTG(t, h, "rule-host-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"20"},
		"Conditions.member.1.Field": {"host-header"},
		"Conditions.member.1.HostHeaderConfig.Values.member.1": {"api.example.com"},
		"Actions.member.1.Type":                                {"forward"},
		"Actions.member.1.TargetGroupArn":                      {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "api.example.com")
}

func TestCreateRule_RedirectAction(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-redir-lb")
	tgArn := b1CreateTG(t, h, "rule-redir-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"30"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old/*"},
		"Actions.member.1.Type":                                 {"redirect"},
		"Actions.member.1.RedirectConfig.StatusCode":            {"HTTP_301"},
		"Actions.member.1.RedirectConfig.Protocol":              {"HTTPS"},
		"Actions.member.1.RedirectConfig.Port":                  {"443"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "redirect")
	assert.Contains(t, body, "HTTP_301")
}

func TestCreateRule_FixedResponse(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-fixed-lb")
	tgArn := b1CreateTG(t, h, "rule-fixed-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"40"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/health"},
		"Actions.member.1.Type":                                 {"fixed-response"},
		"Actions.member.1.FixedResponseConfig.StatusCode":       {"200"},
		"Actions.member.1.FixedResponseConfig.ContentType":      {"application/json"},
		"Actions.member.1.FixedResponseConfig.MessageBody":      {`{"status":"ok"}`},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "fixed-response")
	assert.Contains(t, body, "200")
}

func TestCreateRule_AuthenticateCognito(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-cognito-lb")
	tgArn := b1CreateTG(t, h, "rule-cognito-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"50"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/secure/*"},
		"Actions.member.1.Type":                                 {"authenticate-cognito"},
		"Actions.member.1.Order":                                {"1"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolArn": {
			"arn:aws:cognito-idp:us-east-1:000:userpool/us-east-1_abc",
		},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolClientId": {"clientid123"},
		"Actions.member.1.AuthenticateCognitoConfig.UserPoolDomain":   {"my-pool.auth.us-east-1.amazoncognito.com"},
		"Actions.member.2.Type":           {"forward"},
		"Actions.member.2.Order":          {"2"},
		"Actions.member.2.TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "authenticate-cognito")
}

func TestCreateRule_AuthenticateOIDC(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "rule-oidc-lb")
	tgArn := b1CreateTG(t, h, "rule-oidc-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"60"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1":         {"/oidc/*"},
		"Actions.member.1.Type":                                         {"authenticate-oidc"},
		"Actions.member.1.Order":                                        {"1"},
		"Actions.member.1.AuthenticateOidcConfig.Issuer":                {"https://idp.example.com"},
		"Actions.member.1.AuthenticateOidcConfig.AuthorizationEndpoint": {"https://idp.example.com/auth"},
		"Actions.member.1.AuthenticateOidcConfig.TokenEndpoint":         {"https://idp.example.com/token"},
		"Actions.member.1.AuthenticateOidcConfig.UserInfoEndpoint":      {"https://idp.example.com/userinfo"},
		"Actions.member.1.AuthenticateOidcConfig.ClientId":              {"client-abc"},
		"Actions.member.1.AuthenticateOidcConfig.ClientSecret":          {"secret-xyz"},
		"Actions.member.2.Type":                                         {"forward"},
		"Actions.member.2.Order":                                        {"2"},
		"Actions.member.2.TargetGroupArn":                               {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "authenticate-oidc")
}

func TestModifyRule_ChangeConditions(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	lbArn := b1CreateLB(t, h, "mod-rule-lb")
	tgArn := b1CreateTG(t, h, "mod-rule-tg")
	lArn := b1CreateListener(t, h, lbArn, tgArn)

	cRec := doELBv2(t, h, url.Values{
		"Action":                    {"CreateRule"},
		"Version":                   {"2015-12-01"},
		"ListenerArn":               {lArn},
		"Priority":                  {"200"},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/old"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, cRec.Code)

	var cResp struct {
		Result struct {
			Rules struct {
				Members []struct {
					RuleArn string `xml:"RuleArn"`
				} `xml:"member"`
			} `xml:"Rules"`
		} `xml:"CreateRuleResult"`
	}
	require.NoError(t, xml.Unmarshal(cRec.Body.Bytes(), &cResp))
	ruleArn := cResp.Result.Rules.Members[0].RuleArn

	mRec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyRule"},
		"Version":                   {"2015-12-01"},
		"RuleArn":                   {ruleArn},
		"Conditions.member.1.Field": {"path-pattern"},
		"Conditions.member.1.PathPatternConfig.Values.member.1": {"/new"},
		"Actions.member.1.Type":                                 {"forward"},
		"Actions.member.1.TargetGroupArn":                       {tgArn},
	})
	require.Equal(t, http.StatusOK, mRec.Code)
	assert.Contains(t, mRec.Body.String(), "/new")
}
