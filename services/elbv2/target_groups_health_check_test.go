package elbv2_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestModifyTargetGroupAttributes tests target group attribute modification.
func TestModifyTargetGroupAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "attrs-tg")

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "success",
			vals: url.Values{
				"Action":         {"ModifyTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			vals: url.Values{
				"Action":  {"ModifyTargetGroupAttributes"},
				"Version": {"2015-12-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			vals: url.Values{
				"Action":         {"ModifyTargetGroupAttributes"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {"arn:aws:elasticloadbalancing:us-east-1:123456789012:targetgroup/no-such/0"},
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

// TestTargetGroupMatcherPersisted verifies Matcher (HTTPCode/GrpcCode) is stored and returned.
func TestTargetGroupMatcherPersisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateTargetGroup"},
		"Version":          {"2015-12-01"},
		"Name":             {"matcher-tg"},
		"Protocol":         {"HTTP"},
		"Port":             {"80"},
		"VpcId":            {"vpc-00000000"},
		"Matcher.HTTPCode": {"200-299"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
					Matcher        struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "200-299", resp.Result.TargetGroups.Members[0].Matcher.HTTPCode)
}

// TestTargetGroupGrpcMatcherPersisted verifies GrpcCode matcher is stored.
func TestTargetGroupGrpcMatcherPersisted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":           {"CreateTargetGroup"},
		"Version":          {"2015-12-01"},
		"Name":             {"grpc-matcher-tg"},
		"Protocol":         {"HTTP"},
		"Port":             {"80"},
		"VpcId":            {"vpc-00000000"},
		"Matcher.GrpcCode": {"0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					TargetGroupArn string `xml:"TargetGroupArn"`
					Matcher        struct {
						GrpcCode string `xml:"GrpcCode"`
					} `xml:"Matcher"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "0", resp.Result.TargetGroups.Members[0].Matcher.GrpcCode)
}

// TestCrossZoneLoadBalancingDefault verifies cross-zone load balancing
// defaults to enabled, surfaced the real way -- as the
// "load_balancing.cross_zone.enabled" DescribeTargetGroupAttributes
// attribute (elasticloadbalancingv2@v1.58.5 deserializers.go). Real AWS's
// TargetGroup type (returned by DescribeTargetGroups) has no
// CrossZoneLoadBalancing member at all; gopherstack previously also emitted
// one directly on DescribeTargetGroups, a fabricated field a real client's
// decoder silently drops -- confirmed by hand-reverting: this test's
// predecessor asserted that fabricated field as correct.
func TestCrossZoneLoadBalancingDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "cz-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
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
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	found := false
	for _, m := range resp.Result.Attributes.Members {
		if m.Key == "load_balancing.cross_zone.enabled" {
			found = true
			assert.Equal(t, "true", m.Value)
		}
	}
	assert.True(t, found, "load_balancing.cross_zone.enabled attribute should be present")
}

// TestModifyTargetGroupAttributesPersists verifies deregistration_delay is persisted.
func TestModifyTargetGroupAttributesPersists(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "tg-attrs-persist")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"deregistration_delay.timeout_seconds"},
		"Attributes.member.1.Value": {"120"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeTargetGroupAttributes.
	descRec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
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
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(descRec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "120", attrMap["deregistration_delay.timeout_seconds"])
}

// TestTargetGroupDefaultAttributesOnCreate verifies defaults are set at creation time.
func TestTargetGroupDefaultAttributesOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "default-attrs-tg")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
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
		} `xml:"DescribeTargetGroupAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	attrMap := make(map[string]string)
	for _, a := range resp.Result.Attributes.Members {
		attrMap[a.Key] = a.Value
	}
	assert.Equal(t, "300", attrMap["deregistration_delay.timeout_seconds"])
	assert.Equal(t, "false", attrMap["stickiness.enabled"])
	assert.Equal(t, "round_robin", attrMap["load_balancing.algorithm.type"])
}

// TestHealthCheckDefaults tests that health check defaults are applied.
func TestHealthCheckDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Create TG without specifying health check values.
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"hc-defaults-tg"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					Matcher struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
					HealthCheckIntervalSeconds int32 `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32 `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32 `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32 `xml:"UnhealthyThresholdCount"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, int32(30), tg.HealthCheckIntervalSeconds, "default interval should be 30")
	assert.Equal(t, int32(5), tg.HealthCheckTimeoutSeconds, "default timeout should be 5")
	assert.Equal(t, int32(3), tg.HealthyThresholdCount, "default healthy threshold should be 3")
	assert.Equal(t, int32(3), tg.UnhealthyThresholdCount, "default unhealthy threshold should be 3")
	assert.Equal(t, "200", tg.Matcher.HTTPCode, "default HTTP matcher should be 200")
}

// TestHealthCheckDefaultsCustomValues tests that explicit health check values are preserved.
func TestHealthCheckDefaultsCustomValues(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":                     {"CreateTargetGroup"},
		"Version":                    {"2015-12-01"},
		"Name":                       {"hc-custom-tg"},
		"Protocol":                   {"HTTP"},
		"Port":                       {"80"},
		"VpcId":                      {"vpc-00000000"},
		"HealthCheckIntervalSeconds": {"60"},
		"HealthCheckTimeoutSeconds":  {"10"},
		"HealthyThresholdCount":      {"5"},
		"UnhealthyThresholdCount":    {"2"},
		"Matcher.HTTPCode":           {"200-299"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					Matcher struct {
						HTTPCode string `xml:"HTTPCode"`
					} `xml:"Matcher"`
					HealthCheckIntervalSeconds int32 `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32 `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32 `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32 `xml:"UnhealthyThresholdCount"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)

	tg := resp.Result.TargetGroups.Members[0]
	assert.Equal(t, int32(60), tg.HealthCheckIntervalSeconds)
	assert.Equal(t, int32(10), tg.HealthCheckTimeoutSeconds)
	assert.Equal(t, int32(5), tg.HealthyThresholdCount)
	assert.Equal(t, int32(2), tg.UnhealthyThresholdCount)
	assert.Equal(t, "200-299", tg.Matcher.HTTPCode)
}

// TestProtocolVersion tests that ProtocolVersion is stored and returned.
func TestProtocolVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"pv-tg"},
		"Protocol":        {"HTTP"},
		"ProtocolVersion": {"HTTP2"},
		"Port":            {"80"},
		"VpcId":           {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					ProtocolVersion string `xml:"ProtocolVersion"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	assert.Equal(t, "HTTP2", resp.Result.TargetGroups.Members[0].ProtocolVersion)
}

// TestModifyTargetGroupHealthCheckEnabledOptional verifies that omitting HealthCheckEnabled
// in ModifyTargetGroup does not overwrite the stored value.
func TestModifyTargetGroupHealthCheckEnabledOptional(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "hce-optional-tg")

	// Enable health checks explicitly on creation.
	doELBv2(t, h, url.Values{
		"Action":             {"ModifyTargetGroup"},
		"Version":            {"2015-12-01"},
		"TargetGroupArn":     {tgArn},
		"HealthCheckEnabled": {"true"},
	})

	// Now modify only the path — HealthCheckEnabled must remain true.
	doELBv2(t, h, url.Values{
		"Action":          {"ModifyTargetGroup"},
		"Version":         {"2015-12-01"},
		"TargetGroupArn":  {tgArn},
		"HealthCheckPath": {"/healthz"},
	})

	rec := doELBv2(t, h, url.Values{
		"Action":                   {"DescribeTargetGroups"},
		"Version":                  {"2015-12-01"},
		"TargetGroupArns.member.1": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					HealthCheckPath    string `xml:"HealthCheckPath"`
					HealthCheckEnabled bool   `xml:"HealthCheckEnabled"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"DescribeTargetGroupsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TargetGroups.Members, 1)
	tg := resp.Result.TargetGroups.Members[0]
	assert.True(t, tg.HealthCheckEnabled, "HealthCheckEnabled must not be overwritten by absent param")
	assert.Equal(t, "/healthz", tg.HealthCheckPath)
}

// TestHealthCheckInvalidNumericParams verifies that non-numeric health check params return errors.
func TestHealthCheckInvalidNumericParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	tgArn := mustCreateTG(t, h, "hc-num-tg")

	tests := []struct {
		name  string
		param string
	}{
		{"invalid_interval", "HealthCheckIntervalSeconds"},
		{"invalid_timeout", "HealthCheckTimeoutSeconds"},
		{"invalid_healthy", "HealthyThresholdCount"},
		{"invalid_unhealthy", "UnhealthyThresholdCount"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doELBv2(t, h, url.Values{
				"Action":         {"ModifyTargetGroup"},
				"Version":        {"2015-12-01"},
				"TargetGroupArn": {tgArn},
				tt.param:         {"not-a-number"},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestHealthCheckPathValidation verifies that HealthCheckPath without a leading slash is rejected.
func TestHealthCheckPathValidation(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	t.Run("missing_leading_slash_rejected", func(t *testing.T) {
		t.Parallel()

		rec := doELBv2(t, h, url.Values{
			"Action":          {"CreateTargetGroup"},
			"Version":         {"2015-12-01"},
			"Name":            {"hc-path-bad"},
			"Protocol":        {"HTTP"},
			"Port":            {"80"},
			"VpcId":           {"vpc-00000000"},
			"HealthCheckPath": {"health"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("leading_slash_ok", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler()
		rec := doELBv2(t, h2, url.Values{
			"Action":          {"CreateTargetGroup"},
			"Version":         {"2015-12-01"},
			"Name":            {"hc-path-good"},
			"Protocol":        {"HTTP"},
			"Port":            {"80"},
			"VpcId":           {"vpc-00000000"},
			"HealthCheckPath": {"/health"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("modify_missing_slash_rejected", func(t *testing.T) {
		t.Parallel()

		h3 := newTestHandler()
		tgArn := mustCreateTG(t, h3, "hc-path-modify")
		rec := doELBv2(t, h3, url.Values{
			"Action":          {"ModifyTargetGroup"},
			"Version":         {"2015-12-01"},
			"TargetGroupArn":  {tgArn},
			"HealthCheckPath": {"noslash"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestCreateTG_DefaultHealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":   {"CreateTargetGroup"},
		"Version":  {"2015-12-01"},
		"Name":     {"tg-hc-defaults"},
		"Protocol": {"HTTP"},
		"Port":     {"80"},
		"VpcId":    {"vpc-00000000"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result struct {
			TargetGroups struct {
				Members []struct {
					HealthCheckProtocol        string `xml:"HealthCheckProtocol"`
					HealthCheckIntervalSeconds int32  `xml:"HealthCheckIntervalSeconds"`
					HealthCheckTimeoutSeconds  int32  `xml:"HealthCheckTimeoutSeconds"`
					HealthyThresholdCount      int32  `xml:"HealthyThresholdCount"`
					UnhealthyThresholdCount    int32  `xml:"UnhealthyThresholdCount"`
					HealthCheckEnabled         bool   `xml:"HealthCheckEnabled"`
				} `xml:"member"`
			} `xml:"TargetGroups"`
		} `xml:"CreateTargetGroupResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	tg := resp.Result.TargetGroups.Members[0]
	assert.True(t, tg.HealthCheckEnabled)
	assert.Equal(t, "HTTP", tg.HealthCheckProtocol)
	assert.Equal(t, int32(30), tg.HealthCheckIntervalSeconds)
	assert.Equal(t, int32(5), tg.HealthCheckTimeoutSeconds)
	assert.Equal(t, int32(3), tg.HealthyThresholdCount)
	assert.Equal(t, int32(3), tg.UnhealthyThresholdCount)
}

func TestCreateTG_CustomHealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":                     {"CreateTargetGroup"},
		"Version":                    {"2015-12-01"},
		"Name":                       {"tg-hc-custom"},
		"Protocol":                   {"HTTP"},
		"Port":                       {"80"},
		"VpcId":                      {"vpc-00000000"},
		"HealthCheckPath":            {"/healthz"},
		"HealthCheckIntervalSeconds": {"10"},
		"HealthCheckTimeoutSeconds":  {"3"},
		"HealthyThresholdCount":      {"3"},
		"UnhealthyThresholdCount":    {"2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "/healthz")
	assert.Contains(t, body, "10")
}

func TestModifyTG_HealthCheck(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "modify-tg-hc")

	rec := doELBv2(t, h, url.Values{
		"Action":                     {"ModifyTargetGroup"},
		"Version":                    {"2015-12-01"},
		"TargetGroupArn":             {tgArn},
		"HealthCheckPath":            {"/api/health"},
		"HealthCheckIntervalSeconds": {"15"},
		"HealthyThresholdCount":      {"4"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "/api/health")
	assert.Contains(t, body, "15")
}

func TestTGAttributes_DefaultStickiness(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "tg-sticky-default")

	rec := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "stickiness.enabled")
	assert.Contains(t, body, "deregistration_delay.timeout_seconds")
}

func TestTGAttributes_EnableStickiness(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	tgArn := b1CreateTG(t, h, "tg-sticky-enable")

	rec := doELBv2(t, h, url.Values{
		"Action":                    {"ModifyTargetGroupAttributes"},
		"Version":                   {"2015-12-01"},
		"TargetGroupArn":            {tgArn},
		"Attributes.member.1.Key":   {"stickiness.enabled"},
		"Attributes.member.1.Value": {"true"},
		"Attributes.member.2.Key":   {"stickiness.type"},
		"Attributes.member.2.Value": {"lb_cookie"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELBv2(t, h, url.Values{
		"Action":         {"DescribeTargetGroupAttributes"},
		"Version":        {"2015-12-01"},
		"TargetGroupArn": {tgArn},
	})
	body := rec2.Body.String()
	assert.Contains(t, body, "stickiness.enabled")
	assert.Contains(t, body, "lb_cookie")
}

func TestCreateTG_ProtocolVersionHTTP2(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"tg-http2"},
		"Protocol":        {"HTTP"},
		"Port":            {"80"},
		"VpcId":           {"vpc-00000000"},
		"ProtocolVersion": {"HTTP2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "HTTP2")
}

func TestCreateTG_ProtocolVersionGRPC(t *testing.T) {
	t.Parallel()

	h := newBatch1Handler()
	rec := doELBv2(t, h, url.Values{
		"Action":          {"CreateTargetGroup"},
		"Version":         {"2015-12-01"},
		"Name":            {"tg-grpc"},
		"Protocol":        {"HTTP"},
		"Port":            {"50051"},
		"VpcId":           {"vpc-00000000"},
		"ProtocolVersion": {"GRPC"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "GRPC")
}

// TestLambdaTG_HealthCheckEnabled_Default verifies that creating a
// lambda target group without HealthCheckEnabled defaults to false, whereas
// non-lambda TGs default to true.
func TestLambdaTG_HealthCheckEnabled_Default(t *testing.T) {
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
