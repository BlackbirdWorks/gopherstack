package elb_test

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elb"
)

// TestRegisterAndDeregisterInstances tests instance registration.
func TestRegisterAndDeregisterInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "register_instances",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "reg-lb")
			},
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"reg-lb"},
				"Instances.member.1.InstanceId": {"i-aaa11100"},
				"Instances.member.2.InstanceId": {"i-bbb22200"},
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "register_idempotent",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "idem-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"idem-lb"},
					"Instances.member.1.InstanceId": {"i-abc00000"},
				})
			},
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"idem-lb"},
				"Instances.member.1.InstanceId": {"i-abc00000"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "register_lb_not_found",
			vals: url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"no-lb"},
				"Instances.member.1.InstanceId": {"i-aaa00000"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "register_missing_name",
			vals: url.Values{
				"Action":  {"RegisterInstancesWithLoadBalancer"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "deregister_instances",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dereg-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"dereg-lb"},
					"Instances.member.1.InstanceId": {"i-11100000"},
					"Instances.member.2.InstanceId": {"i-22200000"},
				})
			},
			vals: url.Values{
				"Action":                        {"DeregisterInstancesFromLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"dereg-lb"},
				"Instances.member.1.InstanceId": {"i-11100000"},
			},
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
		{
			name: "deregister_lb_not_found",
			vals: url.Values{
				"Action":                        {"DeregisterInstancesFromLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"no-lb"},
				"Instances.member.1.InstanceId": {"i-aaa00000"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "deregister_missing_name",
			vals: url.Values{
				"Action":  {"DeregisterInstancesFromLoadBalancer"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeInstanceHealth tests the health state of registered instances.
func TestDescribeInstanceHealth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *elb.Handler)
		vals         url.Values
		name         string
		wantStatus   int
		wantStateLen int
	}{
		{
			name: "all_instances_inservice",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"health-lb"},
					"Instances.member.1.InstanceId": {"i-aaa00000"},
					"Instances.member.2.InstanceId": {"i-bbb00000"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeInstanceHealth"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"health-lb"},
			},
			wantStatus:   http.StatusOK,
			wantStateLen: 2,
		},
		{
			name: "specific_instance_health",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health2-lb")
				doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"health2-lb"},
					"Instances.member.1.InstanceId": {"i-abcdef01"},
				})
			},
			vals: url.Values{
				"Action":                        {"DescribeInstanceHealth"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"health2-lb"},
				"Instances.member.1.InstanceId": {"i-abcdef01"},
			},
			wantStatus:   http.StatusOK,
			wantStateLen: 1,
		},
		{
			name: "unregistered_instance_returns_invalid_instance",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "health3-lb")
			},
			vals: url.Values{
				"Action":                        {"DescribeInstanceHealth"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"health3-lb"},
				"Instances.member.1.InstanceId": {"i-dead0000"},
			},
			wantStatus:   http.StatusBadRequest,
			wantStateLen: 0,
		},
		{
			name: "lb_not_found",
			vals: url.Values{
				"Action":           {"DescribeInstanceHealth"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"no-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_lb_name",
			vals: url.Values{
				"Action":  {"DescribeInstanceHealth"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStateLen > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
					Result  struct {
						InstanceStates struct {
							Members []struct {
								InstanceID string `xml:"InstanceId"`
								State      string `xml:"State"`
							} `xml:"member"`
						} `xml:"InstanceStates"`
					} `xml:"DescribeInstanceHealthResult"`
				}
				parseXMLBody(t, rec, &resp)
				assert.Len(t, resp.Result.InstanceStates.Members, tt.wantStateLen)
			}
		})
	}
}

func TestInstanceHealthMultipleInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "ih-multi-lb")

	for _, id := range []string{"i-aaaaaaaa01", "i-bbbbbbbb02", "i-cccccccc03"} {
		doELB(t, h, url.Values{
			"Action":                        {"RegisterInstancesWithLoadBalancer"},
			"Version":                       {"2012-06-01"},
			"LoadBalancerName":              {"ih-multi-lb"},
			"Instances.member.1.InstanceId": {id},
		})
	}

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeInstanceHealth"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"ih-multi-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
		Result  struct {
			InstanceStates struct {
				Members []struct {
					InstanceID  string `xml:"InstanceId"`
					State       string `xml:"State"`
					ReasonCode  string `xml:"ReasonCode"`
					Description string `xml:"Description"`
				} `xml:"member"`
			} `xml:"InstanceStates"`
		} `xml:"DescribeInstanceHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.InstanceStates.Members, 3)

	for _, s := range resp.Result.InstanceStates.Members {
		assert.Equal(t, "InService", s.State)
		assert.Equal(t, "N/A", s.ReasonCode)
		assert.Equal(t, "N/A", s.Description)
	}
}

func TestInstanceHealthFilterByInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "ih-filter-lb")

	for _, id := range []string{"i-aaaaaaaa01", "i-bbbbbbbb02"} {
		doELB(t, h, url.Values{
			"Action":                        {"RegisterInstancesWithLoadBalancer"},
			"Version":                       {"2012-06-01"},
			"LoadBalancerName":              {"ih-filter-lb"},
			"Instances.member.1.InstanceId": {id},
		})
	}

	rec := doELB(t, h, url.Values{
		"Action":                        {"DescribeInstanceHealth"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"ih-filter-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
		Result  struct {
			InstanceStates struct {
				Members []struct {
					InstanceID string `xml:"InstanceId"`
				} `xml:"member"`
			} `xml:"InstanceStates"`
		} `xml:"DescribeInstanceHealthResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.InstanceStates.Members, 1)
	assert.Equal(t, "i-aaaaaaaa01", resp.Result.InstanceStates.Members[0].InstanceID)
}

func TestInstanceHealthDeregisterAndCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "ih-dereg-lb")

	doELB(t, h, url.Values{
		"Action":                        {"RegisterInstancesWithLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"ih-dereg-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})

	doELB(t, h, url.Values{
		"Action":                        {"DeregisterInstancesFromLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"ih-dereg-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})

	// After deregister, requesting health of that instance should fail.
	rec := doELB(t, h, url.Values{
		"Action":                        {"DescribeInstanceHealth"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"ih-dereg-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRegisterInstancesIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "reg-idem-lb")

	doELB(t, h, url.Values{
		"Action":                        {"RegisterInstancesWithLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"reg-idem-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})

	// Register same instance again.
	rec := doELB(t, h, url.Values{
		"Action":                        {"RegisterInstancesWithLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"reg-idem-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"RegisterInstancesWithLoadBalancerResponse"`
		Result  struct {
			Instances struct {
				Members []struct {
					InstanceID string `xml:"InstanceId"`
				} `xml:"member"`
			} `xml:"Instances"`
		} `xml:"RegisterInstancesWithLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Instances.Members, 1, "idempotent register must not duplicate")
}

func TestDeregisterInstancesNonRegisteredIsNoOp(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "dereg-noop-lb")

	rec := doELB(t, h, url.Values{
		"Action":                        {"DeregisterInstancesFromLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"dereg-noop-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDescribeInstanceHealthInvalidFormat verifies that
// DescribeInstanceHealth rejects malformed instance IDs before checking
// registration status.
func TestDescribeInstanceHealthInvalidFormat(t *testing.T) {
	t.Parallel()

	const validID = "i-1234567890abcdef0"

	tests := []struct {
		instanceID string
		name       string
		wantStatus int
		register   bool // whether to pre-register the instance
	}{
		// Valid format + registered → 200.
		{validID, "valid_registered_id_ok", http.StatusOK, true},
		// Valid format + NOT registered → 400 (InvalidInstance: not registered).
		// This is correct AWS behavior; format check passes but registration check fails.
		{validID, "valid_unregistered_id_rejected", http.StatusBadRequest, false},
		// Invalid format → 400 (format error, before registration check).
		{"not-an-instance", "invalid_format_rejected", http.StatusBadRequest, false},
		{"ec2-i-1234567890", "wrong_prefix_rejected", http.StatusBadRequest, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "health-fmt-lb")

			if tt.register {
				rec := doELB(t, h, url.Values{
					"Action":                        {"RegisterInstancesWithLoadBalancer"},
					"Version":                       {"2012-06-01"},
					"LoadBalancerName":              {"health-fmt-lb"},
					"Instances.member.1.InstanceId": {tt.instanceID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doELB(t, h, url.Values{
				"Action":                        {"DescribeInstanceHealth"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"health-fmt-lb"},
				"Instances.member.1.InstanceId": {tt.instanceID},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeInstanceHealthSortedByID verifies that DescribeInstanceHealth
// returns states sorted by instance ID when no specific instances are requested.
func TestDescribeInstanceHealthSortedByID(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "health-sort-lb")

	for _, id := range []string{"i-cccccccc00", "i-aaaaaaaabb", "i-bbbbbbbbcc"} {
		doELB(t, h, url.Values{
			"Action":                        {"RegisterInstancesWithLoadBalancer"},
			"Version":                       {"2012-06-01"},
			"LoadBalancerName":              {"health-sort-lb"},
			"Instances.member.1.InstanceId": {id},
		})
	}

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeInstanceHealth"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"health-sort-lb"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeInstanceHealthResponse"`
		Result  struct {
			InstanceStates struct {
				Members []struct {
					InstanceID string `xml:"InstanceId"`
				} `xml:"member"`
			} `xml:"InstanceStates"`
		} `xml:"DescribeInstanceHealthResult"`
	}

	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	ids := make([]string, 0, 3)
	for _, s := range resp.Result.InstanceStates.Members {
		ids = append(ids, s.InstanceID)
	}

	assert.Equal(t, []string{"i-aaaaaaaabb", "i-bbbbbbbbcc", "i-cccccccc00"}, ids)
}

// TestRegisterInstancesInvalidIDFormat verifies that invalid instance IDs are rejected.
func TestRegisterInstancesInvalidIDFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		instanceID string
		wantStatus int
	}{
		{name: "invalid_format_rejected", instanceID: "invalid-id", wantStatus: http.StatusBadRequest},
		{name: "uppercase_rejected", instanceID: "i-ABCDEF01", wantStatus: http.StatusBadRequest},
		{name: "too_short_rejected", instanceID: "i-abc123", wantStatus: http.StatusBadRequest},
		{name: "valid_8_char_accepted", instanceID: "i-abcdef01", wantStatus: http.StatusOK},
		{name: "valid_17_char_accepted", instanceID: "i-abcdef0123456789a", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			h := elb.NewHandler(b)
			mustCreateLB(t, h, "inst-id-lb")

			rec := doELB(t, h, url.Values{
				"Action":                        {"RegisterInstancesWithLoadBalancer"},
				"Version":                       {"2012-06-01"},
				"LoadBalancerName":              {"inst-id-lb"},
				"Instances.member.1.InstanceId": {tt.instanceID},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDescribeInstanceHealthInvalidInstance verifies ErrInvalidInstance
// is returned when requesting health for an instance not registered with the LB.
func TestDescribeInstanceHealthInvalidInstance(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "health-inv-lb")

	rec := doELB(t, h, url.Values{
		"Action":                        {"DescribeInstanceHealth"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"health-inv-lb"},
		"Instances.member.1.InstanceId": {"i-abcdef01"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidInstance", errResp.Error.Code)
}
