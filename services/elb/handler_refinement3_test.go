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

// TestRefinement3_PersistenceRoundTrip verifies that Snapshot/Restore preserves
// BackendServerDescriptions and all other load balancer fields.
func TestRefinement3_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(t *testing.T, b *elb.InMemoryBackend)
		name          string
		lbName        string
		wantBSDPorts  []int32
		wantListeners int
		wantInstances int
	}{
		{
			name:   "bsd_preserved_across_snapshot_restore",
			lbName: "persist-test-lb",
			setup: func(t *testing.T, b *elb.InMemoryBackend) {
				t.Helper()
				h := elb.NewHandler(b)

				// Create LB with a listener
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"persist-test-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				// Create a policy
				doELB(t, h, url.Values{
					"Action":           {"CreateLBCookieStickinessPolicy"},
					"Version":          {"2012-06-01"},
					"LoadBalancerName": {"persist-test-lb"},
					"PolicyName":       {"my-lb-cookie"},
				})

				// Set backend server descriptions
				doELB(t, h, url.Values{
					"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
					"Version":              {"2012-06-01"},
					"LoadBalancerName":     {"persist-test-lb"},
					"InstancePort":         {"8080"},
					"PolicyNames.member.1": {"my-lb-cookie"},
				})
			},
			wantBSDPorts:  []int32{8080},
			wantListeners: 1,
		},
		{
			name:   "empty_lb_restored_with_non_nil_slices",
			lbName: "empty-lb",
			setup: func(t *testing.T, b *elb.InMemoryBackend) {
				t.Helper()
				h := elb.NewHandler(b)
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"empty-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
			},
			wantBSDPorts:  []int32{},
			wantListeners: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			if tt.setup != nil {
				tt.setup(t, backend)
			}

			// Snapshot and restore into a new backend.
			snap := backend.Snapshot()
			require.NotNil(t, snap)

			restored := elb.NewInMemoryBackend("", "")
			require.NoError(t, restored.Restore(snap))

			// Verify the LB can be described on the restored backend.
			lbs, err := restored.DescribeLoadBalancers([]string{tt.lbName})
			require.NoError(t, err)
			require.Len(t, lbs, 1)

			lb := lbs[0]
			assert.Len(t, lb.Listeners, tt.wantListeners)
			assert.NotNil(t, lb.BackendServerDescriptions, "BackendServerDescriptions must not be nil after restore")

			bsdPorts := make([]int32, 0, len(lb.BackendServerDescriptions))
			for _, bsd := range lb.BackendServerDescriptions {
				bsdPorts = append(bsdPorts, bsd.InstancePort)
			}

			assert.Equal(t, tt.wantBSDPorts, bsdPorts)
		})
	}
}

// TestRefinement3_AddTagsDuplicateKeys verifies that AddTags correctly handles
// duplicate keys in the input (uses len(newKeys) not len(kvs) for limit check).
func TestRefinement3_AddTagsDuplicateKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "duplicate_keys_counted_once_not_rejected",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				// Create an LB with 9 existing tags.
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"tag-dup-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"80"},
				})
				doELB(t, h, url.Values{
					"Action":                     {"AddTags"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"tag-dup-lb"},
					"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
					"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
					"Tags.member.3.Key": {"k3"}, "Tags.member.3.Value": {"v3"},
					"Tags.member.4.Key": {"k4"}, "Tags.member.4.Value": {"v4"},
					"Tags.member.5.Key": {"k5"}, "Tags.member.5.Value": {"v5"},
					"Tags.member.6.Key": {"k6"}, "Tags.member.6.Value": {"v6"},
					"Tags.member.7.Key": {"k7"}, "Tags.member.7.Value": {"v7"},
					"Tags.member.8.Key": {"k8"}, "Tags.member.8.Value": {"v8"},
					"Tags.member.9.Key": {"k9"}, "Tags.member.9.Value": {"v9"},
				})
			},
			// Now add 2 new tags where one is a duplicate of an existing key.
			// Unique new keys = 1 (k10 is new; k9 is overwrite).
			// Total = 9 existing non-overwritten + 1 truly new = 10 → should succeed.
			vals: url.Values{
				"Action":                     {"AddTags"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {"tag-dup-lb"},
				"Tags.member.1.Key":          {"k9"}, "Tags.member.1.Value": {"newv9"},
				"Tags.member.2.Key": {"k10"}, "Tags.member.2.Value": {"v10"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			got := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, got.Code)
		})
	}
}

// TestRefinement3_SSLListenerCertificate verifies that SSL protocol (not just HTTPS)
// is handled in the DescribeLoadBalancers and SetLoadBalancerListenerSSLCertificate flow.
func TestRefinement3_SSLListenerCertificate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		checkVals  url.Values
		name       string
		wantCert   string
		wantStatus int
	}{
		{
			name: "ssl_listener_cert_can_be_set",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				rec := doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssl-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"SSL"},
					"Listeners.member.1.LoadBalancerPort": {"443"},
					"Listeners.member.1.InstancePort":     {"443"},
					"Listeners.member.1.SSLCertificateId": {"arn:aws:iam::123456789012:server-certificate/initial"},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			checkVals: url.Values{
				"Action":           {"SetLoadBalancerListenerSSLCertificate"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"ssl-lb"},
				"LoadBalancerPort": {"443"},
				"SSLCertificateId": {"arn:aws:acm:us-east-1:123456789012:certificate/abc123"},
			},
			wantStatus: http.StatusOK,
			wantCert:   "arn:aws:acm:us-east-1:123456789012:certificate/abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			got := doELB(t, h, tt.checkVals)
			assert.Equal(t, tt.wantStatus, got.Code)

			if tt.wantCert != "" {
				// Verify cert is reflected in describe.
				descResp := doELB(t, h, url.Values{
					"Action":                     {"DescribeLoadBalancers"},
					"Version":                    {"2012-06-01"},
					"LoadBalancerNames.member.1": {"ssl-lb"},
				})
				require.Equal(t, http.StatusOK, descResp.Code)

				var out struct {
					XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
					Result  struct {
						LBs struct {
							Members []struct {
								ListenerDescriptions struct {
									Members []struct {
										Listener struct {
											SSLCertificateID string `xml:"SSLCertificateId"`
											LoadBalancerPort int32  `xml:"LoadBalancerPort"`
										} `xml:"Listener"`
									} `xml:"member"`
								} `xml:"ListenerDescriptions"`
							} `xml:"member"`
						} `xml:"LoadBalancerDescriptions"`
					} `xml:"DescribeLoadBalancersResult"`
				}

				require.NoError(t, xml.Unmarshal(descResp.Body.Bytes(), &out))
				require.Len(t, out.Result.LBs.Members, 1)
				require.Len(t, out.Result.LBs.Members[0].ListenerDescriptions.Members, 1)
				assert.Equal(
					t,
					tt.wantCert,
					out.Result.LBs.Members[0].ListenerDescriptions.Members[0].Listener.SSLCertificateID,
				)
			}
		})
	}
}

// TestRefinement3_DescribeLoadBalancers_InvalidMarker verifies InvalidNextMarker error.
func TestRefinement3_DescribeLoadBalancers_InvalidMarker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals       url.Values
		name       string
		wantStatus int
	}{
		{
			name: "unknown_marker_returns_error",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
				"Marker":  {"nonexistent-lb"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "no_marker_ok",
			vals: url.Values{
				"Action":  {"DescribeLoadBalancers"},
				"Version": {"2012-06-01"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("123456789012", "us-east-1")
			h := elb.NewHandler(backend)

			got := doELB(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, got.Code)
		})
	}
}
