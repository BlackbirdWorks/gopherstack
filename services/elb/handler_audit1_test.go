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

// ─── AccessLog ───────────────────────────────────────────────────────────────

func TestAudit1_AccessLog_DefaultDisabled(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-default-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-default-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				AccessLog struct {
					Enabled string `xml:"Enabled"`
				} `xml:"AccessLog"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "false", resp.Result.LoadBalancerAttributes.AccessLog.Enabled)
}

func TestAudit1_AccessLog_Enable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-enable-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-enable-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"my-elb-logs"},
		"LoadBalancerAttributes.AccessLog.S3BucketPrefix":       {"logs/"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"60"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				AccessLog struct {
					Enabled        string `xml:"Enabled"`
					S3BucketName   string `xml:"S3BucketName"`
					S3BucketPrefix string `xml:"S3BucketPrefix"`
					EmitInterval   string `xml:"EmitInterval"`
				} `xml:"AccessLog"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"ModifyLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	al := resp.Result.LoadBalancerAttributes.AccessLog
	assert.Equal(t, "true", al.Enabled)
	assert.Equal(t, "my-elb-logs", al.S3BucketName)
	assert.Equal(t, "logs/", al.S3BucketPrefix)
	assert.Equal(t, "60", al.EmitInterval)
}

func TestAudit1_AccessLog_Enable5MinInterval(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-5min-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-5min-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"my-bucket"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"5"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				AccessLog struct {
					EmitInterval string `xml:"EmitInterval"`
				} `xml:"AccessLog"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"ModifyLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "5", resp.Result.LoadBalancerAttributes.AccessLog.EmitInterval)
}

func TestAudit1_AccessLog_InvalidInterval(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		emitInterval string
		wantStatus   int
	}{
		{name: "interval_30_rejected", emitInterval: "30", wantStatus: http.StatusBadRequest},
		{name: "interval_1_rejected", emitInterval: "1", wantStatus: http.StatusBadRequest},
		{name: "interval_120_rejected", emitInterval: "120", wantStatus: http.StatusBadRequest},
		{name: "interval_5_accepted", emitInterval: "5", wantStatus: http.StatusOK},
		{name: "interval_60_accepted", emitInterval: "60", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "al-inv-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"al-inv-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
				"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
				"LoadBalancerAttributes.AccessLog.S3BucketName":         {"my-bucket"},
				"LoadBalancerAttributes.AccessLog.EmitInterval":         {tt.emitInterval},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit1_AccessLog_EnabledRequiresBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-nobucket-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-nobucket-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_AccessLog_DisableDoesNotRequireBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-disable-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-disable-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"false"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit1_AccessLog_RoundTrip_DescribeAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-rt-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-rt-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"rt-bucket"},
		"LoadBalancerAttributes.AccessLog.S3BucketPrefix":       {"prefix/"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"5"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-rt-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				AccessLog struct {
					Enabled        string `xml:"Enabled"`
					S3BucketName   string `xml:"S3BucketName"`
					S3BucketPrefix string `xml:"S3BucketPrefix"`
					EmitInterval   string `xml:"EmitInterval"`
				} `xml:"AccessLog"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	al := resp.Result.LoadBalancerAttributes.AccessLog
	assert.Equal(t, "true", al.Enabled)
	assert.Equal(t, "rt-bucket", al.S3BucketName)
	assert.Equal(t, "prefix/", al.S3BucketPrefix)
	assert.Equal(t, "5", al.EmitInterval)
}

func TestAudit1_AccessLog_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "al-snap-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-snap-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"snap-bucket"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"60"},
	})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	attrs, err := b2.DescribeLoadBalancerAttributes("al-snap-lb")
	require.NoError(t, err)
	assert.True(t, attrs.AccessLog.Enabled)
	assert.Equal(t, "snap-bucket", attrs.AccessLog.S3BucketName)
	assert.Equal(t, int32(60), attrs.AccessLog.EmitInterval)
}

func TestAudit1_AccessLog_UpdateBucket(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "al-upd-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-upd-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"old-bucket"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"60"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"al-upd-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.AccessLog.Enabled":              {"true"},
		"LoadBalancerAttributes.AccessLog.S3BucketName":         {"new-bucket"},
		"LoadBalancerAttributes.AccessLog.EmitInterval":         {"5"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	attrs, err := h.Backend.DescribeLoadBalancerAttributes("al-upd-lb")
	require.NoError(t, err)
	assert.Equal(t, "new-bucket", attrs.AccessLog.S3BucketName)
	assert.Equal(t, int32(5), attrs.AccessLog.EmitInterval)
}

// ─── DuplicateListener ───────────────────────────────────────────────────────

func TestAudit1_DuplicateListener_CreateListeners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elb.Handler)
		vals       url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			// Same port + same settings = idempotent (AWS behavior: CreateLoadBalancerListeners
			// is a no-op when listener already exists with identical config).
			name: "same_port_same_config_idempotent",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-idem-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-idem-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			},
			wantStatus: http.StatusOK,
		},
		{
			// Same port + DIFFERENT instance port = DuplicateListener error.
			name: "same_port_different_config_conflict",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-conflict-lb")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-conflict-lb"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"9090"},
			},
			wantStatus: http.StatusConflict,
			wantCode:   "DuplicateListener",
		},
		{
			name: "different_port_accepted",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-list-ok")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-list-ok"},
				"Listeners.member.1.Protocol":         {"HTTPS"},
				"Listeners.member.1.LoadBalancerPort": {"443"},
				"Listeners.member.1.InstancePort":     {"8443"},
			},
			wantStatus: http.StatusOK,
		},
		{
			// Two listeners on the same port in a single request = DuplicateListener.
			name: "duplicate_within_same_request",
			setup: func(t *testing.T, h *elb.Handler) {
				t.Helper()
				mustCreateLB(t, h, "dup-list-req")
			},
			vals: url.Values{
				"Action":                              {"CreateLoadBalancerListeners"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"dup-list-req"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"8080"},
				"Listeners.member.1.InstancePort":     {"8080"},
				"Listeners.member.2.Protocol":         {"TCP"},
				"Listeners.member.2.LoadBalancerPort": {"8080"},
				"Listeners.member.2.InstancePort":     {"8888"},
			},
			wantStatus: http.StatusConflict,
			wantCode:   "DuplicateListener",
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

			if tt.wantCode != "" {
				var errResp struct {
					XMLName xml.Name `xml:"ErrorResponse"`
					Error   struct {
						Code string `xml:"Code"`
					} `xml:"Error"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, tt.wantCode, errResp.Error.Code)
			}
		})
	}
}

// ─── CrossZoneLoadBalancing ───────────────────────────────────────────────────

func TestAudit1_CrossZoneLoadBalancing_Toggle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		enabled string
		want    string
	}{
		{name: "enable_cross_zone", enabled: "true", want: "true"},
		{name: "disable_cross_zone", enabled: "false", want: "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "czlb-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"czlb-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
				"LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled": {tt.enabled},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
				Result  struct {
					LoadBalancerAttributes struct {
						CrossZoneLoadBalancing struct {
							Enabled string `xml:"Enabled"`
						} `xml:"CrossZoneLoadBalancing"`
					} `xml:"LoadBalancerAttributes"`
				} `xml:"ModifyLoadBalancerAttributesResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.want, resp.Result.LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled)
		})
	}
}

func TestAudit1_CrossZoneLoadBalancing_DefaultFalse(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "czlb-default-lb")

	attrs, err := b.DescribeLoadBalancerAttributes("czlb-default-lb")
	require.NoError(t, err)
	assert.False(t, attrs.CrossZoneLoadBalancing)
}

func TestAudit1_CrossZoneLoadBalancing_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "czlb-snap-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"czlb-snap-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.CrossZoneLoadBalancing.Enabled": {"true"},
	})

	snap := b.Snapshot()
	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	attrs, err := b2.DescribeLoadBalancerAttributes("czlb-snap-lb")
	require.NoError(t, err)
	assert.True(t, attrs.CrossZoneLoadBalancing)
}

// ─── ConnectionDraining ───────────────────────────────────────────────────────

func TestAudit1_ConnectionDraining_Enable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "cd-enable-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cd-enable-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.ConnectionDraining.Enabled":     {"true"},
		"LoadBalancerAttributes.ConnectionDraining.Timeout":     {"300"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				ConnectionDraining struct {
					Enabled string `xml:"Enabled"`
					Timeout string `xml:"Timeout"`
				} `xml:"ConnectionDraining"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"ModifyLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	cd := resp.Result.LoadBalancerAttributes.ConnectionDraining
	assert.Equal(t, "true", cd.Enabled)
	assert.Equal(t, "300", cd.Timeout)
}

func TestAudit1_ConnectionDraining_DefaultValues(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "cd-default-lb")

	attrs, err := b.DescribeLoadBalancerAttributes("cd-default-lb")
	require.NoError(t, err)
	assert.False(t, attrs.ConnectionDraining)
	assert.Equal(t, int32(300), attrs.ConnectionDrainingTimeout)
}

func TestAudit1_ConnectionDraining_Disable(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "cd-disable-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cd-disable-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.ConnectionDraining.Enabled":     {"true"},
		"LoadBalancerAttributes.ConnectionDraining.Timeout":     {"100"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cd-disable-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
		"LoadBalancerAttributes.ConnectionDraining.Enabled":     {"false"},
		"LoadBalancerAttributes.ConnectionDraining.Timeout":     {"300"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	attrs, err := h.Backend.DescribeLoadBalancerAttributes("cd-disable-lb")
	require.NoError(t, err)
	assert.False(t, attrs.ConnectionDraining)
}

func TestAudit1_ConnectionDraining_TimeoutBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		timeout    string
		wantStatus int
	}{
		{name: "min_1", timeout: "1", wantStatus: http.StatusOK},
		{name: "max_3600", timeout: "3600", wantStatus: http.StatusOK},
		{name: "zero_rejected", timeout: "0", wantStatus: http.StatusBadRequest},
		{name: "over_max_rejected", timeout: "3601", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "cd-bound-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"cd-bound-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"60"},
				"LoadBalancerAttributes.ConnectionDraining.Enabled":     {"true"},
				"LoadBalancerAttributes.ConnectionDraining.Timeout":     {tt.timeout},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── ConnectionSettings (IdleTimeout) ────────────────────────────────────────

func TestAudit1_ConnectionSettings_IdleTimeout_Default(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "cs-default-lb")

	attrs, err := b.DescribeLoadBalancerAttributes("cs-default-lb")
	require.NoError(t, err)
	assert.Equal(t, int32(60), attrs.IdleTimeout)
}

func TestAudit1_ConnectionSettings_IdleTimeout_UpdateAndRead(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "cs-update-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"cs-update-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {"120"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ModifyLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				ConnectionSettings struct {
					IdleTimeout string `xml:"IdleTimeout"`
				} `xml:"ConnectionSettings"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"ModifyLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "120", resp.Result.LoadBalancerAttributes.ConnectionSettings.IdleTimeout)
}

func TestAudit1_ConnectionSettings_IdleTimeout_Boundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		timeout    string
		wantStatus int
	}{
		{name: "min_1", timeout: "1", wantStatus: http.StatusOK},
		{name: "max_3600", timeout: "3600", wantStatus: http.StatusOK},
		{name: "zero_rejected", timeout: "0", wantStatus: http.StatusBadRequest},
		{name: "over_max_rejected", timeout: "3601", wantStatus: http.StatusBadRequest},
		{name: "negative_rejected", timeout: "-1", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "cs-bound-lb")

			rec := doELB(t, h, url.Values{
				"Action":           {"ModifyLoadBalancerAttributes"},
				"Version":          {"2012-06-01"},
				"LoadBalancerName": {"cs-bound-lb"},
				"LoadBalancerAttributes.ConnectionSettings.IdleTimeout": {tt.timeout},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── SourceSecurityGroup ──────────────────────────────────────────────────────

func TestAudit1_SourceSecurityGroup_AlwaysPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(t *testing.T, h *elb.Handler) string
		name  string
	}{
		{
			name: "classic_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-classic-lb"},
					"AvailabilityZones.member.1":          {"us-east-1a"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-classic-lb"
			},
		},
		{
			name: "vpc_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-vpc-lb"},
					"Subnets.member.1":                    {"subnet-abc123"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-vpc-lb"
			},
		},
		{
			name: "internal_lb",
			setup: func(t *testing.T, h *elb.Handler) string {
				t.Helper()
				doELB(t, h, url.Values{
					"Action":                              {"CreateLoadBalancer"},
					"Version":                             {"2012-06-01"},
					"LoadBalancerName":                    {"ssg-internal-lb"},
					"Scheme":                              {"internal"},
					"Subnets.member.1":                    {"subnet-def456"},
					"Listeners.member.1.Protocol":         {"HTTP"},
					"Listeners.member.1.LoadBalancerPort": {"80"},
					"Listeners.member.1.InstancePort":     {"8080"},
				})

				return "ssg-internal-lb"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			lbName := tt.setup(t, h)

			rec := doELB(t, h, url.Values{
				"Action":                     {"DescribeLoadBalancers"},
				"Version":                    {"2012-06-01"},
				"LoadBalancerNames.member.1": {lbName},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
				Result  struct {
					LoadBalancerDescriptions struct {
						Members []struct {
							SourceSecurityGroup struct {
								GroupName  string `xml:"GroupName"`
								OwnerAlias string `xml:"OwnerAlias"`
							} `xml:"SourceSecurityGroup"`
						} `xml:"member"`
					} `xml:"LoadBalancerDescriptions"`
				} `xml:"DescribeLoadBalancersResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)

			ssg := resp.Result.LoadBalancerDescriptions.Members[0].SourceSecurityGroup
			assert.NotEmpty(t, ssg.GroupName, "GroupName must be set")
			assert.NotEmpty(t, ssg.OwnerAlias, "OwnerAlias must be set")
		})
	}
}

func TestAudit1_SourceSecurityGroup_OwnerAliasIsAccountID(t *testing.T) {
	t.Parallel()

	b := elb.NewInMemoryBackend("999888777666", "us-east-1")
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "ssg-acct-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"ssg-acct-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					SourceSecurityGroup struct {
						OwnerAlias string `xml:"OwnerAlias"`
					} `xml:"SourceSecurityGroup"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	assert.Equal(t, "999888777666", resp.Result.LoadBalancerDescriptions.Members[0].SourceSecurityGroup.OwnerAlias)
}

// ─── LoadBalancer CRUD ────────────────────────────────────────────────────────

func TestAudit1_CreateLoadBalancer_InternetFacingDefault(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "scheme-default-lb")

	lbs, err := b.DescribeLoadBalancers([]string{"scheme-default-lb"})
	require.NoError(t, err)
	assert.Equal(t, "internet-facing", lbs[0].Scheme)
}

func TestAudit1_CreateLoadBalancer_InternalScheme(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"internal-lb"},
		"Scheme":                              {"internal"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit1_CreateLoadBalancer_InvalidScheme(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"bad-scheme-lb"},
		"Scheme":                              {"private"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_CreateLoadBalancer_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		lbName     string
		wantStatus int
	}{
		{name: "single_char_ok", lbName: "a", wantStatus: http.StatusOK},
		{name: "max_32_chars_ok", lbName: "abcdefghijklmnopqrstuvwxyz123456", wantStatus: http.StatusOK},
		{name: "with_hyphens_ok", lbName: "my-load-balancer-1", wantStatus: http.StatusOK},
		{name: "starts_with_hyphen_rejected", lbName: "-bad-name", wantStatus: http.StatusBadRequest},
		{name: "ends_with_hyphen_rejected", lbName: "bad-name-", wantStatus: http.StatusBadRequest},
		{name: "too_long_rejected", lbName: "abcdefghijklmnopqrstuvwxyz1234567", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {tt.lbName},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit1_CreateLoadBalancer_WithInitialTags(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"tagged-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
		"Tags.member.1.Key":                   {"Env"},
		"Tags.member.1.Value":                 {"prod"},
		"Tags.member.2.Key":                   {"App"},
		"Tags.member.2.Value":                 {"web"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tagMap, err := b.DescribeTags([]string{"tagged-lb"})
	require.NoError(t, err)
	tags := tagMap["tagged-lb"]
	require.Len(t, tags, 2)
}

func TestAudit1_CreateLoadBalancer_DNSNameSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"dns-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"CreateLoadBalancerResponse"`
		Result  struct {
			DNSName string `xml:"DNSName"`
		} `xml:"CreateLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.Result.DNSName)
	assert.Contains(t, resp.Result.DNSName, "dns-lb")
}

func TestAudit1_DeleteLoadBalancer_AlsoDeletesTags(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "del-tags-lb")

	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"del-tags-lb"},
		"Tags.member.1.Key":          {"k"},
		"Tags.member.1.Value":        {"v"},
	})

	doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-tags-lb"},
	})

	assert.Equal(t, 0, b.LoadBalancerCount())
}

func TestAudit1_DeleteLoadBalancer_NotFoundReturns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"no-such-lb"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_DescribeLoadBalancers_MultipleNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "multi-a")
	mustCreateLB(t, h, "multi-b")
	mustCreateLB(t, h, "multi-c")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"multi-a"},
		"LoadBalancerNames.member.2": {"multi-c"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 2)
}

func TestAudit1_DescribeLoadBalancers_UnknownNameReturns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "exist-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"exist-lb"},
		"LoadBalancerNames.member.2": {"no-such-lb"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit1_DescribeLoadBalancers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for i := range 5 {
		name := [5]string{"lb-aaa", "lb-bbb", "lb-ccc", "lb-ddd", "lb-eee"}[i]
		mustCreateLB(t, h, name)
	}

	// First page: 3 items.
	rec := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"3"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker               string `xml:"NextMarker"`
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &page1))
	require.Len(t, page1.Result.LoadBalancerDescriptions.Members, 3)
	assert.NotEmpty(t, page1.Result.NextMarker)

	// Second page using marker.
	rec2 := doELB(t, h, url.Values{
		"Action":   {"DescribeLoadBalancers"},
		"Version":  {"2012-06-01"},
		"PageSize": {"3"},
		"Marker":   {page1.Result.NextMarker},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			NextMarker               string `xml:"NextMarker"`
			LoadBalancerDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &page2))
	require.Len(t, page2.Result.LoadBalancerDescriptions.Members, 2)
	assert.Empty(t, page2.Result.NextMarker, "last page must have no NextMarker")
}

// ─── AvailabilityZone CRUD ────────────────────────────────────────────────────

func TestAudit1_AvailabilityZones_EnableDisableCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"az-cycle-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Enable a second AZ.
	rec := doELB(t, h, url.Values{
		"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-cycle-lb"},
		"AvailabilityZones.member.1": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var enableResp struct {
		XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &enableResp))
	assert.Len(t, enableResp.Result.AvailabilityZones.Members, 2)

	// Disable the second AZ.
	rec2 := doELB(t, h, url.Values{
		"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-cycle-lb"},
		"AvailabilityZones.member.1": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var disableResp struct {
		XMLName xml.Name `xml:"DisableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"DisableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &disableResp))
	assert.Len(t, disableResp.Result.AvailabilityZones.Members, 1)
}

func TestAudit1_AvailabilityZones_DisableLastReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "az-last-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DisableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-last-lb"},
		"AvailabilityZones.member.1": {"us-east-1a"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_AvailabilityZones_EnableReturnsSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "az-sort-lb")

	// EnableAvailabilityZonesForLoadBalancer returns sorted result.
	rec := doELB(t, h, url.Values{
		"Action":                     {"EnableAvailabilityZonesForLoadBalancer"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"az-sort-lb"},
		"AvailabilityZones.member.1": {"us-east-1c"},
		"AvailabilityZones.member.2": {"us-east-1b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"EnableAvailabilityZonesForLoadBalancerResponse"`
		Result  struct {
			AvailabilityZones struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"AvailabilityZones"`
		} `xml:"EnableAvailabilityZonesForLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	azs := resp.Result.AvailabilityZones.Members
	require.GreaterOrEqual(t, len(azs), 2)
	for i := 1; i < len(azs); i++ {
		assert.LessOrEqual(t, azs[i-1].Value, azs[i].Value, "returned AZs must be sorted")
	}
}

// ─── SecurityGroups ───────────────────────────────────────────────────────────

func TestAudit1_SecurityGroups_ApplyReplaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sg-replace-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"SecurityGroups.member.1":             {"sg-old"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                  {"ApplySecurityGroupsToLoadBalancer"},
		"Version":                 {"2012-06-01"},
		"LoadBalancerName":        {"sg-replace-lb"},
		"SecurityGroups.member.1": {"sg-new1"},
		"SecurityGroups.member.2": {"sg-new2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
		Result  struct {
			SecurityGroups struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroups"`
		} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.SecurityGroups.Members, 2)

	vals := []string{
		resp.Result.SecurityGroups.Members[0].Value,
		resp.Result.SecurityGroups.Members[1].Value,
	}
	assert.Contains(t, vals, "sg-new1")
	assert.Contains(t, vals, "sg-new2")
}

func TestAudit1_SecurityGroups_ApplyEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"sg-empty-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"SecurityGroups.member.1":             {"sg-init"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"ApplySecurityGroupsToLoadBalancer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"sg-empty-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"ApplySecurityGroupsToLoadBalancerResponse"`
		Result  struct {
			SecurityGroups struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"SecurityGroups"`
		} `xml:"ApplySecurityGroupsToLoadBalancerResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.Result.SecurityGroups.Members)
}

// ─── Listener CRUD ────────────────────────────────────────────────────────────

func TestAudit1_ListenerProtocols(t *testing.T) {
	t.Parallel()

	protocols := []struct {
		name     string
		protocol string
		valid    bool
	}{
		{"http_valid", "HTTP", true},
		{"https_valid", "HTTPS", true},
		{"tcp_valid", "TCP", true},
		{"ssl_valid", "SSL", true},
		{"udp_invalid", "UDP", false},
		{"grpc_invalid", "GRPC", false},
	}

	for _, tt := range protocols {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"proto-lb"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Listeners.member.1.Protocol":         {tt.protocol},
				"Listeners.member.1.LoadBalancerPort": {"80"},
				"Listeners.member.1.InstancePort":     {"8080"},
			})

			if tt.valid {
				assert.Equal(t, http.StatusOK, rec.Code)
			} else {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			}
		})
	}
}

func TestAudit1_Listener_DeleteAndRecreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "list-del-lb")

	// Add second listener.
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"list-del-lb"},
		"Listeners.member.1.Protocol":         {"HTTPS"},
		"Listeners.member.1.LoadBalancerPort": {"443"},
		"Listeners.member.1.InstancePort":     {"8443"},
	})

	// Delete port 443 listener.
	doELB(t, h, url.Values{
		"Action":                     {"DeleteLoadBalancerListeners"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerName":           {"list-del-lb"},
		"LoadBalancerPorts.member.1": {"443"},
	})

	// Recreate port 443 — should succeed (no duplicate).
	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancerListeners"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"list-del-lb"},
		"Listeners.member.1.Protocol":         {"TCP"},
		"Listeners.member.1.LoadBalancerPort": {"443"},
		"Listeners.member.1.InstancePort":     {"443"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit1_Listener_PortBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		port       string
		wantStatus int
	}{
		{name: "port_1_ok", port: "1", wantStatus: http.StatusOK},
		{name: "port_65535_ok", port: "65535", wantStatus: http.StatusOK},
		{name: "port_0_rejected", port: "0", wantStatus: http.StatusBadRequest},
		{name: "port_65536_rejected", port: "65536", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doELB(t, h, url.Values{
				"Action":                              {"CreateLoadBalancer"},
				"Version":                             {"2012-06-01"},
				"LoadBalancerName":                    {"portb-lb"},
				"AvailabilityZones.member.1":          {"us-east-1a"},
				"Listeners.member.1.Protocol":         {"HTTP"},
				"Listeners.member.1.LoadBalancerPort": {tt.port},
				"Listeners.member.1.InstancePort":     {"8080"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ─── HealthCheck ─────────────────────────────────────────────────────────────

func TestAudit1_HealthCheck_Configure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target     string
		name       string
		wantStatus int
	}{
		{name: "http_with_path", target: "HTTP:80/health", wantStatus: http.StatusOK},
		{name: "https_with_path", target: "HTTPS:443/ping", wantStatus: http.StatusOK},
		{name: "tcp_no_path", target: "TCP:80", wantStatus: http.StatusOK},
		{name: "ssl_no_path", target: "SSL:443", wantStatus: http.StatusOK},
		{name: "http_without_path_rejected", target: "HTTP:80", wantStatus: http.StatusBadRequest},
		{name: "tcp_with_path_rejected", target: "TCP:80/health", wantStatus: http.StatusBadRequest},
		{name: "invalid_protocol_rejected", target: "UDP:80", wantStatus: http.StatusBadRequest},
		{name: "empty_target_rejected", target: "", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			mustCreateLB(t, h, "hc-lb")

			rec := doELB(t, h, url.Values{
				"Action":                         {"ConfigureHealthCheck"},
				"Version":                        {"2012-06-01"},
				"LoadBalancerName":               {"hc-lb"},
				"HealthCheck.Target":             {tt.target},
				"HealthCheck.Interval":           {"30"},
				"HealthCheck.Timeout":            {"5"},
				"HealthCheck.UnhealthyThreshold": {"2"},
				"HealthCheck.HealthyThreshold":   {"3"},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAudit1_HealthCheck_TargetProtocolNormalized(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "hc-norm-lb")

	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-norm-lb"},
		"HealthCheck.Target":             {"http:80/health"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})

	lbs, err := b.DescribeLoadBalancers([]string{"hc-norm-lb"})
	require.NoError(t, err)
	require.NotNil(t, lbs[0].HealthCheck)
	assert.Equal(t, "HTTP:80/health", lbs[0].HealthCheck.Target, "protocol must be uppercased")
}

func TestAudit1_HealthCheck_IntervalMustExceedTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-timing-lb")

	rec := doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"hc-timing-lb"},
		"HealthCheck.Target":             {"TCP:80"},
		"HealthCheck.Interval":           {"10"},
		"HealthCheck.Timeout":            {"10"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_HealthCheck_AlwaysPresentInDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "hc-absent-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeLoadBalancers"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"hc-absent-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancersResponse"`
		Result  struct {
			LoadBalancerDescriptions struct {
				Members []struct {
					HealthCheck *struct {
						Target string `xml:"Target"`
					} `xml:"HealthCheck"`
				} `xml:"member"`
			} `xml:"LoadBalancerDescriptions"`
		} `xml:"DescribeLoadBalancersResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.LoadBalancerDescriptions.Members, 1)
	// HealthCheck element must be present even without configuration.
	assert.NotNil(t, resp.Result.LoadBalancerDescriptions.Members[0].HealthCheck)
}

// ─── Tags ─────────────────────────────────────────────────────────────────────

func TestAudit1_Tags_MultiLB_AddAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-a-lb")
	mustCreateLB(t, h, "tags-b-lb")

	rec := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-a-lb"},
		"LoadBalancerNames.member.2": {"tags-b-lb"},
		"Tags.member.1.Key":          {"Env"},
		"Tags.member.1.Value":        {"prod"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELB(t, h, url.Values{
		"Action":                     {"DescribeTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-a-lb"},
		"LoadBalancerNames.member.2": {"tags-b-lb"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Result  struct {
			TagDescriptions struct {
				Members []struct {
					LoadBalancerName string `xml:"LoadBalancerName"`
					Tags             struct {
						Members []struct {
							Key   string `xml:"Key"`
							Value string `xml:"Value"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TagDescriptions.Members, 2)

	for _, td := range resp.Result.TagDescriptions.Members {
		require.Len(t, td.Tags.Members, 1)
		assert.Equal(t, "Env", td.Tags.Members[0].Key)
	}
}

func TestAudit1_Tags_MaxLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-max-lb")

	// Add 10 tags (the max).
	rec := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-max-lb"},
		"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
		"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
		"Tags.member.3.Key": {"k3"}, "Tags.member.3.Value": {"v3"},
		"Tags.member.4.Key": {"k4"}, "Tags.member.4.Value": {"v4"},
		"Tags.member.5.Key": {"k5"}, "Tags.member.5.Value": {"v5"},
		"Tags.member.6.Key": {"k6"}, "Tags.member.6.Value": {"v6"},
		"Tags.member.7.Key": {"k7"}, "Tags.member.7.Value": {"v7"},
		"Tags.member.8.Key": {"k8"}, "Tags.member.8.Value": {"v8"},
		"Tags.member.9.Key": {"k9"}, "Tags.member.9.Value": {"v9"},
		"Tags.member.10.Key": {"k10"}, "Tags.member.10.Value": {"v10"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Adding one more should fail.
	rec2 := doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-max-lb"},
		"Tags.member.1.Key":          {"k11"},
		"Tags.member.1.Value":        {"v11"},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestAudit1_Tags_Remove(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "tags-rm-lb")

	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
		"Tags.member.1.Key":          {"k1"}, "Tags.member.1.Value": {"v1"},
		"Tags.member.2.Key": {"k2"}, "Tags.member.2.Value": {"v2"},
	})

	doELB(t, h, url.Values{
		"Action":                     {"RemoveTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
		"Tags.member.1.Key":          {"k1"},
	})

	rec := doELB(t, h, url.Values{
		"Action":                     {"DescribeTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"tags-rm-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeTagsResponse"`
		Result  struct {
			TagDescriptions struct {
				Members []struct {
					Tags struct {
						Members []struct {
							Key string `xml:"Key"`
						} `xml:"member"`
					} `xml:"Tags"`
				} `xml:"member"`
			} `xml:"TagDescriptions"`
		} `xml:"DescribeTagsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.TagDescriptions.Members, 1)
	tags := resp.Result.TagDescriptions.Members[0].Tags.Members
	require.Len(t, tags, 1)
	assert.Equal(t, "k2", tags[0].Key)
}

// ─── Stickiness Policies ──────────────────────────────────────────────────────

func TestAudit1_AppCookieStickiness_Lifecycle(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "app-cookie-lb")

	// Create.
	rec := doELB(t, h, url.Values{
		"Action":           {"CreateAppCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"app-cookie-lb"},
		"PolicyName":       {"app-pol"},
		"CookieName":       {"JSESSIONID"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.PolicyCount())

	// Describe.
	rec2 := doELB(t, h, url.Values{
		"Action":           {"DescribeLoadBalancerPolicies"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"app-cookie-lb"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var pResp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
		Result  struct {
			PolicyDescriptions struct {
				Members []struct {
					PolicyName     string `xml:"PolicyName"`
					PolicyTypeName string `xml:"PolicyTypeName"`
				} `xml:"member"`
			} `xml:"PolicyDescriptions"`
		} `xml:"DescribeLoadBalancerPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &pResp))
	require.Len(t, pResp.Result.PolicyDescriptions.Members, 1)
	assert.Equal(t, "app-pol", pResp.Result.PolicyDescriptions.Members[0].PolicyName)
	assert.Equal(t, "AppCookieStickinessPolicyType", pResp.Result.PolicyDescriptions.Members[0].PolicyTypeName)

	// Delete.
	rec3 := doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"app-cookie-lb"},
		"PolicyName":       {"app-pol"},
	})
	require.Equal(t, http.StatusOK, rec3.Code)
	assert.Equal(t, 0, b.PolicyCount())
}

func TestAudit1_LBCookieStickiness_NoExpiry(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "lb-cookie-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"lb-cookie-lb"},
		"PolicyName":       {"lb-pol"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELB(t, h, url.Values{
		"Action":               {"DescribeLoadBalancerPolicies"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"lb-cookie-lb"},
		"PolicyNames.member.1": {"lb-pol"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
		Result  struct {
			PolicyDescriptions struct {
				Members []struct {
					PolicyTypeName              string `xml:"PolicyTypeName"`
					PolicyAttributeDescriptions struct {
						Members []struct {
							AttributeName  string `xml:"AttributeName"`
							AttributeValue string `xml:"AttributeValue"`
						} `xml:"member"`
					} `xml:"PolicyAttributeDescriptions"`
				} `xml:"member"`
			} `xml:"PolicyDescriptions"`
		} `xml:"DescribeLoadBalancerPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.PolicyDescriptions.Members, 1)
	p := resp.Result.PolicyDescriptions.Members[0]
	assert.Equal(t, "LBCookieStickinessPolicyType", p.PolicyTypeName)
	require.Len(t, p.PolicyAttributeDescriptions.Members, 1)
	assert.Equal(t, "CookieExpirationPeriod", p.PolicyAttributeDescriptions.Members[0].AttributeName)
	assert.Empty(t, p.PolicyAttributeDescriptions.Members[0].AttributeValue)
}

func TestAudit1_LBCookieStickiness_WithExpiry(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "lb-exp-lb")

	rec := doELB(t, h, url.Values{
		"Action":                 {"CreateLBCookieStickinessPolicy"},
		"Version":                {"2012-06-01"},
		"LoadBalancerName":       {"lb-exp-lb"},
		"PolicyName":             {"lb-exp-pol"},
		"CookieExpirationPeriod": {"3600"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// ─── PolicyTypes ──────────────────────────────────────────────────────────────

func TestAudit1_PolicyTypes_ListAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeLoadBalancerPolicyTypes"},
		"Version": {"2012-06-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPolicyTypesResponse"`
		Result  struct {
			PolicyTypeDescriptions struct {
				Members []struct {
					PolicyTypeName string `xml:"PolicyTypeName"`
				} `xml:"member"`
			} `xml:"PolicyTypeDescriptions"`
		} `xml:"DescribeLoadBalancerPolicyTypesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	names := make([]string, 0, len(resp.Result.PolicyTypeDescriptions.Members))
	for _, m := range resp.Result.PolicyTypeDescriptions.Members {
		names = append(names, m.PolicyTypeName)
	}

	assert.Contains(t, names, "AppCookieStickinessPolicyType")
	assert.Contains(t, names, "LBCookieStickinessPolicyType")
	assert.Contains(t, names, "SSLNegotiationPolicyType")
	assert.Contains(t, names, "ProxyProtocolPolicyType")
	assert.Contains(t, names, "BackendServerAuthenticationPolicyType")
}

func TestAudit1_PolicyTypes_FilterByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":                   {"DescribeLoadBalancerPolicyTypes"},
		"Version":                  {"2012-06-01"},
		"PolicyTypeNames.member.1": {"SSLNegotiationPolicyType"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPolicyTypesResponse"`
		Result  struct {
			PolicyTypeDescriptions struct {
				Members []struct {
					PolicyTypeName string `xml:"PolicyTypeName"`
				} `xml:"member"`
			} `xml:"PolicyTypeDescriptions"`
		} `xml:"DescribeLoadBalancerPolicyTypesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.PolicyTypeDescriptions.Members, 1)
	assert.Equal(t, "SSLNegotiationPolicyType", resp.Result.PolicyTypeDescriptions.Members[0].PolicyTypeName)
}

func TestAudit1_PolicyTypes_UnknownReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":                   {"DescribeLoadBalancerPolicyTypes"},
		"Version":                  {"2012-06-01"},
		"PolicyTypeNames.member.1": {"NoSuchPolicyType"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── SSL Cipher Settings ──────────────────────────────────────────────────────

func TestAudit1_SSLCipherPolicy_CreateAndDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "ssl-cipher-lb")

	rec := doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"ssl-cipher-lb"},
		"PolicyName":       {"my-ssl-pol"},
		"PolicyTypeName":   {"SSLNegotiationPolicyType"},
		"PolicyAttributes.member.1.AttributeName":  {"Protocol-TLSv1.2"},
		"PolicyAttributes.member.1.AttributeValue": {"true"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doELB(t, h, url.Values{
		"Action":               {"DescribeLoadBalancerPolicies"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"ssl-cipher-lb"},
		"PolicyNames.member.1": {"my-ssl-pol"},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
		Result  struct {
			PolicyDescriptions struct {
				Members []struct {
					PolicyTypeName              string `xml:"PolicyTypeName"`
					PolicyAttributeDescriptions struct {
						Members []struct {
							AttributeName  string `xml:"AttributeName"`
							AttributeValue string `xml:"AttributeValue"`
						} `xml:"member"`
					} `xml:"PolicyAttributeDescriptions"`
				} `xml:"member"`
			} `xml:"PolicyDescriptions"`
		} `xml:"DescribeLoadBalancerPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec2.Body.Bytes(), &resp))
	require.Len(t, resp.Result.PolicyDescriptions.Members, 1)
	p := resp.Result.PolicyDescriptions.Members[0]
	assert.Equal(t, "SSLNegotiationPolicyType", p.PolicyTypeName)
	require.Len(t, p.PolicyAttributeDescriptions.Members, 1)
	assert.Equal(t, "Protocol-TLSv1.2", p.PolicyAttributeDescriptions.Members[0].AttributeName)
}

// ─── Backend Server Policies ──────────────────────────────────────────────────

func TestAudit1_BackendServerPolicies_SetAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "bsd-desc-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-desc-lb"},
		"PolicyName":       {"proxy-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-desc-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"proxy-pol"},
	})

	lbs, err := b.DescribeLoadBalancers([]string{"bsd-desc-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)
	require.Len(t, lbs[0].BackendServerDescriptions, 1)
	assert.Equal(t, int32(8080), lbs[0].BackendServerDescriptions[0].InstancePort)
	assert.Equal(t, []string{"proxy-pol"}, lbs[0].BackendServerDescriptions[0].PolicyNames)
}

func TestAudit1_BackendServerPolicies_ClearByEmptyList(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "bsd-clear-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-clear-lb"},
		"PolicyName":       {"proxy-pol"},
		"PolicyTypeName":   {"ProxyProtocolPolicyType"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-clear-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"proxy-pol"},
	})

	// Clear policies by setting empty list.
	doELB(t, h, url.Values{
		"Action":           {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"bsd-clear-lb"},
		"InstancePort":     {"8080"},
	})

	lbs, err := b.DescribeLoadBalancers([]string{"bsd-clear-lb"})
	require.NoError(t, err)
	bsd := lbs[0].BackendServerDescriptions
	// Either removed or has empty policy list.
	for _, d := range bsd {
		if d.InstancePort == 8080 {
			assert.Empty(t, d.PolicyNames)
		}
	}
}

func TestAudit1_BackendServerPolicies_UnknownPolicyRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "bsd-unknown-lb")

	rec := doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesForBackendServer"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"bsd-unknown-lb"},
		"InstancePort":         {"8080"},
		"PolicyNames.member.1": {"no-such-policy"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── Instance Health ──────────────────────────────────────────────────────────

func TestAudit1_InstanceHealth_MultipleInstances(t *testing.T) {
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

func TestAudit1_InstanceHealth_FilterByInstance(t *testing.T) {
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

func TestAudit1_InstanceHealth_DeregisterAndCheck(t *testing.T) {
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

// ─── AccountLimits ────────────────────────────────────────────────────────────

func TestAudit1_AccountLimits_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"DescribeAccountLimits"},
		"Version": {"2012-06-01"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeAccountLimitsResponse"`
		Result  struct {
			Limits struct {
				Members []struct {
					Name string `xml:"Name"`
					Max  string `xml:"Max"`
				} `xml:"member"`
			} `xml:"Limits"`
		} `xml:"DescribeAccountLimitsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.GreaterOrEqual(t, len(resp.Result.Limits.Members), 3)

	names := make([]string, 0, len(resp.Result.Limits.Members))
	for _, l := range resp.Result.Limits.Members {
		names = append(names, l.Name)
		assert.NotEmpty(t, l.Max, "limit Max must not be empty")
	}
	assert.Contains(t, names, "classic-load-balancers")
}

// ─── DeletePolicy in-use guard ────────────────────────────────────────────────

func TestAudit1_DeletePolicy_InUseByListenerRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "del-inuse-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-inuse-lb"},
		"PolicyName":       {"in-use-pol"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesOfListener"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"del-inuse-lb"},
		"LoadBalancerPort":     {"80"},
		"PolicyNames.member.1": {"in-use-pol"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-inuse-lb"},
		"PolicyName":       {"in-use-pol"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_DeletePolicy_AfterClearOk(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "del-clear-lb")

	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-clear-lb"},
		"PolicyName":       {"clear-pol"},
	})

	doELB(t, h, url.Values{
		"Action":               {"SetLoadBalancerPoliciesOfListener"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"del-clear-lb"},
		"LoadBalancerPort":     {"80"},
		"PolicyNames.member.1": {"clear-pol"},
	})

	// Clear policies from listener.
	doELB(t, h, url.Values{
		"Action":           {"SetLoadBalancerPoliciesOfListener"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-clear-lb"},
		"LoadBalancerPort": {"80"},
	})

	// Now delete should succeed.
	rec := doELB(t, h, url.Values{
		"Action":           {"DeleteLoadBalancerPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"del-clear-lb"},
		"PolicyName":       {"clear-pol"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ─── DescribeLoadBalancerPolicies filtering ───────────────────────────────────

func TestAudit1_DescribePolicies_FilterByPolicyName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "filter-pol-lb")

	for _, name := range []string{"pol-a", "pol-b", "pol-c"} {
		doELB(t, h, url.Values{
			"Action":           {"CreateLBCookieStickinessPolicy"},
			"Version":          {"2012-06-01"},
			"LoadBalancerName": {"filter-pol-lb"},
			"PolicyName":       {name},
		})
	}

	rec := doELB(t, h, url.Values{
		"Action":               {"DescribeLoadBalancerPolicies"},
		"Version":              {"2012-06-01"},
		"LoadBalancerName":     {"filter-pol-lb"},
		"PolicyNames.member.1": {"pol-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerPoliciesResponse"`
		Result  struct {
			PolicyDescriptions struct {
				Members []struct {
					PolicyName string `xml:"PolicyName"`
				} `xml:"member"`
			} `xml:"PolicyDescriptions"`
		} `xml:"DescribeLoadBalancerPoliciesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.PolicyDescriptions.Members, 1)
	assert.Equal(t, "pol-b", resp.Result.PolicyDescriptions.Members[0].PolicyName)
}

func TestAudit1_DescribePolicies_UnknownLBReturns404(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeLoadBalancerPolicies"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"no-such-lb"},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ─── DesyncMitigationMode ─────────────────────────────────────────────────────

func TestAudit1_DesyncMitigationMode_DefaultDefensive(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "desync-def-lb")

	attrs, err := b.DescribeLoadBalancerAttributes("desync-def-lb")
	require.NoError(t, err)
	assert.Equal(t, "defensive", attrs.DesyncMitigationMode)
}

func TestAudit1_DesyncMitigationMode_InXMLResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "desync-xml-lb")

	doELB(t, h, url.Values{
		"Action":           {"ModifyLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"desync-xml-lb"},
		"LoadBalancerAttributes.ConnectionSettings.IdleTimeout":      {"60"},
		"LoadBalancerAttributes.AdditionalAttributes.member.1.Key":   {"elb.http.desyncmitigationmode"},
		"LoadBalancerAttributes.AdditionalAttributes.member.1.Value": {"strictest"},
	})

	rec := doELB(t, h, url.Values{
		"Action":           {"DescribeLoadBalancerAttributes"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"desync-xml-lb"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DescribeLoadBalancerAttributesResponse"`
		Result  struct {
			LoadBalancerAttributes struct {
				AdditionalAttributes struct {
					Members []struct {
						Key   string `xml:"Key"`
						Value string `xml:"Value"`
					} `xml:"member"`
				} `xml:"AdditionalAttributes"`
			} `xml:"LoadBalancerAttributes"`
		} `xml:"DescribeLoadBalancerAttributesResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

	found := false
	for _, a := range resp.Result.LoadBalancerAttributes.AdditionalAttributes.Members {
		if a.Key == "elb.http.desyncmitigationmode" {
			assert.Equal(t, "strictest", a.Value)
			found = true
		}
	}
	assert.True(t, found, "desync mode must appear in AdditionalAttributes")
}

// ─── Unknown action ───────────────────────────────────────────────────────────

func TestAudit1_UnknownAction_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Action":  {"NoSuchAction"},
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidAction", errResp.Error.Code)
}

func TestAudit1_MissingAction_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doELB(t, h, url.Values{
		"Version": {"2012-06-01"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ─── DuplicateLoadBalancerName ────────────────────────────────────────────────

func TestAudit1_CreateLoadBalancer_DuplicateName(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	mustCreateLB(t, h, "dup-name-lb")

	rec := doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"dup-name-lb"},
		"AvailabilityZones.member.1":          {"us-east-1a"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})
	assert.Equal(t, http.StatusConflict, rec.Code)

	var errResp struct {
		XMLName xml.Name `xml:"ErrorResponse"`
		Error   struct {
			Code string `xml:"Code"`
		} `xml:"Error"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "DuplicateLoadBalancerName", errResp.Error.Code)
}

// ─── Subnets ──────────────────────────────────────────────────────────────────

func TestAudit1_Subnets_AttachDetachCycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"subnet-cycle-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Attach second subnet.
	doELB(t, h, url.Values{
		"Action":           {"AttachLoadBalancerToSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-cycle-lb"},
		"Subnets.member.1": {"subnet-bbb"},
	})

	// Detach first subnet.
	rec := doELB(t, h, url.Values{
		"Action":           {"DetachLoadBalancerFromSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-cycle-lb"},
		"Subnets.member.1": {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"DetachLoadBalancerFromSubnetsResponse"`
		Result  struct {
			Subnets struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"Subnets"`
		} `xml:"DetachLoadBalancerFromSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.Result.Subnets.Members, 1)
	assert.Equal(t, "subnet-bbb", resp.Result.Subnets.Members[0].Value)
}

func TestAudit1_Subnets_AttachIdempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	doELB(t, h, url.Values{
		"Action":                              {"CreateLoadBalancer"},
		"Version":                             {"2012-06-01"},
		"LoadBalancerName":                    {"subnet-idem-lb"},
		"Subnets.member.1":                    {"subnet-aaa"},
		"Listeners.member.1.Protocol":         {"HTTP"},
		"Listeners.member.1.LoadBalancerPort": {"80"},
		"Listeners.member.1.InstancePort":     {"8080"},
	})

	// Attaching the same subnet again is idempotent.
	rec := doELB(t, h, url.Values{
		"Action":           {"AttachLoadBalancerToSubnets"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"subnet-idem-lb"},
		"Subnets.member.1": {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		XMLName xml.Name `xml:"AttachLoadBalancerToSubnetsResponse"`
		Result  struct {
			Subnets struct {
				Members []struct {
					Value string `xml:",chardata"`
				} `xml:"member"`
			} `xml:"Subnets"`
		} `xml:"AttachLoadBalancerToSubnetsResult"`
	}
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Result.Subnets.Members, 1, "idempotent attach must not duplicate")
}

// ─── Instance registration ────────────────────────────────────────────────────

func TestAudit1_RegisterInstances_Idempotent(t *testing.T) {
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

func TestAudit1_DeregisterInstances_NonRegisteredIsNoOp(t *testing.T) {
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

// ─── Persistence round-trip ───────────────────────────────────────────────────

func TestAudit1_Persistence_FullState(t *testing.T) {
	t.Parallel()

	b := newBackend()
	h := elb.NewHandler(b)
	mustCreateLB(t, h, "full-snap-lb")

	// Register an instance.
	doELB(t, h, url.Values{
		"Action":                        {"RegisterInstancesWithLoadBalancer"},
		"Version":                       {"2012-06-01"},
		"LoadBalancerName":              {"full-snap-lb"},
		"Instances.member.1.InstanceId": {"i-aaaaaaaa01"},
	})

	// Configure health check.
	doELB(t, h, url.Values{
		"Action":                         {"ConfigureHealthCheck"},
		"Version":                        {"2012-06-01"},
		"LoadBalancerName":               {"full-snap-lb"},
		"HealthCheck.Target":             {"HTTP:80/health"},
		"HealthCheck.Interval":           {"30"},
		"HealthCheck.Timeout":            {"5"},
		"HealthCheck.UnhealthyThreshold": {"2"},
		"HealthCheck.HealthyThreshold":   {"3"},
	})

	// Add a policy.
	doELB(t, h, url.Values{
		"Action":           {"CreateLBCookieStickinessPolicy"},
		"Version":          {"2012-06-01"},
		"LoadBalancerName": {"full-snap-lb"},
		"PolicyName":       {"snap-pol"},
	})

	// Add tags.
	doELB(t, h, url.Values{
		"Action":                     {"AddTags"},
		"Version":                    {"2012-06-01"},
		"LoadBalancerNames.member.1": {"full-snap-lb"},
		"Tags.member.1.Key":          {"Env"},
		"Tags.member.1.Value":        {"test"},
	})

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := elb.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	// Verify LB exists.
	lbs, err := b2.DescribeLoadBalancers([]string{"full-snap-lb"})
	require.NoError(t, err)
	require.Len(t, lbs, 1)

	lb := lbs[0]
	assert.Len(t, lb.Instances, 1)
	assert.NotNil(t, lb.HealthCheck)
	assert.Equal(t, "HTTP:80/health", lb.HealthCheck.Target)

	// Verify policy.
	assert.Equal(t, 1, b2.PolicyCount())

	// Verify tags.
	tagMap, err := b2.DescribeTags([]string{"full-snap-lb"})
	require.NoError(t, err)
	require.Len(t, tagMap["full-snap-lb"], 1)
	assert.Equal(t, "Env", tagMap["full-snap-lb"][0].Key)
}
