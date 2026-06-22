package autoscaling_test

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

func newAutoscalingHandler() *autoscaling.Handler {
	return autoscaling.NewHandler(autoscaling.NewInMemoryBackend())
}

func postAutoscalingForm(t *testing.T, h *autoscaling.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestAutoscalingHandler_Name(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	assert.Equal(t, "Autoscaling", h.Name())
}

func TestAutoscalingHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateAutoScalingGroup")
	assert.Contains(t, ops, "DescribeAutoScalingGroups")
	assert.Contains(t, ops, "UpdateAutoScalingGroup")
	assert.Contains(t, ops, "DeleteAutoScalingGroup")
	assert.Contains(t, ops, "CreateLaunchConfiguration")
	assert.Contains(t, ops, "DescribeLaunchConfigurations")
	assert.Contains(t, ops, "DeleteLaunchConfiguration")
	assert.Contains(t, ops, "DescribeScalingActivities")
}

func TestAutoscalingHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	assert.Equal(t, 80, h.MatchPriority())
}

func TestAutoscalingHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   bool
	}{
		{
			name:   "valid autoscaling request",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   true,
		},
		{
			name:   "wrong version",
			method: http.MethodPost,
			path:   "/",
			body:   "Version=2014-10-31&Action=DescribeAutoScalingGroups",
			want:   false,
		},
		{
			name:   "GET method",
			method: http.MethodGet,
			path:   "/",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   false,
		},
		{
			name:   "dashboard path excluded",
			method: http.MethodPost,
			path:   "/dashboard/autoscaling",
			body:   "Version=2011-01-01&Action=DescribeAutoScalingGroups",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestAutoscalingHandler_CreateAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "success",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=test-asg&MinSize=1&MaxSize=5" +
				"&DesiredCapacity=2&AvailabilityZones.member.1=us-east-1a",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_name",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&MinSize=1&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "duplicate_group",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dup-asg&MinSize=1&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			if tt.name == "duplicate_group" {
				// pre-create
				rec := postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dup-asg&MinSize=1&MaxSize=5",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DescribeAutoScalingGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
		wantGroups int
	}{
		{
			name:       "empty",
			body:       "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			wantStatus: http.StatusOK,
			wantGroups: 0,
		},
		{
			name: "with_groups",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=asg-a&MinSize=1&MaxSize=3",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=asg-b&MinSize=2&MaxSize=6",
				)
			},
			body:       "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			wantStatus: http.StatusOK,
			wantGroups: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantGroups > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
					Result  struct {
						AutoScalingGroups struct {
							Members []struct {
								Name string `xml:"AutoScalingGroupName"`
							} `xml:"member"`
						} `xml:"AutoScalingGroups"`
					} `xml:"DescribeAutoScalingGroupsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.AutoScalingGroups.Members, tt.wantGroups)
			}
		})
	}
}

func TestAutoscalingHandler_LaunchConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "create_success",
			body: "Action=CreateLaunchConfiguration&Version=2011-01-01" +
				"&LaunchConfigurationName=my-lc&ImageId=ami-12345678&InstanceType=t2.micro",
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe",
			body:       "Action=DescribeLaunchConfigurations&Version=2011-01-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown_action",
			body:       "Action=UnknownAction&Version=2011-01-01",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DeleteAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "delete_existing",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=del-asg&MinSize=0&MaxSize=0",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=del-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_nonexistent",
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "create_asg",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01",
			want: "CreateAutoScalingGroup",
		},
		{
			name: "describe_asg",
			body: "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			want: "DescribeAutoScalingGroups",
		},
		{
			name: "missing_action",
			body: "Version=2011-01-01",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestAutoscalingHandler_DescribeLaunchConfigurationsWithData(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "with_lcs",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateLaunchConfiguration&Version=2011-01-01"+
						"&LaunchConfigurationName=lc-a&ImageId=ami-111&InstanceType=t2.micro",
				)
				postAutoscalingForm(
					t,
					h,
					"Action=CreateLaunchConfiguration&Version=2011-01-01"+
						"&LaunchConfigurationName=lc-b&ImageId=ami-222&InstanceType=t3.small",
				)
			},
			body:       "Action=DescribeLaunchConfigurations&Version=2011-01-01",
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name: "filter_by_name",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateLaunchConfiguration&Version=2011-01-01"+
						"&LaunchConfigurationName=lc-filter&ImageId=ami-333&InstanceType=t2.micro",
				)
			},
			body:       "Action=DescribeLaunchConfigurations&Version=2011-01-01&LaunchConfigurationNames.member.1=lc-filter",
			wantStatus: http.StatusOK,
			wantCount:  1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantCount > 0 {
				var resp struct {
					XMLName xml.Name `xml:"DescribeLaunchConfigurationsResponse"`
					Result  struct {
						LaunchConfigurations struct {
							Members []struct {
								Name string `xml:"LaunchConfigurationName"`
							} `xml:"member"`
						} `xml:"LaunchConfigurations"`
					} `xml:"DescribeLaunchConfigurationsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.Result.LaunchConfigurations.Members, tt.wantCount)
			}
		})
	}
}

func TestAutoscalingHandler_DescribeScalingActivities(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "with_group",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=act-asg&MinSize=1&MaxSize=3",
				)
			},
			body:       "Action=DescribeScalingActivities&Version=2011-01-01&AutoScalingGroupName=act-asg",
			wantStatus: http.StatusOK,
		},
		{
			name:       "no_group_filter",
			body:       "Action=DescribeScalingActivities&Version=2011-01-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "nonexistent_group",
			body:       "Action=DescribeScalingActivities&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_UpdateAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "update_existing",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=upd-asg&MaxSize=10",
			wantStatus: http.StatusOK,
		},
		{
			name:       "update_nonexistent",
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=no-such&MaxSize=3",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DeleteLaunchConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "delete_existing_lc",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateLaunchConfiguration&Version=2011-01-01"+
						"&LaunchConfigurationName=del-lc&ImageId=ami-abc&InstanceType=t2.micro",
				)
			},
			body:       "Action=DeleteLaunchConfiguration&Version=2011-01-01&LaunchConfigurationName=del-lc",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete_nonexistent_lc",
			body:       "Action=DeleteLaunchConfiguration&Version=2011-01-01&LaunchConfigurationName=no-such-lc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_Persistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *autoscaling.Handler)
		name string
	}{
		{
			name: "snapshot_and_restore",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()

				postAutoscalingForm(t, h, "Action=CreateAutoScalingGroup&Version=2011-01-01"+
					"&AutoScalingGroupName=snap-asg&MinSize=1&MaxSize=3")

				data := h.Snapshot(t.Context())
				require.NotNil(t, data)

				h2 := newAutoscalingHandler()
				err := h2.Restore(t.Context(), data)
				require.NoError(t, err)

				rec := postAutoscalingForm(t, h2, "Action=DescribeAutoScalingGroups&Version=2011-01-01")
				assert.Equal(t, http.StatusOK, rec.Code)
				assert.Contains(t, rec.Body.String(), "snap-asg")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			tt.run(t, h)
		})
	}
}

func TestAutoscalingProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "provider_name",
			run: func(t *testing.T) {
				t.Helper()

				p := &autoscaling.Provider{}
				assert.Equal(t, "Autoscaling", p.Name())
			},
		},
		{
			name: "provider_init",
			run: func(t *testing.T) {
				t.Helper()

				p := &autoscaling.Provider{}
				svc, err := p.Init(nil)
				require.NoError(t, err)
				require.NotNil(t, svc)
				assert.Equal(t, "Autoscaling", svc.Name())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t)
		})
	}
}

func TestAutoscalingHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newAutoscalingHandler()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "with_group_name",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=my-resource-asg",
			want: "my-resource-asg",
		},
		{
			name: "without_group_name",
			body: "Action=DescribeAutoScalingGroups&Version=2011-01-01",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestAutoscalingHandler_ChaosInterface(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *autoscaling.Handler)
		name string
	}{
		{
			name: "chaos_service_name",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				assert.Equal(t, "autoscaling", h.ChaosServiceName())
			},
		},
		{
			name: "chaos_operations",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				ops := h.ChaosOperations()
				assert.NotEmpty(t, ops)
				assert.Contains(t, ops, "CreateAutoScalingGroup")
			},
		},
		{
			name: "chaos_regions",
			run: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				regions := h.ChaosRegions()
				assert.NotEmpty(t, regions)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			tt.run(t, h)
		})
	}
}

func TestAutoscalingHandler_UpdateAllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "update_all_optional_fields",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=full-upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body: "Action=UpdateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=full-upd-asg" +
				"&MinSize=2&MaxSize=8&DesiredCapacity=3" +
				"&DefaultCooldown=120&HealthCheckGracePeriod=60" +
				"&LaunchConfigurationName=my-lc" +
				"&HealthCheckType=ELB" +
				"&AvailabilityZones.member.1=us-east-1b",
			wantStatus: http.StatusOK,
		},
		{
			name: "update_invalid_min_size",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-upd-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-upd-asg&MinSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_max_size",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-max-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-max-asg&MaxSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_desired",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-des-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-des-asg&DesiredCapacity=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_cooldown",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-cool-asg&MinSize=1&MaxSize=5",
				)
			},
			body:       "Action=UpdateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-cool-asg&DefaultCooldown=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "update_invalid_health_grace",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inv-hgp-asg&MinSize=1&MaxSize=5",
				)
			},
			body: "Action=UpdateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-hgp-asg&HealthCheckGracePeriod=bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_CreateWithTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "create_with_tags_and_lc",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=tagged-asg&MinSize=1&MaxSize=5" +
				"&LaunchConfigurationName=my-lc" +
				"&HealthCheckType=EC2&HealthCheckGracePeriod=300" +
				"&DefaultCooldown=300" +
				"&LoadBalancerNames.member.1=my-elb" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_min_size",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-asg&MinSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_max_size",
			body:       "Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=inv-asg2&MinSize=1&MaxSize=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_desired_capacity",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg3&MinSize=1&MaxSize=5&DesiredCapacity=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_cooldown",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg4&MinSize=1&MaxSize=5&DefaultCooldown=bad",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "invalid_health_grace_period",
			body: "Action=CreateAutoScalingGroup&Version=2011-01-01" +
				"&AutoScalingGroupName=inv-asg5&MinSize=1&MaxSize=5&HealthCheckGracePeriod=bad",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_AttachInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_instances_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=attach-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachInstances&Version=2011-01-01" +
				"&AutoScalingGroupName=attach-asg" +
				"&InstanceIds.member.1=i-abc123&InstanceIds.member.2=i-def456",
			wantStatus: http.StatusOK,
		},
		{
			name:       "attach_instances_group_not_found",
			body:       "Action=AttachInstances&Version=2011-01-01&AutoScalingGroupName=no-such&InstanceIds.member.1=i-abc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_AttachLoadBalancerTargetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_tgs_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tg-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachLoadBalancerTargetGroups&Version=2011-01-01" +
				"&AutoScalingGroupName=tg-asg" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_tgs_group_not_found",
			body: "Action=AttachLoadBalancerTargetGroups&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&TargetGroupARNs.member.1=arn:aws:elasticloadbalancing:us-east-1:123:targetgroup/tg/abc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_AttachLoadBalancers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_lbs_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=lb-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachLoadBalancers&Version=2011-01-01" +
				"&AutoScalingGroupName=lb-asg" +
				"&LoadBalancerNames.member.1=my-elb",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_lbs_group_not_found",
			body: "Action=AttachLoadBalancers&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&LoadBalancerNames.member.1=elb",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_AttachTrafficSources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "attach_traffic_sources_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=ts-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=AttachTrafficSources&Version=2011-01-01" +
				"&AutoScalingGroupName=ts-asg" +
				"&TrafficSources.member.1.Identifier=arn:aws:vpc-lattice:us-east-1:123:targetgroup/tg-abc" +
				"&TrafficSources.member.1.Type=vpc-lattice",
			wantStatus: http.StatusOK,
		},
		{
			name: "attach_traffic_sources_group_not_found",
			body: "Action=AttachTrafficSources&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&TrafficSources.member.1.Identifier=arn:abc&TrafficSources.member.1.Type=vpc-lattice",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_BatchDeleteScheduledAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "batch_delete_existing_action",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-asg&MinSize=0&MaxSize=5")
				postAutoscalingForm(t, h,
					"Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-asg"+
						"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=action-1"+
						"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=3")
			},
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=sched-asg" +
				"&ScheduledActionNames.member.1=action-1",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_delete_nonexistent_action_returns_failures",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sched-fail-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=sched-fail-asg" +
				"&ScheduledActionNames.member.1=no-such-action",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_delete_group_not_found",
			body: "Action=BatchDeleteScheduledAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&ScheduledActionNames.member.1=a",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_BatchPutScheduledUpdateGroupAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "batch_put_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=put-sched-asg&MinSize=0&MaxSize=10")
			},
			body: "Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01" +
				"&AutoScalingGroupName=put-sched-asg" +
				"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=scale-up" +
				"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=5" +
				"&ScheduledUpdateGroupActions.member.1.Recurrence=0 9 * * *",
			wantStatus: http.StatusOK,
		},
		{
			name: "batch_put_group_not_found",
			body: "Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such" +
				"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=a",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_CancelInstanceRefresh(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		body       string
		wantStatus int
		wantIDSet  bool
	}{
		{
			name: "cancel_active_refresh",
			setup: func(t *testing.T, _ *autoscaling.Handler, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "refresh-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				require.NoError(t, err)

				err = b.AddInstanceRefresh(autoscaling.InstanceRefresh{
					InstanceRefreshID:    "irs-12345",
					AutoScalingGroupName: "refresh-asg",
					Status:               "InProgress",
				})
				require.NoError(t, err)
			},
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=refresh-asg",
			wantStatus: http.StatusOK,
			wantIDSet:  true,
		},
		{
			name: "cancel_no_active_refresh",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=no-refresh-asg&MinSize=0&MaxSize=5")
			},
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=no-refresh-asg",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "cancel_refresh_group_not_found",
			body:       "Action=CancelInstanceRefresh&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantIDSet {
				assert.Contains(t, rec.Body.String(), "irs-12345")
			}
		})
	}
}

func TestAutoscalingHandler_CompleteLifecycleAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "complete_lifecycle_action_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=lc-action-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=lc-action-asg" +
				"&LifecycleHookName=my-hook" +
				"&LifecycleActionToken=abc123" +
				"&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusOK,
		},
		{
			name: "complete_lifecycle_group_not_found",
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=no-such&LifecycleHookName=h&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "complete_lifecycle_missing_hook_name",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=miss-hook-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=miss-hook-asg&LifecycleActionResult=CONTINUE",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "complete_lifecycle_missing_result",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=miss-result-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CompleteLifecycleAction&Version=2011-01-01" +
				"&AutoScalingGroupName=miss-result-asg&LifecycleHookName=my-hook",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_CreateOrUpdateTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler)
		body       string
		wantStatus int
	}{
		{
			name: "create_or_update_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tag-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg" +
				"&Tags.member.1.ResourceType=auto-scaling-group" +
				"&Tags.member.1.Key=env" +
				"&Tags.member.1.Value=production",
			wantStatus: http.StatusOK,
		},
		{
			name: "create_or_update_tags_group_not_found",
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=no-such" +
				"&Tags.member.1.ResourceType=auto-scaling-group" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_or_update_tags_unknown_resource_type_ignored",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=tag-asg2&MinSize=0&MaxSize=5")
			},
			body: "Action=CreateOrUpdateTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg2" +
				"&Tags.member.1.ResourceType=other-type" +
				"&Tags.member.1.Key=env&Tags.member.1.Value=test",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_DeleteLifecycleHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "delete_existing_hook",
			setup: func(t *testing.T, _ *autoscaling.Handler, b *autoscaling.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
					AutoScalingGroupName: "hook-asg",
					MinSize:              0,
					MaxSize:              5,
				})
				require.NoError(t, err)

				err = b.AddLifecycleHook(autoscaling.LifecycleHook{
					LifecycleHookName:    "my-hook",
					AutoScalingGroupName: "hook-asg",
					LifecycleTransition:  "autoscaling:EC2_INSTANCE_LAUNCHING",
					DefaultResult:        "CONTINUE",
				})
				require.NoError(t, err)
			},
			body:       "Action=DeleteLifecycleHook&Version=2011-01-01&AutoScalingGroupName=hook-asg&LifecycleHookName=my-hook",
			wantStatus: http.StatusOK,
		},
		{
			name: "delete_nonexistent_hook",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=no-hook-asg&MinSize=0&MaxSize=5")
			},
			body: "Action=DeleteLifecycleHook&Version=2011-01-01" +
				"&AutoScalingGroupName=no-hook-asg&LifecycleHookName=ghost-hook",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_hook_group_not_found",
			body:       "Action=DeleteLifecycleHook&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=my-hook",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_SetDesiredCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "set_desired_capacity_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=sdc-asg&MinSize=1&MaxSize=10&DesiredCapacity=2",
				)
			},
			body:       "Action=SetDesiredCapacity&Version=2011-01-01&AutoScalingGroupName=sdc-asg&DesiredCapacity=5",
			wantStatus: http.StatusOK,
		},
		{
			name:       "set_desired_capacity_group_not_found",
			body:       "Action=SetDesiredCapacity&Version=2011-01-01&AutoScalingGroupName=no-such&DesiredCapacity=3",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_TerminateInstanceInAutoScalingGroup(t *testing.T) {
	t.Parallel()

	const terminateAction = "Action=TerminateInstanceInAutoScalingGroup&Version=2011-01-01"

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler, b *autoscaling.InMemoryBackend)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "terminate_instance_success",
			setup: func(t *testing.T, h *autoscaling.Handler, _ *autoscaling.InMemoryBackend) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=term-asg&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			// i-fake not in any group → 400.
			body:       terminateAction + "&InstanceId=i-fake&ShouldDecrementDesiredCapacity=false",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "terminate_instance_not_found",
			body:       terminateAction + "&InstanceId=i-unknown&ShouldDecrementDesiredCapacity=true",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := autoscaling.NewInMemoryBackend()
			h := autoscaling.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_PutAndDescribeLifecycleHooks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "put_lifecycle_hook_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=hook-asg&MinSize=0&MaxSize=5",
				)
			},
			body: "Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=hook-asg" +
				"&LifecycleHookName=my-hook&LifecycleTransition=autoscaling:EC2_INSTANCE_LAUNCHING",
			wantStatus: http.StatusOK,
		},
		{
			name:       "put_lifecycle_hook_group_not_found",
			body:       "Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=no-such&LifecycleHookName=h",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe_lifecycle_hooks_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dlh-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t, h,
					"Action=PutLifecycleHook&Version=2011-01-01&AutoScalingGroupName=dlh-asg&LifecycleHookName=h1",
				)
			},
			body:       "Action=DescribeLifecycleHooks&Version=2011-01-01&AutoScalingGroupName=dlh-asg",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "h1")
			},
		},
		{
			name:       "describe_lifecycle_hooks_group_not_found",
			body:       "Action=DescribeLifecycleHooks&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}

func TestAutoscalingHandler_DescribeScheduledActions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "describe_scheduled_actions_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=sa-asg&MinSize=0&MaxSize=5",
				)
				postAutoscalingForm(
					t, h,
					"Action=BatchPutScheduledUpdateGroupAction&Version=2011-01-01&AutoScalingGroupName=sa-asg"+
						"&ScheduledUpdateGroupActions.member.1.ScheduledActionName=scale-out"+
						"&ScheduledUpdateGroupActions.member.1.DesiredCapacity=5",
				)
			},
			body:       "Action=DescribeScheduledActions&Version=2011-01-01&AutoScalingGroupName=sa-asg",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "scale-out")
			},
		},
		{
			name:       "describe_scheduled_actions_group_not_found",
			body:       "Action=DescribeScheduledActions&Version=2011-01-01&AutoScalingGroupName=no-such",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}

func TestAutoscalingHandler_DeleteAndDescribeTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "delete_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=tag-asg&MinSize=0&MaxSize=5"+
						"&Tags.member.1.Key=env&Tags.member.1.Value=prod",
				)
			},
			body: "Action=DeleteTags&Version=2011-01-01" +
				"&Tags.member.1.ResourceId=tag-asg&Tags.member.1.ResourceType=auto-scaling-group&Tags.member.1.Key=env",
			wantStatus: http.StatusOK,
		},
		{
			name: "describe_tags_success",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t,
					h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=dtag-asg&MinSize=0&MaxSize=5"+
						"&Tags.member.1.Key=team&Tags.member.1.Value=platform",
				)
			},
			body:       "Action=DescribeTags&Version=2011-01-01",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "platform")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}

func TestAutoscalingHandler_DescribeAutoScalingInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		checkBody  func(t *testing.T, body string)
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "describe_instances_empty",
			body:       "Action=DescribeAutoScalingInstances&Version=2011-01-01",
			wantStatus: http.StatusOK,
		},
		{
			name: "describe_instances_with_group",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=inst-asg&MinSize=1&MaxSize=3&DesiredCapacity=1",
				)
			},
			body:       "Action=DescribeAutoScalingInstances&Version=2011-01-01",
			wantStatus: http.StatusOK,
			checkBody: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "InService")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.checkBody != nil {
				tt.checkBody(t, rec.Body.String())
			}
		})
	}
}

func TestAutoscalingHandler_ForceDeleteAutoScalingGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *autoscaling.Handler)
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "delete_with_instances_requires_force",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=force-asg&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=force-asg",
			wantStatus: http.StatusBadRequest, // ForceDelete not set
		},
		{
			name: "delete_with_force_succeeds",
			setup: func(t *testing.T, h *autoscaling.Handler) {
				t.Helper()
				postAutoscalingForm(
					t, h,
					"Action=CreateAutoScalingGroup&Version=2011-01-01"+
						"&AutoScalingGroupName=force-asg2&MinSize=1&MaxSize=5&DesiredCapacity=2",
				)
			},
			body:       "Action=DeleteAutoScalingGroup&Version=2011-01-01&AutoScalingGroupName=force-asg2&ForceDelete=true",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAutoscalingHandler_EnabledMetricsRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		enableMetrics  []string
		wantMetrics    []string
		disableMetrics []string
		wantAfterDis   []string
	}{
		{
			name:          "enable_specific_metrics_visible_in_describe",
			enableMetrics: []string{"GroupMinSize", "GroupMaxSize"},
			wantMetrics:   []string{"GroupMinSize", "GroupMaxSize"},
		},
		{
			name:           "disable_all_metrics_clears_list",
			enableMetrics:  []string{"GroupMinSize", "GroupMaxSize"},
			wantMetrics:    []string{"GroupMinSize", "GroupMaxSize"},
			disableMetrics: nil, // nil = disable all
			wantAfterDis:   []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()

			postAutoscalingForm(t, h,
				"Action=CreateAutoScalingGroup&Version=2011-01-01"+
					"&AutoScalingGroupName=metrics-asg&MinSize=0&MaxSize=5")

			// Enable metrics
			parts := []string{
				"Action=EnableMetricsCollection",
				"Version=2011-01-01",
				"AutoScalingGroupName=metrics-asg",
				"Granularity=1Minute",
			}
			for i, m := range tt.enableMetrics {
				parts = append(parts, fmt.Sprintf("Metrics.member.%d=%s", i+1, m))
			}
			enableBody := strings.Join(parts, "&")
			rec := postAutoscalingForm(t, h, enableBody)
			require.Equal(t, http.StatusOK, rec.Code)

			// Parse DescribeAutoScalingGroups response to get EnabledMetrics
			rec = postAutoscalingForm(t, h,
				"Action=DescribeAutoScalingGroups&Version=2011-01-01"+
					"&AutoScalingGroupNames.member.1=metrics-asg")
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
				Result  struct {
					AutoScalingGroups struct {
						Members []struct {
							Name           string `xml:"AutoScalingGroupName"`
							EnabledMetrics struct {
								Members []struct {
									Metric      string `xml:"Metric"`
									Granularity string `xml:"Granularity"`
								} `xml:"member"`
							} `xml:"EnabledMetrics"`
						} `xml:"member"`
					} `xml:"AutoScalingGroups"`
				} `xml:"DescribeAutoScalingGroupsResult"`
			}
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Result.AutoScalingGroups.Members, 1)

			got := resp.Result.AutoScalingGroups.Members[0].EnabledMetrics.Members
			require.Len(t, got, len(tt.wantMetrics))

			gotNames := make([]string, len(got))
			for i, m := range got {
				gotNames[i] = m.Metric
				assert.Equal(t, "1Minute", m.Granularity)
			}
			assert.ElementsMatch(t, tt.wantMetrics, gotNames)

			if tt.wantAfterDis != nil {
				// Disable all metrics and re-check
				postAutoscalingForm(t, h,
					"Action=DisableMetricsCollection&Version=2011-01-01"+
						"&AutoScalingGroupName=metrics-asg")

				rec = postAutoscalingForm(t, h,
					"Action=DescribeAutoScalingGroups&Version=2011-01-01"+
						"&AutoScalingGroupNames.member.1=metrics-asg")
				require.Equal(t, http.StatusOK, rec.Code)

				var resp2 struct {
					XMLName xml.Name `xml:"DescribeAutoScalingGroupsResponse"`
					Result  struct {
						AutoScalingGroups struct {
							Members []struct {
								EnabledMetrics struct {
									Members []struct {
										Metric string `xml:"Metric"`
									} `xml:"member"`
								} `xml:"EnabledMetrics"`
							} `xml:"member"`
						} `xml:"AutoScalingGroups"`
					} `xml:"DescribeAutoScalingGroupsResult"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp2))
				require.Len(t, resp2.Result.AutoScalingGroups.Members, 1)
				assert.Empty(t, resp2.Result.AutoScalingGroups.Members[0].EnabledMetrics.Members)
			}
		})
	}
}

func TestAutoscalingHandler_CapacityValidation(t *testing.T) {
	t.Parallel()

	const createASGFmt = "Action=CreateAutoScalingGroup&Version=2011-01-01"

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name: "create_desired_less_than_min",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg&MinSize=3&MaxSize=10&DesiredCapacity=1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_desired_exceeds_max",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg2&MinSize=1&MaxSize=5&DesiredCapacity=10",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_min_greater_than_max",
			body:       createASGFmt + "&AutoScalingGroupName=cap-asg3&MinSize=10&MaxSize=5",
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_valid_capacity",
			body: createASGFmt +
				"&AutoScalingGroupName=cap-asg4&MinSize=1&MaxSize=10&DesiredCapacity=3",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAutoscalingHandler()
			rec := postAutoscalingForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
