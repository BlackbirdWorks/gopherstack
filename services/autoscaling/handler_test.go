package autoscaling_test

import (
	"encoding/xml"
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

				data := h.Snapshot()
				require.NotNil(t, data)

				h2 := newAutoscalingHandler()
				err := h2.Restore(data)
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
