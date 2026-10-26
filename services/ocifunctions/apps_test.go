package ocifunctions_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplication_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod        func(*functions.CreateApplicationDetails)
		check      func(*testing.T, functions.Application)
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "stores every field",
			mod: func(d *functions.CreateApplicationDetails) {
				d.Config = map[string]string{"K": "v"}
				d.Shape = functions.CreateApplicationDetailsShapeX86Arm
				d.SyslogUrl = new("tcp://logs.example.com:514")
				d.NetworkSecurityGroupIds = []string{"ocid1.nsg.oc1..a"}
				d.TraceConfig = &functions.ApplicationTraceConfig{IsEnabled: new(true), DomainId: new("apm1")}
				d.ImagePolicyConfig = &functions.ImagePolicyConfig{
					IsPolicyEnabled: new(true), KeyDetails: []functions.KeyDetails{{KmsKeyId: new("ocid1.key.oc1..k")}},
				}
				d.FreeformTags = map[string]string{"a": "b"}
			},
			check: func(t *testing.T, a functions.Application) {
				t.Helper()
				assert.Equal(t, functions.ApplicationLifecycleStateActive, a.LifecycleState)
				assert.Equal(t, functions.ApplicationShapeX86Arm, a.Shape)
				assert.Equal(t, map[string]string{"K": "v"}, a.Config)
				assert.Equal(t, "tcp://logs.example.com:514", *a.SyslogUrl)
				assert.Equal(t, []string{testSubnet}, a.SubnetIds)
				assert.Equal(t, []string{"ocid1.nsg.oc1..a"}, a.NetworkSecurityGroupIds)
				assert.True(t, *a.TraceConfig.IsEnabled)
				assert.Equal(t, "apm1", *a.TraceConfig.DomainId)
				assert.True(t, *a.ImagePolicyConfig.IsPolicyEnabled)
				assert.Equal(t, "ocid1.key.oc1..k", *a.ImagePolicyConfig.KeyDetails[0].KmsKeyId)
				assert.Equal(t, map[string]string{"a": "b"}, a.FreeformTags)
				assert.NotNil(t, a.TimeCreated)
			},
		},
		{
			name: "defaults to x86",
			check: func(t *testing.T, a functions.Application) {
				t.Helper()
				assert.Equal(t, functions.ApplicationShapeX86, a.Shape)
			},
		},
		{
			name:       "no subnets",
			mod:        func(d *functions.CreateApplicationDetails) { d.SubnetIds = []string{} },
			wantStatus: http.StatusBadRequest, wantCode: "MissingParameter",
		},
		{
			name:       "bad syslog scheme",
			mod:        func(d *functions.CreateApplicationDetails) { d.SyslogUrl = new("ftp://x:1") },
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter",
		},
		{
			name: "config too large",
			mod: func(d *functions.CreateApplicationDetails) {
				d.Config = map[string]string{"k": string(make([]byte, 5000))}
			},
			wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter",
		},
		{
			name: "image policy without flag",
			mod: func(d *functions.CreateApplicationDetails) {
				d.ImagePolicyConfig = &functions.ImagePolicyConfig{}
			},
			wantStatus: http.StatusBadRequest, wantCode: "MissingParameter",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			d := functions.CreateApplicationDetails{
				CompartmentId: new(testCompartment), DisplayName: new("app"), SubnetIds: []string{testSubnet},
			}

			if tt.mod != nil {
				tt.mod(&d)
			}

			resp, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{
				CreateApplicationDetails: d,
			})
			if tt.wantStatus != 0 {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantStatus, se.GetHTTPStatusCode())
				assert.Equal(t, tt.wantCode, se.GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, functions.ApplicationLifecycleStateCreating, resp.LifecycleState)
			assert.NotNil(t, resp.OpcWorkRequestId)
			e.settle()

			got, err := e.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: resp.Id})
			require.NoError(t, err)
			tt.check(t, got.Application)
		})
	}
}

func TestApplication_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *env, functions.Application)
		name string
	}{
		{name: "duplicate name", run: func(t *testing.T, e *env, _ functions.Application) {
			t.Helper()

			_, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{
				CompartmentId: new(testCompartment), DisplayName: new("app"), SubnetIds: []string{testSubnet},
			})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "update replaces config and tags", run: func(t *testing.T, e *env, a functions.Application) {
			t.Helper()

			resp, err := e.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
				ApplicationId: a.Id,
				Config:        map[string]string{"N": "2"}, SyslogUrl: new("udp://h:1"),
				FreeformTags: map[string]string{"t": "1"},
			})
			require.NoError(t, err)
			assert.Equal(t, http.StatusAccepted, resp.RawResponse.StatusCode)
			assert.NotNil(t, resp.OpcWorkRequestId)

			got, err := e.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: a.Id})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"N": "2"}, got.Config)
			assert.Equal(t, "udp://h:1", *got.SyslogUrl)
			assert.Equal(t, map[string]string{"t": "1"}, got.FreeformTags)
		}},
		{name: "update with stale etag", run: func(t *testing.T, e *env, a functions.Application) {
			t.Helper()

			_, err := e.mgmt.UpdateApplication(t.Context(), functions.UpdateApplicationRequest{
				ApplicationId: a.Id, IfMatch: new("nope"),
				UpdateApplicationDetails: functions.UpdateApplicationDetails{},
			})
			assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "delete then gone", run: func(t *testing.T, e *env, a functions.Application) {
			t.Helper()

			del, err := e.mgmt.DeleteApplication(t.Context(), functions.DeleteApplicationRequest{ApplicationId: a.Id})
			require.NoError(t, err)
			assert.NotNil(t, del.OpcWorkRequestId)
			assert.Equal(t, http.StatusAccepted, del.RawResponse.StatusCode)

			got, err := e.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: a.Id})
			require.NoError(t, err)
			assert.Equal(t, functions.ApplicationLifecycleStateDeleting, got.LifecycleState)

			_, err = e.mgmt.DeleteApplication(t.Context(), functions.DeleteApplicationRequest{ApplicationId: a.Id})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

			e.settle()
			e.backend.Sweep()

			_, err = e.mgmt.GetApplication(t.Context(), functions.GetApplicationRequest{ApplicationId: a.Id})
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "delete refused with functions", run: func(t *testing.T, e *env, a functions.Application) {
			t.Helper()
			e.mkFn(t, a, "fn", nil)

			_, err := e.mgmt.DeleteApplication(t.Context(), functions.DeleteApplicationRequest{ApplicationId: a.Id})
			se := svcErr(t, err)
			assert.Equal(t, http.StatusConflict, se.GetHTTPStatusCode())
			assert.Equal(t, "IncorrectState", se.GetCode())
		}},
		{name: "change compartment moves functions", run: func(t *testing.T, e *env, a functions.Application) {
			t.Helper()

			fn := e.mkFn(t, a, "fn", nil)
			moved, err := e.mgmt.ChangeApplicationCompartment(
				t.Context(),
				functions.ChangeApplicationCompartmentRequest{
					ApplicationId: a.Id,
					CompartmentId: new("ocid1.compartment.oc1..other"),
				},
			)
			require.NoError(t, err)
			assert.NotNil(t, moved.OpcWorkRequestId)

			got, err := e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: fn.Id})
			require.NoError(t, err)
			assert.Equal(t, "ocid1.compartment.oc1..other", *got.CompartmentId)
		}},
		{name: "get unknown", run: func(t *testing.T, e *env, _ functions.Application) {
			t.Helper()

			_, err := e.mgmt.GetApplication(
				t.Context(),
				functions.GetApplicationRequest{ApplicationId: new("ocid1.fnapp.x")},
			)
			se := svcErr(t, err)
			assert.Equal(t, http.StatusNotFound, se.GetHTTPStatusCode())
			assert.Equal(t, "NotAuthorizedOrNotFound", se.GetCode())
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			tt.run(t, e, e.mkApp(t, "app", nil))
		})
	}
}

func TestApplication_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       func(*env) functions.ListApplicationsRequest
		wantNames []string
		wantNext  bool
	}{
		{
			"all newest first",
			func(*env) functions.ListApplicationsRequest { return functions.ListApplicationsRequest{} },
			[]string{"c", "b", "a"},
			false,
		},
		{"by name asc", func(*env) functions.ListApplicationsRequest {
			return functions.ListApplicationsRequest{
				SortBy: functions.ListApplicationsSortByDisplayname, SortOrder: functions.ListApplicationsSortOrderAsc,
			}
		}, []string{"a", "b", "c"}, false},
		{"filter name", func(*env) functions.ListApplicationsRequest {
			return functions.ListApplicationsRequest{DisplayName: new("b")}
		}, []string{"b"}, false},
		{"filter state", func(*env) functions.ListApplicationsRequest {
			return functions.ListApplicationsRequest{LifecycleState: functions.ApplicationLifecycleStateInactive}
		}, nil, false},
		{"paged", func(*env) functions.ListApplicationsRequest {
			return functions.ListApplicationsRequest{Limit: new(2)}
		}, []string{"c", "b"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			for _, n := range []string{"a", "b", "c"} {
				e.mkApp(t, n, nil)
			}

			r := tt.req(e)
			r.CompartmentId = new(testCompartment)

			resp, err := e.mgmt.ListApplications(t.Context(), r)
			require.NoError(t, err)

			var got []string
			for _, a := range resp.Items {
				got = append(got, *a.DisplayName)
			}

			assert.Equal(t, tt.wantNames, got)
			assert.Equal(t, tt.wantNext, resp.OpcNextPage != nil)
		})
	}
}

func TestApplication_ListNeedsCompartment(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, body, _ := rawDo(t, http.MethodGet, e.base+"/20181201/applications", "", nil)
	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, body, "MissingParameter")
}
