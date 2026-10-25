package ocifunctions_test

import (
	"net/http"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/functions"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFunction_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod        func(*functions.CreateFunctionDetails)
		check      func(*testing.T, *env, functions.Function)
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name: "stores every field",
			mod: func(d *functions.CreateFunctionDetails) {
				d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{
					Image: new("reg.example/ns/fn:2"), ImageDigest: new("sha256:abc"),
				}
				d.MemoryInMBs = new(int64(3072))
				d.TimeoutInSeconds = new(300)
				d.DetachedModeTimeoutInSeconds = new(3600)
				d.Config = map[string]string{"A": "1"}
				d.ProvisionedConcurrencyConfig = functions.ConstantProvisionedConcurrencyConfig{Count: new(40)}
				d.TraceConfig = &functions.FunctionTraceConfig{IsEnabled: new(true)}
				d.FreeformTags = map[string]string{"x": "y"}
			},
			check: func(t *testing.T, e *env, f functions.Function) {
				t.Helper()

				src, ok := f.SourceDetails.(functions.ContainerImageFunctionSourceDetails)
				require.True(t, ok)
				assert.Equal(t, "reg.example/ns/fn:2", *src.Image)
				assert.Equal(t, "sha256:abc", *src.ImageDigest)
				assert.Equal(t, int64(3072), *f.MemoryInMBs)
				assert.Equal(t, 300, *f.TimeoutInSeconds)
				assert.Equal(t, 3600, *f.DetachedModeTimeoutInSeconds)
				assert.Equal(t, map[string]string{"A": "1"}, f.Config)
				pc, ok := f.ProvisionedConcurrencyConfig.(functions.ConstantProvisionedConcurrencyConfig)
				require.True(t, ok)
				assert.Equal(t, 40, *pc.Count)
				assert.True(t, *f.TraceConfig.IsEnabled)
				assert.Equal(t, map[string]string{"x": "y"}, f.FreeformTags)
				assert.Equal(t, e.base, *f.InvokeEndpoint)
				assert.Equal(t, functions.FunctionShapeX86, f.Shape)
				assert.Equal(t, functions.FunctionLifecycleStateActive, f.LifecycleState)
			},
		},
		{
			name: "defaults",
			check: func(t *testing.T, _ *env, f functions.Function) {
				t.Helper()
				assert.Equal(t, 30, *f.TimeoutInSeconds)
				_, ok := f.ProvisionedConcurrencyConfig.(functions.NoneProvisionedConcurrencyConfig)
				assert.True(t, ok)
			},
		},
		{
			name:       "memory too low",
			mod:        func(d *functions.CreateFunctionDetails) { d.MemoryInMBs = new(int64(64)) },
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
		},
		{
			name:       "memory too high",
			mod:        func(d *functions.CreateFunctionDetails) { d.MemoryInMBs = new(int64(4096)) },
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
		},
		{
			name:       "timeout too high",
			mod:        func(d *functions.CreateFunctionDetails) { d.TimeoutInSeconds = new(301) },
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
		},
		{
			name:       "timeout too low",
			mod:        func(d *functions.CreateFunctionDetails) { d.TimeoutInSeconds = new(1) },
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
		},
		{
			name:       "detached too high",
			mod:        func(d *functions.CreateFunctionDetails) { d.DetachedModeTimeoutInSeconds = new(3601) },
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameter",
		},
		{name: "negative concurrency", mod: func(d *functions.CreateFunctionDetails) {
			d.ProvisionedConcurrencyConfig = functions.ConstantProvisionedConcurrencyConfig{Count: new(-1)}
		}, wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "no image", mod: func(d *functions.CreateFunctionDetails) {
			d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{Image: new(" ")}
		}, wantStatus: http.StatusBadRequest, wantCode: "MissingParameter"},
		{name: "bad digest", mod: func(d *functions.CreateFunctionDetails) {
			d.SourceDetails = functions.CreateContainerImageFunctionSourceDetails{
				Image: new("a:b"), ImageDigest: new("md5:1"),
			}
		}, wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{name: "pre-built unsupported", mod: func(d *functions.CreateFunctionDetails) {
			d.SourceDetails = functions.CreatePreBuiltFunctionSourceDetails{PbfListingId: new("ocid1.pbf.oc1..x")}
		}, wantStatus: http.StatusBadRequest, wantCode: "InvalidParameter"},
		{
			name:       "unknown application",
			mod:        func(d *functions.CreateFunctionDetails) { d.ApplicationId = new("ocid1.fnapp.nope") },
			wantStatus: http.StatusNotFound,
			wantCode:   "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			app := e.mkApp(t, "app", nil)
			d := functions.CreateFunctionDetails{
				DisplayName: new("fn"), ApplicationId: app.Id, MemoryInMBs: new(int64(256)),
				SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("example/fn:1")},
			}

			if tt.mod != nil {
				tt.mod(&d)
			}

			resp, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{CreateFunctionDetails: d})
			if tt.wantStatus != 0 {
				se := svcErr(t, err)
				assert.Equal(t, tt.wantStatus, se.GetHTTPStatusCode())
				assert.Equal(t, tt.wantCode, se.GetCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, functions.FunctionLifecycleStateCreating, resp.LifecycleState)
			e.settle()

			got, err := e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: resp.Id})
			require.NoError(t, err)
			tt.check(t, e, got.Function)
		})
	}
}

func TestFunction_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(*testing.T, *env, functions.Application, functions.Function)
		name string
	}{
		{name: "duplicate name", run: func(t *testing.T, e *env, a functions.Application, _ functions.Function) {
			t.Helper()

			_, err := e.mgmt.CreateFunction(t.Context(), functions.CreateFunctionRequest{
				DisplayName: new("fn"), ApplicationId: a.Id, MemoryInMBs: new(int64(128)),
				SourceDetails: functions.CreateContainerImageFunctionSourceDetails{Image: new("i")},
			})
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "update changes fields", run: func(t *testing.T, e *env, _ functions.Application, f functions.Function) {
			t.Helper()

			resp, err := e.mgmt.UpdateFunction(t.Context(), functions.UpdateFunctionRequest{
				FunctionId:       f.Id,
				MemoryInMBs:      new(int64(512)),
				TimeoutInSeconds: new(60),
				Config:           map[string]string{"Z": "9"},
				SourceDetails: functions.UpdateContainerImageFunctionSourceDetails{
					Image: new("example/fn:2"),
				},
				ProvisionedConcurrencyConfig: functions.NoneProvisionedConcurrencyConfig{},
			})
			require.NoError(t, err)
			assert.NotNil(t, resp.OpcWorkRequestId)

			got, err := e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: f.Id})
			require.NoError(t, err)
			assert.Equal(t, int64(512), *got.MemoryInMBs)
			assert.Equal(t, 60, *got.TimeoutInSeconds)
			assert.Equal(t, map[string]string{"Z": "9"}, got.Config)

			src, ok := got.SourceDetails.(functions.ContainerImageFunctionSourceDetails)
			require.True(t, ok)
			assert.Equal(t, "example/fn:2", *src.Image)
		}},
		{name: "update validates", run: func(t *testing.T, e *env, _ functions.Application, f functions.Function) {
			t.Helper()

			_, err := e.mgmt.UpdateFunction(t.Context(), functions.UpdateFunctionRequest{
				FunctionId:  f.Id,
				MemoryInMBs: new(int64(9999)),
			})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		}},
		{name: "delete", run: func(t *testing.T, e *env, _ functions.Application, f functions.Function) {
			t.Helper()

			del, err := e.mgmt.DeleteFunction(t.Context(), functions.DeleteFunctionRequest{FunctionId: f.Id})
			require.NoError(t, err)
			assert.NotNil(t, del.OpcWorkRequestId)

			got, err := e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: f.Id})
			require.NoError(t, err)
			assert.Equal(t, functions.FunctionLifecycleStateDeleting, got.LifecycleState)

			e.settle()
			e.backend.Sweep()

			_, err = e.mgmt.GetFunction(t.Context(), functions.GetFunctionRequest{FunctionId: f.Id})
			assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

			e.mgmt.Host = e.base
			_, err = e.mgmt.DeleteApplication(
				t.Context(),
				functions.DeleteApplicationRequest{ApplicationId: f.ApplicationId},
			)
			require.NoError(t, err)
		}},
		{
			name: "delete with stale etag",
			run: func(t *testing.T, e *env, _ functions.Application, f functions.Function) {
				t.Helper()

				_, err := e.mgmt.DeleteFunction(
					t.Context(),
					functions.DeleteFunctionRequest{FunctionId: f.Id, IfMatch: new("x")},
				)
				assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			app := e.mkApp(t, "app", nil)
			tt.run(t, e, app, e.mkFn(t, app, "fn", nil))
		})
	}
}

func TestFunction_List(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		req       functions.ListFunctionsRequest
		wantNames []string
		wantNext  bool
	}{
		{"newest first", functions.ListFunctionsRequest{}, []string{"c", "b", "a"}, false},
		{"name desc", functions.ListFunctionsRequest{
			SortBy: functions.ListFunctionsSortByDisplayname, SortOrder: functions.ListFunctionsSortOrderDesc,
		}, []string{"c", "b", "a"}, false},
		{"name filter", functions.ListFunctionsRequest{DisplayName: new("a")}, []string{"a"}, false},
		{
			"state filter",
			functions.ListFunctionsRequest{LifecycleState: functions.FunctionLifecycleStateDeleting},
			nil,
			false,
		},
		{"paged", functions.ListFunctionsRequest{Limit: new(1)}, []string{"c"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			app := e.mkApp(t, "app", nil)
			other := e.mkApp(t, "other", nil)
			e.mkFn(t, other, "z", nil)

			for _, n := range []string{"a", "b", "c"} {
				e.mkFn(t, app, n, nil)
			}

			r := tt.req
			r.ApplicationId = app.Id

			resp, err := e.mgmt.ListFunctions(t.Context(), r)
			require.NoError(t, err)

			var got []string
			for _, f := range resp.Items {
				got = append(got, *f.DisplayName)
				assert.Equal(t, e.base, *f.InvokeEndpoint)
			}

			assert.Equal(t, tt.wantNames, got)
			assert.Equal(t, tt.wantNext, resp.OpcNextPage != nil)
		})
	}
}

func TestFunction_ListNeedsApplication(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	status, _, _ := rawDo(t, http.MethodGet, e.base+"/20181201/functions", "", nil)
	assert.Equal(t, http.StatusBadRequest, status)
}

func TestWorkRequests(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	app, err := e.mgmt.CreateApplication(t.Context(), functions.CreateApplicationRequest{
		CompartmentId: new(testCompartment), DisplayName: new("app"), SubnetIds: []string{testSubnet},
	})
	require.NoError(t, err)

	wr, err := e.wrc.GetWorkRequest(t.Context(), functions.GetWorkRequestRequest{WorkRequestId: app.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, functions.OperationStatusInProgress, wr.Status)
	assert.Equal(t, functions.OperationTypeCreateApplication, wr.OperationType)
	assert.Equal(t, app.Id, wr.Resources[0].Identifier)

	e.settle()

	wr, err = e.wrc.GetWorkRequest(t.Context(), functions.GetWorkRequestRequest{WorkRequestId: app.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Equal(t, functions.OperationStatusSucceeded, wr.Status)
	assert.Equal(t, functions.ActionTypeCreated, wr.Resources[0].ActionType)

	list, err := e.wrc.ListWorkRequests(t.Context(), functions.ListWorkRequestsRequest{
		CompartmentId: new(testCompartment), ResourceId: app.Id,
	})
	require.NoError(t, err)
	assert.Len(t, list.Items, 1)

	logs, err := e.wrc.ListWorkRequestLogs(t.Context(), functions.ListWorkRequestLogsRequest{
		WorkRequestId: app.OpcWorkRequestId,
	})
	require.NoError(t, err)
	assert.Len(t, logs.Items, 2)

	errs, err := e.wrc.ListWorkRequestErrors(t.Context(), functions.ListWorkRequestErrorsRequest{
		WorkRequestId: app.OpcWorkRequestId,
	})
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	_, err = e.wrc.GetWorkRequest(t.Context(), functions.GetWorkRequestRequest{WorkRequestId: new("nope")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestRoutingAndAuth(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		enforce    bool
		register   bool
		wantStatus int
	}{
		{
			"documented version path",
			http.MethodGet,
			"/20181201/applications?compartmentId=c",
			false,
			false,
			http.StatusOK,
		},
		{"sdk version path", http.MethodGet, "/20260325/applications?compartmentId=c", false, false, http.StatusOK},
		{"unknown path", http.MethodGet, "/20181201/nothing", false, false, http.StatusNotFound},
		{"wrong method", http.MethodPatch, "/20181201/applications", false, false, http.StatusNotFound},
		{"unsigned rejected when enforced", http.MethodGet, "/20181201/applications?compartmentId=c", true, true,
			http.StatusUnauthorized},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{enforce: tt.enforce, register: tt.register})
			status, _, hdr := rawDo(t, tt.method, e.base+tt.path, "", nil)
			assert.Equal(t, tt.wantStatus, status)
			assert.NotEmpty(t, hdr.Get("Opc-Request-Id"))
		})
	}
}

func TestSignedRequests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		register bool
		wantErr  bool
	}{
		{"registered key accepted", true, false},
		{"unknown key rejected when enforced", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{enforce: true, register: tt.register})
			_, err := e.mgmt.ListApplications(t.Context(), functions.ListApplicationsRequest{
				CompartmentId: new(testCompartment),
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
		})
	}
}
