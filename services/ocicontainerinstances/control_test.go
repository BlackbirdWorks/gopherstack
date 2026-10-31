package ocicontainerinstances_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	ci "github.com/oracle/oci-go-sdk/v65/containerinstances"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	svc "github.com/blackbirdworks/gopherstack/services/ocicontainerinstances"
)

func TestCreateValidation(t *testing.T) {
	t.Parallel()

	manyContainers := func(d *ci.CreateContainerInstanceDetails) {
		d.Containers = nil
		for range 33 {
			d.Containers = append(d.Containers, ci.CreateContainerDetails{ImageUrl: common.String("alpine")})
		}
	}

	tests := []struct {
		mutate   func(*ci.CreateContainerInstanceDetails)
		name     string
		wantCode string
		wantHTTP int
	}{
		{func(d *ci.CreateContainerInstanceDetails) { d.Shape = common.String("CI.Nope") }, "bad-shape", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.ShapeConfig.Ocpus = f32(65) }, "ocpus-high", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.ShapeConfig.Ocpus = f32(0) }, "ocpus-low", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.ShapeConfig.MemoryInGBs = f32(1) }, "memory-low", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.ShapeConfig.MemoryInGBs = f32(2000) }, "memory-high", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Shape = common.String("CI.Standard.A1.Flex")
			d.ShapeConfig.Ocpus = f32(77)
		}, "a1-ocpus-high", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.Containers = []ci.CreateContainerDetails{} }, "no-containers", "MissingParameter", 400},
		{manyContainers, "too-many-containers", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.Containers[0].ImageUrl = common.String("") }, "no-image", "MissingParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) { d.Vnics = []ci.CreateContainerVnicDetails{} }, "no-vnics", "MissingParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Vnics = append(d.Vnics, ci.CreateContainerVnicDetails{SubnetId: common.String(testSubnet)})
		}, "two-vnics", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Vnics[0].SubnetId = common.String("ocid1.subnet.oc1..gone")
		}, "unknown-subnet", "NotAuthorizedOrNotFound", 404},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.CompartmentId = common.String("ocid1.compartment.oc1..gone")
		}, "unknown-compartment", "NotAuthorizedOrNotFound", 404},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].VolumeMounts = []ci.CreateVolumeMountDetails{{MountPath: common.String("/x"), VolumeName: common.String("none")}}
		}, "mount-missing-volume", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Volumes = []ci.CreateContainerVolumeDetails{
				ci.CreateContainerEmptyDirVolumeDetails{Name: common.String("v")},
				ci.CreateContainerEmptyDirVolumeDetails{Name: common.String("v")},
			}
		}, "duplicate-volume", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Volumes = []ci.CreateContainerVolumeDetails{ci.CreateContainerConfigFileVolumeDetails{
				Name: common.String("cfg"), Configs: []ci.ContainerConfigFile{{FileName: common.String("../x"), Data: []byte("a")}},
			}}
		}, "configfile-traversal", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Volumes = []ci.CreateContainerVolumeDetails{ci.CreateContainerConfigFileVolumeDetails{
				Name: common.String("cfg"), Configs: []ci.ContainerConfigFile{{FileName: common.String("a"), Path: common.String("../up"), Data: []byte("a")}},
			}}
		}, "configfile-path-traversal", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{
				ci.CreateContainerTcpHealthCheckDetails{Port: common.Int(0)},
			}
		}, "healthcheck-port", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].HealthChecks = []ci.CreateContainerHealthCheckDetails{
				ci.CreateContainerHttpHealthCheckDetails{Port: common.Int(80), Path: common.String("health")},
			}
		}, "healthcheck-path", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.Containers[0].ResourceConfig = &ci.CreateContainerResourceConfigDetails{VcpusLimit: f32(3)}
		}, "container-vcpus-over-shape", "InvalidParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.ImagePullSecrets = []ci.CreateImagePullSecretDetails{
				ci.CreateBasicImagePullSecretDetails{RegistryEndpoint: common.String("r.io"), Username: common.String("u"), Password: common.String("")},
			}
		}, "basic-secret-no-password", "MissingParameter", 400},
		{func(d *ci.CreateContainerInstanceDetails) {
			d.DnsConfig = &ci.CreateContainerDnsConfigDetails{Nameservers: []string{"not-an-ip"}}
		}, "dns-bad-nameserver", "InvalidParameter", 400},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
				b.SetCompartmentChecker(func(id string) bool { return id == testCompartment || id == otherCompart })
				b.SetSubnetChecker(func(id string) bool { return id == testSubnet })
			}})

			_, err := e.create(t, createOpts{mutate: tc.mutate})
			se := svcErr(t, err)
			assert.Equal(t, tc.wantHTTP, se.GetHTTPStatusCode())
			assert.Equal(t, tc.wantCode, se.GetCode())
			assert.NotEmpty(t, se.GetOpcRequestID())
		})
	}
}

func TestCreateAndStateMachine(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.FreeformTags = map[string]string{"env": "dev"}
		d.Containers[0].DisplayName = common.String("app")
		d.Containers[0].EnvironmentVariables = map[string]string{"A": "1"}
		d.Containers[0].Command = []string{"sh"}
		d.Containers[0].Arguments = []string{"-c", "true"}
		d.Containers[0].WorkingDirectory = common.String("/tmp")
	}})

	require.NotNil(t, resp.OpcWorkRequestId)
	require.NotNil(t, resp.Etag)
	assert.Equal(t, ci.ContainerInstanceLifecycleStateCreating, resp.LifecycleState)
	assert.Equal(t, ci.ContainerInstanceContainerRestartPolicyAlways, resp.ContainerRestartPolicy)

	id := resp.Id
	create := e.wr(t, resp.OpcWorkRequestId)
	assert.Equal(t, ci.OperationStatusInProgress, create.Status)
	assert.Equal(t, ci.OperationTypeCreateContainerInstance, create.OperationType)
	require.Len(t, create.Resources, 1)
	assert.Equal(t, id, create.Resources[0].Identifier)
	assert.Equal(t, ci.ActionTypeCreated, create.Resources[0].ActionType)

	cur := e.get(t, id)
	assert.Equal(t, ci.ContainerInstanceLifecycleStateCreating, cur.LifecycleState)

	e.settle()

	cur = e.get(t, id)
	assert.Equal(t, ci.ContainerInstanceLifecycleStateActive, cur.LifecycleState)
	assert.Equal(t, 1, *cur.ContainerCount)
	assert.InDelta(t, 2, *cur.ShapeConfig.Ocpus, 0)
	assert.InDelta(t, 8, *cur.ShapeConfig.MemoryInGBs, 0)
	assert.InDelta(t, 2, *cur.ShapeConfig.NetworkingBandwidthInGbps, 0)
	assert.NotEmpty(t, *cur.ShapeConfig.ProcessorDescription)
	require.Len(t, cur.Vnics, 1)
	assert.True(t, strings.HasPrefix(*cur.Vnics[0].VnicId, "ocid1.vnic."))
	assert.Equal(t, "dev", cur.FreeformTags["env"])
	assert.Equal(t, ci.OperationStatusSucceeded, e.wr(t, resp.OpcWorkRequestId).Status)

	ctrs, err := e.client.ListContainers(t.Context(), ci.ListContainersRequest{
		CompartmentId: common.String(testCompartment), ContainerInstanceId: id,
	})
	require.NoError(t, err)
	require.Len(t, ctrs.Items, 1)
	assert.Equal(t, ci.ContainerLifecycleStateActive, ctrs.Items[0].LifecycleState)

	got, err := e.client.GetContainer(t.Context(), ci.GetContainerRequest{ContainerId: ctrs.Items[0].Id})
	require.NoError(t, err)
	assert.Equal(t, "app", *got.DisplayName)
	assert.Equal(t, map[string]string{"A": "1"}, got.EnvironmentVariables)
	assert.Equal(t, []string{"sh"}, got.Command)
	assert.Equal(t, []string{"-c", "true"}, got.Arguments)
	assert.Equal(t, "/tmp", *got.WorkingDirectory)

	step := func(name string, do func() (*string, error), mid, end ci.ContainerInstanceLifecycleStateEnum) {
		t.Helper()

		wrID, derr := do()
		require.NoError(t, derr, name)
		assert.Equal(t, mid, e.get(t, id).LifecycleState, name)
		e.settle()
		assert.Equal(t, end, e.get(t, id).LifecycleState, name)
		assert.Equal(t, ci.OperationStatusSucceeded, e.wr(t, wrID).Status, name)
	}

	step("stop", func() (*string, error) {
		r, serr := e.client.StopContainerInstance(t.Context(), ci.StopContainerInstanceRequest{ContainerInstanceId: id})

		return r.OpcWorkRequestId, serr
	}, ci.ContainerInstanceLifecycleStateUpdating, ci.ContainerInstanceLifecycleStateInactive)

	got, err = e.client.GetContainer(t.Context(), ci.GetContainerRequest{ContainerId: ctrs.Items[0].Id})
	require.NoError(t, err)
	assert.Equal(t, ci.ContainerLifecycleStateInactive, got.LifecycleState)

	_, err = e.client.StopContainerInstance(t.Context(), ci.StopContainerInstanceRequest{ContainerInstanceId: id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	step("start", func() (*string, error) {
		r, serr := e.client.StartContainerInstance(t.Context(), ci.StartContainerInstanceRequest{ContainerInstanceId: id})

		return r.OpcWorkRequestId, serr
	}, ci.ContainerInstanceLifecycleStateUpdating, ci.ContainerInstanceLifecycleStateActive)

	_, err = e.client.StartContainerInstance(t.Context(), ci.StartContainerInstanceRequest{ContainerInstanceId: id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())

	step("restart", func() (*string, error) {
		r, serr := e.client.RestartContainerInstance(t.Context(), ci.RestartContainerInstanceRequest{ContainerInstanceId: id})

		return r.OpcWorkRequestId, serr
	}, ci.ContainerInstanceLifecycleStateUpdating, ci.ContainerInstanceLifecycleStateActive)

	step("delete", func() (*string, error) {
		r, serr := e.client.DeleteContainerInstance(t.Context(), ci.DeleteContainerInstanceRequest{ContainerInstanceId: id})

		return r.OpcWorkRequestId, serr
	}, ci.ContainerInstanceLifecycleStateDeleting, ci.ContainerInstanceLifecycleStateDeleted)

	_, err = e.client.UpdateContainerInstance(t.Context(), ci.UpdateContainerInstanceRequest{
		ContainerInstanceId: id, UpdateContainerInstanceDetails: ci.UpdateContainerInstanceDetails{DisplayName: common.String("x")},
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestUpdateMoveAndIfMatch(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{configure: func(b *svc.InMemoryBackend) {
		b.SetCompartmentChecker(func(id string) bool { return id == testCompartment || id == otherCompart })
	}})
	resp := e.mustCreate(t, createOpts{})
	e.settle()

	id := resp.Id
	cur, err := e.client.GetContainerInstance(t.Context(), ci.GetContainerInstanceRequest{ContainerInstanceId: id})
	require.NoError(t, err)

	_, err = e.client.UpdateContainerInstance(t.Context(), ci.UpdateContainerInstanceRequest{
		ContainerInstanceId: id, IfMatch: common.String("stale"),
		UpdateContainerInstanceDetails: ci.UpdateContainerInstanceDetails{DisplayName: common.String("n")},
	})
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.client.UpdateContainerInstance(t.Context(), ci.UpdateContainerInstanceRequest{
		ContainerInstanceId: id, IfMatch: cur.Etag,
		UpdateContainerInstanceDetails: ci.UpdateContainerInstanceDetails{
			DisplayName: common.String("renamed"), FreeformTags: map[string]string{"k": "v"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, ci.OperationTypeUpdateContainerInstance, e.wr(t, upd.OpcWorkRequestId).OperationType)

	after := e.get(t, id)
	assert.Equal(t, "renamed", *after.DisplayName)
	assert.Equal(t, map[string]string{"k": "v"}, after.FreeformTags)
	assert.NotNil(t, after.TimeUpdated)

	mv, err := e.client.ChangeContainerInstanceCompartment(t.Context(), ci.ChangeContainerInstanceCompartmentRequest{
		ContainerInstanceId:                       id,
		ChangeContainerInstanceCompartmentDetails: ci.ChangeContainerInstanceCompartmentDetails{CompartmentId: common.String(otherCompart)},
	})
	require.NoError(t, err)
	assert.Equal(t, ci.OperationTypeMoveContainerInstance, e.wr(t, mv.OpcWorkRequestId).OperationType)
	assert.Equal(t, otherCompart, *e.get(t, id).CompartmentId)

	_, err = e.client.ChangeContainerInstanceCompartment(t.Context(), ci.ChangeContainerInstanceCompartmentRequest{
		ContainerInstanceId:                       id,
		ChangeContainerInstanceCompartmentDetails: ci.ChangeContainerInstanceCompartmentDetails{CompartmentId: common.String("ocid1.compartment.oc1..gone")},
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.GetContainerInstance(t.Context(), ci.GetContainerInstanceRequest{ContainerInstanceId: common.String("ocid1.x.oc1..none")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestListFiltersAndPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	for _, n := range []string{"b", "a", "c"} {
		e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) { d.DisplayName = common.String(n) }})
	}

	e.settle()

	other := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.CompartmentId = common.String(otherCompart)
	}})
	e.settle()
	_, err := e.client.StopContainerInstance(t.Context(), ci.StopContainerInstanceRequest{ContainerInstanceId: other.Id})
	require.NoError(t, err)
	e.settle()

	list := func(r ci.ListContainerInstancesRequest) ci.ListContainerInstancesResponse {
		t.Helper()

		if r.CompartmentId == nil {
			r.CompartmentId = common.String(testCompartment)
		}

		out, lerr := e.client.ListContainerInstances(t.Context(), r)
		require.NoError(t, lerr)

		return out
	}

	assert.Len(t, list(ci.ListContainerInstancesRequest{}).Items, 3)
	assert.Len(t, list(ci.ListContainerInstancesRequest{DisplayName: common.String("a")}).Items, 1)
	assert.Len(t, list(ci.ListContainerInstancesRequest{LifecycleState: ci.ContainerInstanceLifecycleStateInactive}).Items, 0)
	assert.Len(t, list(ci.ListContainerInstancesRequest{
		CompartmentId: common.String(otherCompart), LifecycleState: ci.ContainerInstanceLifecycleStateInactive,
	}).Items, 1)

	var names []string

	var pageTok *string

	for {
		out := list(ci.ListContainerInstancesRequest{
			Limit: common.Int(2), Page: pageTok, SortBy: ci.ListContainerInstancesSortByDisplayname,
			SortOrder: ci.ListContainerInstancesSortOrderAsc,
		})
		for _, it := range out.Items {
			names = append(names, *it.DisplayName)
		}

		if out.OpcNextPage == nil {
			break
		}

		pageTok = out.OpcNextPage
	}

	assert.Equal(t, []string{"a", "b", "c"}, names)

	code, body := rawGet(t, e.base+"/20210415/containerInstances")
	assert.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, body, "MissingParameter")
}

func TestRetryTokenIsIdempotent(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	first := e.mustCreate(t, createOpts{token: "tok-1"})
	second := e.mustCreate(t, createOpts{token: "tok-1"})
	third := e.mustCreate(t, createOpts{token: "tok-2"})

	assert.Equal(t, *first.Id, *second.Id)
	assert.Equal(t, *first.OpcWorkRequestId, *second.OpcWorkRequestId)
	assert.NotEqual(t, *first.Id, *third.Id)
}

func TestContainersUpdateAndNames(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.Containers = []ci.CreateContainerDetails{
			{ImageUrl: common.String("alpine"), DisplayName: common.String("one")},
			{ImageUrl: common.String("alpine")},
		}
	}})
	e.settle()

	list, err := e.client.ListContainers(t.Context(), ci.ListContainersRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)

	var one, two ci.ContainerSummary

	for _, c := range list.Items {
		if *c.DisplayName == "one" {
			one = c
		} else {
			two = c
		}
	}

	assert.Equal(t, "web-container-2", *two.DisplayName)
	assert.Equal(t, *resp.Id, *two.ContainerInstanceId)

	_, err = e.client.UpdateContainer(t.Context(), ci.UpdateContainerRequest{
		ContainerId:            two.Id,
		UpdateContainerDetails: ci.UpdateContainerDetails{DisplayName: common.String("ONE")},
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	upd, err := e.client.UpdateContainer(t.Context(), ci.UpdateContainerRequest{
		ContainerId: one.Id,
		UpdateContainerDetails: ci.UpdateContainerDetails{
			DisplayName: common.String("uno"), FreeformTags: map[string]string{"t": "1"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, ci.OperationTypeUpdateContainer, e.wr(t, upd.OpcWorkRequestId).OperationType)

	got, err := e.client.GetContainer(t.Context(), ci.GetContainerRequest{ContainerId: one.Id})
	require.NoError(t, err)
	assert.Equal(t, "uno", *got.DisplayName)
	assert.Equal(t, "1", got.FreeformTags["t"])
}

func TestShapesCatalog(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	out, err := e.client.ListContainerInstanceShapes(t.Context(), ci.ListContainerInstanceShapesRequest{
		CompartmentId: common.String(testCompartment),
	})
	require.NoError(t, err)

	byName := map[string]ci.ContainerInstanceShapeSummary{}
	for _, s := range out.Items {
		byName[*s.Name] = s
	}

	for _, n := range []string{"CI.Standard.E4.Flex", "CI.Standard.E3.Flex", "CI.Standard.A1.Flex"} {
		s, ok := byName[n]
		require.True(t, ok, n)
		assert.InDelta(t, 1, *s.OcpuOptions.Min, 0)
		assert.Greater(t, *s.OcpuOptions.Max, float32(1))
		assert.Greater(t, *s.MemoryOptions.MaxInGBs, *s.MemoryOptions.MinInGBs)
		assert.NotEmpty(t, *s.ProcessorDescription)
	}

	assert.InDelta(t, 64, *byName["CI.Standard.E4.Flex"].OcpuOptions.Max, 0)
	assert.InDelta(t, 76, *byName["CI.Standard.A1.Flex"].OcpuOptions.Max, 0)
}

func TestMemoryDefaultsFromShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.ShapeConfig = &ci.CreateContainerInstanceShapeConfigDetails{Ocpus: f32(4)}
	}})

	assert.InDelta(t, 64, *resp.ShapeConfig.MemoryInGBs, 0)
}

func TestImagePullSecretsNeverReturned(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	resp := e.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.ImagePullSecrets = []ci.CreateImagePullSecretDetails{
			ci.CreateBasicImagePullSecretDetails{
				RegistryEndpoint: common.String("localhost:8166"), Username: common.String("ns/user"),
				Password: common.String("s3cr3t-token"),
			},
		}
	}})

	require.Len(t, resp.ImagePullSecrets, 1)

	basic, ok := resp.ImagePullSecrets[0].(ci.BasicImagePullSecret)
	require.True(t, ok)
	assert.Equal(t, "localhost:8166", *basic.RegistryEndpoint)

	code, body := rawGet(t, e.base+"/20210415/containerInstances/"+*resp.Id)
	assert.Equal(t, http.StatusOK, code)
	assert.NotContains(t, body, "s3cr3t-token")
	assert.NotContains(t, body, "ns/user")
}

func TestWorkRequestListErrorsAndLogs(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	a := e.mustCreate(t, createOpts{})
	e.settle()

	_, err := e.client.StopContainerInstance(t.Context(), ci.StopContainerInstanceRequest{ContainerInstanceId: a.Id})
	require.NoError(t, err)

	list, err := e.client.ListWorkRequests(t.Context(), ci.ListWorkRequestsRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 2)
	assert.Equal(t, ci.OperationTypeStopContainerInstance, list.Items[0].OperationType)

	inProgress, err := e.client.ListWorkRequests(t.Context(), ci.ListWorkRequestsRequest{
		CompartmentId: common.String(testCompartment), Status: ci.ListWorkRequestsStatusInProgress,
	})
	require.NoError(t, err)
	assert.Len(t, inProgress.Items, 1)

	byRes, err := e.client.ListWorkRequests(t.Context(), ci.ListWorkRequestsRequest{
		CompartmentId: common.String(testCompartment), ResourceId: a.Id, Limit: common.Int(1),
	})
	require.NoError(t, err)
	assert.Len(t, byRes.Items, 1)
	assert.NotNil(t, byRes.OpcNextPage)

	e.settle()

	logs, err := e.client.ListWorkRequestLogs(t.Context(), ci.ListWorkRequestLogsRequest{WorkRequestId: a.OpcWorkRequestId})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(logs.Items), 2)

	errs, err := e.client.ListWorkRequestErrors(t.Context(), ci.ListWorkRequestErrorsRequest{WorkRequestId: a.OpcWorkRequestId})
	require.NoError(t, err)
	assert.Empty(t, errs.Items)

	_, err = e.client.GetWorkRequest(t.Context(), ci.GetWorkRequestRequest{WorkRequestId: common.String("ocid1.x.oc1..none")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestRetrieveLogsNeedsDockerEngine(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mustCreate(t, createOpts{})
	e.settle()

	list, err := e.client.ListContainers(t.Context(), ci.ListContainersRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	_, err = e.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: list.Items[0].Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.client.RetrieveLogs(t.Context(), ci.RetrieveLogsRequest{ContainerId: common.String("ocid1.x.oc1..none")})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestSignatureEnforcement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})

	_, err := e.create(t, createOpts{})
	require.NoError(t, err)

	other := e.newClient(t, newKeyPair(t))
	_, err = other.ListContainerInstances(t.Context(), ci.ListContainerInstancesRequest{CompartmentId: common.String(testCompartment)})
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	code, _ := rawGet(t, e.base+"/20210415/containerInstances?compartmentId="+testCompartment)
	assert.Equal(t, http.StatusUnauthorized, code)
}

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	src := newEnv(t, envOpts{})
	created := src.mustCreate(t, createOpts{mutate: func(d *ci.CreateContainerInstanceDetails) {
		d.Volumes = []ci.CreateContainerVolumeDetails{ci.CreateContainerConfigFileVolumeDetails{
			Name: common.String("cfg"), Configs: []ci.ContainerConfigFile{{FileName: common.String("a.txt"), Data: []byte("hello")}},
		}}
		d.Containers[0].VolumeMounts = []ci.CreateVolumeMountDetails{{MountPath: common.String("/cfg"), VolumeName: common.String("cfg")}}
	}})
	src.settle()

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	got := dst.get(t, created.Id)
	assert.Equal(t, ci.ContainerInstanceLifecycleStateActive, got.LifecycleState)
	require.Len(t, got.Volumes, 1)
	assert.Equal(t, "web", *got.DisplayName)
	assert.Equal(t, ci.OperationStatusSucceeded, dst.wr(t, created.OpcWorkRequestId).Status)

	ctrs, err := dst.client.ListContainers(t.Context(), ci.ListContainersRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	assert.Len(t, ctrs.Items, 1)

	require.Error(t, dst.backend.Restore(t.Context(), []byte(`{"version":1,"instances":{"x":null}}`)))
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	e.mustCreate(t, createOpts{})
	e.backend.Reset()

	out, err := e.client.ListContainerInstances(t.Context(), ci.ListContainerInstancesRequest{CompartmentId: common.String(testCompartment)})
	require.NoError(t, err)
	assert.Empty(t, out.Items)
}

func TestRawRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode string
	}{
		{"bad-restart-policy", `{"compartmentId":"c","availabilityDomain":"ad","shape":"CI.Standard.E4.Flex","shapeConfig":{"ocpus":1},"containerRestartPolicy":"SOMETIMES","containers":[{"imageUrl":"a"}],"vnics":[{"subnetId":"s"}]}`, "InvalidParameter"},
		{"not-json", `{`, "CannotParseRequest"},
		{"empty-body", ``, "MissingParameter"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, e.base+"/20210415/containerInstances",
				strings.NewReader(tc.body))
			require.NoError(t, err)

			req.Header.Set("Content-Type", "application/json")

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			out, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
			assert.Contains(t, string(out), tc.wantCode)
			assert.NotEmpty(t, resp.Header.Get("opc-request-id"))
		})
	}
}
