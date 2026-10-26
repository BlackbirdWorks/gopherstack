package gcprun_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcprun"
)

var errProbeDown = errors.New("probe down")

// fakeProber answers probes from a test-controlled function and records every attempt.
type fakeProber struct {
	fn    func(gcprun.ProbeTarget) error
	calls chan gcprun.ProbeTarget
}

func newFakeProber(fn func(gcprun.ProbeTarget) error) *fakeProber {
	return &fakeProber{fn: fn, calls: make(chan gcprun.ProbeTarget, 1024)}
}

func (p *fakeProber) Probe(_ context.Context, t gcprun.ProbeTarget) error {
	err := p.fn(t)
	p.calls <- t

	return err
}

func (p *fakeProber) next(t *testing.T) gcprun.ProbeTarget {
	t.Helper()

	select {
	case c := <-p.calls:
		return c
	case <-t.Context().Done():
		require.FailNow(t, "no probe attempt")

		return gcprun.ProbeTarget{}
	}
}

func tcpProbe(port int32, period, threshold int32) *runpb.Probe {
	return &runpb.Probe{
		PeriodSeconds: period, FailureThreshold: threshold, TimeoutSeconds: 1,
		ProbeType: &runpb.Probe_TcpSocket{TcpSocket: &runpb.TCPSocketAction{Port: port}},
	}
}

func httpProbe(port int32, period, threshold int32) *runpb.Probe {
	return &runpb.Probe{
		PeriodSeconds: period, FailureThreshold: threshold, TimeoutSeconds: 1,
		ProbeType: &runpb.Probe_HttpGet{HttpGet: &runpb.HTTPGetAction{Port: port, Path: "/healthz"}},
	}
}

func appAndSidecar() (app, sidecar *runpb.Container) {
	app = &runpb.Container{
		Name: "app", Image: "example/app:1",
		Ports:     []*runpb.ContainerPort{{ContainerPort: 8080}},
		DependsOn: []string{"sidecar"},
		Env:       []*runpb.EnvVar{{Name: "MODE", Values: &runpb.EnvVar_Value{Value: "app"}}},
	}
	sidecar = &runpb.Container{Name: "sidecar", Image: "example/sidecar:1"}

	return app, sidecar
}

func multiService(cs ...*runpb.Container) *runpb.Service {
	return &runpb.Service{Template: &runpb.RevisionTemplate{Containers: cs}}
}

func (e *env) deploy(t *testing.T, id string, svc *runpb.Service) (*runpb.Service, error) {
	t.Helper()

	op, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
		Parent: parent, ServiceId: id, Service: svc,
	})
	require.NoError(t, err)

	return op.Wait(t.Context())
}

func TestMultiContainer_SidecarSharesNetworkAndOrdering(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	app, sidecar := appAndSidecar()

	svc, err := e.deploy(t, "web", multiService(app, sidecar))
	require.NoError(t, err)

	specs := e.rt.specs()
	require.Len(t, specs, 2)

	owner, joined := specs[0], specs[1]
	assert.Equal(t, "example/sidecar:1", owner.Image, "dependencies start first")
	assert.Equal(t, "example/app:1", joined.Image)
	assert.Empty(t, owner.Network)
	assert.Equal(t, "container:fake-1", joined.Network, "later containers join the first one's network namespace")
	require.Len(t, owner.Ports, 1)
	assert.True(t, strings.HasSuffix(owner.Ports[0], ":8080"), "the ingress port is published by the namespace owner")
	assert.Empty(t, joined.Ports)

	assert.Equal(t, "8080", envOf(joined, "PORT"))
	assert.Empty(t, envOf(owner, "PORT"), "only the ingress container gets PORT")
	assert.Equal(t, "web", envOf(owner, "K_SERVICE"))
	assert.Equal(t, "web", envOf(joined, "K_SERVICE"))
	assert.Equal(t, "app", envOf(joined, "MODE"))

	code, _ := e.get(t, strings.TrimPrefix(svc.GetUri(), "http://"), "/")
	assert.Equal(t, 200, code)

	op, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: svc.GetName()})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)
	assert.Zero(t, e.rt.running(), "every container of the instance is removed")
}

func TestMultiContainer_StartupProbeGatesDependents(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clock := newFakeClock()
	e.handler.Backend.SetClock(clock)

	var healthy atomic.Bool

	prober := newFakeProber(func(gcprun.ProbeTarget) error {
		if healthy.Load() {
			return nil
		}

		return errProbeDown
	})
	e.handler.Backend.SetProber(prober)

	app, sidecar := appAndSidecar()
	sidecar.StartupProbe = tcpProbe(9000, 10, 5)

	done := make(chan error, 1)

	go func() {
		_, err := e.deploy(t, "web", multiService(app, sidecar))
		done <- err
	}()

	first := prober.next(t)
	assert.Equal(t, gcprun.ProbeTCP, first.Kind)
	assert.Equal(t, "sidecar", first.Container)
	assert.Equal(t, 9000, first.ContainerPort)
	assert.NotZero(t, first.Port, "the probed port is published on the host")
	assert.Len(t, e.rt.specs(), 1, "the dependent waits for the failing startup probe")

	clock.waitTimers(t, 1)
	healthy.Store(true)
	clock.advance(10 * time.Second)

	require.NoError(t, <-done)
	assert.Len(t, e.rt.specs(), 2, "the dependent starts once the probe passes")
}

func TestMultiContainer_StartupProbeFailureFailsDeploy(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clock := newFakeClock()
	e.handler.Backend.SetClock(clock)

	prober := newFakeProber(func(gcprun.ProbeTarget) error { return errProbeDown })
	e.handler.Backend.SetProber(prober)

	app, sidecar := appAndSidecar()
	sidecar.StartupProbe = tcpProbe(9000, 10, 2)

	done := make(chan error, 1)

	go func() {
		_, err := e.deploy(t, "web", multiService(app, sidecar))
		done <- err
	}()

	prober.next(t)
	clock.waitTimers(t, 1)
	clock.advance(10 * time.Second)

	err := <-done
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
	assert.Contains(t, err.Error(), "startup probe")
	assert.Zero(t, e.rt.running(), "the failed instance is cleaned up")
}

func TestMultiContainer_LivenessFailureReplacesInstance(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clock := newFakeClock()
	e.handler.Backend.SetClock(clock)

	var healthy atomic.Bool

	prober := newFakeProber(func(gcprun.ProbeTarget) error {
		if healthy.Load() {
			return nil
		}

		return errProbeDown
	})
	e.handler.Backend.SetProber(prober)

	app, sidecar := appAndSidecar()
	app.LivenessProbe = httpProbe(8080, 10, 3)

	spec := multiService(app, sidecar)
	spec.Template.Scaling = &runpb.RevisionScaling{MinInstanceCount: 1}

	_, err := e.deploy(t, "web", spec)
	require.NoError(t, err)

	first := prober.next(t)
	assert.Equal(t, gcprun.ProbeHTTP, first.Kind)
	assert.Equal(t, "/healthz", first.Path)

	for range 2 {
		clock.waitTimers(t, 2)
		clock.advance(10 * time.Second)
		prober.next(t)
	}

	healthy.Store(true)

	require.Eventually(t, func() bool { return len(e.rt.specs()) == 4 }, 10*time.Second, time.Millisecond,
		"three consecutive failures replace the instance")
	assert.ElementsMatch(t, []string{"fake-1", "fake-2"}, e.rt.removedIDs())
}

func TestMultiContainer_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		build func() *runpb.Service
	}{
		{"unknown dependency", func() *runpb.Service {
			a, s := appAndSidecar()
			a.DependsOn = []string{"ghost"}

			return multiService(a, s)
		}},
		{"dependency cycle", func() *runpb.Service {
			a, s := appAndSidecar()
			s.DependsOn = []string{"app"}

			return multiService(a, s)
		}},
		{"duplicate names", func() *runpb.Service {
			a, s := appAndSidecar()
			s.Name = "app"

			return multiService(a, s)
		}},
		{"probe without a port", func() *runpb.Service {
			a, s := appAndSidecar()
			s.StartupProbe = tcpProbe(0, 10, 3)

			return multiService(a, s)
		}},
		{"tcp liveness probe", func() *runpb.Service {
			a, s := appAndSidecar()
			a.LivenessProbe = tcpProbe(8080, 10, 3)

			return multiService(a, s)
		}},
		{"probe timeout above period", func() *runpb.Service {
			a, s := appAndSidecar()
			a.StartupProbe = &runpb.Probe{
				TimeoutSeconds: 5, PeriodSeconds: 2,
				ProbeType: &runpb.Probe_TcpSocket{TcpSocket: &runpb.TCPSocketAction{}},
			}

			return multiService(a, s)
		}},
		{"mount of unknown volume", func() *runpb.Service {
			a, s := appAndSidecar()
			a.VolumeMounts = []*runpb.VolumeMount{{Name: "nope", MountPath: "/x"}}

			return multiService(a, s)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			_, err := e.services.CreateService(t.Context(), &runpb.CreateServiceRequest{
				Parent: parent, ServiceId: "web", Service: tt.build(),
			})
			assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)
		})
	}
}

func hostMount(t *testing.T, mounts []string, containerPath string) string {
	t.Helper()

	for _, m := range mounts {
		parts := strings.Split(m, ":")
		if len(parts) >= 2 && parts[1] == containerPath {
			return parts[0]
		}
	}

	require.FailNow(t, "no mount at "+containerPath)

	return ""
}

func TestVolumes_EmptyDirSecretAndEnv(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)

	var asked sync.Map

	e.handler.Backend.SetSecretResolver(func(name string) ([]byte, error) {
		asked.Store(name, true)

		return []byte("s3cr3t"), nil
	})

	app, sidecar := appAndSidecar()
	app.VolumeMounts = []*runpb.VolumeMount{
		{Name: "scratch", MountPath: "/data"}, {Name: "creds", MountPath: "/secrets"},
	}
	sidecar.VolumeMounts = []*runpb.VolumeMount{{Name: "scratch", MountPath: "/shared", SubPath: "logs"}}
	app.Env = append(app.Env, &runpb.EnvVar{
		Name: "TOKEN",
		Values: &runpb.EnvVar_ValueSource{ValueSource: &runpb.EnvVarSource{
			SecretKeyRef: &runpb.SecretKeySelector{Secret: "api-token", Version: "3"},
		}},
	})

	svc := multiService(app, sidecar)
	svc.Template.Volumes = []*runpb.Volume{
		{Name: "scratch", VolumeType: &runpb.Volume_EmptyDir{EmptyDir: &runpb.EmptyDirVolumeSource{
			Medium: runpb.EmptyDirVolumeSource_MEMORY,
		}}},
		{Name: "creds", VolumeType: &runpb.Volume_Secret{Secret: &runpb.SecretVolumeSource{
			Secret: "projects/other/secrets/db", DefaultMode: 0o400,
			Items: []*runpb.VersionToPath{{Path: "nested/password", Version: "latest"}},
		}}},
	}

	got, err := e.deploy(t, "web", svc)
	require.NoError(t, err)

	specs := e.rt.specs()
	require.Len(t, specs, 2)

	owner, joined := specs[0], specs[1]
	scratch := hostMount(t, joined.Mounts, "/data")
	assert.Equal(t, scratch+"/logs", hostMount(t, owner.Mounts, "/shared"), "the emptyDir is shared; sub_path narrows it")

	secretDir := hostMount(t, joined.Mounts, "/secrets")
	content, err := os.ReadFile(secretDir + "/nested/password")
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", string(content))
	assert.Contains(t, joined.Mounts[1], ":ro", "secret volumes are read-only")

	assert.Equal(t, "s3cr3t", envOf(joined, "TOKEN"))

	for _, want := range []string{
		"projects/other/secrets/db/versions/latest", "projects/test-project/secrets/api-token/versions/3",
	} {
		_, ok := asked.Load(want)
		assert.True(t, ok, want)
	}

	op, err := e.services.DeleteService(t.Context(), &runpb.DeleteServiceRequest{Name: got.GetName()})
	require.NoError(t, err)
	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	for _, dir := range []string{scratch, secretDir} {
		_, statErr := os.Stat(dir)
		assert.True(t, os.IsNotExist(statErr), "%s is removed with the instance", dir)
	}
}

func TestVolumes_UnsupportedAndUnavailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		volume  *runpb.Volume
		name    string
		resolve func(string) ([]byte, error)
		want    string
	}{
		{
			name:   "cloud storage fuse",
			volume: &runpb.Volume{Name: "v", VolumeType: &runpb.Volume_Gcs{Gcs: &runpb.GCSVolumeSource{Bucket: "b"}}},
			want:   "Cloud Storage FUSE",
		},
		{
			name:   "nfs",
			volume: &runpb.Volume{Name: "v", VolumeType: &runpb.Volume_Nfs{Nfs: &runpb.NFSVolumeSource{Server: "s"}}},
			want:   "NFS",
		},
		{
			name: "secret manager not wired",
			volume: &runpb.Volume{Name: "v", VolumeType: &runpb.Volume_Secret{Secret: &runpb.SecretVolumeSource{
				Secret: "s", Items: []*runpb.VersionToPath{{Path: "p", Version: "1"}},
			}}},
			want: "secret manager is not available",
		},
		{
			name: "missing secret",
			volume: &runpb.Volume{Name: "v", VolumeType: &runpb.Volume_Secret{Secret: &runpb.SecretVolumeSource{
				Secret: "s", Items: []*runpb.VersionToPath{{Path: "p", Version: "1"}},
			}}},
			resolve: func(string) ([]byte, error) { return nil, errProbeDown },
			want:    "probe down",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			if tt.resolve != nil {
				e.handler.Backend.SetSecretResolver(tt.resolve)
			}

			svc := multiService(&runpb.Container{
				Name: "app", Image: "i",
				VolumeMounts: []*runpb.VolumeMount{{Name: "v", MountPath: "/v"}},
			})
			svc.Template.Volumes = []*runpb.Volume{tt.volume}

			_, err := e.deploy(t, "web", svc)
			require.Error(t, err)
			assert.Equal(t, codes.FailedPrecondition, status.Code(err))
			assert.Contains(t, err.Error(), tt.want)
			assert.Zero(t, e.rt.running())
		})
	}
}
