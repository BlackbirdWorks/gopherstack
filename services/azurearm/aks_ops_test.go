package azurearm_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func aksClockBase() time.Time { return time.Date(2026, time.October, 30, 12, 0, 0, 0, time.UTC) }

// aksSteppingClock returns base, base+1s, base+2s, ...: every reading is later than the one before.
func aksSteppingClock() func() time.Time {
	var n atomic.Int64

	return func() time.Time { return aksClockBase().Add(time.Duration(n.Add(1)-1) * time.Second) }
}

func aksZipB64(t *testing.T, files map[string]string) string {
	t.Helper()

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for name, body := range files {
		w, err := zw.Create(name)
		require.NoError(t, err)

		_, err = w.Write([]byte(body))
		require.NoError(t, err)
	}

	require.NoError(t, zw.Close())

	return base64.StdEncoding.EncodeToString(buf.Bytes())
}

func (s *aksStack) runCommand(
	t *testing.T, cluster string, req armcontainerservice.RunCommandRequest,
) (armcontainerservice.RunCommandResult, error) {
	t.Helper()

	poller, err := s.factory.NewManagedClustersClient().BeginRunCommand(t.Context(), aksRG, cluster, req, nil)
	if err != nil {
		return armcontainerservice.RunCommandResult{}, err
	}

	res, err := poller.PollUntilDone(t.Context(), pollFast())

	return res.RunCommandResult, err
}

// failedResult reads the command result a poller reported as an error: the SDK treats provisioningState "failed"
// as a failed operation but the response body still carries the result.
func failedResult(t *testing.T, err error) armcontainerservice.RunCommandResult {
	t.Helper()

	re, ok := errors.AsType[*azcore.ResponseError](err)
	require.True(t, ok, "%v", err)
	require.Equal(t, http.StatusOK, re.StatusCode)

	raw, rerr := io.ReadAll(re.RawResponse.Body)
	require.NoError(t, rerr)

	var out armcontainerservice.RunCommandResult
	require.NoError(t, out.UnmarshalJSON(raw))

	return out
}

func (s *aksStack) stop(t *testing.T, name string) {
	t.Helper()

	poller, err := s.factory.NewManagedClustersClient().BeginStop(t.Context(), aksRG, name, nil)
	require.NoError(t, err)

	_, err = poller.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)
}

func (s *aksStack) clusterURL(name string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + aksRG +
		"/providers/Microsoft.ContainerService/managedClusters/" + name
}

func TestAKSRunCommandWithoutEngine(t *testing.T) {
	t.Parallel()

	zipB64 := aksZipB64(t, map[string]string{"ns.yaml": "kind: Namespace"})

	tests := []struct {
		req      armcontainerservice.RunCommandRequest
		mod      func(*testing.T, *aksStack)
		name     string
		cluster  string
		wantCode string
		wantErr  bool
	}{
		{
			name:    "unschedulable",
			cluster: "run",
			req:     armcontainerservice.RunCommandRequest{Command: new("kubectl get ns")},
		},
		{
			name: "with context", cluster: "run",
			req: armcontainerservice.RunCommandRequest{Command: new("kubectl apply -f ns.yaml"), Context: &zipB64},
		},
		{
			name: "empty command", cluster: "run", wantErr: true, wantCode: "InvalidParameter",
			req: armcontainerservice.RunCommandRequest{Command: new("  ")},
		},
		{
			name: "context not base64", cluster: "run", wantErr: true, wantCode: "InvalidParameter",
			req: armcontainerservice.RunCommandRequest{Command: new("ls"), Context: new("!!!")},
		},
		{
			name: "context not zip", cluster: "run", wantErr: true, wantCode: "InvalidParameter",
			req: armcontainerservice.RunCommandRequest{
				Command: new("ls"), Context: new(base64.StdEncoding.EncodeToString([]byte("plain text"))),
			},
		},
		{
			name: "missing cluster", cluster: "absent", wantErr: true, wantCode: "ResourceNotFound",
			req: armcontainerservice.RunCommandRequest{Command: new("ls")},
		},
		{
			name: "stopped cluster", cluster: "run", wantErr: true, wantCode: "OperationNotAllowed",
			req: armcontainerservice.RunCommandRequest{Command: new("ls")},
			mod: func(t *testing.T, s *aksStack) {
				t.Helper()
				s.stop(t, "run")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.provider.SetClock(aksSteppingClock())
			s.mustPut(t, "run", nil)

			if tt.mod != nil {
				tt.mod(t, s)
			}

			_, err := s.runCommand(t, tt.cluster, tt.req)
			if tt.wantErr {
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			got := failedResult(t, err)
			assert.Regexp(t, `^[0-9a-f]{32}$`, *got.ID)
			assert.Equal(t, "failed", *got.Properties.ProvisioningState)
			assert.Equal(t, aksWantNotStarted(*got.ID), *got.Properties.Reason)
			assert.Nil(t, got.Properties.ExitCode)
		})
	}
}

func TestAKSRunCommandEngine(t *testing.T) {
	t.Parallel()

	zipB64 := aksZipB64(t, map[string]string{"cm.yaml": "kind: ConfigMap"})

	tests := []struct {
		execErr    error
		name       string
		wantState  string
		wantLogs   string
		wantReason string
		req        armcontainerservice.RunCommandRequest
		result     container.ExecResult
		wantExit   int32
	}{
		{
			name: "success", req: armcontainerservice.RunCommandRequest{Command: new("kubectl get nodes")},
			result: container.ExecResult{Stdout: "NAME STATUS\nnode Ready\n"}, wantState: "succeeded",
			wantLogs: "NAME STATUS\nnode Ready\n",
		},
		{
			name:      "non zero exit keeps output",
			req:       armcontainerservice.RunCommandRequest{Command: new("kubectl get x")},
			result:    container.ExecResult{Stderr: "error: the server doesn't have a resource type x\n", ExitCode: 1},
			wantState: "succeeded",
			wantLogs:  "error: the server doesn't have a resource type x\n",
			wantExit:  1,
		},
		{
			name: "with context", wantState: "succeeded", wantLogs: "ok",
			req:    armcontainerservice.RunCommandRequest{Command: new("kubectl apply -f cm.yaml"), Context: &zipB64},
			result: container.ExecResult{Stdout: "ok"},
		},
		{
			name: "exec unsupported", execErr: container.ErrExecUnsupported, wantState: "failed",
			req:        armcontainerservice.RunCommandRequest{Command: new("ls")},
			wantReason: "x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s, rt := newAKSEngineStack(t)
			s.provider.SetClock(aksSteppingClock())
			rt.execFn = func(context.Context, aksExecCall) (container.ExecResult, error) { return tt.result, tt.execErr }
			s.mustPut(t, "eng", nil)

			got, err := s.runCommand(t, "eng", tt.req)
			if tt.wantReason != "" {
				got = failedResult(t, err)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantState, *got.Properties.ProvisioningState)

			if tt.wantReason != "" {
				assert.Equal(t, aksWantNotStarted(*got.ID), *got.Properties.Reason)

				return
			}

			assert.Equal(t, tt.wantExit, *got.Properties.ExitCode)
			assert.Equal(t, tt.wantLogs, *got.Properties.Logs)
			assert.True(t, got.Properties.FinishedAt.After(*got.Properties.StartedAt))
			assert.False(t, got.Properties.StartedAt.Before(aksClockBase()))

			calls := rt.execCalls()
			require.Len(t, calls, 1)
			assert.Equal(t, "/bin/sh", calls[0].cmd[0])

			for _, arg := range calls[0].cmd {
				assert.NotContains(t, arg, *tt.req.Command, "the command must not appear in argv")
			}

			assert.Contains(t, calls[0].env, "GOPHERSTACK_RUN_COMMAND="+*tt.req.Command)

			if tt.req.Context != nil {
				assert.Contains(t, calls[0].env, "GOPHERSTACK_RUN_CONTEXT="+*tt.req.Context)
			}
		})
	}
}

func TestAKSRunCommandPending(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	release := make(chan struct{})
	entered := make(chan struct{})

	var once sync.Once

	rt.execFn = func(context.Context, aksExecCall) (container.ExecResult, error) {
		once.Do(func() { close(entered) })
		<-release

		return container.ExecResult{Stdout: "done"}, nil
	}

	s.mustPut(t, "pend", nil)

	resp := s.raw(t, http.MethodPost, s.clusterURL("pend")+"/runCommand", `{"command":"sleep 1"}`, "2026-08-01")
	require.Equal(t, http.StatusAccepted, resp.StatusCode)

	loc := resp.Header.Get("Location")
	require.Regexp(t, `/managedClusters/pend/commandResults/[0-9a-f]{32}\?api-version=2026-08-01$`, loc)

	<-entered

	_, path, _ := strings.Cut(loc, "://")
	_, path, _ = strings.Cut(path, "/")
	path = "/" + strings.TrimSuffix(path, "?api-version=2026-08-01")
	running := s.raw(t, http.MethodGet, path, "", "2026-08-01")
	assert.Equal(t, http.StatusAccepted, running.StatusCode)
	assert.NotEmpty(t, running.Header.Get("Location"))

	close(release)

	require.Eventually(t, func() bool {
		r := s.raw(t, http.MethodGet, path, "", "2026-08-01")
		if r.StatusCode != http.StatusOK {
			return false
		}

		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)

		var body armcontainerservice.RunCommandResult
		require.NoError(t, body.UnmarshalJSON(raw))

		return *body.Properties.Logs == "done" && *body.Properties.ProvisioningState == "succeeded"
	}, 10*time.Second, time.Millisecond)

	missing := s.raw(t, http.MethodGet, s.clusterURL("pend")+"/commandResults/ffffffffffffffffffffffffffffffff", "",
		"2026-08-01")
	assert.Equal(t, http.StatusNotFound, missing.StatusCode)
}

func TestAKSAbort(t *testing.T) {
	t.Parallel()

	t.Run("nothing running", func(t *testing.T) {
		t.Parallel()

		s := newAKSStack(t)
		s.mustPut(t, "idle", nil)

		_, err := s.factory.NewManagedClustersClient().BeginAbortLatestOperation(t.Context(), aksRG, "idle", nil)
		assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

		_, err = s.factory.NewManagedClustersClient().BeginAbortLatestOperation(t.Context(), aksRG, "absent", nil)
		assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
	})

	t.Run("cancels a starting cluster", func(t *testing.T) {
		t.Parallel()

		s, rt := newAKSEngineStack(t)
		entered := rt.blockStart()
		done := make(chan error, 1)

		go func() {
			_, err := s.put(t, "ab", nil)
			done <- err
		}()

		<-entered

		poller, err := s.factory.NewManagedClustersClient().BeginAbortLatestOperation(t.Context(), aksRG, "ab", nil)
		require.NoError(t, err)

		_, err = poller.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)
		require.ErrorContains(t, <-done, "Canceled")

		got, err := s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "ab", nil)
		require.NoError(t, err)
		assert.Equal(t, "Canceled", *got.Properties.ProvisioningState)
		assert.Empty(t, rt.created())

		again, err := s.put(t, "ab", nil)
		require.NoError(t, err)
		assert.Equal(t, "Succeeded", *again.Properties.ProvisioningState)
		assert.Len(t, rt.created(), 1)
	})
}

func TestAKSRotateCertificates(t *testing.T) {
	t.Parallel()

	t.Run("emulated cluster gets a new CA and token", func(t *testing.T) {
		t.Parallel()

		s := newAKSStack(t)
		s.mustPut(t, "rot", nil)

		before, caBefore, err := s.userKubeconfig(t, "rot")
		require.NoError(t, err)

		poller, err := s.factory.NewManagedClustersClient().
			BeginRotateClusterCertificates(t.Context(), aksRG, "rot", nil)
		require.NoError(t, err)

		_, err = poller.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)

		after, caAfter, err := s.userKubeconfig(t, "rot")
		require.NoError(t, err)
		assert.NotEqual(t, caBefore, caAfter)
		assert.NotEqual(t, before.Users[0].User.Token, after.Users[0].User.Token)
	})

	t.Run("stopped and missing", func(t *testing.T) {
		t.Parallel()

		s := newAKSStack(t)
		s.mustPut(t, "rot", nil)
		s.stop(t, "rot")

		c := s.factory.NewManagedClustersClient()
		_, err := c.BeginRotateClusterCertificates(t.Context(), aksRG, "rot", nil)
		assert.Equal(t, "OperationNotAllowed", sdkErrCode(err))

		_, err = c.BeginRotateClusterCertificates(t.Context(), aksRG, "absent", nil)
		assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
	})

	t.Run("engine rotates the cluster CAs and restarts the node", func(t *testing.T) {
		t.Parallel()

		s, rt := newAKSEngineStack(t)
		s.mustPut(t, "rot", nil)

		poller, err := s.factory.NewManagedClustersClient().
			BeginRotateClusterCertificates(t.Context(), aksRG, "rot", nil)
		require.NoError(t, err)

		_, err = poller.PollUntilDone(t.Context(), pollFast())
		require.NoError(t, err)

		calls := rt.execCalls()
		require.Len(t, calls, 1)
		require.Len(t, calls[0].cmd, 3)
		assert.Contains(t, calls[0].cmd[2], "/bin/k3s certificate rotate-ca --path=")
		assert.Len(t, calls[0].env, 10)
		assert.Len(t, rt.stoppedIDs(), 1)
		assert.Len(t, rt.startedIDs(), 1)

		_, _, err = s.userKubeconfig(t, "rot")
		require.NoError(t, err)
	})

	t.Run("engine failure surfaces and keeps the node usable", func(t *testing.T) {
		t.Parallel()

		s, rt := newAKSEngineStack(t)
		rt.execFn = func(context.Context, aksExecCall) (container.ExecResult, error) {
			return container.ExecResult{ExitCode: 1, Stderr: "boom"}, nil
		}
		s.mustPut(t, "rot", nil)

		_, err := s.factory.NewManagedClustersClient().BeginRotateClusterCertificates(t.Context(), aksRG, "rot", nil)
		assert.Equal(t, "ClusterCertificateRotationFailed", sdkErrCode(err))
		assert.Empty(t, rt.stoppedIDs())

		_, _, err = s.userKubeconfig(t, "rot")
		require.NoError(t, err)
	})
}

func TestAKSResetProfiles(t *testing.T) {
	t.Parallel()

	spCluster := func(m *armcontainerservice.ManagedCluster) {
		m.Identity = nil
		m.Properties.ServicePrincipalProfile = &armcontainerservice.ManagedClusterServicePrincipalProfile{
			ClientID: new("old-client"), Secret: new("old-secret"),
		}
	}
	legacyAAD := armcontainerservice.ManagedClusterAADProfile{
		ClientAppID: new("client-app"), ServerAppID: new("server-app"), ServerAppSecret: new("s3cret"),
		TenantID: new("tenant"),
	}

	tests := []struct {
		call     func(s *aksStack) error
		check    func(t *testing.T, s *aksStack)
		mod      func(*armcontainerservice.ManagedCluster)
		name     string
		wantCode string
	}{
		{
			name: "aad legacy",
			call: func(s *aksStack) error {
				p, err := s.factory.NewManagedClustersClient().
					BeginResetAADProfile(t.Context(), aksRG, "c", legacyAAD, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			check: func(t *testing.T, s *aksStack) {
				t.Helper()

				got, err := s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "c", nil)
				require.NoError(t, err)
				assert.Equal(t, "client-app", *got.Properties.AADProfile.ClientAppID)
				assert.Nil(t, got.Properties.AADProfile.ServerAppSecret, "the secret is write-only")
			},
		},
		{
			name: "aad managed refused", wantCode: "OperationNotAllowed",
			mod: func(m *armcontainerservice.ManagedCluster) {
				m.Properties.AADProfile = &armcontainerservice.ManagedClusterAADProfile{Managed: new(true)}
			},
			call: func(s *aksStack) error {
				_, err := s.factory.NewManagedClustersClient().
					BeginResetAADProfile(t.Context(), aksRG, "c", legacyAAD, nil)

				return err
			},
		},
		{
			name: "service principal",
			mod:  spCluster,
			call: func(s *aksStack) error {
				p, err := s.factory.NewManagedClustersClient().
					BeginResetServicePrincipalProfile(t.Context(), aksRG, "c",
						armcontainerservice.ManagedClusterServicePrincipalProfile{
							ClientID: new("new-client"),
							Secret:   new("new"),
						}, nil)
				if err != nil {
					return err
				}

				_, err = p.PollUntilDone(t.Context(), pollFast())

				return err
			},
			check: func(t *testing.T, s *aksStack) {
				t.Helper()

				got, err := s.factory.NewManagedClustersClient().Get(t.Context(), aksRG, "c", nil)
				require.NoError(t, err)
				assert.Equal(t, "new-client", *got.Properties.ServicePrincipalProfile.ClientID)
				assert.Nil(t, got.Properties.ServicePrincipalProfile.Secret)
			},
		},
		{
			name: "service principal on managed identity cluster", wantCode: "OperationNotAllowed",
			call: func(s *aksStack) error {
				_, err := s.factory.NewManagedClustersClient().
					BeginResetServicePrincipalProfile(t.Context(), aksRG, "c",
						armcontainerservice.ManagedClusterServicePrincipalProfile{
							ClientID: new("x"),
							Secret:   new("y"),
						}, nil)

				return err
			},
		},
		{
			name: "service principal needs clientId", wantCode: "InvalidParameter", mod: spCluster,
			call: func(s *aksStack) error {
				_, err := s.factory.NewManagedClustersClient().
					BeginResetServicePrincipalProfile(t.Context(), aksRG, "c",
						armcontainerservice.ManagedClusterServicePrincipalProfile{Secret: new("y")}, nil)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.mustPut(t, "c", tt.mod)

			err := tt.call(s)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)
			tt.check(t, s)
		})
	}
}

func weekly(day armcontainerservice.WeekDay, every int32) *armcontainerservice.Schedule {
	return &armcontainerservice.Schedule{
		Weekly: &armcontainerservice.WeeklySchedule{DayOfWeek: &day, IntervalWeeks: &every},
	}
}

func window(sched *armcontainerservice.Schedule, hours int32, start string) *armcontainerservice.MaintenanceWindow {
	return &armcontainerservice.MaintenanceWindow{DurationHours: &hours, Schedule: sched, StartTime: &start}
}

func maintCfg(w *armcontainerservice.MaintenanceWindow) armcontainerservice.MaintenanceConfiguration {
	return armcontainerservice.MaintenanceConfiguration{
		Properties: &armcontainerservice.MaintenanceConfigurationProperties{MaintenanceWindow: w},
	}
}

func TestAKSMaintenanceConfigurations(t *testing.T) {
	t.Parallel()

	monday := armcontainerservice.WeekDayMonday
	first := armcontainerservice.TypeFirst
	date := func(s string) *time.Time {
		v, err := time.Parse(time.DateOnly, s)
		require.NoError(t, err)

		return &v
	}

	tests := []struct {
		cfg      armcontainerservice.MaintenanceConfiguration
		name     string
		cfgName  string
		wantCode string
	}{
		{name: "default weekly", cfgName: "default", cfg: maintCfg(window(weekly(monday, 1), 4, "01:00"))},
		{
			name:    "auto upgrade relative monthly",
			cfgName: "aksManagedAutoUpgradeSchedule",
			cfg: maintCfg(
				window(&armcontainerservice.Schedule{RelativeMonthly: &armcontainerservice.RelativeMonthlySchedule{
					DayOfWeek: &monday, IntervalMonths: new(int32(3)), WeekIndex: &first,
				}}, 10, "08:30"),
			),
		},
		{
			name: "node os daily with dates", cfgName: "aksManagedNodeOSUpgradeSchedule",
			cfg: func() armcontainerservice.MaintenanceConfiguration {
				w := window(
					&armcontainerservice.Schedule{
						Daily: &armcontainerservice.DailySchedule{IntervalDays: new(int32(3))},
					},
					4,
					"09:30",
				)
				w.UTCOffset, w.StartDate = new("-07:00"), date("2023-01-01")
				w.NotAllowedDates = []*armcontainerservice.DateSpan{
					{Start: date("2023-02-18"), End: date("2023-02-25")},
				}

				return maintCfg(w)
			}(),
		},
		{
			name:    "absolute monthly",
			cfgName: "aksManagedAutoUpgradeSchedule",
			cfg: maintCfg(
				window(&armcontainerservice.Schedule{AbsoluteMonthly: &armcontainerservice.AbsoluteMonthlySchedule{
					DayOfMonth: new(int32(15)), IntervalMonths: new(int32(3)),
				}}, 5, "08:30"),
			),
		},
		{
			name:    "default time in week",
			cfgName: "default",
			cfg: armcontainerservice.MaintenanceConfiguration{
				Properties: &armcontainerservice.MaintenanceConfigurationProperties{
					TimeInWeek: []*armcontainerservice.TimeInWeek{
						{Day: &monday, HourSlots: []*int32{new(int32(1)), new(int32(2))}},
					},
				},
			},
		},
		{
			name:     "unknown name",
			cfgName:  "weekend",
			wantCode: "InvalidParameter",
			cfg:      maintCfg(window(weekly(monday, 1), 4, "01:00")),
		},
		{
			name:     "duration too short",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg:      maintCfg(window(weekly(monday, 1), 3, "01:00")),
		},
		{
			name:     "duration too long",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg:      maintCfg(window(weekly(monday, 1), 25, "01:00")),
		},
		{
			name:     "bad start time",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg:      maintCfg(window(weekly(monday, 1), 4, "25:00")),
		},
		{
			name:     "bad interval",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg:      maintCfg(window(weekly(monday, 5), 4, "01:00")),
		},
		{
			name:     "default rejects daily",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg: maintCfg(
				window(
					&armcontainerservice.Schedule{
						Daily: &armcontainerservice.DailySchedule{IntervalDays: new(int32(1))},
					},
					4,
					"01:00",
				),
			),
		},
		{
			name: "two schedule types", cfgName: "aksManagedAutoUpgradeSchedule", wantCode: "InvalidParameter",
			cfg: maintCfg(window(&armcontainerservice.Schedule{
				Daily:  &armcontainerservice.DailySchedule{IntervalDays: new(int32(1))},
				Weekly: &armcontainerservice.WeeklySchedule{IntervalWeeks: new(int32(1)), DayOfWeek: &monday},
			}, 4, "01:00")),
		},
		{
			name: "no schedule", cfgName: "aksManagedAutoUpgradeSchedule", wantCode: "InvalidParameter",
			cfg: maintCfg(window(&armcontainerservice.Schedule{}, 4, "01:00")),
		},
		{
			name: "bad utc offset", cfgName: "default", wantCode: "InvalidParameter",
			cfg: func() armcontainerservice.MaintenanceConfiguration {
				w := window(weekly(monday, 1), 4, "01:00")
				w.UTCOffset = new("0530")

				return maintCfg(w)
			}(),
		},
		{
			name: "inverted not allowed dates", cfgName: "default", wantCode: "InvalidParameter",
			cfg: func() armcontainerservice.MaintenanceConfiguration {
				w := window(weekly(monday, 1), 4, "01:00")
				w.NotAllowedDates = []*armcontainerservice.DateSpan{
					{Start: date("2023-03-01"), End: date("2023-02-01")},
				}

				return maintCfg(w)
			}(),
		},
		{
			name:     "managed schedule rejects time in week",
			cfgName:  "aksManagedAutoUpgradeSchedule",
			wantCode: "InvalidParameter",
			cfg: armcontainerservice.MaintenanceConfiguration{
				Properties: &armcontainerservice.MaintenanceConfigurationProperties{
					TimeInWeek: []*armcontainerservice.TimeInWeek{{Day: &monday, HourSlots: []*int32{new(int32(1))}}},
				},
			},
		},
		{
			name:     "empty",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg:      armcontainerservice.MaintenanceConfiguration{},
		},
		{
			name:     "bad hour slot",
			cfgName:  "default",
			wantCode: "InvalidParameter",
			cfg: armcontainerservice.MaintenanceConfiguration{
				Properties: &armcontainerservice.MaintenanceConfigurationProperties{
					TimeInWeek: []*armcontainerservice.TimeInWeek{{Day: &monday, HourSlots: []*int32{new(int32(24))}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.provider.SetClock(aksSteppingClock())
			s.mustPut(t, "m", nil)

			c := s.factory.NewMaintenanceConfigurationsClient()
			res, err := c.CreateOrUpdate(t.Context(), aksRG, "m", tt.cfgName, tt.cfg, nil)

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				_, err = c.Get(t.Context(), aksRG, "m", tt.cfgName, nil)
				assert.Error(t, err, "a refused configuration must not be stored")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.cfgName, *res.Name)
			assert.Equal(t, "Microsoft.ContainerService/managedClusters/maintenanceConfigurations", *res.Type)
			assert.True(t, strings.HasSuffix(*res.ID, "/managedClusters/m/maintenanceConfigurations/"+tt.cfgName))

			got, err := c.Get(t.Context(), aksRG, "m", tt.cfgName, nil)
			require.NoError(t, err)
			assert.Equal(t, res.Properties, got.Properties)

			if w := got.Properties.MaintenanceWindow; w != nil {
				assert.NotNil(t, w.StartDate, "startDate defaults to the creation date")
				assert.NotNil(t, w.UTCOffset, "utcOffset defaults to +00:00")
			}
		})
	}
}

func TestAKSMaintenanceLifecycle(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "m", nil)

	body := `{"properties":{"maintenanceWindow":{"durationHours":4,"startTime":"02:00","schedule":` +
		`{"weekly":{"intervalWeeks":1,"dayOfWeek":"monday"}}}}}`
	url := s.clusterURL("m") + "/maintenanceConfigurations/aksmanagedautoupgradeschedule"

	first := s.raw(t, http.MethodPut, url, body, "2026-08-01")
	assert.Equal(t, http.StatusCreated, first.StatusCode)
	assert.Empty(t, first.Header.Get("Azure-Asyncoperation"), "maintenance configurations are synchronous")

	raw, err := io.ReadAll(first.Body)
	require.NoError(t, err)
	assert.Contains(t, string(raw), `"name":"aksManagedAutoUpgradeSchedule"`)
	assert.Contains(t, string(raw), `"dayOfWeek":"Monday"`)
	assert.Contains(t, string(raw), `"utcOffset":"+00:00"`)

	assert.Equal(t, http.StatusOK, s.raw(t, http.MethodPut, url, body, "2026-08-01").StatusCode)

	require.NoError(
		t,
		s.putMaintenance(t, "default", maintCfg(window(weekly(armcontainerservice.WeekDayFriday, 2), 6, "03:00"))),
	)

	pager := s.factory.NewMaintenanceConfigurationsClient().NewListByManagedClusterPager(aksRG, "m", nil)

	var names []string

	for pager.More() {
		page, perr := pager.NextPage(t.Context())
		require.NoError(t, perr)

		for _, v := range page.Value {
			names = append(names, *v.Name)
		}
	}

	assert.Equal(t, []string{"aksManagedAutoUpgradeSchedule", "default"}, names)

	assert.Equal(t, http.StatusOK, s.raw(t, http.MethodDelete, url, "", "2026-08-01").StatusCode)
	assert.Equal(t, http.StatusNoContent, s.raw(t, http.MethodDelete, url, "", "2026-08-01").StatusCode)
	assert.Equal(t, http.StatusNotFound, s.raw(t, http.MethodGet, url, "", "2026-08-01").StatusCode)

	missing := s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourceGroups/"+aksRG+
		"/providers/Microsoft.ContainerService/managedClusters/absent/maintenanceConfigurations/default", body, "2026-08-01")
	assert.Equal(t, http.StatusNotFound, missing.StatusCode)
}

func (s *aksStack) putMaintenance(t *testing.T, name string, cfg armcontainerservice.MaintenanceConfiguration) error {
	t.Helper()

	_, err := s.factory.NewMaintenanceConfigurationsClient().CreateOrUpdate(t.Context(), aksRG, "m", name, cfg, nil)

	return err
}

const aksMLWorkspace = "/subscriptions/" + aciSub +
	"/resourceGroups/b/providers/Microsoft.MachineLearningServices/workspaces/c"

func TestAKSTrustedAccessRoleBindings(t *testing.T) {
	t.Parallel()

	bind := func(src string, roles ...string) armcontainerservice.TrustedAccessRoleBinding {
		rs := make([]*string, 0, len(roles))
		for _, r := range roles {
			rs = append(rs, new(r))
		}

		return armcontainerservice.TrustedAccessRoleBinding{
			Properties: &armcontainerservice.TrustedAccessRoleBindingProperties{SourceResourceID: &src, Roles: rs},
		}
	}

	tests := []struct {
		binding  armcontainerservice.TrustedAccessRoleBinding
		name     string
		bindName string
		wantCode string
	}{
		{
			name: "spec example", bindName: "binding1",
			binding: bind(aksMLWorkspace, "Microsoft.MachineLearningServices/workspaces/reader",
				"Microsoft.MachineLearningServices/workspaces/writer"),
		},
		{
			name: "role of another type", bindName: "wrong", wantCode: "InvalidParameter",
			binding: bind(aksMLWorkspace, "Microsoft.DataProtection/backupVaults/backup-operator"),
		},
		{
			name: "no roles", bindName: "none", wantCode: "InvalidParameter",
			binding: bind(aksMLWorkspace),
		},
		{
			name: "bad source", bindName: "badsrc", wantCode: "InvalidParameter",
			binding: bind("not-an-id", "Microsoft.MachineLearningServices/workspaces/reader"),
		},
		{
			name: "bad name", bindName: "under_score", wantCode: "InvalidParameter",
			binding: bind(aksMLWorkspace, "Microsoft.MachineLearningServices/workspaces/reader"),
		},
		{
			name: "name too long", bindName: strings.Repeat("a", 25), wantCode: "InvalidParameter",
			binding: bind(aksMLWorkspace, "Microsoft.MachineLearningServices/workspaces/reader"),
		},
		{
			name: "no properties", bindName: "bare", wantCode: "InvalidParameter",
			binding: armcontainerservice.TrustedAccessRoleBinding{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newAKSStack(t)
			s.mustPut(t, "tb", nil)

			c := s.factory.NewTrustedAccessRoleBindingsClient()
			poller, err := c.BeginCreateOrUpdate(t.Context(), aksRG, "tb", tt.bindName, tt.binding, nil)

			if err == nil {
				_, err = poller.PollUntilDone(t.Context(), pollFast())
			}

			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, sdkErrCode(err))

				return
			}

			require.NoError(t, err)

			got, err := c.Get(t.Context(), aksRG, "tb", tt.bindName, nil)
			require.NoError(t, err)
			assert.Equal(t, "Succeeded", string(*got.Properties.ProvisioningState))
			assert.Equal(t, aksMLWorkspace, *got.Properties.SourceResourceID)
			assert.Len(t, got.Properties.Roles, 2)
			assert.Equal(t, "Microsoft.ContainerService/managedClusters/trustedAccessRoleBindings", *got.Type)

			del, err := c.BeginDelete(t.Context(), aksRG, "tb", tt.bindName, nil)
			require.NoError(t, err)

			_, err = del.PollUntilDone(t.Context(), pollFast())
			require.NoError(t, err)

			_, err = c.Get(t.Context(), aksRG, "tb", tt.bindName, nil)
			assert.Equal(t, "ResourceNotFound", sdkErrCode(err))
		})
	}
}

func TestAKSTrustedAccessBindingLimitAndList(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "tb", nil)

	c := s.factory.NewTrustedAccessRoleBindingsClient()
	role := "Microsoft.MachineLearningServices/workspaces/reader"

	put := func(name string) error {
		p, err := c.BeginCreateOrUpdate(t.Context(), aksRG, "tb", name, armcontainerservice.TrustedAccessRoleBinding{
			Properties: &armcontainerservice.TrustedAccessRoleBindingProperties{
				SourceResourceID: new(aksMLWorkspace), Roles: []*string{&role},
			},
		}, nil)
		if err != nil {
			return err
		}

		_, err = p.PollUntilDone(t.Context(), pollFast())

		return err
	}

	for _, n := range []string{"b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8"} {
		require.NoError(t, put(n))
	}

	assert.Equal(t, "OperationNotAllowed", sdkErrCode(put("b9")), "at most 8 bindings per cluster")
	require.NoError(t, put("b1"), "updating an existing binding is not a new one")

	pager := c.NewListPager(aksRG, "tb", nil)
	count := 0

	for pager.More() {
		page, err := pager.NextPage(t.Context())
		require.NoError(t, err)

		count += len(page.Value)
	}

	assert.Equal(t, 8, count)

	resp := s.raw(t, http.MethodDelete, s.clusterURL("tb")+"/trustedAccessRoleBindings/absent", "", "2026-08-01")
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
}

func TestAKSTrustedAccessRoles(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)

	pager := s.factory.NewTrustedAccessRolesClient().NewListPager("westus2", nil)
	page, err := pager.NextPage(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, page.Value)

	role := page.Value[0]
	assert.Equal(t, "reader", *role.Name)
	assert.Equal(t, "Microsoft.MachineLearningServices/workspaces", *role.SourceResourceType)
	require.Len(t, role.Rules, 1)
	assert.Equal(t, "pods", *role.Rules[0].Resources[0])
	assert.Equal(t, "get", *role.Rules[0].Verbs[0])
}

func TestAKSOpsSnapshotRestore(t *testing.T) {
	t.Parallel()

	s := newAKSStack(t)
	s.mustPut(t, "keep", nil)
	require.NoError(t, s.putMaintenanceTo(t, "keep", "default",
		maintCfg(window(weekly(armcontainerservice.WeekDaySaturday, 1), 4, "02:00"))))

	bp, err := s.factory.NewTrustedAccessRoleBindingsClient().BeginCreateOrUpdate(t.Context(), aksRG, "keep", "b1",
		armcontainerservice.TrustedAccessRoleBinding{
			Properties: &armcontainerservice.TrustedAccessRoleBindingProperties{
				SourceResourceID: new(aksMLWorkspace),
				Roles:            []*string{new("Microsoft.MachineLearningServices/workspaces/reader")},
			},
		}, nil)
	require.NoError(t, err)

	_, err = bp.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, err = s.runCommand(t, "keep", armcontainerservice.RunCommandRequest{Command: new("ls")})
	cmd := failedResult(t, err)

	data := s.handler.Snapshot(t.Context())

	armSvc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h2, ok := armSvc.(*azurearm.Handler)
	require.True(t, ok)

	s2 := newAKSStackFor(t, h2)
	require.NoError(t, h2.Restore(t.Context(), data))

	m, err := s2.factory.NewMaintenanceConfigurationsClient().Get(t.Context(), aksRG, "keep", "default", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 4, *m.Properties.MaintenanceWindow.DurationHours)

	b, err := s2.factory.NewTrustedAccessRoleBindingsClient().Get(t.Context(), aksRG, "keep", "b1", nil)
	require.NoError(t, err)
	assert.Equal(t, aksMLWorkspace, *b.Properties.SourceResourceID)

	res, err := s2.factory.NewManagedClustersClient().GetCommandResult(t.Context(), aksRG, "keep", *cmd.ID, nil)
	require.NoError(t, err)
	assert.Equal(t, "failed", *res.Properties.ProvisioningState)

	_, err = s2.put(
		t,
		"keep",
		func(mc *armcontainerservice.ManagedCluster) { mc.Tags = map[string]*string{"k": new("v")} },
	)
	require.NoError(t, err)

	_, err = s2.factory.NewMaintenanceConfigurationsClient().Get(t.Context(), aksRG, "keep", "default", nil)
	require.NoError(t, err, "an update of the cluster keeps its maintenance configurations")
}

func (s *aksStack) putMaintenanceTo(
	t *testing.T, cluster, name string, cfg armcontainerservice.MaintenanceConfiguration,
) error {
	t.Helper()

	_, err := s.factory.NewMaintenanceConfigurationsClient().CreateOrUpdate(t.Context(), aksRG, cluster, name, cfg, nil)

	return err
}

func TestAKSEngineVolumeAndIdentity(t *testing.T) {
	t.Parallel()

	s, rt := newAKSEngineStack(t)
	s.mustPut(t, "Vol", nil)

	specs := rt.created()
	require.Len(t, specs, 1)

	made, firstGone := rt.volumeNames()
	require.Len(t, made, 1)
	assert.Equal(t, made, firstGone, "creating a cluster discards a stale data volume of its name")
	assert.Regexp(t, `^gopherstack-aks-[0-9a-f]{16}$`, made[0])
	assert.Contains(t, specs[0].Mounts, made[0]+":"+k3sengine.DataDir)
	assert.Equal(t, "rg-aks/vol", specs[0].Labels[container.LabelResource])
	assert.Equal(t, "k3s-aks", specs[0].Labels[container.LabelService])
	assert.Regexp(t, `^GOPHERSTACK_NODE_NAME=aks-[0-9a-f]{8}-vmss000000$`, aksEnvOf(specs[0], "GOPHERSTACK_NODE_NAME"))

	token := aksEnvOf(specs[0], "K3S_TOKEN")
	assert.Regexp(t, `^K3S_TOKEN=[0-9a-f]{32}$`, token)

	data := s.handler.Snapshot(t.Context())

	s2 := newAKSStack(t)
	rt2 := enableAKSEngine(t, s2)
	require.NoError(t, s2.handler.Restore(t.Context(), data))

	require.Eventually(t, func() bool { return len(rt2.created()) == 1 }, 10*time.Second, time.Millisecond)

	again := rt2.created()[0]
	assert.Equal(t, token, aksEnvOf(again, "K3S_TOKEN"), "a relaunch keeps the join token so the volume's state loads")
	assert.Contains(t, again.Mounts, made[0]+":"+k3sengine.DataDir, "a relaunch reuses the data volume")

	del, err := s2.factory.NewManagedClustersClient().BeginDelete(t.Context(), aksRG, "Vol", nil)
	require.NoError(t, err)

	_, err = del.PollUntilDone(t.Context(), pollFast())
	require.NoError(t, err)

	_, gone := rt2.volumeNames()
	assert.Equal(t, []string{made[0]}, gone, "deleting the cluster deletes its data volume")
}

func aksEnvOf(spec container.Spec, key string) string {
	for _, e := range spec.Env {
		if strings.HasPrefix(e, key+"=") {
			return e
		}
	}

	return ""
}

func aksWantNotStarted(id string) string {
	return "(KubernetesPerformanceError) Failed to run command due to cluster perf issue, container command-" + id +
		" in aks-command namespace did not start within 30s on your cluster, retry may helps. If issue persist, " +
		"you may need to tune your cluster with better performance (larger node/paid tier)."
}
