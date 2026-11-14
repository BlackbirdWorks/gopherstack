package ocilogging_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

type retryCase struct {
	create func(t *testing.T, e *env, g logging.LogGroup, l logging.Log) (string, error)
	remove func(t *testing.T, e *env, g logging.LogGroup, l logging.Log)
	name   string
}

func retryCases() []retryCase {
	return []retryCase{
		{
			name: "log group",
			create: func(t *testing.T, e *env, _ logging.LogGroup, _ logging.Log) (string, error) {
				t.Helper()

				r, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{
					OpcRetryToken: new("tok"),
					CompartmentId: new(testCompartment), DisplayName: new("rg"),
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, _ logging.LogGroup, _ logging.Log) {
				t.Helper()

				list, err := e.mgmt.ListLogGroups(t.Context(), logging.ListLogGroupsRequest{
					CompartmentId: new(testCompartment), DisplayName: new("rg"),
				})
				require.NoError(t, err)
				require.Len(t, list.Items, 1)

				_, err = e.mgmt.DeleteLogGroup(t.Context(), logging.DeleteLogGroupRequest{LogGroupId: list.Items[0].Id})
				require.NoError(t, err)
			},
		},
		{
			name: "log",
			create: func(t *testing.T, e *env, g logging.LogGroup, _ logging.Log) (string, error) {
				t.Helper()

				r, err := e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{
					OpcRetryToken: new("tok"), LogGroupId: g.Id,
					DisplayName: new("rl"), LogType: logging.CreateLogDetailsLogTypeCustom,
				})
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, g logging.LogGroup, _ logging.Log) {
				t.Helper()

				list, err := e.mgmt.ListLogs(
					t.Context(),
					logging.ListLogsRequest{LogGroupId: g.Id, DisplayName: new("rl")},
				)
				require.NoError(t, err)
				require.Len(t, list.Items, 1)

				_, err = e.mgmt.DeleteLog(
					t.Context(),
					logging.DeleteLogRequest{LogGroupId: g.Id, LogId: list.Items[0].Id},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "agent configuration",
			create: func(t *testing.T, e *env, _ logging.LogGroup, l logging.Log) (string, error) {
				t.Helper()

				r, err := e.mgmt.CreateUnifiedAgentConfiguration(
					t.Context(),
					logging.CreateUnifiedAgentConfigurationRequest{
						OpcRetryToken: new("tok"),
						CompartmentId: new(testCompartment), DisplayName: new("ra"), IsEnabled: new(true),
						ServiceConfiguration: loggingServiceConfig(l.Id, tailSource("app")),
					},
				)
				if err != nil {
					return "", err
				}

				return *r.OpcWorkRequestId, nil
			},
			remove: func(t *testing.T, e *env, _ logging.LogGroup, _ logging.Log) {
				t.Helper()

				list, err := e.mgmt.ListUnifiedAgentConfigurations(
					t.Context(),
					logging.ListUnifiedAgentConfigurationsRequest{
						CompartmentId: new(testCompartment), DisplayName: new("ra"),
					},
				)
				require.NoError(t, err)
				require.Len(t, list.Items, 1)

				_, err = e.mgmt.DeleteUnifiedAgentConfiguration(
					t.Context(),
					logging.DeleteUnifiedAgentConfigurationRequest{
						UnifiedAgentConfigurationId: list.Items[0].Id,
					},
				)
				require.NoError(t, err)
			},
		},
	}
}

func TestCreateRetryToken(t *testing.T) {
	t.Parallel()

	setup := func(t *testing.T) (*env, logging.LogGroup, logging.Log) {
		t.Helper()

		e := newEnv(t, envOpts{})
		g := e.mkGroup(t, "base")

		return e, g, e.mkLog(t, g, "app", nil)
	}

	for _, tt := range retryCases() {
		t.Run(tt.name+" replay", func(t *testing.T) {
			t.Parallel()

			e, g, l := setup(t)
			wr1, err := tt.create(t, e, g, l)
			require.NoError(t, err)

			wr2, err := tt.create(t, e, g, l)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" restored", func(t *testing.T) {
			t.Parallel()

			e, g, l := setup(t)
			wr1, err := tt.create(t, e, g, l)
			require.NoError(t, err)

			fresh := newEnv(t, envOpts{})
			require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

			wr2, err := tt.create(t, fresh, g, l)
			require.NoError(t, err)
			assert.Equal(t, wr1, wr2)
		})

		t.Run(tt.name+" expired", func(t *testing.T) {
			t.Parallel()

			e, g, l := setup(t)
			wr1, err := tt.create(t, e, g, l)
			require.NoError(t, err)

			e.settle()
			e.clock.Advance(25 * time.Hour)
			tt.remove(t, e, g, l)
			e.settle()

			wr2, err := tt.create(t, e, g, l)
			require.NoError(t, err)
			assert.NotEqual(t, wr1, wr2)
		})

		t.Run(tt.name+" invalidated", func(t *testing.T) {
			t.Parallel()

			e, g, l := setup(t)
			_, err := tt.create(t, e, g, l)
			require.NoError(t, err)

			e.settle()
			tt.remove(t, e, g, l)
			e.settle()

			_, err = tt.create(t, e, g, l)
			require.Error(t, err)
			assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
			assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestTagValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		free   map[string]string
		defd   map[string]map[string]any
		name   string
		code   string
		status int
	}{
		{
			name: "valid", free: map[string]string{"a": "b"},
			defd: map[string]map[string]any{"Ops": {"Cost": "1"}}, status: http.StatusOK,
		},
		{
			name: "long value", free: map[string]string{"a": strings.Repeat("x", 257)},
			status: http.StatusBadRequest, code: "InvalidParameter",
		},
		{
			name: "unknown namespace", defd: map[string]map[string]any{"Nope": {"k": "v"}},
			status: http.StatusNotFound, code: "NotAuthorizedOrNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.backend.SetTagLookup(func(ns, key string) (ocitags.Def, bool) {
				return ocitags.Def{}, strings.EqualFold(ns, "ops") && strings.EqualFold(key, "cost")
			})

			g := e.mkGroup(t, "base")
			l := e.mkLog(t, g, "app", nil)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.mgmt.CreateLogGroup(t.Context(), logging.CreateLogGroupRequest{
				CompartmentId: new(testCompartment), DisplayName: new("tagged"),
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.CreateLog(t.Context(), logging.CreateLogRequest{
				LogGroupId:  g.Id,
				DisplayName: new("taggedlog"), LogType: logging.CreateLogDetailsLogTypeCustom,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.CreateUnifiedAgentConfiguration(t.Context(), logging.CreateUnifiedAgentConfigurationRequest{
				CompartmentId: new(testCompartment), DisplayName: new("tagged"), IsEnabled: new(true),
				ServiceConfiguration: loggingServiceConfig(l.Id, tailSource("app")),
				FreeformTags:         tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.UpdateLogGroup(t.Context(), logging.UpdateLogGroupRequest{
				LogGroupId:   g.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.mgmt.UpdateLog(t.Context(), logging.UpdateLogRequest{
				LogGroupId: g.Id, LogId: l.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
