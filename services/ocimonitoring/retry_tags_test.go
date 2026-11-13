package ocimonitoring_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/monitoring"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func (e *env) createWithToken(t *testing.T) (monitoring.CreateAlarmResponse, error) {
	t.Helper()

	return e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
		CreateAlarmDetails: e.alarmDetails(topicOCID, nil), OpcRetryToken: new("tok"),
	})
}

func TestCreateAlarmRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act      func(t *testing.T, e *env, first monitoring.CreateAlarmResponse)
		name     string
		wantCode string
	}{
		{
			name: "replay",
			act: func(t *testing.T, e *env, first monitoring.CreateAlarmResponse) {
				t.Helper()

				again, err := e.createWithToken(t)
				require.NoError(t, err)
				assert.Equal(t, *first.Id, *again.Id)
				assert.Equal(t, *first.Etag, *again.Etag)

				list, err := e.mon.ListAlarms(
					t.Context(),
					monitoring.ListAlarmsRequest{CompartmentId: new(testCompartment)},
				)
				require.NoError(t, err)
				assert.Len(t, list.Items, 1)
			},
		},
		{
			name: "expired",
			act: func(t *testing.T, e *env, first monitoring.CreateAlarmResponse) {
				t.Helper()

				e.clock.Advance(25 * time.Hour)

				again, err := e.createWithToken(t)
				require.NoError(t, err)
				assert.NotEqual(t, *first.Id, *again.Id)
			},
		},
		{
			name: "invalidated",
			act: func(t *testing.T, e *env, first monitoring.CreateAlarmResponse) {
				t.Helper()

				_, err := e.mon.DeleteAlarm(t.Context(), monitoring.DeleteAlarmRequest{AlarmId: first.Id})
				require.NoError(t, err)

				_, err = e.createWithToken(t)
				assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
				assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
			},
		},
		{
			name: "restored",
			act: func(t *testing.T, e *env, first monitoring.CreateAlarmResponse) {
				t.Helper()

				snap := e.backend.Snapshot(t.Context())
				fresh := newEnv(t, envOpts{})
				require.NoError(t, fresh.backend.Restore(t.Context(), snap))

				again, err := fresh.createWithToken(t)
				require.NoError(t, err)
				assert.Equal(t, *first.Id, *again.Id)
			},
		},
		{
			name: "change compartment",
			act: func(t *testing.T, e *env, first monitoring.CreateAlarmResponse) {
				t.Helper()

				move := func(to string) error {
					_, err := e.mon.ChangeAlarmCompartment(t.Context(), monitoring.ChangeAlarmCompartmentRequest{
						AlarmId: first.Id, OpcRetryToken: new("mv"),
						CompartmentId: new(to),
					})

					return err
				}

				require.NoError(t, move("ocid1.compartment.oc1..a"))
				require.NoError(t, move("ocid1.compartment.oc1..b"))

				got, err := e.mon.GetAlarm(t.Context(), monitoring.GetAlarmRequest{AlarmId: first.Id})
				require.NoError(t, err)
				assert.Equal(t, "ocid1.compartment.oc1..a", *got.CompartmentId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			first, err := e.createWithToken(t)
			require.NoError(t, err)

			tt.act(t, e, first)
		})
	}
}

func TestAlarmTagValidation(t *testing.T) {
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

			base := e.mkAlarm(t, topicOCID, nil)

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.mon.CreateAlarm(t.Context(), monitoring.CreateAlarmRequest{
				CreateAlarmDetails: e.alarmDetails(topicOCID, func(d *monitoring.CreateAlarmDetails) {
					d.FreeformTags, d.DefinedTags = tt.free, tt.defd
				}),
			})
			check(err)

			_, err = e.mon.UpdateAlarm(t.Context(), monitoring.UpdateAlarmRequest{
				AlarmId:      base.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
