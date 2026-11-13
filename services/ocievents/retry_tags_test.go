package ocievents_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/events"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ocitags"
)

func (e *env) createRuleToken(t *testing.T, name string) (string, error) {
	t.Helper()

	r, err := e.client.CreateRule(t.Context(), events.CreateRuleRequest{
		OpcRetryToken: new("tok"),
		DisplayName:   new(name), IsEnabled: new(true), Condition: new(matchAll),
		CompartmentId: new(testCompartment),
		Actions:       &events.ActionDetailsList{Actions: []events.ActionDetails{ossAction()}},
	})
	if err != nil {
		return "", err
	}

	return *r.Id, nil
}

func TestCreateRuleRetryToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(t *testing.T, e *env, first string)
		name string
	}{
		{
			name: "replay",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				again, err := e.createRuleToken(t, "rt")
				require.NoError(t, err)
				assert.Equal(t, first, again)
			},
		},
		{
			name: "restored",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				fresh := newEnv(t, envOpts{})
				require.NoError(t, fresh.backend.Restore(t.Context(), e.backend.Snapshot(t.Context())))

				again, err := fresh.createRuleToken(t, "rt")
				require.NoError(t, err)
				assert.Equal(t, first, again)
			},
		},
		{
			name: "expired",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				e.clock.Advance(25 * time.Hour)

				again, err := e.createRuleToken(t, "rt2")
				require.NoError(t, err)
				assert.NotEqual(t, first, again)
			},
		},
		{
			name: "invalidated",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				_, err := e.client.DeleteRule(t.Context(), events.DeleteRuleRequest{RuleId: &first})
				require.NoError(t, err)

				_, err = e.createRuleToken(t, "rt")
				assert.Equal(t, "InvalidatedRetryToken", svcErr(t, err).GetCode())
				assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
			},
		},
		{
			name: "change compartment",
			act: func(t *testing.T, e *env, first string) {
				t.Helper()

				move := func(to string) error {
					_, err := e.client.ChangeRuleCompartment(t.Context(), events.ChangeRuleCompartmentRequest{
						RuleId: &first, OpcRetryToken: new("mv"),
						CompartmentId: new(to),
					})

					return err
				}

				require.NoError(t, move(otherCompart))
				require.NoError(t, move(testCompartment))

				got, err := e.client.GetRule(t.Context(), events.GetRuleRequest{RuleId: &first})
				require.NoError(t, err)
				assert.Equal(t, otherCompart, *got.CompartmentId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			first, err := e.createRuleToken(t, "rt")
			require.NoError(t, err)

			tt.act(t, e, first)
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

			base := e.createRule(t, "base", matchAll, ossAction())

			check := func(err error) {
				t.Helper()

				if tt.code == "" {
					require.NoError(t, err)

					return
				}

				assert.Equal(t, tt.code, svcErr(t, err).GetCode())
				assert.Equal(t, tt.status, svcErr(t, err).GetHTTPStatusCode())
			}

			_, err := e.client.CreateRule(t.Context(), events.CreateRuleRequest{
				DisplayName: new("tagged"), IsEnabled: new(true), Condition: new(matchAll),
				CompartmentId: new(testCompartment),
				Actions:       &events.ActionDetailsList{Actions: []events.ActionDetails{ossAction()}},
				FreeformTags:  tt.free, DefinedTags: tt.defd,
			})
			check(err)

			_, err = e.client.UpdateRule(t.Context(), events.UpdateRuleRequest{
				RuleId:       base.Id,
				FreeformTags: tt.free, DefinedTags: tt.defd,
			})
			check(err)
		})
	}
}
