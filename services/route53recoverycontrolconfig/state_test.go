package route53recoverycontrolconfig_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

type world struct {
	b        *cc.InMemoryBackend
	cluster  string
	panel    string
	a, bb, c string
}

func newWorld(t *testing.T) *world {
	t.Helper()

	b := cc.NewInMemoryBackend(testAccount)
	cl, err := b.CreateCluster("w", "", "", nil)
	require.NoError(t, err)

	w := &world{b: b, cluster: cl.ClusterArn}
	mk := func(name string) string {
		rc, cErr := b.CreateRoutingControl(w.cluster, "", name, "")
		require.NoError(t, cErr)
		w.panel = rc.ControlPanelArn

		return rc.RoutingControlArn
	}

	w.a, w.bb, w.c = mk("a"), mk("b"), mk("c")

	return w
}

func (w *world) rule(t *testing.T, spec *cc.NewRuleSpec) string {
	t.Helper()

	spec.PanelARN = w.panel
	r, err := w.b.CreateSafetyRule(spec, "", nil)
	require.NoError(t, err)

	if r.Gating != nil {
		return r.Gating.SafetyRuleArn
	}

	return r.Assertion.SafetyRuleArn
}

func (w *world) state(t *testing.T, arn string) string {
	t.Helper()

	v, err := w.b.RoutingControlState(arn, "")
	require.NoError(t, err)

	return v.State
}

func (w *world) set(arns []string, state string, overrides ...string) error {
	ups := make([]cc.StateUpdate, 0, len(arns))
	for _, a := range arns {
		ups = append(ups, cc.StateUpdate{ARN: a, State: state})
	}

	return w.b.UpdateRoutingControlStates(ups, overrides, "")
}

func conflictRule(err error) (string, bool) {
	if ce, ok := errors.AsType[*cc.ConflictError](err); ok {
		return ce.RuleARN, true
	}

	return "", false
}

func TestSafetyRuleEvaluation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, w *world)
		name string
	}{
		{name: "atleast one blocks turning everything off", run: func(t *testing.T, w *world) {
			t.Helper()

			rule := w.rule(t, &cc.NewRuleSpec{
				Name: "keep-one", Kind: "ASSERTION", RuleType: "ATLEAST", Threshold: 1, Controls: []string{w.a, w.bb},
			})
			require.NoError(t, w.set([]string{w.a, w.bb}, "On"))

			err := w.set([]string{w.a, w.bb}, "Off")
			got, isConflict := conflictRule(err)
			require.True(t, isConflict, "got %v", err)
			assert.Equal(t, rule, got)
			assert.Contains(t, err.Error(), "keep-one")
			assert.Equal(t, "On", w.state(t, w.a), "a rejected batch changes nothing")
			assert.Equal(t, "On", w.state(t, w.bb))

			require.NoError(t, w.set([]string{w.a}, "Off"), "one control stays On")
			require.Error(t, w.set([]string{w.bb}, "Off"))
		}},
		{name: "override lets the violating update through", run: func(t *testing.T, w *world) {
			t.Helper()

			rule := w.rule(t, &cc.NewRuleSpec{
				Name: "keep-one", Kind: "ASSERTION", RuleType: "ATLEAST", Threshold: 1, Controls: []string{w.a, w.bb},
			})
			require.NoError(t, w.set([]string{w.a}, "On"))
			require.Error(t, w.set([]string{w.a}, "Off"))
			require.NoError(t, w.set([]string{w.a}, "Off", rule))
			assert.Equal(t, "Off", w.state(t, w.a))
		}},
		{name: "and rule needs every control on", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{Name: "all", Kind: "ASSERTION", RuleType: "AND", Controls: []string{w.a, w.bb}})
			require.NoError(t, w.set([]string{w.a, w.bb}, "On"))
			require.Error(t, w.set([]string{w.a}, "Off"))
			require.Error(t, w.set([]string{w.a, w.bb}, "Off"))
			assert.Equal(t, "On", w.state(t, w.bb))
		}},
		{name: "or rule needs one control on", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{Name: "any", Kind: "ASSERTION", RuleType: "OR", Controls: []string{w.a, w.bb}})
			require.NoError(t, w.set([]string{w.a}, "On"))
			require.Error(t, w.set([]string{w.a}, "Off"))
		}},
		{name: "inverted assertion forbids controls being on", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{
				Name:      "none-on",
				Kind:      "ASSERTION",
				RuleType:  "ATLEAST",
				Threshold: 1,
				Inverted:  true,
				Controls:  []string{w.a, w.bb},
			})
			require.Error(t, w.set([]string{w.a}, "On"))
			assert.Equal(t, "Off", w.state(t, w.a))
		}},
		{name: "gating rule guards its targets", run: func(t *testing.T, w *world) {
			t.Helper()

			rule := w.rule(t, &cc.NewRuleSpec{
				Name: "gate", Kind: "GATING", RuleType: "OR", Controls: []string{w.a}, Targets: []string{w.bb},
			})
			err := w.set([]string{w.bb}, "On")
			got, isConflict := conflictRule(err)
			require.True(t, isConflict, "got %v", err)
			assert.Equal(t, rule, got)
			assert.Contains(t, err.Error(), "gate")

			require.NoError(t, w.set([]string{w.a}, "On"), "the gating control itself is not guarded")
			require.NoError(t, w.set([]string{w.bb}, "On"))
		}},
		{name: "gating and target in one batch evaluate the resulting state", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(
				t,
				&cc.NewRuleSpec{
					Name:     "gate",
					Kind:     "GATING",
					RuleType: "OR",
					Controls: []string{w.a},
					Targets:  []string{w.bb},
				},
			)
			require.NoError(t, w.set([]string{w.a, w.bb}, "On"))
		}},
		{name: "gating override", run: func(t *testing.T, w *world) {
			t.Helper()

			rule := w.rule(
				t,
				&cc.NewRuleSpec{
					Name:     "gate",
					Kind:     "GATING",
					RuleType: "OR",
					Controls: []string{w.a},
					Targets:  []string{w.bb},
				},
			)
			require.NoError(t, w.set([]string{w.bb}, "On", rule))
		}},
		{name: "unrelated control is never blocked", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{Name: "all", Kind: "ASSERTION", RuleType: "AND", Controls: []string{w.a, w.bb}})
			require.NoError(t, w.set([]string{w.c}, "On"))
		}},
		{name: "no-op update on a violated rule passes", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{Name: "all", Kind: "ASSERTION", RuleType: "AND", Controls: []string{w.a, w.bb}})
			require.NoError(t, w.set([]string{w.a}, "Off"))
		}},
		{name: "batch is atomic", run: func(t *testing.T, w *world) {
			t.Helper()

			w.rule(t, &cc.NewRuleSpec{Name: "all", Kind: "ASSERTION", RuleType: "AND", Controls: []string{w.a, w.bb}})
			require.Error(t, w.set([]string{w.c, w.a}, "On"))
			assert.Equal(t, "Off", w.state(t, w.c), "c must not change when a's update is rejected")
		}},
		{name: "input validation", run: func(t *testing.T, w *world) {
			t.Helper()

			ups := func(u ...cc.StateUpdate) error { return w.b.UpdateRoutingControlStates(u, nil, "") }

			var re *restroute.Error

			require.ErrorAs(t, ups(), &re)
			require.ErrorAs(t, ups(cc.StateUpdate{ARN: w.a, State: "Maybe"}), &re)
			assert.Equal(t, "ValidationException", re.Code)
			require.ErrorAs(t, ups(cc.StateUpdate{ARN: w.a, State: "On"}, cc.StateUpdate{ARN: w.a, State: "Off"}), &re)
			assert.Equal(t, "ValidationException", re.Code)
			require.ErrorAs(t, ups(cc.StateUpdate{ARN: w.a + "x", State: "On"}), &re)
			assert.Equal(t, "ResourceNotFoundException", re.Code)
			require.ErrorAs(
				t,
				w.b.UpdateRoutingControlStates([]cc.StateUpdate{{ARN: w.a, State: "On"}}, []string{"arn:nope"}, ""),
				&re,
			)
			assert.Equal(t, "ValidationException", re.Code)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, newWorld(t))
		})
	}
}

func TestSafetyRuleWaitPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wait     time.Duration
		override bool
		wantErr  bool
	}{
		{name: "blocked inside the wait period", wait: 500 * time.Millisecond, wantErr: true},
		{name: "allowed after the wait period", wait: 2 * time.Second},
		{name: "override skips the wait period", wait: 500 * time.Millisecond, override: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				w := newWorld(t)
				rule := w.rule(t, &cc.NewRuleSpec{
					Name: "slow", Kind: "ASSERTION", RuleType: "OR", Controls: []string{w.a, w.bb}, WaitPeriodMs: 1000,
				})
				require.NoError(t, w.set([]string{w.a}, "On"))

				time.Sleep(tt.wait)

				var overrides []string
				if tt.override {
					overrides = append(overrides, rule)
				}

				err := w.set([]string{w.bb}, "On", overrides...)
				if tt.wantErr {
					got, isConflict := conflictRule(err)
					require.True(t, isConflict, "got %v", err)
					assert.Equal(t, rule, got)
					assert.Contains(t, err.Error(), "wait period")

					return
				}

				require.NoError(t, err)
			})
		})
	}
}

func TestClusterScopedState(t *testing.T) {
	t.Parallel()

	w := newWorld(t)
	other, err := w.b.CreateCluster("other", "", "", nil)
	require.NoError(t, err)

	otherID := other.ClusterArn[len(other.ClusterArn)-32:]

	_, err = w.b.RoutingControlState(w.a, otherID)
	var re *restroute.Error
	require.ErrorAs(t, err, &re)
	assert.Equal(t, "ResourceNotFoundException", re.Code)

	views, _, err := w.b.ListRoutingControlStates("", otherID, "", 0)
	require.NoError(t, err)
	assert.Empty(t, views)
}
