package appconfig_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

var errTickFailed = errors.New("tick failed")

const (
	testSNSARN   = "arn:aws:sns:us-east-1:000000000000:topic"
	testSQSARN   = "arn:aws:sqs:us-east-1:000000000000:queue"
	testBusARN   = "arn:aws:events:us-east-1:000000000000:event-bus/default"
	statePending = "DEPLOYING"
)

type busEvent struct {
	busARN, source, detailType, detail string
	resources                          []string
}

type snsMsg struct {
	attrs   map[string]string
	topic   string
	message string
}

type fakeDeliverer struct {
	sns  []snsMsg
	sqs  []string
	bus  []busEvent
	typs []string
	mu   sync.Mutex
}

func (f *fakeDeliverer) PublishSNS(topic, message string, attrs map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sns = append(f.sns, snsMsg{topic: topic, message: message, attrs: attrs})
	f.typs = append(f.typs, "sns:"+attrs["MessageType"])

	return nil
}

func (f *fakeDeliverer) SendSQS(_, body string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sqs = append(f.sqs, body)

	return nil
}

func (f *fakeDeliverer) PutBusEvent(
	bus, source, detailType, detail string,
	resources []string,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.bus = append(
		f.bus,
		busEvent{
			busARN:     bus,
			source:     source,
			detailType: detailType,
			detail:     detail,
			resources:  resources,
		},
	)

	return nil
}

func (f *fakeDeliverer) snapshot() ([]snsMsg, []string, []busEvent) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append(
			[]snsMsg(nil),
			f.sns...), append(
			[]string(nil),
			f.sqs...), append(
			[]busEvent(nil),
			f.bus...)
}

type lifecycleInvoker struct {
	hook   func(ev map[string]any) ([]byte, error)
	events []map[string]any
	mu     sync.Mutex
}

func (l *lifecycleInvoker) Invoke(_ context.Context, _ string, payload []byte) ([]byte, error) {
	var ev map[string]any
	if err := json.Unmarshal(payload, &ev); err != nil {
		return nil, err
	}

	l.mu.Lock()
	l.events = append(l.events, ev)
	hook := l.hook
	l.mu.Unlock()

	if hook != nil {
		return hook(ev)
	}

	return nil, nil
}

func (l *lifecycleInvoker) types() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, 0, len(l.events))
	for _, e := range l.events {
		out = append(out, e["Type"].(string))
	}

	return out
}

type lifecycleFixture struct {
	b         *appconfig.InMemoryBackend
	invoker   *lifecycleInvoker
	deliverer *fakeDeliverer
	appID     string
	envID     string
	profileID string
	strategy  string
}

func newLifecycleFixture(
	t *testing.T,
	uris map[string][]string,
	duration, bake int32,
) *lifecycleFixture {
	t.Helper()

	b := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	inv := &lifecycleInvoker{}
	del := &fakeDeliverer{}
	b.SetExtensionLambdaInvoker(inv)
	b.SetExtensionTargetDeliverer(del)

	app, err := b.CreateApplication("app", "", nil)
	require.NoError(t, err)

	env, err := b.CreateEnvironment(app.ID, "env", "", nil, nil)
	require.NoError(t, err)

	profile, err := b.CreateConfigurationProfile(
		app.ID,
		"profile",
		"",
		"hosted",
		"",
		"",
		"",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.CreateHostedConfigurationVersion(
		app.ID,
		profile.ID,
		"text/plain",
		"",
		"",
		[]byte("v"),
		nil,
	)
	require.NoError(t, err)

	strategy, err := b.CreateDeploymentStrategy("s", "", duration, bake, 50, "LINEAR", "NONE", nil)
	require.NoError(t, err)

	actions := map[string][]appconfig.ExtensionAction{}
	for point, list := range uris {
		for _, u := range list {
			actions[point] = append(actions[point], appconfig.ExtensionAction{Name: "a", URI: u})
		}
	}

	ext, err := b.CreateExtension("ext", "", actions, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateExtensionAssociation(
		ext.ID,
		"arn:aws:appconfig:us-east-1:000000000000:application/"+app.ID,
		map[string]string{"k": "v"},
		nil,
		nil,
	)
	require.NoError(t, err)

	return &lifecycleFixture{
		b: b, invoker: inv, deliverer: del, appID: app.ID, envID: env.ID, profileID: profile.ID, strategy: strategy.ID,
	}
}

func (f *lifecycleFixture) start(t *testing.T) *appconfig.Deployment {
	t.Helper()

	d, err := f.b.StartDeployment(
		f.appID,
		f.envID,
		f.profileID,
		f.strategy,
		"1",
		"my deploy",
		nil,
		nil,
		nil,
	)
	require.NoError(t, err)

	return d
}

func (f *lifecycleFixture) state(t *testing.T) string {
	t.Helper()

	d, err := f.b.GetDeployment(f.appID, f.envID, 1)
	require.NoError(t, err)

	return d.State
}

func TestExtensionActions_OnDeploymentTargets(t *testing.T) {
	t.Parallel()

	f := newLifecycleFixture(t, map[string][]string{
		"ON_DEPLOYMENT_START":    {testSNSARN, testSQSARN, testBusARN},
		"ON_DEPLOYMENT_COMPLETE": {testSNSARN, testSQSARN, testBusARN},
	}, 0, 0)

	f.start(t)

	require.Eventually(t, func() bool {
		s, q, e := f.deliverer.snapshot()

		return len(s) == 2 && len(q) == 2 && len(e) == 2
	}, eventuallyWait, eventuallyTick)

	snsMsgs, sqsMsgs, busMsgs := f.deliverer.snapshot()

	tests := []struct {
		got  func(t *testing.T)
		name string
	}{
		{name: "sns", got: func(t *testing.T) {
			t.Helper()

			assert.Equal(t, testSNSARN, snsMsgs[0].topic)
			assert.Equal(t, "OnDeploymentStart", snsMsgs[0].attrs["MessageType"])
			assert.Equal(t, "OnDeploymentComplete", snsMsgs[1].attrs["MessageType"])

			var m map[string]any
			require.NoError(t, json.Unmarshal([]byte(snsMsgs[0].message), &m))
			assert.Equal(t, "1", m["DeploymentNumber"])
			assert.Equal(t, "1", m["ConfigurationVersion"])
			assert.Equal(t, "my deploy", m["Description"])
			assert.Equal(t, map[string]any{"k": "v"}, m["Parameters"])
			assert.Equal(t, map[string]any{"Id": f.appID, "Name": "app"}, m["Application"])
			assert.NotContains(t, m, "Content")
		}},
		{name: "sqs", got: func(t *testing.T) {
			t.Helper()

			var m map[string]any
			require.NoError(t, json.Unmarshal([]byte(sqsMsgs[1]), &m))
			assert.Equal(t, "OnDeploymentComplete", m["Type"])
			assert.Equal(t, "1", m["DeploymentNumber"])
		}},
		{name: "eventbridge", got: func(t *testing.T) {
			t.Helper()

			assert.Equal(t, testBusARN, busMsgs[0].busARN)
			assert.Equal(t, "aws.appconfig", busMsgs[0].source)
			assert.Equal(t, "On Deployment Start", busMsgs[0].detailType)
			assert.Equal(t, "On Deployment Complete", busMsgs[1].detailType)
			require.Len(t, busMsgs[0].resources, 1)
			assert.Contains(t, busMsgs[0].resources[0], ":extensionassociation/")

			var m map[string]any
			require.NoError(t, json.Unmarshal([]byte(busMsgs[0].detail), &m))
			assert.InDelta(t, 1, m["DeploymentNumber"], 0)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.got(t)
		})
	}
}

func TestExtensionActions_OnDeploymentProgression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		want     []string
		duration int32
		bake     int32
	}{
		{
			name: "growth_and_bake", duration: 5, bake: 5,
			want: []string{
				"OnDeploymentStart",
				"OnDeploymentStep",
				"OnDeploymentBaking",
				"OnDeploymentComplete",
			},
		},
		{
			name:     "bake_only",
			duration: 0,
			bake:     5,
			want:     []string{"OnDeploymentStart", "OnDeploymentBaking", "OnDeploymentComplete"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uri := testLambdaARN
			f := newLifecycleFixture(t, map[string][]string{
				"ON_DEPLOYMENT_START":    {uri},
				"ON_DEPLOYMENT_STEP":     {uri},
				"ON_DEPLOYMENT_BAKING":   {uri},
				"ON_DEPLOYMENT_COMPLETE": {uri},
			}, tt.duration, tt.bake)

			f.start(t)

			require.Eventually(
				t,
				func() bool { return f.state(t) == "COMPLETE" },
				eventuallyWait,
				eventuallyTick,
			)
			require.Eventually(
				t,
				func() bool { return len(f.invoker.types()) >= len(tt.want) },
				eventuallyWait,
				eventuallyTick,
			)

			got := f.invoker.types()
			assert.Equal(t, tt.want[0], got[0])
			assert.Equal(t, tt.want[len(tt.want)-1], got[len(got)-1])
			assert.Contains(t, got, "OnDeploymentBaking")
			assert.Equal(t, tt.name == "growth_and_bake", slices.Contains(got, "OnDeploymentStep"))
		})
	}
}

func TestExtensionActions_OnDeploymentErrorsIgnored(t *testing.T) {
	t.Parallel()

	f := newLifecycleFixture(t, map[string][]string{"ON_DEPLOYMENT_START": {testLambdaARN}}, 0, 0)
	f.invoker.hook = func(map[string]any) ([]byte, error) { return nil, errBoom }

	f.start(t)

	require.Eventually(
		t,
		func() bool { return len(f.invoker.types()) == 1 },
		eventuallyWait,
		eventuallyTick,
	)
	assert.Equal(t, "COMPLETE", f.state(t))
}

func TestExtensionActions_OnDeploymentRolledBack(t *testing.T) {
	t.Parallel()

	f := newLifecycleFixture(t, map[string][]string{
		"AT_DEPLOYMENT_TICK":        {testLambdaARN},
		"ON_DEPLOYMENT_ROLLED_BACK": {testSNSARN},
	}, 5, 0)

	release := make(chan struct{})
	f.invoker.hook = func(map[string]any) ([]byte, error) {
		<-release

		return nil, nil
	}

	f.start(t)

	require.Eventually(
		t,
		func() bool { return len(f.invoker.types()) == 1 },
		eventuallyWait,
		eventuallyTick,
	)

	d, err := f.b.StopDeployment(f.appID, f.envID, 1, false)
	require.NoError(t, err)
	assert.Equal(t, "ROLLED_BACK", d.State)

	close(release)

	require.Eventually(t, func() bool {
		s, _, _ := f.deliverer.snapshot()

		return len(s) == 1
	}, eventuallyWait, eventuallyTick)

	s, _, _ := f.deliverer.snapshot()
	assert.Equal(t, "OnDeploymentRolledBack", s[0].attrs["MessageType"])
}

func TestExtensionActions_AtDeploymentTick(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		hook      func(ev map[string]any) ([]byte, error)
		wantState string
		wantEvent string
	}{
		{
			name:      "continue",
			hook:      func(map[string]any) ([]byte, error) { return []byte(`{"Directive":"CONTINUE"}`), nil },
			wantState: "COMPLETE",
		},
		{
			name: "roll_back_directive",
			hook: func(map[string]any) ([]byte, error) {
				return []byte(`{"Directive":"ROLL_BACK","Description":"alarm"}`), nil
			},
			wantState: "ROLLED_BACK",
			wantEvent: "alarm",
		},
		{
			name:      "function_error",
			hook:      func(map[string]any) ([]byte, error) { return nil, errTickFailed },
			wantState: "ROLLED_BACK",
			wantEvent: "tick failed",
		},
		{
			name: "error_response",
			hook: func(map[string]any) ([]byte, error) {
				return []byte(`{"Error":"BadRequestError","Message":"nope"}`), nil
			},
			wantState: "ROLLED_BACK",
			wantEvent: "nope",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newLifecycleFixture(
				t,
				map[string][]string{"AT_DEPLOYMENT_TICK": {testLambdaARN}},
				5,
				0,
			)
			f.invoker.hook = tt.hook

			f.start(t)

			require.Eventually(
				t,
				func() bool { return f.state(t) == tt.wantState },
				eventuallyWait,
				eventuallyTick,
			)

			f.invoker.mu.Lock()
			first := f.invoker.events[0]
			f.invoker.mu.Unlock()

			assert.Equal(t, "AtDeploymentTick", first["Type"])
			assert.Equal(t, statePending, first["DeploymentState"])
			assert.Equal(t, "0.0", first["PercentageComplete"])

			if tt.wantEvent != "" {
				d, err := f.b.GetDeployment(f.appID, f.envID, 1)
				require.NoError(t, err)
				assert.Contains(t, d.EventLog[0].Description, tt.wantEvent)
				assert.Equal(t, "ROLLBACK_COMPLETED", d.EventLog[0].EventType)
			}
		})
	}
}

const (
	eventuallyWait = 5 * time.Second
	eventuallyTick = 5 * time.Millisecond
)
