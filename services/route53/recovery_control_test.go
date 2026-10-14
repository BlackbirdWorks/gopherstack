package route53_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/route53"
	cc "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

// recoveryStack exposes the control-plane handler the way *CLI does.
type recoveryStack struct {
	cc atomic.Pointer[cc.Handler]
}

func (s *recoveryStack) GetRoute53RecoveryControlConfigHandler() service.Registerable {
	if h := s.cc.Load(); h != nil {
		return h
	}

	return nil
}

type recoveryWorld struct {
	backend *route53.InMemoryBackend
	control *cc.InMemoryBackend
	arn     string
	zone    string
}

func newRecoveryWorld(t *testing.T, wire bool) *recoveryWorld {
	t.Helper()

	control := cc.NewInMemoryBackend("123456789012")
	cl, err := control.CreateCluster("c", "", "", nil)
	require.NoError(t, err)
	rcOut, err := control.CreateRoutingControl(cl.ClusterArn, "", "primary-up", "")
	require.NoError(t, err)

	b := route53.NewInMemoryBackend()

	if wire {
		st := &recoveryStack{}
		st.cc.Store(cc.NewHandler(control))
		b.SetAppConfig(st)
	}

	zone, err := b.CreateHostedZone("example.com.", "ref", "", false, "", "", "")
	require.NoError(t, err)

	return &recoveryWorld{backend: b, control: control, arn: rcOut.RoutingControlArn, zone: zone.ID}
}

func (w *recoveryWorld) flip(t *testing.T, state string) {
	t.Helper()
	require.NoError(t, w.control.UpdateRoutingControlStates([]cc.StateUpdate{{ARN: w.arn, State: state}}, nil, ""))
}

func (w *recoveryWorld) healthCheck(t *testing.T, inverted bool) string {
	t.Helper()

	hc, err := w.backend.CreateHealthCheck(fmt.Sprintf("hc-%s-%t", t.Name(), inverted), route53.HealthCheckConfig{
		Type: route53.HealthCheckTypeRecoveryControl, RoutingControlArn: w.arn, Inverted: inverted,
	})
	require.NoError(t, err)

	return hc.ID
}

func (w *recoveryWorld) failoverRecords(t *testing.T, hcID string) {
	t.Helper()

	rec := func(id string, fo route53.FailoverPolicy, ip, hc string) route53.Change {
		return route53.Change{Action: route53.ChangeActionCreate, ResourceRecordSet: route53.ResourceRecordSet{
			Name: "app.example.com.", Type: "A", TTL: 60, SetIdentifier: id, Failover: fo, HealthCheckID: hc,
			Records: []route53.ResourceRecord{{Value: ip}},
		}}
	}

	_, err := w.backend.ChangeResourceRecordSets(w.zone, []route53.Change{
		rec("primary", route53.FailoverPrimary, "192.0.2.1", hcID),
		rec("secondary", route53.FailoverSecondary, "192.0.2.2", ""),
	})
	require.NoError(t, err)
}

func (w *recoveryWorld) answer(t *testing.T) string {
	t.Helper()

	got, err := w.backend.TestDNSAnswer(w.zone, "app.example.com.", "A", route53.DNSQueryContext{})
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func TestRecoveryControlHealthCheckFollowsRoutingControl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		state        string
		wantStatus   string
		wantResolves string
		inverted     bool
	}{
		{name: "on is healthy", state: "On", wantStatus: "Healthy", wantResolves: "192.0.2.1"},
		{name: "off is unhealthy", state: "Off", wantStatus: "Unhealthy", wantResolves: "192.0.2.2"},
		{name: "inverted on fails over", inverted: true, state: "On", wantStatus: "Healthy", wantResolves: "192.0.2.2"},
		{
			name:         "inverted off serves primary",
			inverted:     true,
			state:        "Off",
			wantStatus:   "Unhealthy",
			wantResolves: "192.0.2.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := newRecoveryWorld(t, true)
			hc := w.healthCheck(t, tt.inverted)
			w.failoverRecords(t, hc)
			w.flip(t, tt.state)

			status, err := w.backend.GetHealthCheckStatus(hc)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, status)
			assert.Equal(t, tt.wantResolves, w.answer(t))
		})
	}
}

func TestRecoveryControlFailoverFollowsFlips(t *testing.T) {
	t.Parallel()

	w := newRecoveryWorld(t, true)
	w.failoverRecords(t, w.healthCheck(t, false))

	w.flip(t, "On")
	assert.Equal(t, "192.0.2.1", w.answer(t))
	w.flip(t, "Off")
	assert.Equal(t, "192.0.2.2", w.answer(t), "turning the routing control Off fails over to the secondary")
	w.flip(t, "On")
	assert.Equal(t, "192.0.2.1", w.answer(t), "turning it back On restores the primary")
}

func TestRecoveryControlWithoutSiblingKeepsStoredStatus(t *testing.T) {
	t.Parallel()

	w := newRecoveryWorld(t, false)
	hc := w.healthCheck(t, false)
	w.flip(t, "Off")

	status, err := w.backend.GetHealthCheckStatus(hc)
	require.NoError(t, err)
	assert.Equal(t, "Healthy", status, "no routing control store is reachable, so the stored status is kept")
}

func TestRecoveryControlDeletedControlKeepsStoredStatus(t *testing.T) {
	t.Parallel()

	w := newRecoveryWorld(t, true)
	hc := w.healthCheck(t, false)
	w.flip(t, "Off")
	require.NoError(t, w.control.DeleteRoutingControl(w.arn))

	status, err := w.backend.GetHealthCheckStatus(hc)
	require.NoError(t, err)
	assert.Equal(t, "Healthy", status)
}

func TestRecoveryControlHealthCheckIDs(t *testing.T) {
	t.Parallel()

	w := newRecoveryWorld(t, true)
	a, b := w.healthCheck(t, false), w.healthCheck(t, true)

	_, err := w.backend.CreateHealthCheck(
		"tcp",
		route53.HealthCheckConfig{Type: route53.HealthCheckTypeTCP, IPAddress: "192.0.2.9", Port: 80},
	)
	require.NoError(t, err)

	assert.ElementsMatch(t, []string{a, b}, w.backend.RecoveryControlHealthCheckIDs(w.arn))
	assert.Empty(t, w.backend.RecoveryControlHealthCheckIDs(w.arn+"x"))
}
