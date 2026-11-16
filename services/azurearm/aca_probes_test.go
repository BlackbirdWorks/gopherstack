package azurearm_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/appcontainers/armappcontainers/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func TestACA_ProbesRunInTheReplicaEngine(t *testing.T) {
	t.Parallel()

	probe := func(typ armappcontainers.Type) *armappcontainers.ContainerAppProbe {
		return &armappcontainers.ContainerAppProbe{
			Type: &typ,
			HTTPGet: &armappcontainers.ContainerAppProbeHTTPGet{
				Port: new(int32(8080)), Path: new("/healthz"), Scheme: new(armappcontainers.SchemeHTTPS),
				HTTPHeaders: []*armappcontainers.ContainerAppProbeHTTPGetHTTPHeadersItem{
					{Name: new("X-Probe"), Value: new("yes")},
				},
			},
			PeriodSeconds: new(int32(5)),
		}
	}

	tests := []struct {
		name     string
		typ      armappcontainers.Type
		wantPath string
	}{
		{name: "liveness", typ: armappcontainers.TypeLiveness, wantPath: "/healthz"},
		{name: "readiness", typ: armappcontainers.TypeReadiness, wantPath: "/healthz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			got := make(chan azurearm.ACIHTTPProbe, 1)

			s.engine.SetProbeHooks(
				func(ctx context.Context, _ time.Duration) bool {
					<-ctx.Done()

					return false
				},
				func(_ context.Context, p azurearm.ACIHTTPProbe) (int, error) {
					select {
					case got <- p:
					default:
					}

					return http.StatusOK, nil
				})

			env := s.mustEnv(t, "rg", "env1")
			s.mustApp(t, "rg", "probed", *env.ID, acaApp{probes: []*armappcontainers.ContainerAppProbe{probe(tt.typ)}})

			select {
			case p := <-got:
				assert.Contains(t, p.URL, tt.wantPath)
				assert.Equal(t, "yes", p.Headers.Get("X-Probe"))
			case <-time.After(aciEventWait):
				require.Fail(t, "the engine never ran the container app probe")
			}
		})
	}
}

func TestACA_ProbeRanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		mod   func(*armappcontainers.ContainerAppProbe)
		typ   armappcontainers.Type
		valid bool
	}{
		{name: "defaults", typ: armappcontainers.TypeLiveness, valid: true},
		{name: "period too long", typ: armappcontainers.TypeLiveness,
			mod: func(p *armappcontainers.ContainerAppProbe) { p.PeriodSeconds = new(int32(241)) }},
		{name: "failure threshold zero", typ: armappcontainers.TypeLiveness,
			mod: func(p *armappcontainers.ContainerAppProbe) { p.FailureThreshold = new(int32(0)) }},
		{name: "delay too long", typ: armappcontainers.TypeReadiness,
			mod: func(p *armappcontainers.ContainerAppProbe) { p.InitialDelaySeconds = new(int32(61)) }},
		{name: "liveness success 2", typ: armappcontainers.TypeLiveness,
			mod: func(p *armappcontainers.ContainerAppProbe) { p.SuccessThreshold = new(int32(2)) }},
		{name: "readiness success 2", typ: armappcontainers.TypeReadiness, valid: true,
			mod: func(p *armappcontainers.ContainerAppProbe) { p.SuccessThreshold = new(int32(2)) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newACAStack(t)
			env := s.mustEnv(t, "rg", "env1")

			p := &armappcontainers.ContainerAppProbe{
				Type:    &tt.typ,
				HTTPGet: &armappcontainers.ContainerAppProbeHTTPGet{Port: new(int32(8080))},
			}
			if tt.mod != nil {
				tt.mod(p)
			}

			_, err := s.putApp(t, "rg", "ranged", *env.ID, acaApp{probes: []*armappcontainers.ContainerAppProbe{p}})
			if tt.valid {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			code, status := respCode(t, err)
			assert.Equal(t, http.StatusBadRequest, status)
			assert.NotEmpty(t, code)
		})
	}
}
