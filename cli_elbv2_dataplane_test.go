package main

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestSplitFunctionARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		arn       string
		wantName  string
		wantQuali string
	}{
		{"unqualified", "arn:aws:lambda:us-east-1:1:function:f", "arn:aws:lambda:us-east-1:1:function:f", ""},
		{"alias", "arn:aws:lambda:us-east-1:1:function:f:live", "arn:aws:lambda:us-east-1:1:function:f", "live"},
		{"bare", "f", "f", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			name, q := splitFunctionARN(tt.arn)
			assert.Equal(t, tt.wantName, name)
			assert.Equal(t, tt.wantQuali, q)
		})
	}
}

func TestWireELBv2DataPlane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wire bool
		want int
	}{
		{"unwired", false, http.StatusServiceUnavailable},
		{"wired", true, http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := elbv2backend.NewInMemoryBackend("123456789012", "us-east-1")
			t.Cleanup(bk.Close)
			h := elbv2backend.NewHandler(bk)

			lb, err := bk.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{Name: "wired-lb"})
			require.NoError(t, err)
			tg, err := bk.CreateTargetGroup(elbv2backend.CreateTargetGroupInput{Name: "fn", TargetType: "lambda"})
			require.NoError(t, err)
			require.NoError(t, bk.RegisterTargets(tg.TargetGroupArn, []elbv2backend.Target{
				{ID: "arn:aws:lambda:us-east-1:123456789012:function:missing"},
			}))
			_, err = bk.CreateListener(elbv2backend.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancerArn, Protocol: "HTTP", Port: 80,
				DefaultActions: []elbv2backend.Action{{Type: "forward", TargetGroupArn: tg.TargetGroupArn}},
			})
			require.NoError(t, err)

			if tt.wire {
				wireELBv2DataPlane(map[string]service.Registerable{"ELBv2": h})
			}

			e := echo.New()
			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_gopherstack/elbv2/wired-lb/x", nil)
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))
			assert.Equal(t, tt.want, rec.Code)
		})
	}
}
