package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cloudfrontbackend "github.com/blackbirdworks/gopherstack/services/cloudfront"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

const albDistributionConfig = `<DistributionConfig><CallerReference>alb</CallerReference><Comment>alb</Comment>` +
	`<Enabled>true</Enabled><Origins><Quantity>1</Quantity><Items><Origin><Id>alb</Id>` +
	`<DomainName>%s</DomainName><CustomOriginConfig><HTTPPort>80</HTTPPort><HTTPSPort>443</HTTPSPort>` +
	`<OriginProtocolPolicy>http-only</OriginProtocolPolicy></CustomOriginConfig></Origin></Items></Origins>` +
	`<DefaultCacheBehavior><TargetOriginId>alb</TargetOriginId><ViewerProtocolPolicy>allow-all</ViewerProtocolPolicy>` +
	`<CachePolicyId>4135ea2d-6df8-44a3-9df3-4b5a84be39ad</CachePolicyId></DefaultCacheBehavior></DistributionConfig>`

func TestWireCloudFrontDataPlane_ALBOrigin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		wire bool
		want int
	}{
		{"unwired", false, http.StatusBadGateway},
		{"wired", true, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			elbBackend := elbv2backend.NewInMemoryBackend("123456789012", "us-east-1")
			t.Cleanup(elbBackend.Close)
			elbHandler := elbv2backend.NewHandler(elbBackend)

			lb, err := elbBackend.CreateLoadBalancer(elbv2backend.CreateLoadBalancerInput{Name: "cf-alb"})
			require.NoError(t, err)
			_, err = elbBackend.CreateListener(elbv2backend.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancerArn, Protocol: "HTTP", Port: 80,
				DefaultActions: []elbv2backend.Action{{
					Type: "fixed-response",
					FixedResponseConfig: &elbv2backend.FixedResponseConfig{
						StatusCode: "200", ContentType: "text/plain", MessageBody: "hello-alb",
					},
				}},
			})
			require.NoError(t, err)

			cfBackend := cloudfrontbackend.NewInMemoryBackend(t.Context(), "123456789012", config.DefaultRegion)
			t.Cleanup(cfBackend.Close)
			cfHandler := cloudfrontbackend.NewHandler(cfBackend)

			raw := []byte(fmt.Sprintf(albDistributionConfig, lb.DNSName))
			dist, err := cfBackend.CreateDistribution("alb", "alb", true, raw)
			require.NoError(t, err)

			if tt.wire {
				wireCloudFrontDataPlane(map[string]service.Registerable{"CloudFront": cfHandler, "ELBv2": elbHandler})
			}

			req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/hello", nil)
			req.Host = dist.DomainName
			rec := httptest.NewRecorder()
			require.NoError(t, cfHandler.Handler()(echo.New().NewContext(req, rec)))

			assert.Equal(t, tt.want, rec.Code)

			if tt.wire {
				assert.Equal(t, "hello-alb", rec.Body.String())
			}
		})
	}
}
