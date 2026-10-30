package app

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	acmbackend "github.com/blackbirdworks/gopherstack/services/acm"
	elbv2backend "github.com/blackbirdworks/gopherstack/services/elbv2"
)

func TestInitializeServices_ELBv2RealListenersTerminateACMCert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mode    string
		wantTLS bool
	}{
		{name: "real mode terminates tls with imported acm cert", mode: "real", wantTLS: true},
		{name: "routed mode binds nothing", mode: "routed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli := &CLI{AccountID: "000000000000", ELBv2: ELBv2Settings{Listeners: tt.mode}}
			cli.faultStore = chaos.NewFaultStore()

			services, err := initializeServicesT(t, &service.AppContext{
				Logger: slog.Default(), Config: cli, JanitorCtx: t.Context(),
			})
			require.NoError(t, err)

			byName := serviceByName(services)
			elbH, ok := byName["ELBv2"].(*elbv2backend.Handler)
			require.True(t, ok)

			t.Cleanup(func() { elbH.Shutdown(context.Background()) })

			acmH, ok := byName["ACM"].(*acmbackend.Handler)
			require.True(t, ok)

			certPEM, keyPEM, err := devtls.GenerateSelfSignedCertPEM("127.0.0.1")
			require.NoError(t, err)

			cert, err := acmH.Backend.ImportCertificate(t.Context(), string(certPEM), string(keyPEM), "", "")
			require.NoError(t, err)

			target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte("from-target"))
			}))
			t.Cleanup(target.Close)

			e := echo.New()
			registry := service.NewRegistry()
			require.NoError(t, registry.Register(elbH))
			e.Use(service.NewServiceRouter(registry).RouteHandler())

			srv := httptest.NewServer(e)
			t.Cleanup(srv.Close)

			awsCfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
				awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
			require.NoError(t, err)

			client := elbv2sdk.NewFromConfig(awsCfg, func(o *elbv2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) })

			lb, err := client.CreateLoadBalancer(
				t.Context(),
				&elbv2sdk.CreateLoadBalancerInput{Name: aws.String("real-lb")},
			)
			require.NoError(t, err)

			tgOut, err := client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
				Name: aws.String("real-tg"), TargetType: elbv2types.TargetTypeEnumIp,
				Protocol: elbv2types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"),
			})
			require.NoError(t, err)

			tgArn := aws.ToString(tgOut.TargetGroups[0].TargetGroupArn)
			host, port := splitHostPort(t, target.Listener.Addr().String())

			_, err = client.RegisterTargets(t.Context(), &elbv2sdk.RegisterTargetsInput{
				TargetGroupArn: aws.String(tgArn),
				Targets:        []elbv2types.TargetDescription{{Id: aws.String(host), Port: aws.Int32(port)}},
			})
			require.NoError(t, err)

			lOut, err := client.CreateListener(t.Context(), &elbv2sdk.CreateListenerInput{
				LoadBalancerArn: lb.LoadBalancers[0].LoadBalancerArn, Protocol: elbv2types.ProtocolEnumHttps,
				Port: aws.Int32(443), SslPolicy: aws.String("ELBSecurityPolicy-TLS13-1-2-2021-06"),
				Certificates: []elbv2types.Certificate{{CertificateArn: aws.String(cert.ARN)}},
				DefaultActions: []elbv2types.Action{
					{Type: elbv2types.ActionTypeEnumForward, TargetGroupArn: aws.String(tgArn)},
				},
			})
			require.NoError(t, err)

			be, ok := elbH.Backend.(*elbv2backend.InMemoryBackend)
			require.True(t, ok)

			ep, bound := be.ListenerEndpoint(aws.ToString(lOut.Listeners[0].ListenerArn))
			require.Equal(t, tt.wantTLS, bound)

			if !tt.wantTLS {
				return
			}

			pool := x509.NewCertPool()
			require.True(t, pool.AppendCertsFromPEM(certPEM))

			tr := &http.Transport{
				DisableKeepAlives: true,
				TLSClientConfig:   &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
			}
			t.Cleanup(tr.CloseIdleConnections)

			resp, err := (&http.Client{Transport: tr}).Get("https://" + ep.Address + "/")
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.Equal(t, "from-target", string(body))
		})
	}
}

func splitHostPort(t *testing.T, addr string) (string, int32) {
	t.Helper()

	h, p, err := net.SplitHostPort(addr)
	require.NoError(t, err)

	n, err := strconv.Atoi(p)
	require.NoError(t, err)

	return h, int32(n)
}
