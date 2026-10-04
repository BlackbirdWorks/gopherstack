package eks_test

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

type recorderDoer struct{ h http.Handler }

func (d recorderDoer) Do(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	d.h.ServeHTTP(rec, r)

	return rec.Result(), nil
}

func newInProcessEKSClient(t *testing.T, h *eks.Handler) *ekssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithHTTPClient(recorderDoer{h: e}),
	)
	require.NoError(t, err)

	return ekssdk.NewFromConfig(cfg, func(o *ekssdk.Options) {
		o.BaseEndpoint = aws.String("http://eks.test")
	})
}

func TestClientRequestToken_24hWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		advance      time.Duration
		wantSameResp bool
	}{
		{name: "within_window_replays", advance: 23 * time.Hour, wantSameResp: true},
		{name: "after_window_creates_again", advance: 25 * time.Hour, wantSameResp: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				h := newTestEKSHandler(t)
				client := newInProcessEKSClient(t, h)
				ctx := t.Context()

				_, err := client.CreateCluster(ctx, &ekssdk.CreateClusterInput{
					Name:               aws.String("ttl-cluster"),
					RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks"),
					ResourcesVpcConfig: &ekstypes.VpcConfigRequest{},
				})
				require.NoError(t, err)

				in := &ekssdk.CreateFargateProfileInput{
					ClusterName:         aws.String("ttl-cluster"),
					FargateProfileName:  aws.String("ttl-fp"),
					PodExecutionRoleArn: aws.String("arn:aws:iam::123456789012:role/fargate"),
					ClientRequestToken:  aws.String("ttl-token"),
				}

				_, err = client.CreateFargateProfile(ctx, in)
				require.NoError(t, err)

				time.Sleep(tt.advance)

				_, err = client.CreateFargateProfile(ctx, in)
				if tt.wantSameResp {
					require.NoError(t, err)

					return
				}

				var dup *ekstypes.InvalidParameterException
				assert.ErrorAs(t, err, &dup)
			})
		})
	}
}
