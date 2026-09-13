package elb_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

// statusCapturingTransport records the HTTP status of the last round trip so
// tests can assert the real wire status independently of the SDK's error
// deserializer, which keys error type purely off the XML <Code> text and
// never inspects StatusCode past the generic 200-299 success gate (verified
// in awsAwsquery_deserializeOpError* funcs, elasticloadbalancing@v1.36.4
// deserializers.go).
type statusCapturingTransport struct {
	status int
}

func (rt *statusCapturingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if resp != nil {
		rt.status = resp.StatusCode
	}

	return resp, err
}

func newStatusCapturingELBClient(t *testing.T, h *elb.Handler) (*elbsdk.Client, *statusCapturingTransport) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	transport := &statusCapturingTransport{}

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
		awscfg.WithHTTPClient(&http.Client{Transport: transport}),
	)
	require.NoError(t, err)

	client := elbsdk.NewFromConfig(cfg, func(o *elbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	return client, transport
}

// TestErrorHTTPStatus proves classic ELB's client errors carry the real AWS
// wire HTTP status, not the REST-JSON-style 404/409 this service used to
// return (gopherstack-miw): every exception in aws-sdk-go@v1.55.8
// models/apis/elasticloadbalancing/2012-06-01/api-2.json is httpStatusCode
// 400 except InvalidConfigurationRequestException, which is genuinely 409 --
// the one sentinel that must NOT be flattened to 400.
func TestErrorHTTPStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run        func(t *testing.T, client *elbsdk.Client) error
		checkErr   func(t *testing.T, err error)
		name       string
		wantStatus int
	}{
		{
			name: "describe_load_balancers_unknown_name_is_400",
			run: func(t *testing.T, client *elbsdk.Client) error {
				t.Helper()

				_, err := client.DescribeLoadBalancers(t.Context(), &elbsdk.DescribeLoadBalancersInput{
					LoadBalancerNames: []string{"no-such-lb"},
				})

				return err
			},
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var notFound *types.AccessPointNotFoundException
				assert.ErrorAs(t, err, &notFound, "expected AccessPointNotFoundException, got %v", err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_load_balancer_duplicate_name_is_400",
			run: func(t *testing.T, client *elbsdk.Client) error {
				t.Helper()

				input := &elbsdk.CreateLoadBalancerInput{
					LoadBalancerName:  aws.String("dup-lb"),
					AvailabilityZones: []string{"us-east-1a"},
					Listeners: []types.Listener{
						{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
					},
				}

				_, err := client.CreateLoadBalancer(t.Context(), input)
				require.NoError(t, err)

				_, err = client.CreateLoadBalancer(t.Context(), input)

				return err
			},
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var dup *types.DuplicateAccessPointNameException
				assert.ErrorAs(t, err, &dup, "expected DuplicateAccessPointNameException, got %v", err)
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "attach_subnets_to_classic_lb_invalid_configuration_is_409",
			run: func(t *testing.T, client *elbsdk.Client) error {
				t.Helper()

				_, err := client.CreateLoadBalancer(t.Context(), &elbsdk.CreateLoadBalancerInput{
					LoadBalancerName:  aws.String("classic-lb"),
					AvailabilityZones: []string{"us-east-1a"},
					Listeners: []types.Listener{
						{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
					},
				})
				require.NoError(t, err)

				_, err = client.AttachLoadBalancerToSubnets(t.Context(), &elbsdk.AttachLoadBalancerToSubnetsInput{
					LoadBalancerName: aws.String("classic-lb"),
					Subnets:          []string{"subnet-0123456789abcdef0"},
				})

				return err
			},
			checkErr: func(t *testing.T, err error) {
				t.Helper()

				var invalidConfig *types.InvalidConfigurationRequestException
				assert.ErrorAs(t, err, &invalidConfig,
					"expected InvalidConfigurationRequestException, got %v", err)
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
			h := elb.NewHandler(backend)
			client, transport := newStatusCapturingELBClient(t, h)

			err := tt.run(t, client)
			require.Error(t, err)
			tt.checkErr(t, err)
			assert.Equal(t, tt.wantStatus, transport.status)
		})
	}
}
