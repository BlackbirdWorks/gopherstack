package route53recoverycluster_test

import (
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/route53recoverycluster"
	rccfg "github.com/aws/aws-sdk-go-v2/service/route53recoverycontrolconfig"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	cluster "github.com/blackbirdworks/gopherstack/services/route53recoverycluster"
	controlconfig "github.com/blackbirdworks/gopherstack/services/route53recoverycontrolconfig"
)

const testAccount = "123456789012"

// stack is a minimal composition root: the control plane, the data plane and a config that exposes the sibling.
type stack struct {
	controlPlane *controlconfig.Handler
	endpoint     atomic.Value
}

func (s *stack) GetRoute53RecoveryControlConfigHandler() service.Registerable { return s.controlPlane }

func (s *stack) GetEndpoint() string { return s.endpoint.Load().(string) }

type fixture struct {
	stack    *stack
	ccClient *rccfg.Client
	cfg      aws.Config
	url      string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	st := &stack{}
	st.endpoint.Store("")

	st.controlPlane = controlconfig.NewHandler(controlconfig.NewInMemoryBackend(testAccount))
	st.controlPlane.Backend.SetAppConfig(st)

	url := sdktest.Serve(t, st.controlPlane, cluster.NewHandler(st))
	st.endpoint.Store(url)

	cfg, err := awscfg.LoadDefaultConfig(t.Context(),
		awscfg.WithRegion("us-west-2"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return &fixture{
		stack: st, cfg: cfg, url: url,
		ccClient: rccfg.NewFromConfig(cfg, func(o *rccfg.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func (f *fixture) dataPlane(endpoint string) *route53recoverycluster.Client {
	return route53recoverycluster.NewFromConfig(f.cfg, func(o *route53recoverycluster.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}
