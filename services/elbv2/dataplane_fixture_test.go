package elbv2_test

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"

	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

type dpFixture struct {
	h       *elbv2.Handler
	client  *elbv2sdk.Client
	base    string
	lbName  string
	lbArn   string
	lbDNS   string
	srvAddr string
}

func newDPFixture(t *testing.T) *dpFixture {
	t.Helper()

	b := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	h := elbv2.NewHandler(b)
	h.EnableRegions()

	client, srvURL := newTestELBv2ClientURL(t, h)
	u, err := url.Parse(srvURL)
	require.NoError(t, err)

	f := &dpFixture{h: h, client: client, base: srvURL, lbName: "dp-lb", srvAddr: u.Host}

	out, err := client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{Name: aws.String(f.lbName)})
	require.NoError(t, err)
	f.lbArn = aws.ToString(out.LoadBalancers[0].LoadBalancerArn)
	f.lbDNS = aws.ToString(out.LoadBalancers[0].DNSName)

	return f
}

func (f *dpFixture) tg(t *testing.T, name string, ttype elbv2types.TargetTypeEnum, hcPath string) string {
	t.Helper()

	in := &elbv2sdk.CreateTargetGroupInput{
		Name: aws.String(name), TargetType: ttype,
		Protocol: elbv2types.ProtocolEnumHttp, Port: aws.Int32(80), VpcId: aws.String("vpc-1"),
	}
	if ttype == elbv2types.TargetTypeEnumLambda {
		in.Protocol, in.Port, in.VpcId = "", nil, nil
	}

	if hcPath != "" {
		in.HealthCheckPath = aws.String(hcPath)
	}

	out, err := f.client.CreateTargetGroup(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.TargetGroups[0].TargetGroupArn)
}

func (f *dpFixture) register(t *testing.T, tgArn string, ids ...string) {
	t.Helper()

	targets := make([]elbv2types.TargetDescription, 0, len(ids))

	for _, id := range ids {
		d := elbv2types.TargetDescription{Id: aws.String(id)}
		if host, port, err := net.SplitHostPort(id); err == nil {
			p, _ := strconv.Atoi(port)
			d.Id, d.Port = aws.String(host), aws.Int32(int32(p))
		}

		targets = append(targets, d)
	}

	_, err := f.client.RegisterTargets(t.Context(), &elbv2sdk.RegisterTargetsInput{
		TargetGroupArn: aws.String(tgArn), Targets: targets,
	})
	require.NoError(t, err)
}

func (f *dpFixture) listener(t *testing.T, actions ...elbv2types.Action) string {
	t.Helper()

	out, err := f.client.CreateListener(t.Context(), &elbv2sdk.CreateListenerInput{
		LoadBalancerArn: aws.String(f.lbArn), Protocol: elbv2types.ProtocolEnumHttp,
		Port: aws.Int32(80), DefaultActions: actions,
	})
	require.NoError(t, err)

	return aws.ToString(out.Listeners[0].ListenerArn)
}

func (f *dpFixture) rule(
	t *testing.T, listenerArn string, prio int32, conds []elbv2types.RuleCondition, actions ...elbv2types.Action,
) {
	t.Helper()

	_, err := f.client.CreateRule(t.Context(), &elbv2sdk.CreateRuleInput{
		ListenerArn: aws.String(listenerArn), Priority: aws.Int32(prio), Conditions: conds, Actions: actions,
	})
	require.NoError(t, err)
}

func fixedAction(status, body string) elbv2types.Action {
	return elbv2types.Action{
		Type: elbv2types.ActionTypeEnumFixedResponse,
		FixedResponseConfig: &elbv2types.FixedResponseActionConfig{
			StatusCode: aws.String(status), ContentType: aws.String("text/plain"), MessageBody: aws.String(body),
		},
	}
}

func forwardAction(tgArn string) elbv2types.Action {
	return elbv2types.Action{Type: elbv2types.ActionTypeEnumForward, TargetGroupArn: aws.String(tgArn)}
}

func weightedForward(arns []string, weights []int32) elbv2types.Action {
	tuples := make([]elbv2types.TargetGroupTuple, 0, len(arns))
	for i, a := range arns {
		tuples = append(
			tuples,
			elbv2types.TargetGroupTuple{TargetGroupArn: aws.String(a), Weight: aws.Int32(weights[i])},
		)
	}

	return elbv2types.Action{
		Type:          elbv2types.ActionTypeEnumForward,
		ForwardConfig: &elbv2types.ForwardActionConfig{TargetGroups: tuples},
	}
}

type dpResult struct {
	header http.Header
	body   string
	status int
}

type dpReq struct {
	method  string
	path    string
	host    string
	headers map[string]string
	body    string
}

func (f *dpFixture) do(t *testing.T, r dpReq) dpResult {
	t.Helper()

	method := r.method
	if method == "" {
		method = http.MethodGet
	}

	target := f.base + "/_gopherstack/elbv2/" + f.lbName + r.path
	if strings.Contains(r.host, ".elb.") {
		target = f.base + r.path
	}

	req, err := http.NewRequestWithContext(t.Context(), method, target, stringReader(r.body))
	require.NoError(t, err)

	if r.host != "" {
		req.Host = r.host
	}

	for k, v := range r.headers {
		req.Header.Set(k, v)
	}

	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return dpResult{status: resp.StatusCode, body: string(body), header: resp.Header}
}

func stringReader(s string) io.Reader {
	if s == "" {
		return nil
	}

	return strings.NewReader(s)
}

type recordingBackend struct {
	srv  *httptest.Server
	reqs []*http.Request
	hits int
	mu   sync.Mutex
}

// newBackend starts an httptest server replying with name; health requests get healthStatus.
func newBackend(t *testing.T, name string, healthStatus int) *recordingBackend {
	t.Helper()

	rb := &recordingBackend{}
	rb.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(healthStatus)

			return
		}

		rb.mu.Lock()
		rb.hits++
		rb.reqs = append(rb.reqs, r.Clone(context.Background()))
		rb.mu.Unlock()

		w.Header().Set("X-Backend", name)
		_, _ = w.Write([]byte(name + ":" + r.URL.RequestURI()))
	}))
	t.Cleanup(rb.srv.Close)

	return rb
}

func (rb *recordingBackend) addr() string { return rb.srv.Listener.Addr().String() }

func (rb *recordingBackend) count() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.hits
}

func (rb *recordingBackend) last() *http.Request {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	return rb.reqs[len(rb.reqs)-1]
}

func newTestELBv2ClientURL(t *testing.T, h *elbv2.Handler) (*elbv2sdk.Client, string) {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return elbv2sdk.NewFromConfig(cfg, func(o *elbv2sdk.Options) { o.BaseEndpoint = aws.String(srv.URL) }), srv.URL
}
