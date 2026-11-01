package elbv2_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	elbv2sdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2"
	elbv2types "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancingv2/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/devtls"
	"github.com/blackbirdworks/gopherstack/services/elbv2"
)

type pemPair struct{ cert, key []byte }

type realCertResolver struct{ certs map[string]pemPair }

func (r *realCertResolver) ResolveCertificate(arn string) bool {
	_, ok := r.certs[arn]

	return ok
}
func (r *realCertResolver) AddInUseBy(string, string)    {}
func (r *realCertResolver) RemoveInUseBy(string, string) {}

func (r *realCertResolver) CertificateKeyPair(arn string) ([]byte, []byte, bool) {
	p, ok := r.certs[arn]

	return p.cert, p.key, ok
}

type realFixture struct {
	be     *elbv2.InMemoryBackend
	client *elbv2sdk.Client
	ca     *devtls.CA
	res    *realCertResolver
}

func newRealFixture(t *testing.T) *realFixture {
	t.Helper()

	ca, err := devtls.NewCA()
	require.NoError(t, err)

	res := &realCertResolver{certs: map[string]pemPair{}}
	be := elbv2.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(be.Close)
	be.SetCertificateResolver(res)
	be.EnableRealListeners(elbv2.RealListenerConfig{})

	h := elbv2.NewHandler(be)
	client, _ := newTestELBv2ClientURL(t, h)

	return &realFixture{be: be, client: client, ca: ca, res: res}
}

// cert issues a CA-signed certificate for hosts and registers it under a fresh ARN.
func (f *realFixture) cert(t *testing.T, name string, hosts ...string) string {
	t.Helper()

	c, err := f.ca.Issue(hosts...)
	require.NoError(t, err)

	keyDER, err := x509.MarshalPKCS8PrivateKey(c.PrivateKey)
	require.NoError(t, err)

	arn := "arn:aws:acm:us-east-1:123456789012:certificate/" + name
	f.res.certs[arn] = pemPair{
		cert: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: c.Certificate[0]}),
		key:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}

	return arn
}

func (f *realFixture) lb(t *testing.T, name string, typ elbv2types.LoadBalancerTypeEnum) string {
	t.Helper()

	out, err := f.client.CreateLoadBalancer(t.Context(), &elbv2sdk.CreateLoadBalancerInput{
		Name: aws.String(name), Type: typ,
	})
	require.NoError(t, err)

	return aws.ToString(out.LoadBalancers[0].LoadBalancerArn)
}

func (f *realFixture) tg(t *testing.T, name string, proto elbv2types.ProtocolEnum, target string) string {
	t.Helper()

	out, err := f.client.CreateTargetGroup(t.Context(), &elbv2sdk.CreateTargetGroupInput{
		Name: aws.String(name), TargetType: elbv2types.TargetTypeEnumIp, Protocol: proto,
		Port: aws.Int32(80), VpcId: aws.String("vpc-1"),
	})
	require.NoError(t, err)

	arn := aws.ToString(out.TargetGroups[0].TargetGroupArn)
	host, port, err := net.SplitHostPort(target)
	require.NoError(t, err)

	p, err := strconv.Atoi(port)
	require.NoError(t, err)

	_, err = f.client.RegisterTargets(t.Context(), &elbv2sdk.RegisterTargetsInput{
		TargetGroupArn: aws.String(arn),
		Targets:        []elbv2types.TargetDescription{{Id: aws.String(host), Port: aws.Int32(int32(p))}},
	})
	require.NoError(t, err)

	return arn
}

func (f *realFixture) attrs(t *testing.T, arn string, kv map[string]string) {
	t.Helper()

	if len(kv) == 0 {
		return
	}

	var attrs []elbv2types.LoadBalancerAttribute
	for k, v := range kv {
		attrs = append(attrs, elbv2types.LoadBalancerAttribute{Key: aws.String(k), Value: aws.String(v)})
	}

	_, err := f.client.ModifyLoadBalancerAttributes(t.Context(), &elbv2sdk.ModifyLoadBalancerAttributesInput{
		LoadBalancerArn: aws.String(arn), Attributes: attrs,
	})
	require.NoError(t, err)
}

type listenerOpts struct {
	policy string
	certs  []string
	alpn   []string
}

func (f *realFixture) listener(
	t *testing.T, lbArn string, proto elbv2types.ProtocolEnum, port int32, tgArn string, o listenerOpts,
) string {
	t.Helper()

	in := &elbv2sdk.CreateListenerInput{
		LoadBalancerArn: aws.String(lbArn), Protocol: proto, Port: aws.Int32(port),
		DefaultActions: []elbv2types.Action{forwardAction(tgArn)},
	}
	if o.policy != "" {
		in.SslPolicy = aws.String(o.policy)
	}

	for _, c := range o.certs {
		in.Certificates = append(in.Certificates, elbv2types.Certificate{CertificateArn: aws.String(c)})
	}

	in.AlpnPolicy = o.alpn

	out, err := f.client.CreateListener(t.Context(), in)
	require.NoError(t, err)

	return aws.ToString(out.Listeners[0].ListenerArn)
}

func (f *realFixture) addr(t *testing.T, listenerArn string) string {
	t.Helper()

	ep, ok := f.be.ListenerEndpoint(listenerArn)
	require.True(t, ok, "listener must be bound")

	return ep.Address
}

func (f *realFixture) httpClient(t *testing.T, serverName string, maxVersion uint16) *http.Client {
	t.Helper()

	tr := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig: &tls.Config{
			RootCAs: f.ca.Pool(), ServerName: serverName, MinVersion: tls.VersionTLS12, MaxVersion: maxVersion,
		},
	}
	t.Cleanup(tr.CloseIdleConnections)

	return &http.Client{Transport: tr}
}
