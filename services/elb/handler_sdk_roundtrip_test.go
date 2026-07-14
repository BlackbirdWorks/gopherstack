package elb_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	elbsdk "github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing"
	"github.com/aws/aws-sdk-go-v2/service/elasticloadbalancing/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/elb"
)

const rtTestRegion = "us-east-1"

// newTestELBClient stands up the real aws-sdk-go-v2 ELB (Classic) client
// against an httptest server running this package's Handler, wired through
// the same pkgs/service registry/router used in production. Round-tripping
// through the genuine SDK serializer/deserializer (rather than string-matching
// the raw XML body, as most other tests in this package do) is what actually
// proves a response is wire-compatible: a response can look plausible as a
// string yet still make the SDK's XML deserializer fail outright, e.g. a
// missing "*Result" element the deserializer unconditionally looks for via
// NodeDecoder.GetElement -- exactly the bug class this file targets.
func newTestELBClient(t *testing.T, h *elb.Handler) *elbsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(rtTestRegion),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return elbsdk.NewFromConfig(cfg, func(o *elbsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_VoidOps_NoDeserializationError exercises every op whose
// response struct was previously missing its required "<Op>Result" wrapper
// element (CreateAppCookieStickinessPolicy, CreateLBCookieStickinessPolicy,
// CreateLoadBalancerPolicy, DeleteLoadBalancerPolicy,
// SetLoadBalancerListenerSSLCertificate, SetLoadBalancerPoliciesOfListener,
// SetLoadBalancerPoliciesForBackendServer, DeleteLoadBalancer). The real
// deserializer for each of these ops calls
// decoder.GetElement("<Op>Result") unconditionally, which returns
// `"<Op>Result node not found"` when the element is absent -- so every one of
// these calls used to fail client-side with a deserialization error even
// though the operation succeeded on the server. A real client sequence
// exercising all eight in one realistic flow proves the fix.
func Test_SDKRoundTrip_VoidOps_NoDeserializationError(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	const lbName = "rt-void-lb"

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName: aws.String(lbName),
		Listeners: []types.Listener{
			{
				Protocol:         aws.String("HTTP"),
				LoadBalancerPort: 80,
				InstancePort:     aws.Int32(8080),
			},
			{
				Protocol:         aws.String("HTTPS"),
				LoadBalancerPort: 443,
				InstancePort:     aws.Int32(8443),
				SSLCertificateId: aws.String("arn:aws:iam::000000000000:server-certificate/initial-cert"),
			},
		},
		AvailabilityZones: []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = client.CreateAppCookieStickinessPolicy(ctx, &elbsdk.CreateAppCookieStickinessPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-app-pol"),
		CookieName:       aws.String("JSESSIONID"),
	})
	require.NoError(t, err, "CreateAppCookieStickinessPolicy")

	_, err = client.CreateLBCookieStickinessPolicy(ctx, &elbsdk.CreateLBCookieStickinessPolicyInput{
		LoadBalancerName:       aws.String(lbName),
		PolicyName:             aws.String("rt-lbc-pol"),
		CookieExpirationPeriod: aws.Int64(60),
	})
	require.NoError(t, err, "CreateLBCookieStickinessPolicy")

	_, err = client.CreateLoadBalancerPolicy(ctx, &elbsdk.CreateLoadBalancerPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-generic-pol"),
		PolicyTypeName:   aws.String("ProxyProtocolPolicyType"),
		PolicyAttributes: []types.PolicyAttribute{
			{AttributeName: aws.String("ProxyProtocol"), AttributeValue: aws.String("true")},
		},
	})
	require.NoError(t, err, "CreateLoadBalancerPolicy (attached)")

	_, err = client.CreateLoadBalancerPolicy(ctx, &elbsdk.CreateLoadBalancerPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-unused-pol"),
		PolicyTypeName:   aws.String("ProxyProtocolPolicyType"),
		PolicyAttributes: []types.PolicyAttribute{
			{AttributeName: aws.String("ProxyProtocol"), AttributeValue: aws.String("false")},
		},
	})
	require.NoError(t, err, "CreateLoadBalancerPolicy (unused)")

	_, err = client.SetLoadBalancerPoliciesOfListener(ctx, &elbsdk.SetLoadBalancerPoliciesOfListenerInput{
		LoadBalancerName: aws.String(lbName),
		LoadBalancerPort: 80,
		PolicyNames:      []string{"rt-app-pol"},
	})
	require.NoError(t, err, "SetLoadBalancerPoliciesOfListener")

	_, err = client.SetLoadBalancerPoliciesForBackendServer(ctx, &elbsdk.SetLoadBalancerPoliciesForBackendServerInput{
		LoadBalancerName: aws.String(lbName),
		InstancePort:     aws.Int32(8080),
		PolicyNames:      []string{"rt-generic-pol"},
	})
	require.NoError(t, err, "SetLoadBalancerPoliciesForBackendServer")

	_, err = client.SetLoadBalancerListenerSSLCertificate(ctx, &elbsdk.SetLoadBalancerListenerSSLCertificateInput{
		LoadBalancerName: aws.String(lbName),
		LoadBalancerPort: 443,
		SSLCertificateId: aws.String("arn:aws:acm:us-east-1:000000000000:certificate/abcd-efgh-1234"),
	})
	require.NoError(t, err, "SetLoadBalancerListenerSSLCertificate")

	_, err = client.DeleteLoadBalancerPolicy(ctx, &elbsdk.DeleteLoadBalancerPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-unused-pol"),
	})
	require.NoError(t, err, "DeleteLoadBalancerPolicy")

	_, err = client.DeleteLoadBalancer(ctx, &elbsdk.DeleteLoadBalancerInput{
		LoadBalancerName: aws.String(lbName),
	})
	require.NoError(t, err, "DeleteLoadBalancer")
}

// Test_SDKRoundTrip_TooManyLoadBalancers_IsTyped proves the real SDK client
// can type-assert the load-balancer-limit error into
// *types.TooManyAccessPointsException (wire code "TooManyLoadBalancers").
// Before the fix, the backend returned a generic ValidationError sentinel for
// this condition, which the real deserializer only maps to
// *smithy.GenericAPIError -- callers doing errors.As against the typed limit
// exception would never match.
func Test_SDKRoundTrip_TooManyLoadBalancers_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	for i := range 20 {
		_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
			LoadBalancerName:  aws.String(rtLBName(i)),
			AvailabilityZones: []string{"us-east-1a"},
			Listeners: []types.Listener{
				{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
			},
		})
		require.NoError(t, err)
	}

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String("rt-over-limit"),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.Error(t, err)

	var tooMany *types.TooManyAccessPointsException
	require.ErrorAs(t, err, &tooMany, "expected a typed TooManyAccessPointsException, got %v", err)
}

// rtLBName returns a deterministic, 32-char-limit-safe load balancer name for
// index i.
func rtLBName(i int) string {
	return "rt-max-lb-" + string(rune('a'+i))
}

// Test_SDKRoundTrip_TooManyTags_IsTyped proves the tag-count limit maps to the
// real *types.TooManyTagsException (wire code "TooManyTags"), not a generic
// ValidationError.
func Test_SDKRoundTrip_TooManyTags_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	const lbName = "rt-tags-lb"

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String(lbName),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)

	tags := make([]types.Tag, 0, 10)
	for i := range 10 {
		tags = append(tags, types.Tag{Key: aws.String(rtTagKey(i)), Value: aws.String("v")})
	}

	_, err = client.AddTags(ctx, &elbsdk.AddTagsInput{
		LoadBalancerNames: []string{lbName},
		Tags:              tags,
	})
	require.NoError(t, err)

	_, err = client.AddTags(ctx, &elbsdk.AddTagsInput{
		LoadBalancerNames: []string{lbName},
		Tags:              []types.Tag{{Key: aws.String("one-too-many"), Value: aws.String("v")}},
	})
	require.Error(t, err)

	var tooMany *types.TooManyTagsException
	require.ErrorAs(t, err, &tooMany, "expected a typed TooManyTagsException, got %v", err)
}

// rtTagKey returns a deterministic tag key for index i.
func rtTagKey(i int) string {
	return "k" + string(rune('a'+i))
}

// Test_SDKRoundTrip_DuplicateTagKeys_IsTyped proves that specifying the same
// tag key twice within one AddTags request maps to the real
// *types.DuplicateTagKeysException (wire code "DuplicateTagKeys"). Before the
// fix there was no such validation at all -- the second value silently
// overwrote the first with no error.
func Test_SDKRoundTrip_DuplicateTagKeys_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	const lbName = "rt-duptags-lb"

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String(lbName),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)

	_, err = client.AddTags(ctx, &elbsdk.AddTagsInput{
		LoadBalancerNames: []string{lbName},
		Tags: []types.Tag{
			{Key: aws.String("Env"), Value: aws.String("prod")},
			{Key: aws.String("Env"), Value: aws.String("staging")},
		},
	})
	require.Error(t, err)

	var dup *types.DuplicateTagKeysException
	require.ErrorAs(t, err, &dup, "expected a typed DuplicateTagKeysException, got %v", err)
}

// Test_SDKRoundTrip_InvalidScheme_IsTyped proves an invalid Scheme value maps
// to the real *types.InvalidSchemeException (wire code "InvalidScheme"), not
// a generic ValidationError.
func Test_SDKRoundTrip_InvalidScheme_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String("rt-badscheme-lb"),
		Scheme:            aws.String("not-a-real-scheme"),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.Error(t, err)

	var invalidScheme *types.InvalidSchemeException
	require.ErrorAs(t, err, &invalidScheme, "expected a typed InvalidSchemeException, got %v", err)
}

// Test_SDKRoundTrip_UnsupportedProtocol_IsTyped proves an invalid listener
// Protocol value maps to the real *types.UnsupportedProtocolException (wire
// code "UnsupportedProtocol"), not a generic ValidationError. The SDK does
// not validate Protocol client-side (it is an unconstrained *string), so the
// server-side check is what a real client actually depends on.
func Test_SDKRoundTrip_UnsupportedProtocol_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String("rt-badproto-lb"),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("FTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.Error(t, err)

	var unsupported *types.UnsupportedProtocolException
	require.ErrorAs(t, err, &unsupported, "expected a typed UnsupportedProtocolException, got %v", err)
}

// Test_SDKRoundTrip_PolicyTypeNotFound_IsTyped proves that referencing an
// unknown policy type name -- both from CreateLoadBalancerPolicy and
// DescribeLoadBalancerPolicyTypes -- maps to the real
// *types.PolicyTypeNotFoundException (wire code "PolicyTypeNotFound").
// Before the fix, CreateLoadBalancerPolicy returned a generic ValidationError
// and DescribeLoadBalancerPolicyTypes returned the *wrong* typed exception
// (PolicyNotFoundException, meant for policy *instances* not policy *types*).
func Test_SDKRoundTrip_PolicyTypeNotFound_IsTyped(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	const lbName = "rt-badpolicytype-lb"

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String(lbName),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateLoadBalancerPolicy(ctx, &elbsdk.CreateLoadBalancerPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-bad-pol"),
		PolicyTypeName:   aws.String("NoSuchPolicyType"),
	})
	require.Error(t, err)

	var notFound *types.PolicyTypeNotFoundException
	require.ErrorAs(
		t,
		err,
		&notFound,
		"expected a typed PolicyTypeNotFoundException from CreateLoadBalancerPolicy, got %v",
		err,
	)

	_, err = client.DescribeLoadBalancerPolicyTypes(ctx, &elbsdk.DescribeLoadBalancerPolicyTypesInput{
		PolicyTypeNames: []string{"NoSuchPolicyType"},
	})
	require.Error(t, err)

	notFound = nil
	require.ErrorAs(t, err, &notFound,
		"expected a typed PolicyTypeNotFoundException from DescribeLoadBalancerPolicyTypes, got %v", err)
}

// Test_SDKRoundTrip_PublicKeyPolicyType_IsAccepted proves that
// "PublicKeyPolicyType" -- a real, built-in Classic ELB policy type returned
// by DescribeLoadBalancerPolicyTypes and used for back-end server
// authentication -- is accepted by CreateLoadBalancerPolicy. It was
// previously missing from the handler's allowlist and so was wrongly
// rejected as an unknown policy type for every real client.
func Test_SDKRoundTrip_PublicKeyPolicyType_IsAccepted(t *testing.T) {
	t.Parallel()

	backend := elb.NewInMemoryBackend("000000000000", rtTestRegion)
	h := elb.NewHandler(backend)
	client := newTestELBClient(t, h)
	ctx := t.Context()

	const lbName = "rt-pubkey-lb"

	_, err := client.CreateLoadBalancer(ctx, &elbsdk.CreateLoadBalancerInput{
		LoadBalancerName:  aws.String(lbName),
		AvailabilityZones: []string{"us-east-1a"},
		Listeners: []types.Listener{
			{Protocol: aws.String("HTTP"), LoadBalancerPort: 80, InstancePort: aws.Int32(8080)},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateLoadBalancerPolicy(ctx, &elbsdk.CreateLoadBalancerPolicyInput{
		LoadBalancerName: aws.String(lbName),
		PolicyName:       aws.String("rt-pubkey-pol"),
		PolicyTypeName:   aws.String("PublicKeyPolicyType"),
		PolicyAttributes: []types.PolicyAttribute{
			{
				AttributeName:  aws.String("PublicKey"),
				AttributeValue: aws.String("-----BEGIN CERTIFICATE-----abcd-----END CERTIFICATE-----"),
			},
		},
	})
	require.NoError(t, err)
}
