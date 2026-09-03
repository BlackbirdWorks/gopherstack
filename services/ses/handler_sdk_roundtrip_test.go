package ses_test

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sessdk "github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ses"
)

const rtTestRegion = "us-east-1"

// newTestSESClient stands up the real aws-sdk-go-v2 SES client against an
// httptest server running this package's Handler, wired through the same
// pkgs/service registry/router used in production. Round-tripping through
// the genuine SDK deserializer (rather than string-matching the raw XML
// body) is what proves a response is wire-compatible: unrecognized
// element names are skipped silently by the deserializer rather than
// erroring, so a plausible-looking response can still decode to an empty
// field or slice.
func newTestSESClient(t *testing.T, h *ses.Handler) *sessdk.Client {
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

	return sessdk.NewFromConfig(cfg, func(o *sessdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestSDKRoundTrip_MemberShapeFixes covers two independent decoding bugs
// found by diffing gopherstack's ses XML list tags against the pinned
// SDK's deserializer (ses@v1.37.4): ConfigurationSets and TemplatesMetadata
// are lists of objects (each carrying a Name field, not a bare string), but
// the handler emitted them via the generic chardata <member>name</member>
// shape used elsewhere for real string lists (Identities, DkimTokens, ...).
// A real client always decoded Name as nil for every item.
func TestSDKRoundTrip_MemberShapeFixes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client)
		name string
	}{
		{testListConfigurationSets, "list configuration sets"},
		{testListTemplates, "list templates"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := ses.NewInMemoryBackend()
			h := ses.NewHandler(backend)
			client := newTestSESClient(t, h)

			tc.run(t, backend, client)
		})
	}
}

// testListConfigurationSets: the handler wrapped each entry in
// <member>name</member> chardata, but the real deserializer
// (ses@v1.37.4 deserializers.go:10043) reads types.ConfigurationSet as an
// object with a nested <Name> element.
func testListConfigurationSets(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, backend.CreateConfigurationSet("rt-config-set"))

	out, err := client.ListConfigurationSets(ctx, &sessdk.ListConfigurationSetsInput{})
	require.NoError(t, err)
	require.Len(t, out.ConfigurationSets, 1)
	assert.Equal(t, "rt-config-set", aws.ToString(out.ConfigurationSets[0].Name))
}

// testListTemplates: the handler wrapped each entry in <member>name</member>
// chardata, but the real deserializer (ses@v1.37.4 deserializers.go:14808)
// reads types.TemplateMetadata as an object with a nested <Name> element.
func testListTemplates(t *testing.T, backend *ses.InMemoryBackend, client *sessdk.Client) {
	t.Helper()
	ctx := t.Context()

	require.NoError(t, backend.CreateTemplate(ses.EmailTemplate{
		TemplateName: "rt-template",
		SubjectPart:  "hello",
	}))

	out, err := client.ListTemplates(ctx, &sessdk.ListTemplatesInput{})
	require.NoError(t, err)
	require.Len(t, out.TemplatesMetadata, 1)
	assert.Equal(t, "rt-template", aws.ToString(out.TemplatesMetadata[0].Name))
}

// TestListReceiptRuleSets_Pagination proves ListReceiptRuleSetsInput.NextToken
// (api_op_ListReceiptRuleSets.go) is actually plumbed through: the handler
// previously took no query params at all and always returned every rule set
// in one page.
func TestListReceiptRuleSets_Pagination(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)
	client := newTestSESClient(t, h)
	ctx := t.Context()

	// ListReceiptRuleSetsInput has no MaxItems (real AWS hardcodes the page
	// size at 100 -- see its NextToken doc comment), so proving truncation
	// needs more than 100 rule sets.
	const total = 101
	for i := range total {
		require.NoError(t, backend.CreateReceiptRuleSet(fmt.Sprintf("rs-%03d", i)))
	}

	page1, err := client.ListReceiptRuleSets(ctx, &sessdk.ListReceiptRuleSetsInput{})
	require.NoError(t, err)
	require.Len(t, page1.RuleSets, 100)
	require.NotNil(t, page1.NextToken, "a truncated page must return a NextToken")

	page2, err := client.ListReceiptRuleSets(ctx, &sessdk.ListReceiptRuleSetsInput{
		NextToken: page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.RuleSets, 1)
}

// TestListCustomVerificationEmailTemplates_Pagination proves
// ListCustomVerificationEmailTemplatesInput.MaxResults/NextToken
// (api_op_ListCustomVerificationEmailTemplates.go) are honoured: the
// handler previously took no query params at all and always returned every
// template in one page.
func TestListCustomVerificationEmailTemplates_Pagination(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)
	client := newTestSESClient(t, h)
	ctx := t.Context()

	for _, name := range []string{"tmpl-a", "tmpl-b", "tmpl-c"} {
		require.NoError(t, backend.CreateCustomVerificationEmailTemplate(ses.CustomVerificationEmailTemplate{
			TemplateName:          name,
			FromEmailAddress:      "sender@example.com",
			TemplateSubject:       "Verify",
			TemplateContent:       "<a>{{RedirectUrl}}</a>",
			SuccessRedirectionURL: "https://example.com/success",
			FailureRedirectionURL: "https://example.com/failure",
		}))
	}

	out, err := client.ListCustomVerificationEmailTemplates(ctx, &sessdk.ListCustomVerificationEmailTemplatesInput{
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.CustomVerificationEmailTemplates, 2)
	require.NotNil(t, out.NextToken, "a truncated page must return a NextToken")

	next, err := client.ListCustomVerificationEmailTemplates(ctx, &sessdk.ListCustomVerificationEmailTemplatesInput{
		NextToken: out.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, next.CustomVerificationEmailTemplates, 1)
}

// TestListTemplates_DefaultAndCappedPageSize proves ListTemplatesInput.MaxItems'
// documented default and cap (api_op_ListTemplates.go: "must be at least 1 and
// less than or equal to 100... automatically set to 100... If you do not
// specify a value, 10 is the default page size").
func TestListTemplates_DefaultAndCappedPageSize(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)
	client := newTestSESClient(t, h)
	ctx := t.Context()

	for i := range 105 {
		require.NoError(t, backend.CreateTemplate(ses.EmailTemplate{
			TemplateName: fmt.Sprintf("tmpl-%03d", i),
			SubjectPart:  "hello",
		}))
	}

	byDefault, err := client.ListTemplates(ctx, &sessdk.ListTemplatesInput{})
	require.NoError(t, err)
	assert.Len(t, byDefault.TemplatesMetadata, 10, "an absent MaxItems must default to 10, not sesDefaultMaxItems")

	capped, err := client.ListTemplates(ctx, &sessdk.ListTemplatesInput{MaxItems: aws.Int32(1000)})
	require.NoError(t, err)
	assert.Len(t, capped.TemplatesMetadata, 100, "MaxItems above 100 must be capped at 100, not unlimited")
}

// TestDescribeConfigurationSet_AttributeNames proves
// DescribeConfigurationSetInput.ConfigurationSetAttributeNames
// (api_op_DescribeConfigurationSet.go: "A list of configuration set
// attributes to return") gates which optional sub-objects come back --
// real SES only returns EventDestinations/TrackingOptions/DeliveryOptions/
// ReputationOptions when their name is explicitly requested.
func TestDescribeConfigurationSet_AttributeNames(t *testing.T) {
	t.Parallel()

	backend := ses.NewInMemoryBackend()
	h := ses.NewHandler(backend)
	client := newTestSESClient(t, h)
	ctx := t.Context()

	require.NoError(t, backend.CreateConfigurationSet("attr-cs"))
	require.NoError(t, backend.CreateConfigurationSetTrackingOptions("attr-cs", "track.example.com"))

	none, err := client.DescribeConfigurationSet(ctx, &sessdk.DescribeConfigurationSetInput{
		ConfigurationSetName: aws.String("attr-cs"),
	})
	require.NoError(t, err)
	assert.Nil(t, none.TrackingOptions, "TrackingOptions must be absent when not requested")
	assert.Nil(t, none.ReputationOptions, "ReputationOptions must be absent when not requested")
	assert.Nil(t, none.DeliveryOptions, "DeliveryOptions must be absent when not requested")
	assert.Empty(t, none.EventDestinations, "EventDestinations must be absent when not requested")

	withTracking, err := client.DescribeConfigurationSet(ctx, &sessdk.DescribeConfigurationSetInput{
		ConfigurationSetName: aws.String("attr-cs"),
		ConfigurationSetAttributeNames: []types.ConfigurationSetAttribute{
			types.ConfigurationSetAttributeTrackingOptions,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, withTracking.TrackingOptions)
	assert.Equal(t, "track.example.com", aws.ToString(withTracking.TrackingOptions.CustomRedirectDomain))
	assert.Nil(
		t,
		withTracking.ReputationOptions,
		"ReputationOptions must stay absent when only trackingOptions is requested",
	)
}
