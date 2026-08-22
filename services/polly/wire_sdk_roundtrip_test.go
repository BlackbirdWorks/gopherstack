package polly_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pollysdk "github.com/aws/aws-sdk-go-v2/service/polly"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/polly"
)

// newTestPollySDKClient stands up the real aws-sdk-go-v2 polly client against
// an httptest server running this package's Handler through the same
// pkgs/service registry/router used in production -- so a shape is verified
// by the real client's own deserializer, not gopherstack's own JSON tags.
func newTestPollySDKClient(t *testing.T, h *polly.Handler) *pollysdk.Client {
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
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return pollysdk.NewFromConfig(cfg, func(o *pollysdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestListLexicons_AttributesNested_SDKRoundTrip proves ListLexicons wires
// each entry as types.LexiconDescription{Name, Attributes}, with the
// metadata fields (Alphabet/LanguageCode/LastModified/LexemesCount/
// LexiconArn/Size) nested one level down under a JSON "Attributes" key --
// NOT flattened onto the list item alongside Name, which is the shape
// GetLexicon uses for its separate root-level LexiconAttributes member.
// Confirmed against aws-sdk-go-v2/service/polly/deserializers.go's
// awsRestjson1_deserializeDocumentLexiconDescription (reads only "Attributes"
// and "Name" keys per item) and awsRestjson1_deserializeDocumentLexiconAttributes.
func TestListLexicons_AttributesNested_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := polly.NewInMemoryBackend()
	h := polly.NewHandler(backend)
	client := newTestPollySDKClient(t, h)

	const lexiconName = "roundtrip"
	require.NoError(t, backend.PutLexicon(lexiconName, `<lexicon alphabet="ipa" xml:lang="en-US"></lexicon>`))

	out, err := client.ListLexicons(t.Context(), &pollysdk.ListLexiconsInput{})
	require.NoError(t, err)
	require.Len(t, out.Lexicons, 1)

	entry := out.Lexicons[0]
	assert.Equal(t, lexiconName, aws.ToString(entry.Name))
	require.NotNil(t, entry.Attributes, "Attributes must be nested under the LexiconDescription item, not flattened")
	assert.Equal(t, "ipa", aws.ToString(entry.Attributes.Alphabet))
	assert.NotEmpty(t, string(entry.Attributes.LanguageCode))
}
