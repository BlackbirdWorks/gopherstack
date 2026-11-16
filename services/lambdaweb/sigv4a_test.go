package lambdaweb_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/smithy-go"
	smithyauth "github.com/aws/smithy-go/auth"
	smithyendpoints "github.com/aws/smithy-go/endpoints"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

type lambdaEndpoint struct{ uri string }

func (e lambdaEndpoint) ResolveEndpoint(
	_ context.Context, _ eventbridge.EndpointParameters,
) (smithyendpoints.Endpoint, error) {
	u, err := url.Parse(e.uri)
	if err != nil {
		return smithyendpoints.Endpoint{}, err
	}

	var props smithy.Properties

	var sp smithy.Properties

	smithyhttp.SetSigV4ASigningName(&sp, "lambda")
	smithyhttp.SetSigV4ASigningRegions(&sp, []string{"*"})
	smithyauth.SetAuthOptions(&props, []*smithyauth.Option{{SchemeID: smithyauth.SchemeIDSigV4A, SignerProperties: sp}})

	return smithyendpoints.Endpoint{URI: *u, Properties: props}, nil
}

// signedMultiRegionRequest returns a request signed by the SDK's own SigV4a signer for service lambda.
func signedMultiRegionRequest(t *testing.T, secret string) *http.Request {
	t.Helper()

	var got *http.Request

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got = r.Clone(r.Context())
		got.Host = r.Host
		got.Body = io.NopCloser(bytes.NewReader(body))
		sum := sha256.Sum256(body)
		got.Header.Set("X-Amz-Content-Sha256", hex.EncodeToString(sum[:]))
		_, _ = w.Write([]byte(`{"FailedEntryCount":0,"Entries":[]}`))
	}))
	t.Cleanup(srv.Close)

	opts := eventbridge.Options{
		Region:             "us-east-1",
		Credentials:        credentials.NewStaticCredentialsProvider("AKIDEXAMPLE", secret, ""),
		EndpointResolverV2: lambdaEndpoint{uri: srv.URL},
	}

	_, err := eventbridge.New(opts).PutEvents(t.Context(), &eventbridge.PutEventsInput{
		EndpointId: aws.String("abc123.xyz"),
		Entries: []ebtypes.PutEventsRequestEntry{
			{Source: aws.String("s"), DetailType: aws.String("d"), Detail: aws.String("{}")},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, got)

	return got
}

func TestMultiRegionSigV4aSignature(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tamper func(*http.Request)
		name   string
		secret string
		want   bool
	}{
		{name: "valid", secret: "test", want: true},
		{name: "wrong secret", secret: "other"},
		{name: "tampered", secret: "test", tamper: func(r *http.Request) { r.Header.Set("X-Amz-Target", "x") }},
		{name: "header shape only", secret: "test", tamper: func(r *http.Request) {
			r.Header.Set(
				"Authorization",
				"AWS4-ECDSA-P256-SHA256 Credential=a/20260101/lambda/aws4_request, SignedHeaders=host, Signature=00",
			)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := signedMultiRegionRequest(t, tt.secret)
			if tt.tamper != nil {
				tt.tamper(r)
			}

			b := lambdaweb.NewInMemoryBackend(testAccountID)
			t.Cleanup(b.Close)

			assert.Equal(t, tt.want, b.ValidMultiRegionSignature(r))
		})
	}
}
