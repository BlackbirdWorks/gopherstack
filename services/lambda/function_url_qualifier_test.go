package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func newURLQualifierBackend(t *testing.T, portStart int) (*lambda.InMemoryBackend, *mockDNSRegistrar) {
	t.Helper()

	pa, err := portalloc.New(portStart, portStart+10)
	require.NoError(t, err)

	b := lambda.NewInMemoryBackend(nil, pa, lambda.DefaultSettings(), "000000000000", "us-east-1")
	closeBackend(t, b)

	dns := &mockDNSRegistrar{}
	lambda.SetDNSRegistrarExported(b, dns)

	require.NoError(t, b.CreateFunction(&lambda.FunctionConfiguration{
		FunctionName: "url-q-fn", PackageType: lambda.PackageTypeImage, ImageURI: "test:latest",
	}))

	_, err = b.PublishVersion("url-q-fn", "v1")
	require.NoError(t, err)

	_, err = b.CreateAlias("url-q-fn", &lambda.CreateAliasInput{Name: "prod", FunctionVersion: "1"})
	require.NoError(t, err)

	return b, dns
}

func TestFunctionURLConfig_QualifierScoped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		qualifier string
	}{
		{name: "alias", qualifier: "prod"},
		{name: "latest_is_unqualified", qualifier: "$LATEST", wantErr: lambda.ErrFunctionAlreadyExists},
		{name: "unknown_alias", qualifier: "nope", wantErr: lambda.ErrFunctionNotFound},
		{name: "version_number_is_not_an_alias", qualifier: "1", wantErr: lambda.ErrFunctionNotFound},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newURLQualifierBackend(t, 21000+i*20)

			_, err := b.CreateFunctionURLConfig(t.Context(), "url-q-fn", "NONE", nil, "")
			require.NoError(t, err)

			cfg, err := b.CreateFunctionURLConfigQualified(t.Context(), "url-q-fn", tt.qualifier, "AWS_IAM", nil, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "AWS_IAM", cfg.AuthType)
			assert.Contains(t, cfg.FunctionArn, ":function:url-q-fn:prod")

			base, err := b.GetFunctionURLConfig("url-q-fn")
			require.NoError(t, err)
			assert.Equal(t, "NONE", base.AuthType, "unqualified URL stays independent")
			assert.NotEqual(t, base.FunctionURL, cfg.FunctionURL)

			got, err := b.GetFunctionURLConfigQualified("url-q-fn", "prod")
			require.NoError(t, err)
			assert.Equal(t, cfg.FunctionURL, got.FunctionURL)

			updated, err := b.UpdateFunctionURLConfigQualified("url-q-fn", "prod", "NONE", nil, "")
			require.NoError(t, err)
			assert.Equal(t, "NONE", updated.AuthType)

			all, _, err := b.ListFunctionURLConfigsForFunction("url-q-fn", "", 0)
			require.NoError(t, err)
			assert.Len(t, all, 2)

			require.NoError(t, b.DeleteFunctionURLConfigQualified("url-q-fn", "prod"))

			_, err = b.GetFunctionURLConfigQualified("url-q-fn", "prod")
			require.ErrorIs(t, err, lambda.ErrFunctionURLNotFound)

			_, err = b.GetFunctionURLConfig("url-q-fn")
			require.NoError(t, err)
		})
	}
}

func TestDeleteFunction_ReleasesFunctionURLListeners(t *testing.T) {
	t.Parallel()

	b, dns := newURLQualifierBackend(t, 21200)

	_, err := b.CreateFunctionURLConfig(t.Context(), "url-q-fn", "NONE", nil, "")
	require.NoError(t, err)

	_, err = b.CreateFunctionURLConfigQualified(t.Context(), "url-q-fn", "prod", "NONE", nil, "")
	require.NoError(t, err)

	dns.mu.Lock()
	registered := len(dns.registered)
	dns.mu.Unlock()
	require.Equal(t, 2, registered)

	require.NoError(t, b.DeleteFunction("url-q-fn"))

	dns.mu.Lock()
	deregistered := len(dns.deregistered)
	dns.mu.Unlock()
	assert.Equal(t, 2, deregistered, "deleting the function deregisters both URL hostnames")

	_, _, err = b.ListFunctionURLConfigsForFunction("url-q-fn", "", 0)
	require.ErrorIs(t, err, lambda.ErrFunctionNotFound)
	assert.Empty(t, b.ListFunctionURLConfigs())
}
