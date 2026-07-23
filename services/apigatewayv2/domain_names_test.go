package apigatewayv2_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/services/apigatewayv2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_DomainNames(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	// CreateDomainName.
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123:certificate/abc", EndpointType: "REGIONAL"},
		},
		Tags: map[string]string{"env": "prod"},
	})
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", dn.DomainNameValue)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, "REGIONAL", dn.DomainNameConfigurations[0].EndpointType)

	// GetDomainName.
	got, err := b.GetDomainName("api.example.com")
	require.NoError(t, err)
	assert.Equal(t, "api.example.com", got.DomainNameValue)

	// GetDomainNames.
	all, err := b.GetDomainNames()
	require.NoError(t, err)
	assert.Len(t, all, 1)

	// UpdateDomainName.
	upd, err := b.UpdateDomainName("api.example.com", apigatewayv2.UpdateDomainNameInput{
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123:certificate/xyz", EndpointType: "EDGE"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "EDGE", upd.DomainNameConfigurations[0].EndpointType)

	// DeleteDomainName.
	err = b.DeleteDomainName("api.example.com")
	require.NoError(t, err)

	_, err = b.GetDomainName("api.example.com")
	require.ErrorIs(t, err, apigatewayv2.ErrDomainNameNotFound)
}

func TestDomainNameConfiguration_SecurityPolicy_Defaults(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "tls.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc",
				EndpointType:   "REGIONAL",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)

	cfg := dn.DomainNameConfigurations[0]
	assert.Equal(t, "TLS_1_2", cfg.SecurityPolicy)
	assert.Equal(t, "AVAILABLE", cfg.DomainNameStatus)
	assert.NotEmpty(t, cfg.APIGatewayDomainName)
	assert.NotEmpty(t, cfg.HostedZoneID)
}

func TestDomainNameConfiguration_CustomSecurityPolicy(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "custom-tls.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/xyz",
				EndpointType:   "REGIONAL",
				SecurityPolicy: "TLS_1_0",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, "TLS_1_0", dn.DomainNameConfigurations[0].SecurityPolicy)
}

func TestDomainNameConfiguration_ApiGatewayDomainName_Contains_DomainName(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.mycompany.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{CertificateArn: "arn:aws:acm:us-east-1:123456789012:certificate/abc", EndpointType: "REGIONAL"},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Contains(t, dn.DomainNameConfigurations[0].APIGatewayDomainName, "api.mycompany.com")
}

func TestDomainNameConfiguration_CustomApiGatewayDomainName_Preserved(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()
	customDomain := "d-abc123.execute-api.us-east-1.amazonaws.com"
	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "custom.example.com",
		DomainNameConfigurations: []apigatewayv2.DomainNameConfiguration{
			{
				CertificateArn:       "arn:aws:acm:us-east-1:123456789012:certificate/abc",
				EndpointType:         "REGIONAL",
				APIGatewayDomainName: customDomain,
				HostedZoneID:         "Z1HUB23UULQXV",
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, dn.DomainNameConfigurations, 1)
	assert.Equal(t, customDomain, dn.DomainNameConfigurations[0].APIGatewayDomainName)
	assert.Equal(t, "Z1HUB23UULQXV", dn.DomainNameConfigurations[0].HostedZoneID)
}

// Test_DomainName_ArnAndMutualTLS proves CreateDomainName populates a
// well-formed domainNameArn and round-trips mutualTlsAuthentication, and that
// UpdateDomainName can replace the mTLS configuration. Before this fix
// DomainName had neither field, so both were silently dropped.
func Test_DomainName_ArnAndMutualTLS(t *testing.T) {
	t.Parallel()

	ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account:   "555566667777",
		Region:    "eu-west-1",
		Partition: "aws",
	})

	b := apigatewayv2.NewInMemoryBackend()

	dn, err := b.CreateDomainName(ctx, apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "api.example.com",
		MutualTLSAuthentication: &apigatewayv2.MutualTLSAuthentication{
			TruststoreURI:     "s3://bucket/truststore.pem",
			TruststoreVersion: "v1",
		},
	})
	require.NoError(t, err)

	assert.Equal(t, "arn:aws:apigateway:eu-west-1::/domainnames/api.example.com", dn.DomainNameArn)
	require.NotNil(t, dn.MutualTLSAuthentication)
	assert.Equal(t, "s3://bucket/truststore.pem", dn.MutualTLSAuthentication.TruststoreURI)
	assert.Equal(t, "v1", dn.MutualTLSAuthentication.TruststoreVersion)
	assert.Empty(t, dn.MutualTLSAuthentication.TruststoreWarnings)

	updated, err := b.UpdateDomainName("api.example.com", apigatewayv2.UpdateDomainNameInput{
		MutualTLSAuthentication: &apigatewayv2.MutualTLSAuthentication{
			TruststoreURI:     "s3://bucket/truststore-v2.pem",
			TruststoreVersion: "v2",
		},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.MutualTLSAuthentication)
	assert.Equal(t, "s3://bucket/truststore-v2.pem", updated.MutualTLSAuthentication.TruststoreURI)

	// DomainNameArn is stable across updates.
	assert.Equal(t, dn.DomainNameArn, updated.DomainNameArn)
}

// Test_DomainName_RoutingMode covers DomainName.RoutingMode, which the real
// AWS SDK carries on DomainName/CreateDomainNameInput/UpdateDomainNameInput
// but was entirely absent from gopherstack's shapes, so a caller-supplied
// routingMode was silently dropped on decode and GetDomainName always
// returned "" instead of the AWS default ("API_MAPPING_ONLY").
func Test_DomainName_RoutingMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "defaults_to_api_mapping_only", input: "", want: "API_MAPPING_ONLY"},
		{name: "explicit_routing_rule_only", input: "ROUTING_RULE_ONLY", want: "ROUTING_RULE_ONLY"},
		{
			name: "explicit_routing_rule_then_api_mapping", input: "ROUTING_RULE_THEN_API_MAPPING",
			want: "ROUTING_RULE_THEN_API_MAPPING",
		},
		{name: "rejects_invalid_value", input: "SOMETHING_ELSE", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := apigatewayv2.NewInMemoryBackend()

			dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
				DomainNameValue: tt.name + ".example.com",
				RoutingMode:     tt.input,
			})

			if tt.wantErr {
				require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, dn.RoutingMode)

			got, err := b.GetDomainName(tt.name + ".example.com")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.RoutingMode)
		})
	}
}

// Test_UpdateDomainName_RoutingMode covers updating an existing domain
// name's RoutingMode, and that an invalid value is rejected rather than
// silently applied.
func Test_UpdateDomainName_RoutingMode(t *testing.T) {
	t.Parallel()

	b := apigatewayv2.NewInMemoryBackend()

	dn, err := b.CreateDomainName(context.Background(), apigatewayv2.CreateDomainNameInput{
		DomainNameValue: "routingmode-update.example.com",
	})
	require.NoError(t, err)
	require.Equal(t, "API_MAPPING_ONLY", dn.RoutingMode)

	updated, err := b.UpdateDomainName(dn.DomainNameValue, apigatewayv2.UpdateDomainNameInput{
		RoutingMode: "ROUTING_RULE_ONLY",
	})
	require.NoError(t, err)
	assert.Equal(t, "ROUTING_RULE_ONLY", updated.RoutingMode)

	_, err = b.UpdateDomainName(dn.DomainNameValue, apigatewayv2.UpdateDomainNameInput{RoutingMode: "BOGUS"})
	require.ErrorIs(t, err, apigatewayv2.ErrBadRequest)
}
