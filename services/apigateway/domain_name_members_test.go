package apigateway_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	apigwtypes "github.com/aws/aws-sdk-go-v2/service/apigateway/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestDomainName_PrivateDomainNameID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		idFn    func(actual string) *string
		name    string
		private bool
		wantErr bool
	}{
		{name: "private_matching_id", private: true, idFn: aws.String},
		{name: "private_missing_id", private: true, idFn: func(string) *string { return nil }, wantErr: true},
		{
			name:    "private_wrong_id",
			private: true,
			idFn:    func(string) *string { return aws.String("zzz") },
			wantErr: true,
		},
		{name: "regional_needs_none", idFn: func(string) *string { return nil }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

			endpoint := apigwtypes.EndpointTypeRegional
			if tt.private {
				endpoint = apigwtypes.EndpointTypePrivate
			}

			created, err := client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
				DomainName:            aws.String("d.example.com"),
				EndpointConfiguration: &apigwtypes.EndpointConfiguration{Types: []apigwtypes.EndpointType{endpoint}},
			})
			require.NoError(t, err)
			assert.Equal(t, tt.private, aws.ToString(created.DomainNameId) != "")

			got, err := client.GetDomainName(t.Context(), &apigwsdk.GetDomainNameInput{
				DomainName: aws.String("d.example.com"), DomainNameId: tt.idFn(aws.ToString(created.DomainNameId)),
			})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, aws.ToString(created.DomainNameId), aws.ToString(got.DomainNameId))
		})
	}
}

func TestCreateDomainName_CertificateUploadDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       *string
		name       string
		wantUpload bool
	}{
		{name: "uploaded", body: aws.String("-----BEGIN CERTIFICATE-----"), wantUpload: true},
		{name: "no_upload"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestAPIGatewayClient(t, apigateway.NewHandler(apigateway.NewInMemoryBackend()))

			out, err := client.CreateDomainName(t.Context(), &apigwsdk.CreateDomainNameInput{
				DomainName:      aws.String("c.example.com"),
				CertificateName: aws.String("c"),
				CertificateBody: tt.body,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantUpload, out.CertificateUploadDate != nil)
		})
	}
}
