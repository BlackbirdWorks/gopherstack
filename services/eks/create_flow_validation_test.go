package eks_test

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDescribeCluster_CertificateAuthorityIsPEM(t *testing.T) {
	t.Parallel()

	client := newTestEKSClient(t, newRealClientHandler(t))
	createTestCluster(t, client, "ca-cluster")

	out, err := client.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("ca-cluster")})
	require.NoError(t, err)
	require.NotNil(t, out.Cluster.CertificateAuthority)

	raw, err := base64.StdEncoding.DecodeString(aws.ToString(out.Cluster.CertificateAuthority.Data))
	require.NoError(t, err)

	block, _ := pem.Decode(raw)
	require.NotNil(t, block, "certificateAuthority.data must be base64 PEM")

	_, err = x509.ParseCertificate(block.Bytes)
	require.NoError(t, err)
}

func TestCreateAccessEntry_NodeTypesNeedRolePrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		principal string
		typ       string
		wantErr   bool
	}{
		{name: "standard user", principal: "arn:aws:iam::123456789012:user/u", typ: "STANDARD"},
		{name: "default type user", principal: "arn:aws:iam::123456789012:user/u"},
		{name: "ec2 linux role", principal: "arn:aws:iam::123456789012:role/r", typ: "EC2_LINUX"},
		{name: "ec2 linux user", principal: "arn:aws:iam::123456789012:user/u", typ: "EC2_LINUX", wantErr: true},
		{name: "fargate user", principal: "arn:aws:iam::123456789012:user/u", typ: "FARGATE_LINUX", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			createTestCluster(t, client, "ae")

			in := &ekssdk.CreateAccessEntryInput{
				ClusterName: aws.String("ae"), PrincipalArn: aws.String(tt.principal),
			}
			if tt.typ != "" {
				in.Type = aws.String(tt.typ)
			}

			_, err := client.CreateAccessEntry(t.Context(), in)
			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterException")

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestAssociateIdentityProviderConfig_IssuerMustBeHTTPS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		issuer  string
		wantErr bool
	}{
		{name: "https", issuer: "https://idp.example.com"},
		{name: "http", issuer: "http://idp.example.com", wantErr: true},
		{name: "bare host", issuer: "idp.example.com", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEKSClient(t, newRealClientHandler(t))
			createTestCluster(t, client, "idp")

			_, err := client.AssociateIdentityProviderConfig(t.Context(), &ekssdk.AssociateIdentityProviderConfigInput{
				ClusterName: aws.String("idp"),
				Oidc: &ekstypes.OidcIdentityProviderConfigRequest{
					IdentityProviderConfigName: aws.String("o"),
					IssuerUrl:                  aws.String(tt.issuer),
					ClientId:                   aws.String("c"),
				},
			})
			if tt.wantErr {
				require.ErrorContains(t, err, "InvalidParameterException")

				return
			}

			assert.NoError(t, err)
		})
	}
}
