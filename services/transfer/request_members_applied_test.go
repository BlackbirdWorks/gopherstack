package transfer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func newMembersClient(t *testing.T) *transfersdk.Client {
	t.Helper()

	backend := transfer.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	return newTestTransferClient(t, transfer.NewHandler(backend))
}

func TestUpdateAgreement_AppliesProfilesRoleDirectories(t *testing.T) {
	t.Parallel()

	dirs := &transfertypes.CustomDirectoriesType{
		FailedFilesDirectory:    aws.String("/b/failed"),
		MdnFilesDirectory:       aws.String("/b/mdn"),
		PayloadFilesDirectory:   aws.String("/b/payload"),
		StatusFilesDirectory:    aws.String("/b/status"),
		TemporaryFilesDirectory: aws.String("/b/tmp"),
	}

	tests := []struct {
		update func(srv, agr *string) *transfersdk.UpdateAgreementInput
		check  func(t *testing.T, a *transfertypes.DescribedAgreement)
		name   string
	}{
		{
			name: "role base dir and profiles",
			update: func(srv, agr *string) *transfersdk.UpdateAgreementInput {
				return &transfersdk.UpdateAgreementInput{
					ServerId: srv, AgreementId: agr,
					AccessRole:       aws.String("arn:aws:iam::123456789012:role/new"),
					BaseDirectory:    aws.String("/new/base"),
					LocalProfileId:   aws.String("p-localnew"),
					PartnerProfileId: aws.String("p-partnernew"),
				}
			},
			check: func(t *testing.T, a *transfertypes.DescribedAgreement) {
				t.Helper()
				assert.Equal(t, "arn:aws:iam::123456789012:role/new", aws.ToString(a.AccessRole))
				assert.Equal(t, "/new/base", aws.ToString(a.BaseDirectory))
				assert.Equal(t, "p-localnew", aws.ToString(a.LocalProfileId))
				assert.Equal(t, "p-partnernew", aws.ToString(a.PartnerProfileId))
			},
		},
		{
			name: "custom directories",
			update: func(srv, agr *string) *transfersdk.UpdateAgreementInput {
				return &transfersdk.UpdateAgreementInput{ServerId: srv, AgreementId: agr, CustomDirectories: dirs}
			},
			check: func(t *testing.T, a *transfertypes.DescribedAgreement) {
				t.Helper()
				require.NotNil(t, a.CustomDirectories)
				assert.Equal(t, "/b/mdn", aws.ToString(a.CustomDirectories.MdnFilesDirectory))
				assert.Equal(t, "/b/tmp", aws.ToString(a.CustomDirectories.TemporaryFilesDirectory))
				assert.Equal(t, "/old/base", aws.ToString(a.BaseDirectory), "omitted members stay")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newMembersClient(t)

			srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
			require.NoError(t, err)

			created, err := client.CreateAgreement(ctx, &transfersdk.CreateAgreementInput{
				ServerId:         srv.ServerId,
				LocalProfileId:   aws.String("p-local"),
				PartnerProfileId: aws.String("p-partner"),
				BaseDirectory:    aws.String("/old/base"),
				AccessRole:       aws.String("arn:aws:iam::123456789012:role/old"),
			})
			require.NoError(t, err)

			_, err = client.UpdateAgreement(ctx, tt.update(srv.ServerId, created.AgreementId))
			require.NoError(t, err)

			desc, err := client.DescribeAgreement(ctx, &transfersdk.DescribeAgreementInput{
				ServerId: srv.ServerId, AgreementId: created.AgreementId,
			})
			require.NoError(t, err)
			tt.check(t, desc.Agreement)
		})
	}
}

func TestCreateAgreement_StoresCustomDirectories(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	client := newMembersClient(t)

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	created, err := client.CreateAgreement(ctx, &transfersdk.CreateAgreementInput{
		ServerId:         srv.ServerId,
		LocalProfileId:   aws.String("p-local"),
		PartnerProfileId: aws.String("p-partner"),
		BaseDirectory:    aws.String("/base"),
		AccessRole:       aws.String("arn:aws:iam::123456789012:role/r"),
		CustomDirectories: &transfertypes.CustomDirectoriesType{
			FailedFilesDirectory:    aws.String("/f"),
			MdnFilesDirectory:       aws.String("/m"),
			PayloadFilesDirectory:   aws.String("/p"),
			StatusFilesDirectory:    aws.String("/s"),
			TemporaryFilesDirectory: aws.String("/t"),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeAgreement(ctx, &transfersdk.DescribeAgreementInput{
		ServerId: srv.ServerId, AgreementId: created.AgreementId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Agreement.CustomDirectories)
	assert.Equal(t, "/f", aws.ToString(desc.Agreement.CustomDirectories.FailedFilesDirectory))
	assert.Equal(t, "/p", aws.ToString(desc.Agreement.CustomDirectories.PayloadFilesDirectory))
}

func TestConnector_EgressConfig(t *testing.T) {
	t.Parallel()

	lattice := func(arn string, port *int32) *transfertypes.ConnectorEgressConfigMemberVpcLattice {
		return &transfertypes.ConnectorEgressConfigMemberVpcLattice{
			Value: transfertypes.ConnectorVpcLatticeEgressConfig{
				ResourceConfigurationArn: aws.String(arn), PortNumber: port,
			},
		}
	}

	updateLattice := func(arn string, port *int32) *transfertypes.UpdateConnectorEgressConfigMemberVpcLattice {
		cfg := transfertypes.UpdateConnectorVpcLatticeEgressConfig{PortNumber: port}
		if arn != "" {
			cfg.ResourceConfigurationArn = aws.String(arn)
		}

		return &transfertypes.UpdateConnectorEgressConfigMemberVpcLattice{Value: cfg}
	}

	tests := []struct {
		create       transfertypes.ConnectorEgressConfig
		update       transfertypes.UpdateConnectorEgressConfig
		name         string
		wantType     transfertypes.ConnectorEgressType
		wantArn      string
		wantPort     int32
		wantNoConfig bool
	}{
		{
			name:         "none is service managed",
			wantType:     transfertypes.ConnectorEgressTypeServiceManaged,
			wantNoConfig: true,
		},
		{
			name:     "create lattice defaults port",
			create:   lattice("arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-1", nil),
			wantType: transfertypes.ConnectorEgressTypeVpcLattice,
			wantArn:  "arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-1",
			wantPort: 22,
		},
		{
			name:   "update replaces lattice",
			create: lattice("arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-1", aws.Int32(2222)),
			update: updateLattice(
				"arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-2",
				aws.Int32(2200),
			),
			wantType: transfertypes.ConnectorEgressTypeVpcLattice,
			wantArn:  "arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-2",
			wantPort: 2200,
		},
		{
			name:     "partial update keeps arn",
			create:   lattice("arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-1", aws.Int32(2222)),
			update:   updateLattice("", aws.Int32(2300)),
			wantType: transfertypes.ConnectorEgressTypeVpcLattice,
			wantArn:  "arn:aws:vpc-lattice:us-east-1:123456789012:resourceconfiguration/rc-1",
			wantPort: 2300,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newMembersClient(t)

			created, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
				Url:          aws.String("sftp://example.com"),
				AccessRole:   aws.String("arn:aws:iam::123456789012:role/r"),
				SftpConfig:   &transfertypes.SftpConnectorConfig{},
				EgressConfig: tt.create,
			})
			require.NoError(t, err)

			if tt.update != nil {
				_, err = client.UpdateConnector(ctx, &transfersdk.UpdateConnectorInput{
					ConnectorId: created.ConnectorId, EgressConfig: tt.update,
				})
				require.NoError(t, err)
			}

			desc, err := client.DescribeConnector(
				ctx,
				&transfersdk.DescribeConnectorInput{ConnectorId: created.ConnectorId},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, desc.Connector.EgressType)
			assert.Equal(t, transfertypes.ConnectorStatusActive, desc.Connector.Status)

			if tt.wantNoConfig {
				assert.Nil(t, desc.Connector.EgressConfig)

				return
			}

			vl, ok := desc.Connector.EgressConfig.(*transfertypes.DescribedConnectorEgressConfigMemberVpcLattice)
			require.True(t, ok)
			assert.Equal(t, tt.wantArn, aws.ToString(vl.Value.ResourceConfigurationArn))
			assert.Equal(t, tt.wantPort, aws.ToInt32(vl.Value.PortNumber))
		})
	}
}

func TestCreateProfile_StoresCertificateIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []string
	}{
		{name: "two certs", ids: []string{"cert-1", "cert-2"}},
		{name: "none", ids: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newMembersClient(t)

			created, err := client.CreateProfile(ctx, &transfersdk.CreateProfileInput{
				As2Id: aws.String("LOCALAS2"), ProfileType: transfertypes.ProfileTypeLocal, CertificateIds: tt.ids,
			})
			require.NoError(t, err)

			desc, err := client.DescribeProfile(ctx, &transfersdk.DescribeProfileInput{ProfileId: created.ProfileId})
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.ids, desc.Profile.CertificateIds)
		})
	}
}

func TestCreateUser_ImportsSshPublicKeyBody(t *testing.T) {
	t.Parallel()

	const body = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl user@host"

	tests := []struct {
		name    string
		body    string
		wantKey int
	}{
		{name: "with key", body: body, wantKey: 1},
		{name: "without key", body: "", wantKey: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := newMembersClient(t)

			srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
			require.NoError(t, err)

			in := &transfersdk.CreateUserInput{
				ServerId: srv.ServerId, UserName: aws.String("alice"),
				Role: aws.String("arn:aws:iam::123456789012:role/r"),
			}
			if tt.body != "" {
				in.SshPublicKeyBody = aws.String(tt.body)
			}

			_, err = client.CreateUser(ctx, in)
			require.NoError(t, err)

			desc, err := client.DescribeUser(ctx, &transfersdk.DescribeUserInput{
				ServerId: srv.ServerId, UserName: aws.String("alice"),
			})
			require.NoError(t, err)
			require.Len(t, desc.User.SshPublicKeys, tt.wantKey)

			if tt.wantKey > 0 {
				assert.Equal(t, body, aws.ToString(desc.User.SshPublicKeys[0].SshPublicKeyBody))
			}
		})
	}
}
