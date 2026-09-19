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

// TestUpdate_OmittedMembersPreserveState locks in the zeroguard census fix
// (gopherstack-101r follow-up): an Update op's optional *string member must
// be applied only when the caller sends it. Before the fix these fields
// decoded as plain strings (sometimes gated by a "Set*" flag itself derived
// from `field != ""`, the same bug one layer up), so a second update that
// simply omitted a field silently blanked it instead of leaving the stored
// value alone. Each case creates a resource, sets a field, sends a second
// update that omits it and asserts the earlier value survived, then sends
// an explicit empty string and asserts it is applied (not treated as
// omitted).
func TestUpdate_OmittedMembersPreserveState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "access_role_homedir_policy", run: testAccessFieldsPreserved},
		{name: "user_role_homedir_policy", run: testUserFieldsPreserved},
		{name: "certificate_description", run: testCertificateDescriptionPreserved},
		{name: "connector_url_access_role_logging_role_security_policy", run: testConnectorFieldsPreserved},
		{name: "web_app_access_endpoint", run: testWebAppAccessEndpointPreserved},
		{name: "server_fields", run: testServerFieldsPreserved},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testAccessFieldsPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	const externalID = "S-1-1-11-1111111111-1111111111-1111111111-1111"

	_, err = client.CreateAccess(ctx, &transfersdk.CreateAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: aws.String(externalID),
		Role:       aws.String("arn:aws:iam::000000000000:role/first"),
	})
	require.NoError(t, err)

	_, err = client.UpdateAccess(ctx, &transfersdk.UpdateAccessInput{
		ServerId:      srv.ServerId,
		ExternalId:    aws.String(externalID),
		Role:          aws.String("arn:aws:iam::000000000000:role/second"),
		HomeDirectory: aws.String("/bucket/home"),
		Policy:        aws.String(`{"Version":"2012-10-17"}`),
	})
	require.NoError(t, err)

	// Omits all three -- must survive.
	_, err = client.UpdateAccess(ctx, &transfersdk.UpdateAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: aws.String(externalID),
	})
	require.NoError(t, err)

	desc, err := client.DescribeAccess(ctx, &transfersdk.DescribeAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: aws.String(externalID),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(desc.Access.Role),
		"Role must survive an update that omits it")
	assert.Equal(t, "/bucket/home", aws.ToString(desc.Access.HomeDirectory),
		"HomeDirectory must survive an update that omits it")
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, aws.ToString(desc.Access.Policy),
		"Policy must survive an update that omits it")

	// Explicit empty Policy clears it, others untouched.
	_, err = client.UpdateAccess(ctx, &transfersdk.UpdateAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: aws.String(externalID),
		Policy:     aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeAccess(ctx, &transfersdk.DescribeAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: aws.String(externalID),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Access.Policy))
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(cleared.Access.Role))
}

func testUserFieldsPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	created, err := client.CreateUser(ctx, &transfersdk.CreateUserInput{
		ServerId: srv.ServerId,
		UserName: aws.String("omit-user"),
		Role:     aws.String("arn:aws:iam::000000000000:role/first"),
	})
	require.NoError(t, err)

	_, err = client.UpdateUser(ctx, &transfersdk.UpdateUserInput{
		ServerId:      srv.ServerId,
		UserName:      created.UserName,
		Role:          aws.String("arn:aws:iam::000000000000:role/second"),
		HomeDirectory: aws.String("/bucket/home"),
		Policy:        aws.String(`{"Version":"2012-10-17"}`),
	})
	require.NoError(t, err)

	// Omits all three -- must survive.
	_, err = client.UpdateUser(ctx, &transfersdk.UpdateUserInput{
		ServerId: srv.ServerId,
		UserName: created.UserName,
	})
	require.NoError(t, err)

	desc, err := client.DescribeUser(ctx, &transfersdk.DescribeUserInput{
		ServerId: srv.ServerId,
		UserName: created.UserName,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(desc.User.Role),
		"Role must survive an update that omits it")
	assert.Equal(t, "/bucket/home", aws.ToString(desc.User.HomeDirectory),
		"HomeDirectory must survive an update that omits it")
	assert.JSONEq(t, `{"Version":"2012-10-17"}`, aws.ToString(desc.User.Policy),
		"Policy must survive an update that omits it")

	// Explicit empty Policy clears it, others untouched.
	_, err = client.UpdateUser(ctx, &transfersdk.UpdateUserInput{
		ServerId: srv.ServerId,
		UserName: created.UserName,
		Policy:   aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeUser(ctx, &transfersdk.DescribeUserInput{
		ServerId: srv.ServerId,
		UserName: created.UserName,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.User.Policy))
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(cleared.User.Role))
}

func testCertificateDescriptionPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))

	imported, err := client.ImportCertificate(ctx, &transfersdk.ImportCertificateInput{
		Usage:       transfertypes.CertificateUsageTypeSigning,
		Certificate: aws.String(testCertPEM),
		Description: aws.String("first description"),
	})
	require.NoError(t, err)

	// Omits Description -- must survive (ActiveDate is unrelated but exercises
	// the same update call).
	_, err = client.UpdateCertificate(ctx, &transfersdk.UpdateCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)

	desc, err := client.DescribeCertificate(ctx, &transfersdk.DescribeCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(desc.Certificate.Description),
		"Description must survive an update that omits it")

	// Explicit empty Description clears it.
	_, err = client.UpdateCertificate(ctx, &transfersdk.UpdateCertificateInput{
		CertificateId: imported.CertificateId,
		Description:   aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeCertificate(ctx, &transfersdk.DescribeCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Certificate.Description))
}

func testConnectorFieldsPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))

	created, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://first.example.com"),
		AccessRole: aws.String("arn:aws:iam::000000000000:role/first"),
		SftpConfig: &transfertypes.SftpConnectorConfig{
			UserSecretId: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
			TrustedHostKeys: []string{
				"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test-key",
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateConnector(ctx, &transfersdk.UpdateConnectorInput{
		ConnectorId:        created.ConnectorId,
		Url:                aws.String("sftp://second.example.com"),
		AccessRole:         aws.String("arn:aws:iam::000000000000:role/second"),
		LoggingRole:        aws.String("arn:aws:iam::000000000000:role/logging"),
		SecurityPolicyName: aws.String("TransferSFTPConnectorSecurityPolicy-2024-03"),
	})
	require.NoError(t, err)

	// Omits all four -- must survive.
	_, err = client.UpdateConnector(ctx, &transfersdk.UpdateConnectorInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)

	desc, err := client.DescribeConnector(ctx, &transfersdk.DescribeConnectorInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)
	assert.Equal(t, "sftp://second.example.com", aws.ToString(desc.Connector.Url),
		"Url must survive an update that omits it")
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(desc.Connector.AccessRole),
		"AccessRole must survive an update that omits it")
	assert.Equal(t, "arn:aws:iam::000000000000:role/logging", aws.ToString(desc.Connector.LoggingRole),
		"LoggingRole must survive an update that omits it")
	assert.Equal(
		t,
		"TransferSFTPConnectorSecurityPolicy-2024-03",
		aws.ToString(desc.Connector.SecurityPolicyName),
		"SecurityPolicyName must survive an update that omits it",
	)

	// Explicit empty Url clears it, others untouched.
	_, err = client.UpdateConnector(ctx, &transfersdk.UpdateConnectorInput{
		ConnectorId: created.ConnectorId,
		Url:         aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeConnector(ctx, &transfersdk.DescribeConnectorInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Connector.Url))
	assert.Equal(t, "arn:aws:iam::000000000000:role/second", aws.ToString(cleared.Connector.AccessRole))
}

func testWebAppAccessEndpointPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))

	created, err := client.CreateWebApp(ctx, &transfersdk.CreateWebAppInput{
		IdentityProviderDetails: &transfertypes.WebAppIdentityProviderDetailsMemberIdentityCenterConfig{
			Value: transfertypes.IdentityCenterConfig{
				InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
				Role:        aws.String("arn:aws:iam::000000000000:role/access"),
			},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateWebApp(ctx, &transfersdk.UpdateWebAppInput{
		WebAppId:       created.WebAppId,
		AccessEndpoint: aws.String("https://first.example.com"),
	})
	require.NoError(t, err)

	// Omits AccessEndpoint but touches WebAppUnits -- AccessEndpoint must survive.
	_, err = client.UpdateWebApp(ctx, &transfersdk.UpdateWebAppInput{
		WebAppId:    created.WebAppId,
		WebAppUnits: &transfertypes.WebAppUnitsMemberProvisioned{Value: 2},
	})
	require.NoError(t, err)

	desc, err := client.DescribeWebApp(ctx, &transfersdk.DescribeWebAppInput{WebAppId: created.WebAppId})
	require.NoError(t, err)
	assert.Equal(t, "https://first.example.com", aws.ToString(desc.WebApp.AccessEndpoint),
		"AccessEndpoint must survive an update that omits it")

	// Explicit empty AccessEndpoint clears it.
	_, err = client.UpdateWebApp(ctx, &transfersdk.UpdateWebAppInput{
		WebAppId:       created.WebAppId,
		AccessEndpoint: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeWebApp(ctx, &transfersdk.DescribeWebAppInput{WebAppId: created.WebAppId})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.WebApp.AccessEndpoint))
}

func testServerFieldsPreserved(t *testing.T) {
	t.Helper()

	ctx := t.Context()
	backend := newTestTransferBackend()
	client := newTestTransferClient(t, transfer.NewHandler(backend))

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	_, err = client.UpdateServer(ctx, &transfersdk.UpdateServerInput{
		ServerId:                      srv.ServerId,
		Certificate:                   aws.String("cert-first"),
		HostKey:                       aws.String("ssh-rsa AAAAB3NzaC1yc2EAAAADAQABfake"),
		LoggingRole:                   aws.String("arn:aws:iam::000000000000:role/logging-first"),
		PreAuthenticationLoginBanner:  aws.String("pre-first"),
		PostAuthenticationLoginBanner: aws.String("post-first"),
		SecurityPolicyName:            aws.String("TransferSecurityPolicy-2024-01"),
	})
	require.NoError(t, err)

	// Omits every field above but still touches the call (empty Protocols is a
	// no-op) -- all six must survive.
	_, err = client.UpdateServer(ctx, &transfersdk.UpdateServerInput{
		ServerId: srv.ServerId,
	})
	require.NoError(t, err)

	desc, err := client.DescribeServer(ctx, &transfersdk.DescribeServerInput{ServerId: srv.ServerId})
	require.NoError(t, err)
	assert.Equal(t, "cert-first", aws.ToString(desc.Server.Certificate),
		"Certificate must survive an update that omits it")
	assert.Equal(t, "arn:aws:iam::000000000000:role/logging-first", aws.ToString(desc.Server.LoggingRole),
		"LoggingRole must survive an update that omits it")
	assert.Equal(t, "pre-first", aws.ToString(desc.Server.PreAuthenticationLoginBanner),
		"PreAuthenticationLoginBanner must survive an update that omits it")
	assert.Equal(t, "post-first", aws.ToString(desc.Server.PostAuthenticationLoginBanner),
		"PostAuthenticationLoginBanner must survive an update that omits it")
	assert.Equal(
		t,
		"TransferSecurityPolicy-2024-01",
		aws.ToString(desc.Server.SecurityPolicyName),
		"SecurityPolicyName must survive an update that omits it",
	)

	// HostKey is never echoed by DescribeServer (real AWS never returns key
	// material either -- only HostKeyFingerprint), so check backend state
	// directly.
	internal, err := backend.DescribeServer(aws.ToString(srv.ServerId))
	require.NoError(t, err)
	assert.Equal(t, "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABfake", internal.HostKey,
		"HostKey must survive an update that omits it")

	// Explicit empty LoggingRole clears it, others untouched.
	_, err = client.UpdateServer(ctx, &transfersdk.UpdateServerInput{
		ServerId:    srv.ServerId,
		LoggingRole: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeServer(ctx, &transfersdk.DescribeServerInput{ServerId: srv.ServerId})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Server.LoggingRole))
	assert.Equal(t, "cert-first", aws.ToString(cleared.Server.Certificate))
}
