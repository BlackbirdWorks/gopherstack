package transfer_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	transfersdk "github.com/aws/aws-sdk-go-v2/service/transfer"
	transfertypes "github.com/aws/aws-sdk-go-v2/service/transfer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func newTestTransferBackend() *transfer.InMemoryBackend {
	return transfer.NewInMemoryBackend(context.Background(), "000000000000", transferTagsRTRegion)
}

// TestSlice32Transfer_AccessLifecycle drives Create/Describe/List/Update/DeleteAccess.
func TestSlice32Transfer_AccessLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	created, err := client.CreateAccess(ctx, &transfersdk.CreateAccessInput{
		ServerId:      srv.ServerId,
		ExternalId:    aws.String("S-1-1-11-1111111111-1111111111-1111111111-1111"),
		Role:          aws.String("arn:aws:iam::000000000000:role/access"),
		HomeDirectory: aws.String("/bucket/home"),
		PosixProfile: &transfertypes.PosixProfile{
			Uid: aws.Int64(1000),
			Gid: aws.Int64(1000),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(srv.ServerId), aws.ToString(created.ServerId))

	desc, err := client.DescribeAccess(ctx, &transfersdk.DescribeAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: created.ExternalId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Access)
	assert.Equal(t, "/bucket/home", aws.ToString(desc.Access.HomeDirectory))
	assert.Equal(t, "arn:aws:iam::000000000000:role/access", aws.ToString(desc.Access.Role))
	require.NotNil(t, desc.Access.PosixProfile)
	assert.EqualValues(t, 1000, aws.ToInt64(desc.Access.PosixProfile.Uid))

	listed, err := client.ListAccesses(ctx, &transfersdk.ListAccessesInput{ServerId: srv.ServerId})
	require.NoError(t, err)
	require.Len(t, listed.Accesses, 1)
	assert.Equal(t, aws.ToString(created.ExternalId), aws.ToString(listed.Accesses[0].ExternalId))

	updated, err := client.UpdateAccess(ctx, &transfersdk.UpdateAccessInput{
		ServerId:      srv.ServerId,
		ExternalId:    created.ExternalId,
		HomeDirectory: aws.String("/bucket/newhome"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.ExternalId), aws.ToString(updated.ExternalId))

	desc, err = client.DescribeAccess(ctx, &transfersdk.DescribeAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: created.ExternalId,
	})
	require.NoError(t, err)
	assert.Equal(t, "/bucket/newhome", aws.ToString(desc.Access.HomeDirectory))

	_, err = client.DeleteAccess(ctx, &transfersdk.DeleteAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: created.ExternalId,
	})
	require.NoError(t, err)

	_, err = client.DescribeAccess(ctx, &transfersdk.DescribeAccessInput{
		ServerId:   srv.ServerId,
		ExternalId: created.ExternalId,
	})
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_AgreementLifecycle drives Update/ListAgreements/DeleteAgreement.
// ListAgreements asserts ServerId is present on every list item -- this test
// caught and locked a real bug: handleListAgreements previously omitted
// ServerId from each summary entry even though DescribeAgreement (and real
// types.ListedAgreement, transfer@v1.75.4/types/types.go:1843) both carry it.
func TestSlice32Transfer_AgreementLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	local, err := client.CreateProfile(ctx, &transfersdk.CreateProfileInput{
		As2Id:       aws.String("LOCALAS2"),
		ProfileType: transfertypes.ProfileTypeLocal,
	})
	require.NoError(t, err)

	partner, err := client.CreateProfile(ctx, &transfersdk.CreateProfileInput{
		As2Id:       aws.String("PARTNERAS2"),
		ProfileType: transfertypes.ProfileTypePartner,
	})
	require.NoError(t, err)

	created, err := client.CreateAgreement(ctx, &transfersdk.CreateAgreementInput{
		ServerId:         srv.ServerId,
		LocalProfileId:   local.ProfileId,
		PartnerProfileId: partner.ProfileId,
		AccessRole:       aws.String("arn:aws:iam::000000000000:role/access"),
		Description:      aws.String("initial"),
	})
	require.NoError(t, err)

	listed, err := client.ListAgreements(
		ctx,
		&transfersdk.ListAgreementsInput{ServerId: srv.ServerId},
	)
	require.NoError(t, err)
	require.Len(t, listed.Agreements, 1)
	assert.Equal(
		t,
		aws.ToString(created.AgreementId),
		aws.ToString(listed.Agreements[0].AgreementId),
	)
	assert.Equal(t, aws.ToString(srv.ServerId), aws.ToString(listed.Agreements[0].ServerId))
	assert.Equal(t, "initial", aws.ToString(listed.Agreements[0].Description))

	updated, err := client.UpdateAgreement(ctx, &transfersdk.UpdateAgreementInput{
		ServerId:    srv.ServerId,
		AgreementId: created.AgreementId,
		Description: aws.String("updated"),
		Status:      transfertypes.AgreementStatusTypeInactive,
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.AgreementId), aws.ToString(updated.AgreementId))

	desc, err := client.DescribeAgreement(ctx, &transfersdk.DescribeAgreementInput{
		ServerId:    srv.ServerId,
		AgreementId: created.AgreementId,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.Agreement.Description))
	assert.Equal(t, transfertypes.AgreementStatusTypeInactive, desc.Agreement.Status)

	_, err = client.DeleteAgreement(ctx, &transfersdk.DeleteAgreementInput{
		ServerId:    srv.ServerId,
		AgreementId: created.AgreementId,
	})
	require.NoError(t, err)

	_, err = client.DescribeAgreement(ctx, &transfersdk.DescribeAgreementInput{
		ServerId:    srv.ServerId,
		AgreementId: created.AgreementId,
	})
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_CertificateLifecycle drives Describe/Update/DeleteCertificate.
func TestSlice32Transfer_CertificateLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	imported, err := client.ImportCertificate(ctx, &transfersdk.ImportCertificateInput{
		Usage:       transfertypes.CertificateUsageTypeSigning,
		Certificate: aws.String(testCertPEM),
		Description: aws.String("initial"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeCertificate(ctx, &transfersdk.DescribeCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.Certificate)
	assert.Equal(t, "initial", aws.ToString(desc.Certificate.Description))
	assert.Equal(t, transfertypes.CertificateUsageTypeSigning, desc.Certificate.Usage)

	updated, err := client.UpdateCertificate(ctx, &transfersdk.UpdateCertificateInput{
		CertificateId: imported.CertificateId,
		Description:   aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(imported.CertificateId), aws.ToString(updated.CertificateId))

	desc, err = client.DescribeCertificate(ctx, &transfersdk.DescribeCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.Certificate.Description))

	_, err = client.DeleteCertificate(ctx, &transfersdk.DeleteCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.NoError(t, err)

	_, err = client.DescribeCertificate(ctx, &transfersdk.DescribeCertificateInput{
		CertificateId: imported.CertificateId,
	})
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_ConnectorLifecycle drives Describe/List/UpdateConnector,
// TestConnection, StartDirectoryListing, StartRemoteDelete, StartRemoteMove,
// and DeleteConnector.
func TestSlice32Transfer_ConnectorLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	created, err := client.CreateConnector(ctx, &transfersdk.CreateConnectorInput{
		Url:        aws.String("sftp://example.com"),
		AccessRole: aws.String("arn:aws:iam::000000000000:role/access"),
		SftpConfig: &transfertypes.SftpConnectorConfig{
			UserSecretId: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
			TrustedHostKeys: []string{
				"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test-key",
			},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeConnector(ctx, &transfersdk.DescribeConnectorInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)
	assert.Equal(t, "sftp://example.com", aws.ToString(desc.Connector.Url))

	listed, err := client.ListConnectors(ctx, &transfersdk.ListConnectorsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Connectors, 1)
	assert.Equal(
		t,
		aws.ToString(created.ConnectorId),
		aws.ToString(listed.Connectors[0].ConnectorId),
	)

	updated, err := client.UpdateConnector(ctx, &transfersdk.UpdateConnectorInput{
		ConnectorId: created.ConnectorId,
		Url:         aws.String("sftp://updated.example.com"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.ConnectorId), aws.ToString(updated.ConnectorId))

	desc, err = client.DescribeConnector(ctx, &transfersdk.DescribeConnectorInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)
	assert.Equal(t, "sftp://updated.example.com", aws.ToString(desc.Connector.Url))

	testOut, err := client.TestConnection(ctx, &transfersdk.TestConnectionInput{
		ConnectorId: created.ConnectorId,
	})
	require.NoError(t, err)
	assert.Equal(t, "OK", aws.ToString(testOut.Status))
	assert.Equal(t, aws.ToString(created.ConnectorId), aws.ToString(testOut.ConnectorId))

	listing, err := client.StartDirectoryListing(ctx, &transfersdk.StartDirectoryListingInput{
		ConnectorId:         created.ConnectorId,
		OutputDirectoryPath: aws.String("/out"),
		RemoteDirectoryPath: aws.String("/remote"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(listing.ListingId))
	assert.Contains(t, aws.ToString(listing.OutputFileName), aws.ToString(created.ConnectorId))

	del, err := client.StartRemoteDelete(ctx, &transfersdk.StartRemoteDeleteInput{
		ConnectorId: created.ConnectorId,
		DeletePath:  aws.String("/remote/file.txt"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(del.DeleteId))

	mv, err := client.StartRemoteMove(ctx, &transfersdk.StartRemoteMoveInput{
		ConnectorId: created.ConnectorId,
		SourcePath:  aws.String("/remote/a.txt"),
		TargetPath:  aws.String("/remote/b.txt"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(mv.MoveId))

	_, err = client.DeleteConnector(
		ctx,
		&transfersdk.DeleteConnectorInput{ConnectorId: created.ConnectorId},
	)
	require.NoError(t, err)

	_, err = client.DescribeConnector(
		ctx,
		&transfersdk.DescribeConnectorInput{ConnectorId: created.ConnectorId},
	)
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_HostKeyLifecycle drives Describe/Update/DeleteHostKey.
func TestSlice32Transfer_HostKeyLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	imported, err := client.ImportHostKey(ctx, &transfersdk.ImportHostKeyInput{
		ServerId: srv.ServerId,
		HostKeyBody: aws.String(
			"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test-key",
		),
		Description: aws.String("initial"),
	})
	require.NoError(t, err)

	desc, err := client.DescribeHostKey(ctx, &transfersdk.DescribeHostKeyInput{
		ServerId:  srv.ServerId,
		HostKeyId: imported.HostKeyId,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.HostKey)
	assert.Equal(t, "initial", aws.ToString(desc.HostKey.Description))
	assert.NotEmpty(t, aws.ToString(desc.HostKey.HostKeyFingerprint))

	updated, err := client.UpdateHostKey(ctx, &transfersdk.UpdateHostKeyInput{
		ServerId:    srv.ServerId,
		HostKeyId:   imported.HostKeyId,
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(imported.HostKeyId), aws.ToString(updated.HostKeyId))

	desc, err = client.DescribeHostKey(ctx, &transfersdk.DescribeHostKeyInput{
		ServerId:  srv.ServerId,
		HostKeyId: imported.HostKeyId,
	})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(desc.HostKey.Description))

	_, err = client.DeleteHostKey(ctx, &transfersdk.DeleteHostKeyInput{
		ServerId:  srv.ServerId,
		HostKeyId: imported.HostKeyId,
	})
	require.NoError(t, err)

	_, err = client.DescribeHostKey(ctx, &transfersdk.DescribeHostKeyInput{
		ServerId:  srv.ServerId,
		HostKeyId: imported.HostKeyId,
	})
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_ProfileLifecycle drives ListProfiles/UpdateProfile/DeleteProfile.
func TestSlice32Transfer_ProfileLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	created, err := client.CreateProfile(ctx, &transfersdk.CreateProfileInput{
		As2Id:       aws.String("AS2ID"),
		ProfileType: transfertypes.ProfileTypeLocal,
	})
	require.NoError(t, err)

	listed, err := client.ListProfiles(ctx, &transfersdk.ListProfilesInput{
		ProfileType: transfertypes.ProfileTypeLocal,
	})
	require.NoError(t, err)
	require.Len(t, listed.Profiles, 1)
	assert.Equal(t, aws.ToString(created.ProfileId), aws.ToString(listed.Profiles[0].ProfileId))
	assert.Equal(t, "AS2ID", aws.ToString(listed.Profiles[0].As2Id))

	updated, err := client.UpdateProfile(ctx, &transfersdk.UpdateProfileInput{
		ProfileId:      created.ProfileId,
		CertificateIds: []string{"cert-abc"},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.ProfileId), aws.ToString(updated.ProfileId))

	desc, err := client.DescribeProfile(
		ctx,
		&transfersdk.DescribeProfileInput{ProfileId: created.ProfileId},
	)
	require.NoError(t, err)
	assert.Equal(t, []string{"cert-abc"}, desc.Profile.CertificateIds)

	_, err = client.DeleteProfile(
		ctx,
		&transfersdk.DeleteProfileInput{ProfileId: created.ProfileId},
	)
	require.NoError(t, err)

	_, err = client.DescribeProfile(
		ctx,
		&transfersdk.DescribeProfileInput{ProfileId: created.ProfileId},
	)
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_SshPublicKeyLifecycle drives ImportSshPublicKey and
// DeleteSshPublicKey.
func TestSlice32Transfer_SshPublicKeyLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	user, err := client.CreateUser(ctx, &transfersdk.CreateUserInput{
		ServerId: srv.ServerId,
		UserName: aws.String("keyuser"),
		Role:     aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	imported, err := client.ImportSshPublicKey(ctx, &transfersdk.ImportSshPublicKeyInput{
		ServerId: srv.ServerId,
		UserName: user.UserName,
		SshPublicKeyBody: aws.String(
			"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl test-key",
		),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(srv.ServerId), aws.ToString(imported.ServerId))
	assert.Equal(t, "keyuser", aws.ToString(imported.UserName))
	assert.NotEmpty(t, aws.ToString(imported.SshPublicKeyId))

	_, err = client.DeleteSshPublicKey(ctx, &transfersdk.DeleteSshPublicKeyInput{
		ServerId:       srv.ServerId,
		UserName:       user.UserName,
		SshPublicKeyId: imported.SshPublicKeyId,
	})
	require.NoError(t, err)

	_, err = client.DeleteSshPublicKey(ctx, &transfersdk.DeleteSshPublicKeyInput{
		ServerId:       srv.ServerId,
		UserName:       user.UserName,
		SshPublicKeyId: imported.SshPublicKeyId,
	})
	require.Error(t, err, "deleting an already-deleted key must fail, not silently succeed")
}

// TestSlice32Transfer_WebAppLifecycle drives Describe/List/UpdateWebApp,
// DeleteWebAppCustomization and DeleteWebApp.
func TestSlice32Transfer_WebAppLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	created, err := client.CreateWebApp(ctx, &transfersdk.CreateWebAppInput{
		IdentityProviderDetails: &transfertypes.WebAppIdentityProviderDetailsMemberIdentityCenterConfig{
			Value: transfertypes.IdentityCenterConfig{
				InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890"),
				Role:        aws.String("arn:aws:iam::000000000000:role/access"),
			},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListWebApps(ctx, &transfersdk.ListWebAppsInput{})
	require.NoError(t, err)
	require.Len(t, listed.WebApps, 1)
	assert.Equal(t, aws.ToString(created.WebAppId), aws.ToString(listed.WebApps[0].WebAppId))

	updated, err := client.UpdateWebApp(ctx, &transfersdk.UpdateWebAppInput{
		WebAppId:       created.WebAppId,
		AccessEndpoint: aws.String("https://custom.example.com"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.WebAppId), aws.ToString(updated.WebAppId))

	desc, err := client.DescribeWebApp(
		ctx,
		&transfersdk.DescribeWebAppInput{WebAppId: created.WebAppId},
	)
	require.NoError(t, err)
	assert.Equal(t, "https://custom.example.com", aws.ToString(desc.WebApp.AccessEndpoint))

	_, err = client.UpdateWebAppCustomization(ctx, &transfersdk.UpdateWebAppCustomizationInput{
		WebAppId: created.WebAppId,
		Title:    aws.String("My App"),
	})
	require.NoError(t, err)

	_, err = client.DeleteWebAppCustomization(ctx, &transfersdk.DeleteWebAppCustomizationInput{
		WebAppId: created.WebAppId,
	})
	require.NoError(t, err)

	custom, err := client.DescribeWebAppCustomization(
		ctx,
		&transfersdk.DescribeWebAppCustomizationInput{
			WebAppId: created.WebAppId,
		},
	)
	require.NoError(t, err)
	assert.Empty(
		t,
		aws.ToString(custom.WebAppCustomization.Title),
		"DeleteWebAppCustomization must clear the customization",
	)

	_, err = client.DeleteWebApp(ctx, &transfersdk.DeleteWebAppInput{WebAppId: created.WebAppId})
	require.NoError(t, err)

	_, err = client.DescribeWebApp(
		ctx,
		&transfersdk.DescribeWebAppInput{WebAppId: created.WebAppId},
	)
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_WorkflowLifecycle drives ListWorkflows and DeleteWorkflow.
func TestSlice32Transfer_WorkflowLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	created, err := client.CreateWorkflow(ctx, &transfersdk.CreateWorkflowInput{
		Description: aws.String("my workflow"),
		Steps:       []transfertypes.WorkflowStep{},
	})
	require.NoError(t, err)

	listed, err := client.ListWorkflows(ctx, &transfersdk.ListWorkflowsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Workflows, 1)
	assert.Equal(t, aws.ToString(created.WorkflowId), aws.ToString(listed.Workflows[0].WorkflowId))
	assert.Equal(t, "my workflow", aws.ToString(listed.Workflows[0].Description))

	_, err = client.DeleteWorkflow(
		ctx,
		&transfersdk.DeleteWorkflowInput{WorkflowId: created.WorkflowId},
	)
	require.NoError(t, err)

	_, err = client.DescribeWorkflow(
		ctx,
		&transfersdk.DescribeWorkflowInput{WorkflowId: created.WorkflowId},
	)
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_ListExecutions seeds an execution through the backend's
// exported (but not wire-reachable -- there is no real CreateExecution
// operation; AWS creates these automatically on a real file event) seam and
// proves ListExecutions decodes it through the real client.
func TestSlice32Transfer_ListExecutions(t *testing.T) {
	t.Parallel()

	backend := newTestTransferBackend()
	client := newTestTransferClient(t, transfer.NewHandler(backend))
	ctx := t.Context()

	created, err := client.CreateWorkflow(
		ctx,
		&transfersdk.CreateWorkflowInput{Steps: []transfertypes.WorkflowStep{}},
	)
	require.NoError(t, err)

	workflowID := aws.ToString(created.WorkflowId)

	exec, err := backend.CreateExecution(workflowID)
	require.NoError(t, err)

	listed, err := client.ListExecutions(
		ctx,
		&transfersdk.ListExecutionsInput{WorkflowId: created.WorkflowId},
	)
	require.NoError(t, err)
	require.Len(t, listed.Executions, 1)
	assert.Equal(t, exec.ExecutionID, aws.ToString(listed.Executions[0].ExecutionId))
	assert.Equal(t, transfertypes.ExecutionStatusInProgress, listed.Executions[0].Status)
}

// TestSlice32Transfer_ServerLifecycle drives StartServer, StopServer and
// UpdateServer. State transitions are asynchronous (100ms delay) so this uses
// require.Eventually rather than a sleep.
func TestSlice32Transfer_ServerLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	_, err = client.StartServer(ctx, &transfersdk.StartServerInput{ServerId: srv.ServerId})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeServer(
			ctx,
			&transfersdk.DescribeServerInput{ServerId: srv.ServerId},
		)

		return descErr == nil && desc.Server.State == transfertypes.StateOnline
	}, 2*time.Second, 10*time.Millisecond, "server must reach ONLINE after StartServer")

	_, err = client.StopServer(ctx, &transfersdk.StopServerInput{ServerId: srv.ServerId})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		desc, descErr := client.DescribeServer(
			ctx,
			&transfersdk.DescribeServerInput{ServerId: srv.ServerId},
		)

		return descErr == nil && desc.Server.State == transfertypes.StateOffline
	}, 2*time.Second, 10*time.Millisecond, "server must reach OFFLINE after StopServer")

	updated, err := client.UpdateServer(ctx, &transfersdk.UpdateServerInput{
		ServerId:  srv.ServerId,
		Protocols: []transfertypes.Protocol{transfertypes.ProtocolFtps},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(srv.ServerId), aws.ToString(updated.ServerId))

	desc, err := client.DescribeServer(
		ctx,
		&transfersdk.DescribeServerInput{ServerId: srv.ServerId},
	)
	require.NoError(t, err)
	assert.Equal(t, []transfertypes.Protocol{transfertypes.ProtocolFtps}, desc.Server.Protocols)
}

// TestSlice32Transfer_TestIdentityProvider drives TestIdentityProvider for
// both a known and an unknown user.
func TestSlice32Transfer_TestIdentityProvider(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	_, err = client.CreateUser(ctx, &transfersdk.CreateUserInput{
		ServerId: srv.ServerId,
		UserName: aws.String("idpuser"),
		Role:     aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	ok, err := client.TestIdentityProvider(ctx, &transfersdk.TestIdentityProviderInput{
		ServerId: srv.ServerId,
		UserName: aws.String("idpuser"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 200, ok.StatusCode)

	unauthorized, err := client.TestIdentityProvider(ctx, &transfersdk.TestIdentityProviderInput{
		ServerId: srv.ServerId,
		UserName: aws.String("no-such-user"),
	})
	require.NoError(t, err)
	assert.EqualValues(t, 401, unauthorized.StatusCode)
	assert.Equal(t, "user not found", aws.ToString(unauthorized.Message))
}

// TestSlice32Transfer_SecurityPolicies drives DescribeSecurityPolicy and
// ListSecurityPolicies against the static built-in catalog.
func TestSlice32Transfer_SecurityPolicies(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	desc, err := client.DescribeSecurityPolicy(ctx, &transfersdk.DescribeSecurityPolicyInput{
		SecurityPolicyName: aws.String("TransferSecurityPolicy-2024-01"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.SecurityPolicy)
	assert.Equal(
		t,
		"TransferSecurityPolicy-2024-01",
		aws.ToString(desc.SecurityPolicy.SecurityPolicyName),
	)
	assert.NotEmpty(t, desc.SecurityPolicy.SshCiphers)
	assert.NotEmpty(t, desc.SecurityPolicy.Protocols)

	listed, err := client.ListSecurityPolicies(ctx, &transfersdk.ListSecurityPoliciesInput{})
	require.NoError(t, err)
	assert.Contains(t, listed.SecurityPolicyNames, "TransferSecurityPolicy-2024-01")

	_, err = client.DescribeSecurityPolicy(ctx, &transfersdk.DescribeSecurityPolicyInput{
		SecurityPolicyName: aws.String("no-such-policy"),
	})
	require.Error(t, err)

	var notFound *transfertypes.ResourceNotFoundException
	require.ErrorAs(t, err, &notFound)
}

// TestSlice32Transfer_UpdateUser drives UpdateUser.
func TestSlice32Transfer_UpdateUser(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	created, err := client.CreateUser(ctx, &transfersdk.CreateUserInput{
		ServerId: srv.ServerId,
		UserName: aws.String("updateuser"),
		Role:     aws.String("arn:aws:iam::000000000000:role/access"),
	})
	require.NoError(t, err)

	updated, err := client.UpdateUser(ctx, &transfersdk.UpdateUserInput{
		ServerId:      srv.ServerId,
		UserName:      created.UserName,
		HomeDirectory: aws.String("/bucket/newhome"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(srv.ServerId), aws.ToString(updated.ServerId))
	assert.Equal(t, "updateuser", aws.ToString(updated.UserName))

	desc, err := client.DescribeUser(ctx, &transfersdk.DescribeUserInput{
		ServerId: srv.ServerId,
		UserName: created.UserName,
	})
	require.NoError(t, err)
	assert.Equal(t, "/bucket/newhome", aws.ToString(desc.User.HomeDirectory))
}

// TestSlice32Transfer_ResourceTags drives TagResource and UntagResource
// against a real agreement ARN (ListTagsForResource is already
// typed-covered by TestCreateOpsWithTags_RoundTrip).
func TestSlice32Transfer_ResourceTags(t *testing.T) {
	t.Parallel()

	client := newTestTransferClient(t, transfer.NewHandler(newTestTransferBackend()))
	ctx := t.Context()

	srv, err := client.CreateServer(ctx, &transfersdk.CreateServerInput{})
	require.NoError(t, err)

	desc, err := client.DescribeServer(
		ctx,
		&transfersdk.DescribeServerInput{ServerId: srv.ServerId},
	)
	require.NoError(t, err)

	resourceARN := aws.ToString(desc.Server.Arn)

	_, err = client.TagResource(ctx, &transfersdk.TagResourceInput{
		Arn:  aws.String(resourceARN),
		Tags: []transfertypes.Tag{{Key: aws.String("team"), Value: aws.String("data")}},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(
		ctx,
		&transfersdk.ListTagsForResourceInput{Arn: aws.String(resourceARN)},
	)
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "team", aws.ToString(got.Tags[0].Key))

	_, err = client.UntagResource(ctx, &transfersdk.UntagResourceInput{
		Arn:     aws.String(resourceARN),
		TagKeys: []string{"team"},
	})
	require.NoError(t, err)

	got, err = client.ListTagsForResource(
		ctx,
		&transfersdk.ListTagsForResourceInput{Arn: aws.String(resourceARN)},
	)
	require.NoError(t, err)
	assert.Empty(t, got.Tags)
}
