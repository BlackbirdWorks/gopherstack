package ec2_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ec2"
)

func newTrunkTestENIs(t *testing.T, b *ec2.InMemoryBackend) (string, string) {
	t.Helper()

	branchENI, err := b.CreateNetworkInterface("subnet-default", "branch")
	require.NoError(t, err)

	trunkENI, err := b.CreateNetworkInterface("subnet-default", "trunk")
	require.NoError(t, err)

	return branchENI.ID, trunkENI.ID
}

func TestBackend_TrunkInterface_AssociateDescribeDisassociate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	branch, trunk := newTrunkTestENIs(t, b)

	assoc, err := b.AssociateTrunkInterface(branch, trunk, 5, 0, map[string]string{"Name": "trunk1"})
	require.NoError(t, err)
	assert.Contains(t, assoc.AssociationID, "trunk-assoc-")
	assert.Equal(t, "VLAN", assoc.InterfaceProtocol)
	assert.Equal(t, int32(5), assoc.VlanID)

	assocs := b.DescribeTrunkInterfaceAssociations([]string{assoc.AssociationID})
	require.Len(t, assocs, 1)
	assert.Equal(t, branch, assocs[0].BranchInterfaceID)

	require.NoError(t, b.DisassociateTrunkInterface(assoc.AssociationID))
	assert.Empty(t, b.DescribeTrunkInterfaceAssociations([]string{assoc.AssociationID}))
}

func TestBackend_TrunkInterface_GreProtocol(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	branch, trunk := newTrunkTestENIs(t, b)

	assoc, err := b.AssociateTrunkInterface(branch, trunk, 0, 7, nil)
	require.NoError(t, err)
	assert.Equal(t, "GRE", assoc.InterfaceProtocol)
	assert.Equal(t, int32(7), assoc.GreKey)
}

func TestBackend_TrunkInterface_RequiresExactlyOneProtocolSelector(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	branch, trunk := newTrunkTestENIs(t, b)

	_, err := b.AssociateTrunkInterface(branch, trunk, 0, 0, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)

	_, err = b.AssociateTrunkInterface(branch, trunk, 5, 7, nil)
	require.ErrorIs(t, err, ec2.ErrInvalidParameter)
}

func TestBackend_TrunkInterface_UnknownENIFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	_, trunk := newTrunkTestENIs(t, b)

	_, err := b.AssociateTrunkInterface("eni-doesnotexist", trunk, 5, 0, nil)
	require.ErrorIs(t, err, ec2.ErrNetworkInterfaceNotFound)
}

func TestBackend_TrunkInterface_DisassociateNotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.DisassociateTrunkInterface("trunk-assoc-doesnotexist")
	require.ErrorIs(t, err, ec2.ErrTrunkAssociationNotFound)
}

func TestBackend_EnclaveCertIamRole_AssociateGetDisassociate(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/abc"
	roleArn := "arn:aws:iam::123456789012:role/enclave-role"

	assoc, err := b.AssociateEnclaveCertificateIamRole(certArn, roleArn)
	require.NoError(t, err)
	assert.NotEmpty(t, assoc.CertificateS3BucketName)
	assert.Contains(t, assoc.CertificateS3ObjectKey, roleArn)
	assert.Contains(t, assoc.EncryptionKmsKeyID, "arn:aws:kms:")

	roles, err := b.GetAssociatedEnclaveCertificateIamRoles(certArn)
	require.NoError(t, err)
	require.Len(t, roles, 1)
	assert.Equal(t, roleArn, roles[0].RoleArn)

	// Re-associating the same pair is idempotent.
	assoc2, err := b.AssociateEnclaveCertificateIamRole(certArn, roleArn)
	require.NoError(t, err)
	assert.Equal(t, assoc.CertificateS3ObjectKey, assoc2.CertificateS3ObjectKey)

	roles, err = b.GetAssociatedEnclaveCertificateIamRoles(certArn)
	require.NoError(t, err)
	require.Len(t, roles, 1)

	require.NoError(t, b.DisassociateEnclaveCertificateIamRole(certArn, roleArn))

	roles, err = b.GetAssociatedEnclaveCertificateIamRoles(certArn)
	require.NoError(t, err)
	assert.Empty(t, roles)
}

func TestBackend_EnclaveCertIamRole_GetUnknownCertReturnsEmpty(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	roles, err := b.GetAssociatedEnclaveCertificateIamRoles("arn:aws:acm:us-east-1:123456789012:certificate/none")
	require.NoError(t, err)
	assert.Empty(t, roles)
}

func TestBackend_EnclaveCertIamRole_DisassociateNotFound(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")

	err := b.DisassociateEnclaveCertificateIamRole("arn:cert", "arn:role")
	require.ErrorIs(t, err, ec2.ErrEnclaveCertRoleAssociationNotFound)
}

func TestBackend_EnclaveCertIamRole_TooManyRolesFails(t *testing.T) {
	t.Parallel()

	b := ec2.NewInMemoryBackend("123456789012", "us-east-1")
	certArn := "arn:aws:acm:us-east-1:123456789012:certificate/many"

	for i := range 16 {
		roleArn := "arn:aws:iam::123456789012:role/role" + string(rune('a'+i))
		_, err := b.AssociateEnclaveCertificateIamRole(certArn, roleArn)
		require.NoError(t, err)
	}

	_, err := b.AssociateEnclaveCertificateIamRole(certArn, "arn:aws:iam::123456789012:role/one-too-many")
	require.ErrorIs(t, err, ec2.ErrTooManyEnclaveCertRoles)
}
