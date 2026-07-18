package ec2

import (
	"errors"
	"fmt"
	"sort"

	"github.com/google/uuid"
)

// trunk_enclave.go implements two small, unrelated families that share a file for
// cohesion with their similarly-small handler counterpart: Trunk Interface associations
// (AssociateTrunkInterface/DisassociateTrunkInterface/DescribeTrunkInterfaceAssociations),
// modeled against existing ENI state, and Enclave Certificate IAM Role associations
// (AssociateEnclaveCertificateIamRole/DisassociateEnclaveCertificateIamRole/
// GetAssociatedEnclaveCertificateIamRoles).

// ---- Errors ----

var (
	// ErrTrunkAssociationNotFound is returned when a trunk interface association ID does not
	// exist.
	ErrTrunkAssociationNotFound = errors.New("InvalidAssociationID.NotFound")

	// ErrEnclaveCertRoleAssociationNotFound is returned when a certificate/role pair has no
	// association to disassociate.
	ErrEnclaveCertRoleAssociationNotFound = errors.New("InvalidParameterValue")

	// ErrTooManyEnclaveCertRoles is returned when a certificate already has the maximum
	// number of associated IAM roles (16, per AWS's documented limit).
	ErrTooManyEnclaveCertRoles = errors.New("LimitExceeded")
)

// maxEnclaveCertRolesPerCertificate is the maximum number of IAM roles that can be
// associated with a single ACM certificate for Nitro Enclaves use.
const maxEnclaveCertRolesPerCertificate = 16

// interfaceProtocolVLAN and interfaceProtocolGRE are the two supported trunk interface
// protocols.
const (
	interfaceProtocolVLAN = "VLAN"
	interfaceProtocolGRE  = "GRE"
)

// ---- Data types ----

// TrunkInterfaceAssociation represents an association between a branch network interface
// and a trunk network interface, allowing multiple logical networks to be multiplexed over
// a single ENI attachment.
type TrunkInterfaceAssociation struct {
	Tags              map[string]string `json:"tags,omitempty"`
	AssociationID     string            `json:"associationId,omitempty"`
	BranchInterfaceID string            `json:"branchInterfaceId,omitempty"`
	TrunkInterfaceID  string            `json:"trunkInterfaceId,omitempty"`
	InterfaceProtocol string            `json:"interfaceProtocol,omitempty"`
	VlanID            int32             `json:"vlanId,omitempty"`
	GreKey            int32             `json:"greKey,omitempty"`
}

// EnclaveCertIamRoleAssociation represents an IAM role granted access to the Amazon S3
// object holding an ACM certificate bundle for use inside a Nitro Enclave.
type EnclaveCertIamRoleAssociation struct {
	CertificateArn          string `json:"certificateArn,omitempty"`
	RoleArn                 string `json:"roleArn,omitempty"`
	CertificateS3BucketName string `json:"certificateS3BucketName,omitempty"`
	CertificateS3ObjectKey  string `json:"certificateS3ObjectKey,omitempty"`
	EncryptionKmsKeyID      string `json:"encryptionKmsKeyId,omitempty"`
}

// ---- Reset ----

// resetTrunkEnclaveMapsLocked re-initialises all maps owned by this file. Must be called
// with b.mu held.
func (b *InMemoryBackend) resetTrunkEnclaveMapsLocked() {
	b.enclaveCertIamRoles = make(map[string][]*EnclaveCertIamRoleAssociation)
}

// ---- Trunk Interface associations ----

// AssociateTrunkInterface associates a branch network interface with a trunk network
// interface. Exactly one of vlanID or greKey must be provided (greater than zero) to select
// the multiplexing protocol.
func (b *InMemoryBackend) AssociateTrunkInterface(
	branchInterfaceID, trunkInterfaceID string, vlanID, greKey int32, tags map[string]string,
) (*TrunkInterfaceAssociation, error) {
	if branchInterfaceID == "" || trunkInterfaceID == "" {
		return nil, fmt.Errorf("%w: BranchInterfaceId and TrunkInterfaceId are required", ErrInvalidParameter)
	}

	if branchInterfaceID == trunkInterfaceID {
		return nil, fmt.Errorf(
			"%w: BranchInterfaceId and TrunkInterfaceId must be different", ErrInvalidParameter,
		)
	}

	protocol, err := trunkInterfaceProtocol(vlanID, greKey)
	if err != nil {
		return nil, err
	}

	b.mu.Lock("AssociateTrunkInterface")
	defer b.mu.Unlock()

	if _, ok := b.networkInterfaces.Get(branchInterfaceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, branchInterfaceID)
	}

	if _, ok := b.networkInterfaces.Get(trunkInterfaceID); !ok {
		return nil, fmt.Errorf("%w: %s", ErrNetworkInterfaceNotFound, trunkInterfaceID)
	}

	assoc := &TrunkInterfaceAssociation{
		AssociationID:     "trunk-assoc-" + uuid.New().String()[:17],
		BranchInterfaceID: branchInterfaceID,
		TrunkInterfaceID:  trunkInterfaceID,
		InterfaceProtocol: protocol,
		VlanID:            vlanID,
		GreKey:            greKey,
		Tags:              copyStringMap(tags),
	}
	b.trunkInterfaceAssociations.Put(assoc)

	return copyTrunkInterfaceAssociation(assoc), nil
}

// trunkInterfaceProtocol determines the trunk interface protocol from the caller-supplied
// VlanId/GreKey, requiring exactly one to be set.
func trunkInterfaceProtocol(vlanID, greKey int32) (string, error) {
	switch {
	case vlanID > 0 && greKey > 0:
		return "", fmt.Errorf("%w: only one of VlanId or GreKey may be specified", ErrInvalidParameter)
	case vlanID > 0:
		return interfaceProtocolVLAN, nil
	case greKey > 0:
		return interfaceProtocolGRE, nil
	default:
		return "", fmt.Errorf("%w: one of VlanId or GreKey is required", ErrInvalidParameter)
	}
}

// DisassociateTrunkInterface removes a trunk interface association.
func (b *InMemoryBackend) DisassociateTrunkInterface(associationID string) error {
	if associationID == "" {
		return fmt.Errorf("%w: AssociationId is required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateTrunkInterface")
	defer b.mu.Unlock()

	if _, ok := b.trunkInterfaceAssociations.Get(associationID); !ok {
		return fmt.Errorf("%w: %s", ErrTrunkAssociationNotFound, associationID)
	}
	b.trunkInterfaceAssociations.Delete(associationID)

	return nil
}

// DescribeTrunkInterfaceAssociations returns trunk interface associations, optionally
// filtered by association ID.
func (b *InMemoryBackend) DescribeTrunkInterfaceAssociations(ids []string) []*TrunkInterfaceAssociation {
	b.mu.RLock("DescribeTrunkInterfaceAssociations")
	defer b.mu.RUnlock()

	filter := make(map[string]bool, len(ids))
	for _, id := range ids {
		filter[id] = true
	}

	out := make([]*TrunkInterfaceAssociation, 0, b.trunkInterfaceAssociations.Len())

	for _, assoc := range b.trunkInterfaceAssociations.All() {
		if len(filter) > 0 && !filter[assoc.AssociationID] {
			continue
		}

		out = append(out, copyTrunkInterfaceAssociation(assoc))
	}

	sort.Slice(out, func(i, j int) bool { return out[i].AssociationID < out[j].AssociationID })

	return out
}

func copyTrunkInterfaceAssociation(a *TrunkInterfaceAssociation) *TrunkInterfaceAssociation {
	cp := *a
	cp.Tags = copyStringMap(a.Tags)

	return &cp
}

// ---- Enclave Certificate IAM Role associations ----

// AssociateEnclaveCertificateIamRole associates an IAM role with an ACM certificate so
// that Nitro Enclaves applications running under that role can access the certificate
// bundle from Amazon S3. Re-associating the same certificateArn/roleArn pair is idempotent
// and returns the existing association.
func (b *InMemoryBackend) AssociateEnclaveCertificateIamRole(
	certificateArn, roleArn string,
) (*EnclaveCertIamRoleAssociation, error) {
	if certificateArn == "" || roleArn == "" {
		return nil, fmt.Errorf("%w: CertificateArn and RoleArn are required", ErrInvalidParameter)
	}

	b.mu.Lock("AssociateEnclaveCertificateIamRole")
	defer b.mu.Unlock()

	for _, assoc := range b.enclaveCertIamRoles[certificateArn] {
		if assoc.RoleArn == roleArn {
			cp := *assoc

			return &cp, nil
		}
	}

	if len(b.enclaveCertIamRoles[certificateArn]) >= maxEnclaveCertRolesPerCertificate {
		return nil, fmt.Errorf("%w: certificate %s already has the maximum of %d associated roles",
			ErrTooManyEnclaveCertRoles, certificateArn, maxEnclaveCertRolesPerCertificate)
	}

	assoc := &EnclaveCertIamRoleAssociation{
		CertificateArn:          certificateArn,
		RoleArn:                 roleArn,
		CertificateS3BucketName: "aws-nitro-enclaves-" + b.AccountID,
		CertificateS3ObjectKey:  roleArn + "/" + certificateArn,
		EncryptionKmsKeyID:      "arn:aws:kms:" + b.Region + ":" + b.AccountID + ":key/" + uuid.New().String(),
	}
	b.enclaveCertIamRoles[certificateArn] = append(b.enclaveCertIamRoles[certificateArn], assoc)

	cp := *assoc

	return &cp, nil
}

// DisassociateEnclaveCertificateIamRole removes the association between an IAM role and an
// ACM certificate.
func (b *InMemoryBackend) DisassociateEnclaveCertificateIamRole(certificateArn, roleArn string) error {
	if certificateArn == "" || roleArn == "" {
		return fmt.Errorf("%w: CertificateArn and RoleArn are required", ErrInvalidParameter)
	}

	b.mu.Lock("DisassociateEnclaveCertificateIamRole")
	defer b.mu.Unlock()

	assocs := b.enclaveCertIamRoles[certificateArn]

	for i, assoc := range assocs {
		if assoc.RoleArn == roleArn {
			b.enclaveCertIamRoles[certificateArn] = append(assocs[:i:i], assocs[i+1:]...)

			return nil
		}
	}

	return fmt.Errorf(
		"%w: no association between %s and %s", ErrEnclaveCertRoleAssociationNotFound, certificateArn, roleArn,
	)
}

// GetAssociatedEnclaveCertificateIamRoles returns all IAM roles associated with an ACM
// certificate. An empty slice (not an error) is returned when the certificate has no
// associated roles.
func (b *InMemoryBackend) GetAssociatedEnclaveCertificateIamRoles(
	certificateArn string,
) ([]*EnclaveCertIamRoleAssociation, error) {
	if certificateArn == "" {
		return nil, fmt.Errorf("%w: CertificateArn is required", ErrInvalidParameter)
	}

	b.mu.RLock("GetAssociatedEnclaveCertificateIamRoles")
	defer b.mu.RUnlock()

	assocs := b.enclaveCertIamRoles[certificateArn]
	out := make([]*EnclaveCertIamRoleAssociation, 0, len(assocs))

	for _, assoc := range assocs {
		cp := *assoc
		out = append(out, &cp)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].RoleArn < out[j].RoleArn })

	return out, nil
}
