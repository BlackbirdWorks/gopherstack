package eks

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"fmt"
	"math/big"
	"sort"
	"time"
)

// gopherstack-lruaw (2026-09-11): implements the five EKS Hybrid Nodes
// CertificateAuthority ops added in eks@v1.98.0 (previously entirely
// unimplemented -- see PARITY.md gaps). Every wire fact below is verified
// against aws-sdk-go-v2/service/eks@v1.98.0's api_op_*CertificateAuthorit*.go,
// types/types.go, types/enums.go, validators.go, serializers.go, and
// deserializers.go -- not against this handler's own output.

const (
	caCreatedByCustomer   = "CUSTOMER"
	caActivatedByCustomer = "CUSTOMER"

	caSigningStatusNotUsed    = "NOT_USED"
	caSigningStatusActivating = "ACTIVATING"
	caSigningStatusInUse      = "IN_USE"

	caDistributionStatusInProgress = "IN_PROGRESS"
	caDistributionStatusComplete   = "COMPLETE"
	caDistributionStatusDeleting   = "DELETING"

	// updateTypeCertificateAuthority is types.UpdateTypeCertificateAuthorityUpdate
	// (eks@v1.98.0 types/enums.go:1332) -- the single UpdateType real EKS uses
	// for all three of Create/Activate/DeleteCertificateAuthority's async
	// Update records.
	updateTypeCertificateAuthority = "CertificateAuthorityUpdate"

	// updateParamCertificateAuthorityID/updateParamSigningStatus are real
	// types.UpdateParamType enum values (types/enums.go:1213-1214) -- not
	// invented strings.
	updateParamCertificateAuthorityID = "CertificateAuthorityId"
	updateParamSigningStatus          = "SigningStatus"

	// maxCertificateAuthoritiesPerCluster is a fixed structural rule stated
	// directly in api_op_CreateCertificateAuthority.go's doc comment ("Each
	// cluster can have at most two certificate authorities at a time"), not
	// a numbered entry on the Service Quotas page -- same treatment as
	// capabilities.go's one-capability-per-type rule (see limits.go's doc
	// comment). It is not part of resourceLimits/WithResourceLimits because
	// it is not an AWS-adjustable per-account quota.
	maxCertificateAuthoritiesPerCluster = 2

	// certificateAuthorityDistributionDelay/certificateAuthorityActivationDelay
	// are the async delays before a new CA's distributionStatus reaches
	// COMPLETE and before an activating CA's signingStatus reaches IN_USE,
	// matching the 100ms scale used by every other CREATING/InProgress
	// transition in this service (clusterTransitionDelay et al.).
	certificateAuthorityDistributionDelay = 100 * time.Millisecond
	certificateAuthorityActivationDelay   = 100 * time.Millisecond

	// certificateAuthorityValidity is the certificate's NotBefore..NotAfter
	// span. Real EKS documents no fixed lifetime for its internally managed
	// CA certificates; 10 years matches this repo's other self-signed CA
	// generation (services/acmpca/crypto.go's selfSignCA).
	certificateAuthorityValidity = 10 * 365 * 24 * time.Hour

	caSerialBitLen = 128
)

// generateCertificateAuthorityCert creates a real self-signed ECDSA P-256 CA
// certificate. Real EKS generates and manages this key material entirely
// server-side: neither CreateCertificateAuthorityInput nor
// ActivateCertificateAuthorityInput accepts a client-supplied key, CSR, or
// certificate (verified: neither api_op_*CertificateAuthorit*.go nor
// types/types.go declares any such member anywhere in this op family), so
// there is no real client-observable wire feature for a CSR/upload flow to
// emulate -- only the resulting Data/Validity fields ever reach the wire.
// The private key is discarded immediately after signing: nothing in this
// op family ever uses it again (Activate promotes an existing CA by
// reference, it never signs anything new).
func generateCertificateAuthorityCert(clusterName string, notBefore time.Time) (string, time.Time, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), cryptorand.Reader)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("generate certificate authority key: %w", err)
	}

	serial, err := cryptorand.Int(cryptorand.Reader, new(big.Int).Lsh(big.NewInt(1), caSerialBitLen))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("generate certificate authority serial: %w", err)
	}

	notAfter := notBefore.Add(certificateAuthorityValidity)

	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "eks-hybrid-nodes-ca." + clusterName},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}

	der, err := x509.CreateCertificate(cryptorand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create certificate authority certificate: %w", err)
	}

	return base64.StdEncoding.EncodeToString(der), notAfter, nil
}

func deepCopyCertificateAuthority(ca *CertificateAuthority) *CertificateAuthority {
	cp := *ca

	if ca.ActivatedAt != nil {
		t := *ca.ActivatedAt
		cp.ActivatedAt = &t
	}

	return &cp
}

// scheduleCertificateAuthorityDistribution schedules the async
// IN_PROGRESS -> COMPLETE distributionStatus transition after
// CreateCertificateAuthority, matching CreateCertificateAuthority's doc
// comment ("Amazon EKS then distributes the successor CA ... you can track
// this through the CA's distributionStatus").
func (b *InMemoryBackend) scheduleCertificateAuthorityDistribution(clusterName, id string) {
	b.work.After("CertificateAuthorityDistribution", certificateAuthorityDistributionDelay, func() {
		b.mu.Lock("CertificateAuthorityDistribution-async")
		defer b.mu.Unlock()

		key := certificateAuthorityKey(clusterName, id)
		if ca, ok := b.certificateAuthorities.Get(key); ok && ca.DistributionStatus == caDistributionStatusInProgress {
			ca.DistributionStatus = caDistributionStatusComplete
		}
	})
}

// scheduleCertificateAuthorityActivation schedules the async
// ACTIVATING -> IN_USE signingStatus transition after
// ActivateCertificateAuthority, matching its doc comment ("Amazon EKS
// promotes it to be the cluster's signer").
func (b *InMemoryBackend) scheduleCertificateAuthorityActivation(clusterName, id string) {
	b.work.After("CertificateAuthorityActivation", certificateAuthorityActivationDelay, func() {
		b.mu.Lock("CertificateAuthorityActivation-async")
		defer b.mu.Unlock()

		key := certificateAuthorityKey(clusterName, id)
		if ca, ok := b.certificateAuthorities.Get(key); ok && ca.SigningStatus == caSigningStatusActivating {
			ca.SigningStatus = caSigningStatusInUse
		}
	})
}

// newCertificateAuthorityUpdate builds the async Update record every
// Create/Activate/DeleteCertificateAuthority response carries under its
// "update" key (types.Update, deserializers.go's
// awsRestjson1_deserializeOpDocument<Op>Output "update" case). All three
// share types.UpdateTypeCertificateAuthorityUpdate.
func newCertificateAuthorityUpdate(clusterName, id string, params []UpdateParam) *Update {
	return &Update{
		ID:          stableID(clusterName + "/ca-update/" + id + "/" + time.Now().String()),
		ClusterName: clusterName,
		Status:      statusInProgress,
		Type:        updateTypeCertificateAuthority,
		Params:      params,
		CreatedAt:   time.Now().UTC(),
	}
}

// CreateCertificateAuthority appends a successor certificate authority to a
// cluster's trust bundle, beginning a CA rotation.
func (b *InMemoryBackend) CreateCertificateAuthority(
	clusterName string,
) (*CertificateAuthority, *Update, error) {
	b.mu.Lock("CreateCertificateAuthority")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	if n := len(b.certificateAuthoritiesByCluster.Get(clusterName)); n >= maxCertificateAuthoritiesPerCluster {
		return nil, nil, resourceLimitExceededErr(
			"certificate authorities per cluster", maxCertificateAuthoritiesPerCluster,
		)
	}

	now := time.Now().UTC()

	id := randomHex16()

	data, notAfter, err := generateCertificateAuthorityCert(clusterName, now)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrValidation, err)
	}

	ca := &CertificateAuthority{
		CreatedAt:          now,
		NotBefore:          now,
		NotAfter:           notAfter,
		ID:                 id,
		ClusterName:        clusterName,
		Data:               data,
		CreatedBy:          caCreatedByCustomer,
		DistributionStatus: caDistributionStatusInProgress,
		SigningStatus:      caSigningStatusNotUsed,
	}
	b.certificateAuthorities.Put(ca)
	b.scheduleCertificateAuthorityDistribution(clusterName, id)

	upd := newCertificateAuthorityUpdate(clusterName, id, []UpdateParam{
		{Type: updateParamCertificateAuthorityID, Value: id},
	})
	b.storeUpdateLocked(upd)
	b.scheduleUpdateTransition(clusterName, upd.ID)

	return deepCopyCertificateAuthority(ca), upd.clone(), nil
}

// ActivateCertificateAuthority promotes a successor certificate authority to
// be the cluster's signer, retiring the previously active one.
func (b *InMemoryBackend) ActivateCertificateAuthority(
	clusterName, id string,
) (*CertificateAuthority, *Update, error) {
	b.mu.Lock("ActivateCertificateAuthority")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ca, ok := b.certificateAuthorities.Get(certificateAuthorityKey(clusterName, id))
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: certificate authority %s not found in cluster %s",
			ErrNotFound,
			id,
			clusterName,
		)
	}

	// ActivateCertificateAuthority's own deserializer (deserializers.go) has
	// no ResourceInUseException/ResourceLimitExceededException case -- only
	// InvalidParameterException/ResourceNotFoundException/ServerException/
	// ServiceUnavailableException -- so a CA that is not eligible for
	// activation is ErrValidation, matching api_op_ActivateCertificateAuthority.go's
	// doc comment: "must already be present on the cluster and fully
	// distributed (its distributionStatus must be COMPLETE)".
	if ca.SigningStatus != caSigningStatusNotUsed {
		return nil, nil, fmt.Errorf(
			"%w: certificate authority %s is not eligible for activation (signingStatus %s)",
			ErrValidation, id, ca.SigningStatus,
		)
	}

	if ca.DistributionStatus != caDistributionStatusComplete {
		return nil, nil, fmt.Errorf(
			"%w: certificate authority %s cannot be activated until distribution is complete (distributionStatus %s)",
			ErrValidation, id, ca.DistributionStatus,
		)
	}

	now := time.Now().UTC()
	ca.SigningStatus = caSigningStatusActivating
	ca.ActivatedAt = &now
	ca.ActivatedBy = caActivatedByCustomer
	b.scheduleCertificateAuthorityActivation(clusterName, id)

	// Retire the outgoing signer, if any -- "the outgoing CA is retired
	// (NOT_USED)" per the op's doc comment. RollbackAvailable is set true
	// ("For a limited period after activation, CA rollback is available")
	// but never expired: no TTL sweep exists for it, the same disclosed
	// simplification as idempotency.go's ClientRequestToken 24h window.
	for _, other := range b.certificateAuthoritiesByCluster.Get(clusterName) {
		if other.ID != id && other.SigningStatus == caSigningStatusInUse {
			other.SigningStatus = caSigningStatusNotUsed
			other.RollbackAvailable = true
		}
	}

	upd := newCertificateAuthorityUpdate(clusterName, id, []UpdateParam{
		{Type: updateParamCertificateAuthorityID, Value: id},
		{Type: updateParamSigningStatus, Value: caSigningStatusInUse},
	})
	b.storeUpdateLocked(upd)
	b.scheduleUpdateTransition(clusterName, upd.ID)

	return deepCopyCertificateAuthority(ca), upd.clone(), nil
}

// DeleteCertificateAuthority removes a certificate authority from a
// cluster's trust bundle. It cannot be the CA currently signing certificates.
func (b *InMemoryBackend) DeleteCertificateAuthority(
	clusterName, id string,
) (*CertificateAuthority, *Update, error) {
	b.mu.Lock("DeleteCertificateAuthority")
	defer b.mu.Unlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ca, ok := b.certificateAuthorities.Get(certificateAuthorityKey(clusterName, id))
	if !ok {
		return nil, nil, fmt.Errorf(
			"%w: certificate authority %s not found in cluster %s",
			ErrNotFound,
			id,
			clusterName,
		)
	}

	if ca.SigningStatus == caSigningStatusInUse {
		return nil, nil, fmt.Errorf(
			"%w: certificate authority %s is currently signing certificates for cluster %s; "+
				"activate a successor before deleting it",
			ErrAlreadyExists, id, clusterName,
		)
	}

	cp := deepCopyCertificateAuthority(ca)
	b.certificateAuthorities.Delete(certificateAuthorityKey(clusterName, id))
	cp.DistributionStatus = caDistributionStatusDeleting

	upd := newCertificateAuthorityUpdate(clusterName, id, []UpdateParam{
		{Type: updateParamCertificateAuthorityID, Value: id},
	})
	b.storeUpdateLocked(upd)
	b.scheduleUpdateTransition(clusterName, upd.ID)

	return cp, upd.clone(), nil
}

// DescribeCertificateAuthority returns detailed information about a single
// certificate authority.
func (b *InMemoryBackend) DescribeCertificateAuthority(clusterName, id string) (*CertificateAuthority, error) {
	b.mu.RLock("DescribeCertificateAuthority")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	ca, ok := b.certificateAuthorities.Get(certificateAuthorityKey(clusterName, id))
	if !ok {
		return nil, fmt.Errorf("%w: certificate authority %s not found in cluster %s", ErrNotFound, id, clusterName)
	}

	return deepCopyCertificateAuthority(ca), nil
}

// ListCertificateAuthorities returns every certificate authority in a
// cluster, sorted by ID for deterministic responses.
func (b *InMemoryBackend) ListCertificateAuthorities(clusterName string) ([]*CertificateAuthority, error) {
	b.mu.RLock("ListCertificateAuthorities")
	defer b.mu.RUnlock()

	if _, ok := b.clusters.Get(clusterName); !ok {
		return nil, fmt.Errorf("%w: cluster %s not found", ErrNotFound, clusterName)
	}

	items := b.certificateAuthoritiesByCluster.Get(clusterName)
	list := make([]*CertificateAuthority, len(items))

	for i, ca := range items {
		list[i] = deepCopyCertificateAuthority(ca)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].ID < list[j].ID })

	return list, nil
}
