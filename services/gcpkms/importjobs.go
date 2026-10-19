package gcpkms

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"fmt"

	"cloud.google.com/go/kms/apiv1/kmspb"
	"google.golang.org/protobuf/proto"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func cloneJob(j *kmspb.ImportJob) *kmspb.ImportJob {
	out, _ := proto.Clone(j).(*kmspb.ImportJob)

	return out
}

func wrappingBits(m kmspb.ImportJob_ImportMethod) (int, bool) {
	switch m {
	case kmspb.ImportJob_RSA_OAEP_3072_SHA256_AES_256, kmspb.ImportJob_RSA_OAEP_3072_SHA256:
		return bits3072, true
	case kmspb.ImportJob_RSA_OAEP_4096_SHA256_AES_256, kmspb.ImportJob_RSA_OAEP_4096_SHA256:
		return bits4096, true
	default:
		return 0, false
	}
}

// CreateImportJob creates an ACTIVE import job with a fresh RSA wrapping key.
func (b *InMemoryBackend) CreateImportJob(req *kmspb.CreateImportJobRequest) (*kmspb.ImportJob, error) {
	if err := parseResource(req.GetParent(), depthKeyRing); err != nil {
		return nil, err
	}

	if err := validID("ImportJob", req.GetImportJobId()); err != nil {
		return nil, err
	}

	in := req.GetImportJob()

	bits, ok := wrappingBits(in.GetImportMethod())
	if !ok {
		return nil, errInvalid("import_method %s is not supported; use an RSA_OAEP_*_SHA256 method.",
			in.GetImportMethod())
	}

	level := in.GetProtectionLevel()
	if level != kmspb.ProtectionLevel_SOFTWARE && level != kmspb.ProtectionLevel_HSM {
		return nil, errInvalid("protection_level must be SOFTWARE or HSM.")
	}

	priv, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: generate wrapping key: %w", err)
	}

	pubDER, err := x509.MarshalPKIXPublicKey(&priv.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: marshal wrapping key: %w", err)
	}

	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("gcpkms: marshal wrapping key: %w", err)
	}

	name := req.GetParent() + "/importJobs/" + req.GetImportJobId()

	b.mu.Lock("CreateImportJob")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, ok = b.rings[req.GetParent()]; !ok {
		return nil, errNotFound("KeyRing", req.GetParent())
	}

	if _, ok = b.jobs[name]; ok {
		return nil, errExists("ImportJob", name)
	}

	now := b.now()
	j := &jobRec{private: privDER, pb: &kmspb.ImportJob{
		Name: name, ImportMethod: in.GetImportMethod(), ProtectionLevel: level,
		CreateTime: tsOf(now), GenerateTime: tsOf(now), ExpireTime: tsOf(now.Add(importJobTTL)),
		State:     kmspb.ImportJob_ACTIVE,
		PublicKey: &kmspb.ImportJob_WrappingPublicKey{Pem: pemOf(pubDER)},
	}}
	b.jobs[name] = j

	return cloneJob(j.pb), nil
}

// GetImportJob returns an import job.
func (b *InMemoryBackend) GetImportJob(name string) (*kmspb.ImportJob, error) {
	if err := parseJobName(name); err != nil {
		return nil, err
	}

	b.mu.Lock("GetImportJob")
	defer b.mu.Unlock()

	b.sweepLocked()

	j, ok := b.jobs[name]
	if !ok {
		return nil, errNotFound("ImportJob", name)
	}

	return cloneJob(j.pb), nil
}

// ListImportJobs lists a key ring's import jobs sorted by name.
func (b *InMemoryBackend) ListImportJobs(req *kmspb.ListImportJobsRequest) ([]*kmspb.ImportJob, string, int, error) {
	if err := parseResource(req.GetParent(), depthKeyRing); err != nil {
		return nil, "", 0, err
	}

	if err := rejectListOptions(req.GetFilter(), req.GetOrderBy()); err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.Lock("ListImportJobs")
	defer b.mu.Unlock()

	b.sweepLocked()

	if _, ok := b.rings[req.GetParent()]; !ok {
		return nil, "", 0, errNotFound("KeyRing", req.GetParent())
	}

	names := childrenOf(b.jobs, req.GetParent(), "importJobs")
	all := make([]*kmspb.ImportJob, 0, len(names))

	for _, n := range names {
		all = append(all, cloneJob(b.jobs[n].pb))
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}

// ImportCryptoKeyVersion imports wrapped key material as a new ENABLED version, or re-imports into a
// DESTROYED reimport-eligible version.
func (b *InMemoryBackend) ImportCryptoKeyVersion(
	req *kmspb.ImportCryptoKeyVersionRequest,
) (*kmspb.CryptoKeyVersion, error) {
	if err := parseResource(req.GetParent(), depthKey); err != nil {
		return nil, err
	}

	if err := parseJobName(req.GetImportJob()); err != nil {
		return nil, err
	}

	wrapped := req.GetRsaAesWrappedKey()
	if len(wrapped) == 0 {
		wrapped = req.GetWrappedKey()
	}

	if len(wrapped) == 0 {
		return nil, errInvalid("rsa_aes_wrapped_key must be specified.")
	}

	b.mu.Lock("ImportCryptoKeyVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	k, job, err := b.importTargetsLocked(req)
	if err != nil {
		return nil, err
	}

	spec, err := specFor(k.pb.GetPurpose(), req.GetAlgorithm())
	if err != nil {
		return nil, err
	}

	material, err := b.unwrapLocked(job, spec, wrapped)
	if err != nil {
		return nil, err
	}

	return b.storeImportedLocked(k, job, req, material)
}

func (b *InMemoryBackend) importTargetsLocked(req *kmspb.ImportCryptoKeyVersionRequest) (*keyRec, *jobRec, error) {
	k, ok := b.keys[req.GetParent()]
	if !ok {
		return nil, nil, errNotFound("CryptoKey", req.GetParent())
	}

	job, ok := b.jobs[req.GetImportJob()]
	if !ok {
		return nil, nil, errNotFound("ImportJob", req.GetImportJob())
	}

	if job.pb.GetState() != kmspb.ImportJob_ACTIVE {
		return nil, nil, errPrecondition("ImportJob %s is %s, not ACTIVE.", req.GetImportJob(), job.pb.GetState())
	}

	return k, job, nil
}

func (b *InMemoryBackend) unwrapLocked(job *jobRec, spec algSpec, wrapped []byte) ([]byte, error) {
	k, err := parsePrivate(job.private)
	if err != nil {
		return nil, err
	}

	priv, ok := k.(*rsa.PrivateKey)
	if !ok {
		return nil, errInvalid("Import job wrapping key is invalid.")
	}

	material, err := unwrapImport(job.pb.GetImportMethod(), priv, wrapped)
	if err != nil {
		return nil, err
	}

	if err = validateImported(spec, material); err != nil {
		return nil, err
	}

	return material, nil
}

func (b *InMemoryBackend) storeImportedLocked(
	k *keyRec, job *jobRec, req *kmspb.ImportCryptoKeyVersionRequest, material []byte,
) (*kmspb.CryptoKeyVersion, error) {
	now := b.now()

	var v *versionRec

	if name := req.GetCryptoKeyVersion(); name != "" {
		existing, err := b.reimportTargetLocked(k, name, req.GetAlgorithm())
		if err != nil {
			return nil, err
		}

		v = existing
		v.material = material
		v.pb.State = kmspb.CryptoKeyVersion_ENABLED
		v.pb.DestroyTime = nil
		v.pb.DestroyEventTime = nil
	} else {
		v = b.appendVersionLocked(k, req.GetAlgorithm(), job.pb.GetProtectionLevel(), material, now)
	}

	v.pb.ImportJob = job.pb.GetName()
	v.pb.ImportTime = tsOf(now)
	v.pb.ReimportEligible = true

	return cloneVersion(v.pb), nil
}

func (b *InMemoryBackend) reimportTargetLocked(k *keyRec, name string, a alg) (*versionRec, error) {
	keyName, id, err := splitVersion(name)
	if err != nil || keyName != k.pb.GetName() {
		return nil, errInvalid("crypto_key_version %q does not belong to %s.", name, k.pb.GetName())
	}

	v := k.version(id)
	if v == nil {
		return nil, errNotFound("CryptoKeyVersion", name)
	}

	if v.pb.GetState() != kmspb.CryptoKeyVersion_DESTROYED || !v.pb.GetReimportEligible() {
		return nil, errPrecondition("CryptoKeyVersion %s is not eligible for re-import.", name)
	}

	if v.pb.GetAlgorithm() != a {
		return nil, errInvalid("Re-import must use the version's original algorithm %s.", v.pb.GetAlgorithm())
	}

	return v, nil
}
