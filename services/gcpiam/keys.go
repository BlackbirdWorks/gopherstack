package gcpiam

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"slices"
	"strings"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func cloneKey(k *adminpb.ServiceAccountKey) *adminpb.ServiceAccountKey {
	out, _ := proto.Clone(k).(*adminpb.ServiceAccountKey)

	return out
}

func keyName(acct *adminpb.ServiceAccount, id string) string {
	return accountName(acct.GetProjectId(), acct.GetEmail()) + "/keys/" + id
}

func (r *accountRec) userKeyCount() int { return len(r.keys) }

// CreateServiceAccountKey generates a real RSA key; the private part is returned only here.
func (b *InMemoryBackend) CreateServiceAccountKey(
	req *adminpb.CreateServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	ptype := req.GetPrivateKeyType()
	if ptype == adminpb.ServiceAccountPrivateKeyType_TYPE_UNSPECIFIED {
		ptype = adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE
	}

	if ptype != adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE &&
		ptype != adminpb.ServiceAccountPrivateKeyType_TYPE_PKCS12_FILE {
		return nil, errInvalid("Unsupported private_key_type %v.", ptype)
	}

	b.mu.RLock("CreateServiceAccountKey")
	rec, err := b.findAccountLocked(req.GetName())

	var acct *adminpb.ServiceAccount
	if err == nil {
		acct = cloneAccount(rec.pb)
		if rec.userKeyCount() >= maxUserKeys {
			err = errPrecondition("Maximum number of keys on account reached.")
		}
	}
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bits, alg, err := keyBits(req.GetKeyAlgorithm())
	if err != nil {
		return nil, err
	}

	now := b.now()

	mat, err := generateMaterial(bits, acct.GetEmail(), now, farFuture())
	if err != nil {
		return nil, err
	}

	id, err := newKeyID()
	if err != nil {
		return nil, err
	}

	var data []byte
	if ptype == adminpb.ServiceAccountPrivateKeyType_TYPE_PKCS12_FILE {
		data, err = mat.pkcs12()
	} else {
		data, err = mat.credentials(acct, id)
	}

	if err != nil {
		return nil, err
	}

	pb := &adminpb.ServiceAccountKey{
		Name:            keyName(acct, id),
		KeyAlgorithm:    alg,
		KeyOrigin:       adminpb.ServiceAccountKeyOrigin_GOOGLE_PROVIDED,
		KeyType:         adminpb.ListServiceAccountKeysRequest_USER_MANAGED,
		ValidAfterTime:  timestamppb.New(now),
		ValidBeforeTime: timestamppb.New(farFuture()),
	}

	if err = b.addUserKey(acct.GetUniqueId(), id, &keyRec{pb: pb, cert: mat.certPEM, public: mat.pubDER}); err != nil {
		return nil, err
	}

	out := cloneKey(pb)
	out.PrivateKeyType = ptype
	out.PrivateKeyData = data

	return out, nil
}

func (b *InMemoryBackend) addUserKey(uid, id string, k *keyRec) error {
	b.mu.Lock("addUserKey")
	defer b.mu.Unlock()

	rec, ok := b.accounts[uid]
	if !ok || !rec.active() {
		return errNotFound("Service account does not exist.")
	}

	if rec.userKeyCount() >= maxUserKeys {
		return errPrecondition("Maximum number of keys on account reached.")
	}

	rec.keys[id] = k

	return nil
}

// UploadServiceAccountKey registers a caller-supplied RSA public key wrapped in an X.509 certificate.
func (b *InMemoryBackend) UploadServiceAccountKey(
	req *adminpb.UploadServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	block, _ := pem.Decode(req.GetPublicKeyData())
	if block == nil || block.Type != pemCertificate {
		return nil, errInvalid("public_key_data must be a PEM-encoded X.509 certificate.")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, errInvalid("public_key_data is not a valid X.509 certificate.")
	}

	pub, ok := cert.PublicKey.(*rsa.PublicKey)
	if !ok || pub.N.BitLen() < bitsSmall {
		return nil, errInvalid("The certificate must carry an RSA public key of at least %d bits.", bitsSmall)
	}

	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return nil, errInvalid("Unsupported public key.")
	}

	id, err := newKeyID()
	if err != nil {
		return nil, err
	}

	alg := adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048
	if pub.N.BitLen() < bitsDefault {
		alg = adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_1024
	}

	b.mu.RLock("UploadServiceAccountKey")
	rec, err := b.findAccountLocked(req.GetName())

	var acct *adminpb.ServiceAccount
	if err == nil {
		acct = cloneAccount(rec.pb)
	}
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	pb := &adminpb.ServiceAccountKey{
		Name:            keyName(acct, id),
		KeyAlgorithm:    alg,
		KeyOrigin:       adminpb.ServiceAccountKeyOrigin_USER_PROVIDED,
		KeyType:         adminpb.ListServiceAccountKeysRequest_USER_MANAGED,
		ValidAfterTime:  timestamppb.New(cert.NotBefore),
		ValidBeforeTime: timestamppb.New(cert.NotAfter),
	}

	rk := &keyRec{pb: pb, cert: pem.EncodeToMemory(&pem.Block{Type: pemCertificate, Bytes: cert.Raw}), public: der}
	if err = b.addUserKey(acct.GetUniqueId(), id, rk); err != nil {
		return nil, err
	}

	return cloneKey(pb), nil
}

func keyTypeSelected(
	types []adminpb.ListServiceAccountKeysRequest_KeyType,
	t adminpb.ListServiceAccountKeysRequest_KeyType,
) bool {
	return len(types) == 0 || slices.Contains(types, t) ||
		slices.Contains(types, adminpb.ListServiceAccountKeysRequest_KEY_TYPE_UNSPECIFIED)
}

// ListServiceAccountKeys lists user- and/or system-managed keys without key material.
func (b *InMemoryBackend) ListServiceAccountKeys(
	req *adminpb.ListServiceAccountKeysRequest,
) ([]*adminpb.ServiceAccountKey, error) {
	types := slices.Clone(req.GetKeyTypes())
	slices.Sort(types)

	if dup := slices.Compact(slices.Clone(types)); len(dup) != len(types) {
		return nil, errInvalid("Duplicate key types are not allowed.")
	}

	var out []*adminpb.ServiceAccountKey

	if keyTypeSelected(req.GetKeyTypes(), adminpb.ListServiceAccountKeysRequest_SYSTEM_MANAGED) {
		sys, err := b.ensureSystemKey(req.GetName())
		if err != nil {
			return nil, err
		}

		out = append(out, cloneKey(sys.pb))
	}

	b.mu.RLock("ListServiceAccountKeys")
	defer b.mu.RUnlock()

	rec, err := b.findAccountLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	if keyTypeSelected(req.GetKeyTypes(), adminpb.ListServiceAccountKeysRequest_USER_MANAGED) {
		for _, k := range rec.keys {
			out = append(out, cloneKey(k.pb))
		}
	}

	slices.SortFunc(out, func(x, y *adminpb.ServiceAccountKey) int {
		if c := x.GetValidAfterTime().AsTime().Compare(y.GetValidAfterTime().AsTime()); c != 0 {
			return c
		}

		return strings.Compare(x.GetName(), y.GetName())
	})

	return out, nil
}

// keyLocked resolves a key (user or system) of an active account.
func (b *InMemoryBackend) keyLocked(name string) (*accountRec, *keyRec, error) {
	acctName, id, err := parseKeyName(name)
	if err != nil {
		return nil, nil, err
	}

	rec, err := b.findAccountLocked(acctName)
	if err != nil {
		return nil, nil, err
	}

	if k, ok := rec.keys[id]; ok {
		return rec, k, nil
	}

	if rec.system != nil && strings.HasSuffix(rec.system.pb.GetName(), "/keys/"+id) {
		return rec, rec.system, nil
	}

	return nil, nil, errNotFound("Service account key %s does not exist.", name)
}

// GetServiceAccountKey returns key metadata plus the public key in the requested format.
func (b *InMemoryBackend) GetServiceAccountKey(
	req *adminpb.GetServiceAccountKeyRequest,
) (*adminpb.ServiceAccountKey, error) {
	b.mu.RLock("GetServiceAccountKey")
	defer b.mu.RUnlock()

	_, k, err := b.keyLocked(req.GetName())
	if err != nil {
		return nil, err
	}

	out := cloneKey(k.pb)

	switch req.GetPublicKeyType() {
	case adminpb.ServiceAccountPublicKeyType_TYPE_NONE:
	case adminpb.ServiceAccountPublicKeyType_TYPE_X509_PEM_FILE:
		out.PublicKeyData = slices.Clone(k.cert)
	case adminpb.ServiceAccountPublicKeyType_TYPE_RAW_PUBLIC_KEY:
		out.PublicKeyData = publicPEM(k.public)
	default:
		return nil, errInvalid("Unsupported public_key_type %v.", req.GetPublicKeyType())
	}

	return out, nil
}

// DeleteServiceAccountKey removes a user-managed key.
func (b *InMemoryBackend) DeleteServiceAccountKey(name string) (*emptypb.Empty, error) {
	b.mu.Lock("DeleteServiceAccountKey")
	defer b.mu.Unlock()

	rec, k, err := b.keyLocked(name)
	if err != nil {
		return nil, err
	}

	if k == rec.system {
		return nil, errPrecondition("System-managed keys cannot be deleted.")
	}

	_, id, _ := parseKeyName(name)
	delete(rec.keys, id)

	return &emptypb.Empty{}, nil
}

// SetServiceAccountKeyDisabled enables or disables a user-managed key.
func (b *InMemoryBackend) SetServiceAccountKeyDisabled(name string, disabled bool) (*emptypb.Empty, error) {
	b.mu.Lock("SetServiceAccountKeyDisabled")
	defer b.mu.Unlock()

	rec, k, err := b.keyLocked(name)
	if err != nil {
		return nil, err
	}

	if k == rec.system {
		return nil, errPrecondition("System-managed keys cannot be enabled or disabled.")
	}

	k.pb.Disabled = disabled

	return &emptypb.Empty{}, nil
}
