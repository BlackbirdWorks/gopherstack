package gcpsecretmanager

import (
	"hash/crc32"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func crc32c(data []byte) int64 { return int64(crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))) }

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func (r *secretRec) find(n int64) *versionRec {
	for _, v := range r.versions {
		if v.num == n {
			return v
		}
	}

	return nil
}

func cloneVersion(v *versionRec) *secretmanagerpb.SecretVersion {
	out, _ := proto.Clone(v.pb).(*secretmanagerpb.SecretVersion)

	return out
}

func replicationStatus(s *secretmanagerpb.Secret, n int64) *secretmanagerpb.ReplicationStatus {
	keyVersion := func(c *secretmanagerpb.CustomerManagedEncryption) *secretmanagerpb.CustomerManagedEncryptionStatus {
		if c == nil {
			return nil
		}

		return &secretmanagerpb.CustomerManagedEncryptionStatus{
			KmsKeyVersionName: c.GetKmsKeyName() + "/cryptoKeyVersions/" + itoa(n),
		}
	}

	if isRegional(s.GetName()) {
		return nil
	}

	switch r := s.GetReplication().GetReplication().(type) {
	case *secretmanagerpb.Replication_UserManaged_:
		replicas := make([]*secretmanagerpb.ReplicationStatus_UserManagedStatus_ReplicaStatus, 0,
			len(r.UserManaged.GetReplicas()))

		for _, rp := range r.UserManaged.GetReplicas() {
			replicas = append(replicas, &secretmanagerpb.ReplicationStatus_UserManagedStatus_ReplicaStatus{
				Location:                  rp.GetLocation(),
				CustomerManagedEncryption: keyVersion(rp.GetCustomerManagedEncryption()),
			})
		}

		return &secretmanagerpb.ReplicationStatus{ReplicationStatus: &secretmanagerpb.ReplicationStatus_UserManaged{
			UserManaged: &secretmanagerpb.ReplicationStatus_UserManagedStatus{Replicas: replicas},
		}}
	default:
		return &secretmanagerpb.ReplicationStatus{ReplicationStatus: &secretmanagerpb.ReplicationStatus_Automatic{
			Automatic: &secretmanagerpb.ReplicationStatus_AutomaticStatus{
				CustomerManagedEncryption: keyVersion(s.GetReplication().GetAutomatic().GetCustomerManagedEncryption()),
			},
		}}
	}
}

// AddSecretVersion stores a new ENABLED version holding the payload.
func (b *InMemoryBackend) AddSecretVersion(
	req *secretmanagerpb.AddSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	payload := req.GetPayload()
	if payload == nil {
		return nil, errInvalid("Payload must be specified.")
	}

	if len(payload.GetData()) > maxPayloadBytes {
		return nil, errInvalid("Secret payload data too big. Limit is %d bytes.", maxPayloadBytes)
	}

	if payload.DataCrc32C != nil && payload.GetDataCrc32C() != crc32c(payload.GetData()) {
		return nil, errInvalid("Data crc32c mismatch: the supplied data_crc32c does not match the payload.")
	}

	b.mu.Lock("AddSecretVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetParent())
	if err != nil {
		return nil, err
	}

	return cloneVersion(b.addVersionLocked(rec, req.GetParent(), payload.GetData(), payload.DataCrc32C != nil)), nil
}

func (b *InMemoryBackend) addVersionLocked(rec *secretRec, secret string, data []byte, clientSum bool) *versionRec {
	n := rec.nextVer
	rec.nextVer++

	v := &versionRec{
		num:  n,
		data: slices.Clone(data),
		pb: &secretmanagerpb.SecretVersion{
			Name:                           secret + "/versions/" + itoa(n),
			CreateTime:                     timestamppb.New(b.now()),
			State:                          secretmanagerpb.SecretVersion_ENABLED,
			ReplicationStatus:              replicationStatus(rec.pb, n),
			Etag:                           b.nextEtag(),
			ClientSpecifiedPayloadChecksum: clientSum,
		},
	}
	rec.versions = append(rec.versions, v)

	return v
}

// parseVersionName splits projects/p/secrets/s/versions/v into the secret name and version ref.
func parseVersionName(name string) (string, string, error) {
	idx := strings.LastIndex(name, "/versions/")
	if idx < 0 || idx+len("/versions/") >= len(name) {
		return "", "", errInvalid(
			"Invalid secret version name %q. Expected projects/{p}/secrets/{s}/versions/{v}.",
			name,
		)
	}

	secret, err := parseSecretName(name[:idx])
	if err != nil {
		return "", "", err
	}

	return secret, name[idx+len("/versions/"):], nil
}

func (b *InMemoryBackend) resolveLocked(name string) (*secretRec, *versionRec, error) {
	secret, ref, err := parseVersionName(name)
	if err != nil {
		return nil, nil, err
	}

	rec, err := b.getLocked(secret)
	if err != nil {
		return nil, nil, err
	}

	var v *versionRec

	switch n, perr := strconv.ParseInt(ref, 10, 64); {
	case ref == "latest":
		if len(rec.versions) > 0 {
			v = rec.versions[len(rec.versions)-1]
		}
	case perr == nil && n > 0:
		v = rec.find(n)
	case perr == nil:
		return nil, nil, errInvalid("Invalid secret version %q.", ref)
	default:
		if target, ok := rec.pb.GetVersionAliases()[ref]; ok {
			v = rec.find(target)
		}
	}

	if v == nil {
		return nil, nil, errVersionNotFound(name)
	}

	return rec, v, nil
}

// GetSecretVersion returns version metadata (no payload); name may use latest or an alias.
func (b *InMemoryBackend) GetSecretVersion(name string) (*secretmanagerpb.SecretVersion, error) {
	b.mu.Lock("GetSecretVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	_, v, err := b.resolveLocked(name)
	if err != nil {
		return nil, err
	}

	return cloneVersion(v), nil
}

// AccessSecretVersion returns the payload of an ENABLED version.
func (b *InMemoryBackend) AccessSecretVersion(name string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	b.mu.Lock("AccessSecretVersion")
	defer b.mu.Unlock()

	b.sweepLocked()

	_, v, err := b.resolveLocked(name)
	if err != nil {
		return nil, err
	}

	if st := v.pb.GetState(); st != secretmanagerpb.SecretVersion_ENABLED {
		return nil, errPrecondition("Secret Version [%s] is in %s state.", v.pb.GetName(), st)
	}

	crc := crc32c(v.data)

	return &secretmanagerpb.AccessSecretVersionResponse{
		Name:    v.pb.GetName(),
		Payload: &secretmanagerpb.SecretPayload{Data: slices.Clone(v.data), DataCrc32C: &crc},
	}, nil
}

// ListSecretVersions returns a secret's versions newest first, and the total match count.
func (b *InMemoryBackend) ListSecretVersions(
	req *secretmanagerpb.ListSecretVersionsRequest,
) ([]*secretmanagerpb.SecretVersion, string, int, error) {
	match, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.Lock("ListSecretVersions")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetParent())
	if err != nil {
		return nil, "", 0, err
	}

	var all []*secretmanagerpb.SecretVersion

	for _, v := range slices.Backward(rec.versions) {
		if match(itemOfVersion(v.pb)) {
			all = append(all, cloneVersion(v))
		}
	}

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}

func itemOfVersion(pb *secretmanagerpb.SecretVersion) item {
	it := item{
		"name":        {pb.GetName()},
		"state":       {pb.GetState().String()},
		"create_time": {pb.GetCreateTime().AsTime().Format(time.RFC3339Nano)},
		"*":           {pb.GetName(), pb.GetState().String()},
	}

	if dt := pb.GetDestroyTime(); dt != nil {
		it["destroy_time"] = []string{dt.AsTime().Format(time.RFC3339Nano)}
	}

	return it
}

func (b *InMemoryBackend) finalizeDestroyLocked(v *versionRec, at time.Time) {
	v.pb.State = secretmanagerpb.SecretVersion_DESTROYED
	v.pb.DestroyTime = timestamppb.New(at)
	v.pb.ScheduledDestroyTime = nil
	v.pb.Etag = b.nextEtag()
	v.data = nil
}

func (b *InMemoryBackend) versionOp(
	op, name, etag string,
	mutate func(rec *secretRec, v *versionRec) error,
) (*secretmanagerpb.SecretVersion, error) {
	b.mu.Lock(op)
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, v, err := b.resolveLocked(name)
	if err != nil {
		return nil, err
	}

	if etag != "" && etag != v.pb.GetEtag() {
		return nil, errEtag("Secret Version", v.pb.GetName())
	}

	if err = mutate(rec, v); err != nil {
		return nil, err
	}

	return cloneVersion(v), nil
}

// EnableSecretVersion moves a DISABLED version back to ENABLED, cancelling any scheduled destruction.
func (b *InMemoryBackend) EnableSecretVersion(
	req *secretmanagerpb.EnableSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return b.versionOp("EnableSecretVersion", req.GetName(), req.GetEtag(), func(_ *secretRec, v *versionRec) error {
		switch v.pb.GetState() {
		case secretmanagerpb.SecretVersion_DESTROYED:
			return errPrecondition("Secret Version [%s] is in DESTROYED state.", v.pb.GetName())
		case secretmanagerpb.SecretVersion_ENABLED:
			return nil
		default:
			v.pb.State = secretmanagerpb.SecretVersion_ENABLED
			v.pb.ScheduledDestroyTime = nil
			v.pb.Etag = b.nextEtag()

			return nil
		}
	})
}

// DisableSecretVersion moves an ENABLED version to DISABLED.
func (b *InMemoryBackend) DisableSecretVersion(
	req *secretmanagerpb.DisableSecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return b.versionOp("DisableSecretVersion", req.GetName(), req.GetEtag(), func(_ *secretRec, v *versionRec) error {
		switch v.pb.GetState() {
		case secretmanagerpb.SecretVersion_DESTROYED:
			return errPrecondition("Secret Version [%s] is in DESTROYED state.", v.pb.GetName())
		case secretmanagerpb.SecretVersion_DISABLED:
			return nil
		default:
			v.pb.State = secretmanagerpb.SecretVersion_DISABLED
			v.pb.Etag = b.nextEtag()

			return nil
		}
	})
}

// DestroySecretVersion destroys a version now, or schedules it when the secret has version_destroy_ttl.
func (b *InMemoryBackend) DestroySecretVersion(
	req *secretmanagerpb.DestroySecretVersionRequest,
) (*secretmanagerpb.SecretVersion, error) {
	return b.versionOp("DestroySecretVersion", req.GetName(), req.GetEtag(), func(rec *secretRec, v *versionRec) error {
		if v.pb.GetState() == secretmanagerpb.SecretVersion_DESTROYED {
			return errPrecondition("Secret Version [%s] is in DESTROYED state.", v.pb.GetName())
		}

		now := b.now()

		if ttl := rec.pb.GetVersionDestroyTtl(); ttl != nil {
			if v.pb.GetScheduledDestroyTime() == nil {
				v.pb.State = secretmanagerpb.SecretVersion_DISABLED
				v.pb.ScheduledDestroyTime = timestamppb.New(now.Add(ttl.AsDuration()))
				v.pb.Etag = b.nextEtag()
			}

			return nil
		}

		b.finalizeDestroyLocked(v, now)

		return nil
	})
}
