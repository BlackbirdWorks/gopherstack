package gcpsecretmanager

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"regexp"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxPayloadBytes  = 64 << 10
	maxLabels        = 64
	maxLabelLen      = 63
	maxAnnotations   = 64
	maxAnnotationsSz = 16 << 10
	maxTopics        = 10
	maxAliases       = 50
	maxReplicas      = 40
	minRotationEvery = time.Hour
	minDestroyTTL    = 24 * time.Hour
	defaultPageSize  = 25
	maxPageSize      = 25000
	kindLabels       = "labels"
	kindAnnotations  = "annotations"
)

var (
	secretIDRe = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,255}$`)
	aliasRe    = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9_-]{0,62}$`)
	topicRe    = regexp.MustCompile(`^projects/[^/]+/topics/[^/]+$`)
)

type versionRec struct {
	pb   *secretmanagerpb.SecretVersion
	data []byte
	num  int64
}

type secretRec struct {
	pb       *secretmanagerpb.Secret
	policy   *iampb.Policy
	versions []*versionRec
	nextVer  int64
}

// InMemoryBackend holds all secrets, versions and IAM policies under one lock.
type InMemoryBackend struct {
	now       func() time.Time
	mu        *lockmetrics.RWMutex
	secrets   map[string]*secretRec
	publisher TopicPublisher
	etagSeq   uint64
}

// NewInMemoryBackend creates an empty backend.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu:      lockmetrics.New("gcpsecretmanager"),
		now:     time.Now,
		secrets: make(map[string]*secretRec),
	}
}

// Reset clears all state.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.secrets = make(map[string]*secretRec)
	b.etagSeq = 0
}

// RunJanitor sweeps expired secrets and due version destructions until ctx ends.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.Sweep()
		}
	}
}

// Sweep deletes expired secrets and finalises version destructions that are due.
func (b *InMemoryBackend) Sweep() {
	b.mu.Lock("Sweep")
	defer b.mu.Unlock()

	b.sweepLocked()
}

func (b *InMemoryBackend) sweepLocked() {
	now := b.now()

	for name, rec := range b.secrets {
		if exp := rec.pb.GetExpireTime(); exp != nil && !exp.AsTime().After(now) {
			delete(b.secrets, name)

			continue
		}

		for _, v := range rec.versions {
			sched := v.pb.GetScheduledDestroyTime()
			if v.pb.GetState() == secretmanagerpb.SecretVersion_DISABLED && sched != nil && !sched.AsTime().After(now) {
				b.finalizeDestroyLocked(v, sched.AsTime())
			}
		}
	}
}

func (b *InMemoryBackend) nextEtag() string {
	b.etagSeq++

	var raw [8]byte

	binary.BigEndian.PutUint64(raw[:], b.etagSeq)
	sum := sha256.Sum256(raw[:])

	return `"` + hex.EncodeToString(sum[:8]) + `"`
}

func secretName(project, id string) string { return "projects/" + project + "/secrets/" + id }

// parseProject returns "project" or "project/locations/location" for a secret parent.
func parseProject(parent string) (string, error) {
	parts := strings.Split(parent, "/")
	global := len(parts) == 2 && parts[0] == "projects" && parts[1] != ""
	regional := len(parts) == 4 && parts[0] == "projects" && parts[2] == "locations" && parts[1] != "" && parts[3] != ""

	if !global && !regional {
		return "", errInvalid(
			"Invalid parent %q. Expected projects/{project} or projects/{project}/locations/{location}.", parent)
	}

	return strings.TrimPrefix(parent, "projects/"), nil
}

// isRegional reports whether a resource name lives under projects/*/locations/*.
func isRegional(name string) bool {
	parts := strings.Split(name, "/")

	return len(parts) > 3 && parts[2] == "locations"
}

func parseSecretName(name string) (string, error) {
	parts := strings.Split(name, "/")
	n := len(parts)
	global := n == 4 && parts[2] == "secrets"
	regional := n == 6 && parts[2] == "locations" && parts[4] == "secrets" && parts[3] != ""

	if parts[0] != "projects" || parts[1] == "" || (!global && !regional) || !secretIDRe.MatchString(parts[n-1]) {
		return "", errInvalid("Invalid secret name %q. Expected projects/{project}/secrets/{secret} or "+
			"projects/{project}/locations/{location}/secrets/{secret}.", name)
	}

	return name, nil
}

func validateKMS(c *secretmanagerpb.CustomerManagedEncryption) error {
	if c != nil && c.GetKmsKeyName() == "" {
		return errInvalid("customer_managed_encryption.kms_key_name must be set.")
	}

	return nil
}

func validateReplication(r *secretmanagerpb.Replication) error {
	switch v := r.GetReplication().(type) {
	case *secretmanagerpb.Replication_Automatic_:
		return validateKMS(v.Automatic.GetCustomerManagedEncryption())
	case *secretmanagerpb.Replication_UserManaged_:
		replicas := v.UserManaged.GetReplicas()
		if len(replicas) == 0 || len(replicas) > maxReplicas {
			return errInvalid("user_managed replication requires between 1 and %d replicas.", maxReplicas)
		}

		seen := map[string]bool{}

		for _, rp := range replicas {
			if rp.GetLocation() == "" || seen[rp.GetLocation()] {
				return errInvalid("replica locations must be non-empty and unique.")
			}

			seen[rp.GetLocation()] = true

			if err := validateKMS(rp.GetCustomerManagedEncryption()); err != nil {
				return err
			}
		}

		return nil
	default:
		return errInvalid("Replication must be specified.")
	}
}

func validateStringMap(kind string, m map[string]string, maxN int) error {
	if len(m) > maxN {
		return errInvalid("A secret can have at most %d %s.", maxN, kind)
	}

	size := 0

	for k, v := range m {
		if k == "" || len(k) > maxLabelLen || (kind == kindLabels && len(v) > maxLabelLen) {
			return errInvalid("Invalid %s entry %q.", kind, k)
		}

		size += len(k) + len(v)
	}

	if kind == kindAnnotations && size > maxAnnotationsSz {
		return errInvalid("Annotations exceed %d bytes.", maxAnnotationsSz)
	}

	return nil
}

func validateTopics(topics []*secretmanagerpb.Topic) error {
	if len(topics) > maxTopics {
		return errInvalid("A secret can have at most %d topics.", maxTopics)
	}

	for _, t := range topics {
		if !topicRe.MatchString(t.GetName()) {
			return errInvalid("Invalid topic name %q. Expected projects/{project}/topics/{topic}.", t.GetName())
		}
	}

	return nil
}

func validateRotation(r *secretmanagerpb.Rotation, topics []*secretmanagerpb.Topic) error {
	if r == nil {
		return nil
	}

	if len(topics) == 0 {
		return errInvalid("Rotation requires at least one topic.")
	}

	if r.GetRotationPeriod() != nil {
		if r.GetNextRotationTime() == nil {
			return errInvalid("next_rotation_time must be set when rotation_period is set.")
		}

		if r.GetRotationPeriod().AsDuration() < minRotationEvery {
			return errInvalid("rotation_period must be at least %s.", minRotationEvery)
		}
	}

	return nil
}

func (b *InMemoryBackend) validateAliases(rec *secretRec, aliases map[string]int64, secret string) error {
	if len(aliases) > maxAliases {
		return errInvalid("A secret can have at most %d version aliases.", maxAliases)
	}

	for alias, n := range aliases {
		if !aliasRe.MatchString(alias) || alias == "latest" {
			return errInvalid("Invalid version alias %q.", alias)
		}

		if rec == nil || rec.find(n) == nil {
			return errVersionNotFound(secret + "/versions/" + itoa(n))
		}
	}

	return nil
}

// validateSecret checks the user-settable fields; future-time checks apply only when checkFuture is set.
func (b *InMemoryBackend) validateSecret(pb *secretmanagerpb.Secret, checkFuture bool) error {
	if err := validateStringMap(kindLabels, pb.GetLabels(), maxLabels); err != nil {
		return err
	}

	if err := validateStringMap(kindAnnotations, pb.GetAnnotations(), maxAnnotations); err != nil {
		return err
	}

	if err := validateTopics(pb.GetTopics()); err != nil {
		return err
	}

	if err := validateRotation(pb.GetRotation(), pb.GetTopics()); err != nil {
		return err
	}

	if d := pb.GetVersionDestroyTtl(); d != nil && d.AsDuration() < minDestroyTTL {
		return errInvalid("version_destroy_ttl must be at least %s.", minDestroyTTL)
	}

	if err := validateKMS(pb.GetCustomerManagedEncryption()); err != nil {
		return err
	}

	return validateRotationTimes(pb.GetRotation(), b.now(), checkFuture)
}

// resolveExpiry converts an input ttl to a stored expire_time.
func (b *InMemoryBackend) resolveExpiry(pb *secretmanagerpb.Secret) error {
	switch e := pb.GetExpiration().(type) {
	case *secretmanagerpb.Secret_Ttl:
		if e.Ttl.AsDuration() <= 0 {
			return errInvalid("ttl must be positive.")
		}

		pb.Expiration = &secretmanagerpb.Secret_ExpireTime{ExpireTime: timestamppb.New(b.now().Add(e.Ttl.AsDuration()))}
	case *secretmanagerpb.Secret_ExpireTime:
		if !e.ExpireTime.AsTime().After(b.now()) {
			return errInvalid("expire_time must be in the future.")
		}
	}

	return nil
}

// CreateSecret creates a secret under parent.
func (b *InMemoryBackend) CreateSecret(req *secretmanagerpb.CreateSecretRequest) (*secretmanagerpb.Secret, error) {
	project, err := parseProject(req.GetParent())
	if err != nil {
		return nil, err
	}

	if !secretIDRe.MatchString(req.GetSecretId()) {
		return nil, errInvalid("Invalid secret ID %q. Must match [a-zA-Z0-9_-]{1,255}.", req.GetSecretId())
	}

	pb, _ := proto.Clone(req.GetSecret()).(*secretmanagerpb.Secret)
	if pb == nil {
		pb = &secretmanagerpb.Secret{}
	}

	if strings.Contains(project, "/") {
		if pb.GetReplication() != nil {
			return nil, errInvalid("replication cannot be set on a regional secret.")
		}
	} else if err = validateReplication(pb.GetReplication()); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateSecret")
	defer b.mu.Unlock()

	b.sweepLocked()

	name := secretName(project, req.GetSecretId())
	if _, ok := b.secrets[name]; ok {
		return nil, errSecretExists(name)
	}

	if err = b.resolveExpiry(pb); err != nil {
		return nil, err
	}

	if err = b.validateSecret(pb, true); err != nil {
		return nil, err
	}

	if err = b.validateAliases(nil, pb.GetVersionAliases(), name); err != nil {
		return nil, err
	}

	pb.Name = name
	pb.CreateTime = timestamppb.New(b.now())
	pb.Etag = b.nextEtag()
	pb.PolicyMember = nil
	b.secrets[name] = &secretRec{pb: pb, nextVer: 1}

	return cloneSecret(pb), nil
}

func cloneSecret(pb *secretmanagerpb.Secret) *secretmanagerpb.Secret {
	out, _ := proto.Clone(pb).(*secretmanagerpb.Secret)

	return out
}

func (b *InMemoryBackend) getLocked(name string) (*secretRec, error) {
	if _, err := parseSecretName(name); err != nil {
		return nil, err
	}

	rec, ok := b.secrets[name]
	if !ok {
		return nil, errSecretNotFound(name)
	}

	return rec, nil
}

// GetSecret returns a secret.
func (b *InMemoryBackend) GetSecret(name string) (*secretmanagerpb.Secret, error) {
	b.mu.Lock("GetSecret")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(name)
	if err != nil {
		return nil, err
	}

	return cloneSecret(rec.pb), nil
}

// DeleteSecret removes a secret and all its versions; a non-empty etag must match.
func (b *InMemoryBackend) DeleteSecret(req *secretmanagerpb.DeleteSecretRequest) error {
	b.mu.Lock("DeleteSecret")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetName())
	if err != nil {
		return err
	}

	if req.GetEtag() != "" && req.GetEtag() != rec.pb.GetEtag() {
		return errEtag("Secret", req.GetName())
	}

	delete(b.secrets, req.GetName())

	return nil
}

func isMutablePath(p string) bool {
	switch p {
	case kindLabels, kindAnnotations, "topics", "expire_time", "ttl", "rotation", "rotation.next_rotation_time",
		"rotation.rotation_period", "version_aliases", "version_destroy_ttl":
		return true
	}

	return false
}

// UpdateSecret applies the masked fields of req.Secret to the stored secret.
func (b *InMemoryBackend) UpdateSecret(req *secretmanagerpb.UpdateSecretRequest) (*secretmanagerpb.Secret, error) {
	in := req.GetSecret()
	if in == nil {
		return nil, errInvalid("secret must be specified.")
	}

	paths := req.GetUpdateMask().GetPaths()
	if len(paths) == 0 {
		return nil, errInvalid("update_mask must be specified.")
	}

	for _, p := range paths {
		if !isMutablePath(p) {
			return nil, errInvalid("Field %q cannot be updated.", p)
		}
	}

	b.mu.Lock("UpdateSecret")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(in.GetName())
	if err != nil {
		return nil, err
	}

	if in.GetEtag() != "" && in.GetEtag() != rec.pb.GetEtag() {
		return nil, errEtag("Secret", in.GetName())
	}

	next := cloneSecret(rec.pb)
	applyMask(next, in, paths)

	if err = b.resolveExpiry(next); err != nil {
		return nil, err
	}

	if err = b.validateSecret(next, slices.ContainsFunc(paths, func(p string) bool {
		return strings.HasPrefix(p, "rotation")
	})); err != nil {
		return nil, err
	}

	if err = b.validateAliases(rec, next.GetVersionAliases(), next.GetName()); err != nil {
		return nil, err
	}

	next.Etag = b.nextEtag()
	rec.pb = next

	return cloneSecret(next), nil
}

func applyMask(dst, src *secretmanagerpb.Secret, paths []string) {
	for _, p := range paths {
		switch p {
		case kindLabels:
			dst.Labels = src.GetLabels()
		case kindAnnotations:
			dst.Annotations = src.GetAnnotations()
		case "topics":
			dst.Topics = src.GetTopics()
		case "version_aliases":
			dst.VersionAliases = src.GetVersionAliases()
		case "version_destroy_ttl":
			dst.VersionDestroyTtl = src.GetVersionDestroyTtl()
		case "rotation":
			dst.Rotation = src.GetRotation()
		case "rotation.next_rotation_time":
			ensureRotation(dst).NextRotationTime = src.GetRotation().GetNextRotationTime()
		case "rotation.rotation_period":
			ensureRotation(dst).RotationPeriod = src.GetRotation().GetRotationPeriod()
		case "expire_time":
			dst.Expiration = nil
			if et := src.GetExpireTime(); et != nil {
				dst.Expiration = &secretmanagerpb.Secret_ExpireTime{ExpireTime: et}
			}
		case "ttl":
			dst.Expiration = nil
			if ttl := src.GetTtl(); ttl != nil {
				dst.Expiration = &secretmanagerpb.Secret_Ttl{Ttl: ttl}
			}
		}
	}
}

func ensureRotation(s *secretmanagerpb.Secret) *secretmanagerpb.Rotation {
	if s.GetRotation() == nil {
		s.Rotation = &secretmanagerpb.Rotation{}
	}

	return s.GetRotation()
}

func itemOfSecret(pb *secretmanagerpb.Secret) item {
	it := item{
		"name":        {pb.GetName()},
		"create_time": {pb.GetCreateTime().AsTime().Format(time.RFC3339Nano)},
	}

	all := make([]string, 1, 1+2*len(pb.GetLabels())+2*len(pb.GetAnnotations())+len(pb.GetTopics()))
	all[0] = pb.GetName()

	if et := pb.GetExpireTime(); et != nil {
		it["expire_time"] = []string{et.AsTime().Format(time.RFC3339Nano)}
	}

	for k, v := range pb.GetLabels() {
		it["labels."+k] = []string{v}
		all = append(all, k, v)
	}

	for k, v := range pb.GetAnnotations() {
		it["annotations."+k] = []string{v}
		all = append(all, k, v)
	}

	for _, t := range pb.GetTopics() {
		it["topics.name"] = append(it["topics.name"], t.GetName())
		all = append(all, t.GetName())
	}

	it["*"] = all

	return it
}

func pageSizeOf(n int32) (int, error) {
	if n < 0 {
		return 0, errInvalid("page_size must not be negative.")
	}

	return min(int(n), maxPageSize), nil
}

// ListSecrets returns the matching secrets of a project sorted by name, and the total match count.
func (b *InMemoryBackend) ListSecrets(
	req *secretmanagerpb.ListSecretsRequest,
) ([]*secretmanagerpb.Secret, string, int, error) {
	project, err := parseProject(req.GetParent())
	if err != nil {
		return nil, "", 0, err
	}

	match, err := compileFilter(req.GetFilter())
	if err != nil {
		return nil, "", 0, err
	}

	size, err := pageSizeOf(req.GetPageSize())
	if err != nil {
		return nil, "", 0, err
	}

	b.mu.Lock("ListSecrets")
	defer b.mu.Unlock()

	b.sweepLocked()

	prefix := "projects/" + project + "/secrets/"

	var all []*secretmanagerpb.Secret

	for name, rec := range b.secrets {
		if strings.HasPrefix(name, prefix) && match(itemOfSecret(rec.pb)) {
			all = append(all, cloneSecret(rec.pb))
		}
	}

	slices.SortFunc(all, func(a, c *secretmanagerpb.Secret) int { return strings.Compare(a.GetName(), c.GetName()) })

	pg := page.New(all, req.GetPageToken(), size, defaultPageSize)

	return pg.Data, pg.Next, len(all), nil
}
