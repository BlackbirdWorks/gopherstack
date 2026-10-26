package gcpsecretmanager

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"slices"
	"strings"
	"time"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	minRotationLead    = 5 * time.Minute
	maxRotationSpan    = 3153600000 * time.Second
	rotationRetryLimit = 7 * 24 * time.Hour
	generatedPwBytes   = 24
	eventSecretRotate  = "SECRET_ROTATE"
	dataFormatJSON     = "JSON_API_V1"
)

// TopicPublisher publishes a message to a Pub/Sub topic resource name (projects/p/topics/t).
type TopicPublisher interface {
	Publish(topic string, data []byte, attrs map[string]string) error
}

// SetPublisher sets where SECRET_ROTATE notifications are delivered; nil disables delivery.
func (b *InMemoryBackend) SetPublisher(p TopicPublisher) {
	b.mu.Lock("SetPublisher")
	defer b.mu.Unlock()

	b.publisher = p
}

type rotationEvent struct {
	secret string
	data   []byte
	topics []string
}

// dueRotationsLocked advances every due rotation and returns the notifications to send.
func (b *InMemoryBackend) dueRotationsLocked() []rotationEvent {
	now := b.now()

	var events []rotationEvent

	for name, rec := range b.secrets {
		rot := rec.pb.GetRotation()
		next := rot.GetNextRotationTime()

		if next == nil || next.AsTime().After(now) || len(rec.pb.GetTopics()) == 0 {
			continue
		}

		if b.publisher == nil && now.Sub(next.AsTime()) < rotationRetryLimit {
			continue
		}

		data, _ := protojson.Marshal(rec.pb)
		topics := make([]string, 0, len(rec.pb.GetTopics()))

		for _, t := range rec.pb.GetTopics() {
			topics = append(topics, t.GetName())
		}

		events = append(events, rotationEvent{secret: name, data: data, topics: topics})

		b.advanceRotationLocked(rec, now)
	}

	slices.SortFunc(events, func(a, c rotationEvent) int { return strings.Compare(a.secret, c.secret) })

	return events
}

// advanceRotationLocked moves next_rotation_time past now by whole periods, or clears it for a one-shot rotation.
func (b *InMemoryBackend) advanceRotationLocked(rec *secretRec, now time.Time) {
	rot := rec.pb.GetRotation()

	period := rot.GetRotationPeriod().AsDuration()
	if period <= 0 {
		rot.NextRotationTime = nil
		rec.pb.Etag = b.nextEtag()

		return
	}

	next := rot.GetNextRotationTime().AsTime()
	skipped := int64(now.Sub(next)/period) + 1
	rot.NextRotationTime = timestamppb.New(next.Add(time.Duration(skipped) * period))
	rec.pb.Etag = b.nextEtag()
}

// FireRotations delivers SECRET_ROTATE notifications for every secret whose next_rotation_time has passed.
func (b *InMemoryBackend) FireRotations() {
	b.mu.Lock("FireRotations")
	events := b.dueRotationsLocked()
	pub := b.publisher
	now := b.now()
	b.mu.Unlock()

	if pub == nil {
		return
	}

	for _, ev := range events {
		attrs := map[string]string{
			"eventType":  eventSecretRotate,
			"secretId":   ev.secret,
			"dataFormat": dataFormatJSON,
			"timestamp":  now.UTC().Format(time.RFC3339Nano),
		}

		for _, topic := range ev.topics {
			_ = pub.Publish(topic, ev.data, attrs)
		}
	}
}

func validateRotationTimes(r *secretmanagerpb.Rotation, now time.Time, checkFuture bool) error {
	if p := r.GetRotationPeriod(); p != nil && p.AsDuration() > maxRotationSpan {
		return errInvalid("rotation_period must be at most %s.", maxRotationSpan)
	}

	if t := r.GetNextRotationTime(); t != nil {
		if checkFuture && t.AsTime().Before(now.Add(minRotationLead)) {
			return errInvalid("next_rotation_time must be at least %s in the future.", minRotationLead)
		}

		if t.AsTime().After(now.Add(maxRotationSpan)) {
			return errInvalid("next_rotation_time must be at most %s in the future.", maxRotationSpan)
		}
	}

	return nil
}

func randomPassword() ([]byte, error) {
	raw := make([]byte, generatedPwBytes)
	if _, err := rand.Read(raw); err != nil {
		return nil, status.Error(codes.Internal, "could not generate a password")
	}

	return []byte(base64.RawURLEncoding.EncodeToString(raw)), nil
}

func errManaged(format string, a ...any) error {
	return status.Errorf(codes.FailedPrecondition, format, a...)
}

func managedActive(rec *secretRec) bool {
	return rec.pb.GetRotation().GetManagedRotationStatus().GetState() ==
		secretmanagerpb.Rotation_ManagedRotationStatus_ACTIVE
}

// EnableManagedRotation turns on managed rotation for a Cloud SQL credentials secret and adds the first version.
func (b *InMemoryBackend) EnableManagedRotation(
	req *secretmanagerpb.EnableManagedRotationRequest,
) (*secretmanagerpb.SecretVersion, error) {
	creds := req.GetCloudSqlSingleUserCredentials()
	if creds.GetInstanceId() == "" || creds.GetUsername() == "" {
		return nil, errInvalid("cloud_sql_single_user_credentials with instance_id and username must be set.")
	}

	password := []byte(creds.GetPassword())
	if len(password) == 0 {
		var err error
		if password, err = randomPassword(); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("EnableManagedRotation")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetParent())
	if err != nil {
		return nil, err
	}

	if rec.pb.GetSecretType() != secretmanagerpb.Secret_CLOUD_SQL_DB_CREDENTIALS {
		return nil, errManaged("Managed rotation requires secret_type CLOUD_SQL_DB_CREDENTIALS.")
	}

	if managedActive(rec) {
		return nil, errManaged("Managed rotation is already enabled for %s.", req.GetParent())
	}

	ensureRotation(rec.pb).ManagedRotationStatus = &secretmanagerpb.Rotation_ManagedRotationStatus{
		State: secretmanagerpb.Rotation_ManagedRotationStatus_ACTIVE,
	}
	rec.pb.Etag = b.nextEtag()

	return cloneVersion(b.addVersionLocked(rec, req.GetParent(), password, false)), nil
}

// RotateSecret adds a freshly generated password version to a secret with managed rotation enabled.
func (b *InMemoryBackend) RotateSecret(
	req *secretmanagerpb.RotateSecretRequest,
) (*secretmanagerpb.SecretVersion, error) {
	password, err := randomPassword()
	if err != nil {
		return nil, err
	}

	b.mu.Lock("RotateSecret")
	defer b.mu.Unlock()

	b.sweepLocked()

	rec, err := b.getLocked(req.GetParent())
	if err != nil {
		return nil, err
	}

	if !managedActive(rec) {
		return nil, errManaged("Managed rotation is not enabled for %s.", req.GetParent())
	}

	return cloneVersion(b.addVersionLocked(rec, req.GetParent(), password, false)), nil
}

// RunRotations fires due rotations every interval until ctx ends.
func (b *InMemoryBackend) RunRotations(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			b.FireRotations()
		}
	}
}
