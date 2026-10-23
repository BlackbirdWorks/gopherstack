package gcpmonitoring

import (
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	mutatedBy        = "gopherstack"
	verificationTTL  = 24 * time.Hour
	verificationSize = 6
	maxUserLabels    = 64
)

func cloneChannel(c *monitoringpb.NotificationChannel) *monitoringpb.NotificationChannel {
	out, _ := proto.Clone(c).(*monitoringpb.NotificationChannel)

	return out
}

func (b *InMemoryBackend) mutation() *monitoringpb.MutationRecord {
	return &monitoringpb.MutationRecord{MutateTime: timestamppb.New(b.now()), MutatedBy: mutatedBy}
}

// splitSensitive separates the sensitive labels, which are never returned by the API.
func splitSensitive(k channelKind, labels map[string]string) (map[string]string, map[string]string) {
	plain, secret := map[string]string{}, map[string]string{}

	for key, v := range labels {
		if k.isSensitive(key) {
			secret[key] = v
		} else {
			plain[key] = v
		}
	}

	return plain, secret
}

func validateUserLabels(m map[string]string) error {
	if len(m) > maxUserLabels {
		return errInvalid("At most %d user labels are allowed.", maxUserLabels)
	}

	return nil
}

// CreateNotificationChannel stores a channel; email and SMS channels start unverified.
func (b *InMemoryBackend) CreateNotificationChannel(
	req *monitoringpb.CreateNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, err
	}

	in := req.GetNotificationChannel()
	if in == nil {
		return nil, errInvalid("notification_channel is required.")
	}

	k, ok := kindOf(in.GetType())
	if !ok {
		return nil, errInvalid("Field notification_channel.type had an invalid value of %q: "+
			"unknown notification channel type.", in.GetType())
	}

	if err = errors2(validateChannelLabels(k, in.GetLabels()), validateUserLabels(in.GetUserLabels())); err != nil {
		return nil, err
	}

	plain, secret := splitSensitive(k, in.GetLabels())

	b.mu.Lock("CreateNotificationChannel")
	defer b.mu.Unlock()

	c := cloneChannel(in)
	c.Name = projectScope + project + "/" + collChannels + "/" + newNumericID()
	c.Labels = plain
	c.VerificationStatus = monitoringpb.NotificationChannel_VERIFIED

	if k.needsVerify {
		c.VerificationStatus = monitoringpb.NotificationChannel_UNVERIFIED
	}

	if c.GetEnabled() == nil {
		c.Enabled = wrapperspb.Bool(true)
	}

	c.CreationRecord = b.mutation()
	c.MutationRecords = nil
	b.channels[c.GetName()] = &channelRec{pb: c, sensitive: secret}

	return cloneChannel(c), nil
}

func errors2(errs ...error) error {
	for _, e := range errs {
		if e != nil {
			return e
		}
	}

	return nil
}

// GetNotificationChannel returns one channel without its sensitive labels.
func (b *InMemoryBackend) GetNotificationChannel(name string) (*monitoringpb.NotificationChannel, error) {
	if _, _, err := splitName(name, collChannels); err != nil {
		return nil, err
	}

	b.mu.RLock("GetNotificationChannel")
	defer b.mu.RUnlock()

	c, ok := b.channels[name]
	if !ok {
		return nil, errNotFound("Notification channel %s does not exist.", name)
	}

	return cloneChannel(c.pb), nil
}

type channelEnv struct {
	c *monitoringpb.NotificationChannel
}

func (e channelEnv) values(path string) []string {
	switch path {
	case fldType:
		return []string{e.c.GetType()}
	case fldDisplayName:
		return []string{e.c.GetDisplayName()}
	case fldName:
		return []string{e.c.GetName()}
	case "verification_status":
		return []string{e.c.GetVerificationStatus().String()}
	}

	if key, ok := cutPrefix(path, "labels."); ok {
		return one(e.c.GetLabels(), key)
	}

	if key, ok := cutPrefix(path, "user_labels."); ok {
		return one(e.c.GetUserLabels(), key)
	}

	return nil
}

// ListNotificationChannels pages the project's channels ordered by name.
func (b *InMemoryBackend) ListNotificationChannels(
	req *monitoringpb.ListNotificationChannelsRequest,
) ([]*monitoringpb.NotificationChannel, string, int32, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, "", 0, err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", 0, err
	}

	f, err := parseFilter(req.GetFilter())
	if err != nil {
		return nil, "", 0, err
	}

	prefix := projectScope + project + "/" + collChannels + "/"

	b.mu.RLock("ListNotificationChannels")

	var all []*monitoringpb.NotificationChannel

	for name, c := range b.channels {
		if hasPrefixStr(name, prefix) && f.matches(channelEnv{c: c.pb}) {
			all = append(all, cloneChannel(c.pb))
		}
	}

	b.mu.RUnlock()

	sort.Slice(all, func(i, j int) bool { return all[i].GetName() < all[j].GetName() })
	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, int32(len(all)), nil //nolint:gosec // bounded by the channel count
}

// UpdateNotificationChannel replaces the fields named in the mask (all mutable fields when empty).
func (b *InMemoryBackend) UpdateNotificationChannel(
	req *monitoringpb.UpdateNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	in := req.GetNotificationChannel()
	if in == nil {
		return nil, errInvalid("notification_channel is required.")
	}

	if _, _, err := splitName(in.GetName(), collChannels); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdateNotificationChannel")
	defer b.mu.Unlock()

	rec, ok := b.channels[in.GetName()]
	if !ok {
		return nil, errNotFound("Notification channel %s does not exist.", in.GetName())
	}

	next, secret := cloneChannel(rec.pb), maps.Clone(rec.sensitive)

	if err := b.applyChannelMask(rec, next, secret, in, req.GetUpdateMask()); err != nil {
		return nil, err
	}

	next.MutationRecords = append(next.MutationRecords, b.mutation())
	rec.pb, rec.sensitive = next, secret

	return cloneChannel(next), nil
}

func (b *InMemoryBackend) applyChannelMask(
	rec *channelRec, next *monitoringpb.NotificationChannel, secret map[string]string,
	in *monitoringpb.NotificationChannel, mask *fieldmaskpb.FieldMask,
) error {
	paths := mask.GetPaths()
	if len(paths) == 0 {
		paths = []string{fldDisplayName, fldDescription, fldLabels, fldUserLabels, fldEnabled}
	}

	k, _ := kindOf(next.GetType())

	for _, p := range paths {
		switch p {
		case fldDisplayName:
			next.DisplayName = in.GetDisplayName()
		case fldDescription:
			next.Description = in.GetDescription()
		case fldEnabled:
			next.Enabled = in.GetEnabled()
		case fldUserLabels:
			if err := validateUserLabels(in.GetUserLabels()); err != nil {
				return err
			}

			next.UserLabels = in.GetUserLabels()
		case fldLabels:
			if err := mergeChannelLabels(k, next, secret, in.GetLabels()); err != nil {
				return err
			}
		default:
			return errInvalid("Field update_mask had an invalid value of %q: the field cannot be updated.", p)
		}
	}

	_ = rec

	return nil
}

func mergeChannelLabels(k channelKind, next *monitoringpb.NotificationChannel, secret, labels map[string]string) error {
	plain, newSecret := splitSensitive(k, labels)

	if err := validateChannelLabels(k, plain); err != nil {
		return err
	}

	next.Labels = plain

	maps.Copy(secret, newSecret)

	return nil
}

// DeleteNotificationChannel removes a channel; policies that use it block deletion unless forced.
func (b *InMemoryBackend) DeleteNotificationChannel(req *monitoringpb.DeleteNotificationChannelRequest) error {
	if _, _, err := splitName(req.GetName(), collChannels); err != nil {
		return err
	}

	b.mu.Lock("DeleteNotificationChannel")
	defer b.mu.Unlock()

	if _, ok := b.channels[req.GetName()]; !ok {
		return errNotFound("Notification channel %s does not exist.", req.GetName())
	}

	if !req.GetForce() {
		for _, p := range b.policies {
			if slices.Contains(p.GetNotificationChannels(), req.GetName()) {
				return errPrecondition("Notification channel %s is used by alert policy %s; "+
					"delete with force to remove it from the policy.", req.GetName(), p.GetName())
			}
		}
	}

	for _, p := range b.policies {
		p.NotificationChannels = slices.DeleteFunc(
			p.GetNotificationChannels(),
			func(n string) bool { return n == req.GetName() },
		)
	}

	delete(b.channels, req.GetName())

	return nil
}

// SendNotificationChannelVerificationCode issues a code for an email or SMS channel.
func (b *InMemoryBackend) SendNotificationChannelVerificationCode(name string) error {
	if _, _, err := splitName(name, collChannels); err != nil {
		return err
	}

	b.mu.Lock("SendVerificationCode")
	defer b.mu.Unlock()

	rec, ok := b.channels[name]
	if !ok {
		return errNotFound("Notification channel %s does not exist.", name)
	}

	if k, _ := kindOf(rec.pb.GetType()); !k.needsVerify {
		return errPrecondition("Notification channels of type %s do not require verification.", rec.pb.GetType())
	}

	rec.code, rec.codeAt = numericID(verificationSize), b.now()

	return nil
}

// GetNotificationChannelVerificationCode returns a fresh code for a channel and when it expires.
func (b *InMemoryBackend) GetNotificationChannelVerificationCode(
	req *monitoringpb.GetNotificationChannelVerificationCodeRequest,
) (*monitoringpb.GetNotificationChannelVerificationCodeResponse, error) {
	if _, _, err := splitName(req.GetName(), collChannels); err != nil {
		return nil, err
	}

	b.mu.Lock("GetVerificationCode")
	defer b.mu.Unlock()

	rec, ok := b.channels[req.GetName()]
	if !ok {
		return nil, errNotFound("Notification channel %s does not exist.", req.GetName())
	}

	now := b.now()
	rec.code, rec.codeAt = numericID(verificationSize), now
	expire := now.Add(verificationTTL)

	if req.GetExpireTime() != nil && req.GetExpireTime().AsTime().Before(expire) {
		expire = req.GetExpireTime().AsTime()
	}

	return &monitoringpb.GetNotificationChannelVerificationCodeResponse{
		Code: rec.code, ExpireTime: timestamppb.New(expire),
	}, nil
}

// VerifyNotificationChannel marks a channel verified when the code matches one issued for it or an equivalent channel.
func (b *InMemoryBackend) VerifyNotificationChannel(
	req *monitoringpb.VerifyNotificationChannelRequest,
) (*monitoringpb.NotificationChannel, error) {
	if _, _, err := splitName(req.GetName(), collChannels); err != nil {
		return nil, err
	}

	b.mu.Lock("VerifyNotificationChannel")
	defer b.mu.Unlock()

	rec, ok := b.channels[req.GetName()]
	if !ok {
		return nil, errNotFound("Notification channel %s does not exist.", req.GetName())
	}

	if req.GetCode() == "" || !b.codeValidLocked(rec, req.GetCode()) {
		return nil, errInvalid("Field code had an invalid value: the verification code is wrong or has expired.")
	}

	rec.pb.VerificationStatus = monitoringpb.NotificationChannel_VERIFIED
	rec.code = ""

	return cloneChannel(rec.pb), nil
}

func (b *InMemoryBackend) codeValidLocked(target *channelRec, code string) bool {
	now := b.now()

	for _, other := range b.channels {
		if other.code != code || now.Sub(other.codeAt) > verificationTTL {
			continue
		}

		if other == target ||
			(other.pb.GetType() == target.pb.GetType() && maps.Equal(other.pb.GetLabels(), target.pb.GetLabels())) {
			return true
		}
	}

	return false
}

func cutPrefix(s, prefix string) (string, bool) { return strings.CutPrefix(s, prefix) }

func hasPrefixStr(s, prefix string) bool { return strings.HasPrefix(s, prefix) }
