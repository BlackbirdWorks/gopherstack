package connect

import (
	"context"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	labelNotification     = "Notification"
	labelUser             = "User"
	labelPushRegistration = "PushNotificationRegistration"
	notifSourceCustomer   = "CUSTOMER"
	notifStatusUnread     = "UNREAD"
	notifPriorityLow      = "LOW"
	maxNotifRecipients    = 200
	maxNotifCreateContent = 250
	maxNotifUpdateContent = 500
	defaultNotifTTL       = 7 * 24 * time.Hour
	maxNotifListPage      = 100
	maxUserNotifPage      = 1000
)

type notificationRec struct {
	ExpiresAt          *awstime.EpochTime `json:"expiresAt"`
	CreatedAt          *awstime.EpochTime `json:"createdAt"`
	LastModified       *awstime.EpochTime `json:"lastModified"`
	Content            map[string]string  `json:"content"`
	Tags               map[string]string  `json:"tags,omitempty"`
	UserStatus         map[string]string  `json:"userStatus,omitempty"`
	InstanceID         string             `json:"instanceId"`
	ID                 string             `json:"id"`
	Arn                string             `json:"arn"`
	Priority           string             `json:"priority"`
	LastModifiedRegion string             `json:"lastModifiedRegion"`
	Recipients         []string           `json:"recipients"`
}

type pushRegistrationRec struct {
	InstanceID        string `json:"instanceId"`
	ID                string `json:"id"`
	PinpointAppArn    string `json:"pinpointAppArn"`
	DeviceToken       string `json:"deviceToken"`
	DeviceType        string `json:"deviceType"`
	ContactID         string `json:"contactId"`
	ParticipantRole   string `json:"participantRole"`
	IncludeRawMessage bool   `json:"includeRawMessage,omitempty"`
}

// Notification is a message shown to Connect users.
type Notification struct {
	LastModifiedTime   *awstime.EpochTime `json:"LastModifiedTime"`
	CreatedAt          *awstime.EpochTime `json:"CreatedAt,omitempty"`
	ExpiresAt          *awstime.EpochTime `json:"ExpiresAt,omitempty"`
	Content            map[string]string  `json:"Content"`
	Tags               map[string]string  `json:"Tags,omitempty"`
	Arn                string             `json:"Arn"`
	ID                 string             `json:"Id"`
	LastModifiedRegion string             `json:"LastModifiedRegion,omitempty"`
	Priority           string             `json:"Priority,omitempty"`
	Recipients         []string           `json:"Recipients"`
}

// NotificationSearchSummary is a Notification as search returns it.
type NotificationSearchSummary struct {
	InstanceID string `json:"InstanceId"`
	Notification
}

// UserNotificationSummary is a notification as one user sees it.
type UserNotificationSummary struct {
	CreatedAt          *awstime.EpochTime `json:"CreatedAt,omitempty"`
	ExpiresAt          *awstime.EpochTime `json:"ExpiresAt,omitempty"`
	Content            map[string]string  `json:"Content"`
	InstanceID         string             `json:"InstanceId"`
	NotificationID     string             `json:"NotificationId"`
	NotificationStatus string             `json:"NotificationStatus"`
	Priority           string             `json:"Priority,omitempty"`
	RecipientID        string             `json:"RecipientId"`
	Source             string             `json:"Source"`
}

// CreateNotificationInput is the CreateNotification request.
type CreateNotificationInput struct {
	ExpiresAt                *awstime.EpochTime
	Content                  map[string]string
	Tags                     map[string]string
	ClientToken              string
	Priority                 string
	PredefinedNotificationID string `json:"PredefinedNotificationId"`
	instPath
	Recipients []string
}

type notificationRef struct {
	NotificationID string `json:"-" path:"NotificationId"`
	instPath
}

// UpdateNotificationContentInput is the UpdateNotificationContent request.
type UpdateNotificationContentInput struct {
	Content map[string]string
	notificationRef
}

// UpdateUserNotificationStatusInput is the UpdateUserNotificationStatus request.
type UpdateUserNotificationStatusInput struct {
	UserID string `json:"-" path:"UserId"`
	Status string
	notificationRef
}

type notificationCreated struct {
	NotificationID  string `json:"NotificationId"`
	NotificationArn string `json:"NotificationArn"`
}

// ContactConfiguration names the chat contact a push registration belongs to.
type ContactConfiguration struct {
	ContactID         string `json:"ContactId"`
	ParticipantRole   string `json:"ParticipantRole,omitempty"`
	IncludeRawMessage bool   `json:"IncludeRawMessage,omitempty"`
}

// CreatePushNotificationRegistrationInput is the CreatePushNotificationRegistration request.
type CreatePushNotificationRegistrationInput struct {
	ContactConfiguration *ContactConfiguration
	ClientToken          string
	PinpointAppArn       string
	DeviceToken          string
	DeviceType           string
	instPath
}

type pushRegistrationCreated struct {
	RegistrationID string `json:"RegistrationId"`
}

// DeletePushNotificationRegistrationInput is the DeletePushNotificationRegistration request.
type DeletePushNotificationRegistrationInput struct {
	RegistrationID string `json:"-" path:"RegistrationId"`
	ContactID      string `json:"-"                       query:"contactId"`
	instPath
}

func checkNotificationContent(c map[string]string, maxLen int) error {
	if len(c) == 0 {
		return invalidParamErr("Content is required")
	}

	for locale, text := range c {
		if locale == "" || strings.TrimSpace(text) == "" || len([]rune(text)) > maxLen {
			return invalidParamErr("Content for locale %q must be 1-%d characters", locale, maxLen)
		}
	}

	return nil
}

func (r *notificationRec) view() Notification {
	return Notification{
		LastModifiedTime: r.LastModified, CreatedAt: r.CreatedAt, ExpiresAt: r.ExpiresAt, Content: cloneTags(r.Content),
		Tags: cloneTags(r.Tags), Arn: r.Arn, ID: r.ID, LastModifiedRegion: r.LastModifiedRegion, Priority: r.Priority,
		Recipients: slices.Clone(r.Recipients),
	}
}

// parseRecipient reports whether a recipient ARN names the instance itself, or else its user ID.
func parseRecipient(inst, recipient string) (bool, string, bool) {
	_, rest, ok := strings.Cut(recipient, ":instance/")
	if !ok || !strings.HasPrefix(rest, inst) {
		return false, "", false
	}

	switch tail := strings.TrimPrefix(rest, inst); {
	case tail == "":
		return true, "", true
	case strings.HasPrefix(tail, "/agent/") && len(tail) > len("/agent/"):
		return false, strings.TrimPrefix(tail, "/agent/"), true
	}

	return false, "", false
}

// reaches reports whether the notification addresses the user, directly or through the instance ARN.
func (r *notificationRec) reaches(userID string) bool {
	return slices.ContainsFunc(r.Recipients, func(rc string) bool {
		whole, id, ok := parseRecipient(r.InstanceID, rc)

		return ok && (whole || id == userID)
	})
}

func (b *Backend) checkRecipients(inst string, recipients []string) error {
	if n := len(recipients); n < 1 || n > maxNotifRecipients {
		return invalidParamErr("Recipients must hold 1-%d items", maxNotifRecipients)
	}

	for _, r := range recipients {
		whole, id, ok := parseRecipient(inst, r)
		if !ok {
			return invalidParamErr("recipient %q must be a user ARN of this instance or the instance ARN", r)
		}

		if !whole && !b.users.has(inst, id) {
			return notFoundErr(labelUser, id)
		}
	}

	return nil
}

// CreateNotification creates a notification for users of the instance.
func (b *Backend) CreateNotification(ctx context.Context, in *CreateNotificationInput) (*notificationCreated, error) {
	if err := firstError(
		checkNotificationContent(in.Content, maxNotifCreateContent), checkTags(in.Tags),
		checkOptional(in.Priority, "Priority", "HIGH", notifPriorityLow),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateNotification", in.InstanceID, func(inst string) (*notificationCreated, error) {
		if err := b.checkRecipients(inst, in.Recipients); err != nil {
			return nil, err
		}

		now := b.now()
		expires := now.Add(defaultNotifTTL)

		if in.ExpiresAt != nil {
			expires = in.ExpiresAt.Time
		}

		if !expires.After(now) {
			return nil, invalidParamErr("ExpiresAt must be in the future")
		}

		id := in.PredefinedNotificationID
		if id == "" {
			id = newID()
		} else if b.notifications.has(inst, id) {
			return nil, duplicateErr(labelNotification, id)
		}

		_, region := b.scope(ctx)
		created, exp := awstime.NewEpochTime(now), awstime.NewEpochTime(expires)
		rec := &notificationRec{
			InstanceID: inst,
			ID:         id,
			Arn:        b.buildARN(ctx, "instance/"+inst+"/notification/"+id),
			Content: cloneTags(
				in.Content,
			),
			Priority:           orDefault(in.Priority, notifPriorityLow),
			Recipients:         slices.Clone(in.Recipients),
			CreatedAt:          &created,
			LastModified:       &created,
			ExpiresAt:          &exp,
			LastModifiedRegion: region,
			Tags:               cloneTags(in.Tags),
		}
		b.notifications.put(rec)

		return &notificationCreated{NotificationID: id, NotificationArn: rec.Arn}, nil
	})
}

func checkOptional(v, field string, allowed ...string) error {
	if v == "" {
		return nil
	}

	return oneOf(field, v, allowed...)
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

// DescribeNotification returns a notification.
func (b *Backend) DescribeNotification(inst, id string) (*Notification, error) {
	return readInstance(b, "DescribeNotification", inst, func(inst string) (*Notification, error) {
		r, err := b.notifications.get(inst, id)
		if err != nil {
			return nil, err
		}

		v := r.view()

		return &v, nil
	})
}

// UpdateNotificationContent replaces the content of a notification.
func (b *Backend) UpdateNotificationContent(ctx context.Context, in *UpdateNotificationContentInput) (*empty, error) {
	if err := checkNotificationContent(in.Content, maxNotifUpdateContent); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateNotificationContent", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.notifications.get(inst, in.NotificationID)
		if err != nil {
			return nil, err
		}

		_, region := b.scope(ctx)
		t := awstime.NewEpochTime(b.now())
		r.Content, r.LastModified, r.LastModifiedRegion = cloneTags(in.Content), &t, region

		return &empty{}, nil
	})
}

// DeleteNotification removes a notification.
func (b *Backend) DeleteNotification(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteNotification", inst, func(inst string) (*empty, error) {
		return nil, b.notifications.del(inst, id)
	})
}

// ListNotifications pages through the notifications of an instance, newest first.
func (b *Backend) ListNotifications(inst, token string, maxResults int32) ([]Notification, string, error) {
	if maxResults > maxNotifListPage {
		return nil, "", invalidParamErr("maxResults must be 1-%d", maxNotifListPage)
	}

	all, err := readInstance(b, "ListNotifications", inst, func(inst string) ([]Notification, error) {
		recs := b.notifications.in(inst)
		slices.SortStableFunc(
			recs,
			func(a, c *notificationRec) int { return c.CreatedAt.Time.Compare(a.CreatedAt.Time) },
		)

		out := make([]Notification, 0, len(recs))
		for _, r := range recs {
			out = append(out, r.view())
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, token, maxResults)
}

// SearchNotifications searches notifications by id or content and by tags.
func (b *Backend) SearchNotifications(in *SearchRequest) (*SearchResult, error) {
	if in.MaxResults > maxNotifListPage {
		return nil, invalidParamErr("MaxResults must be 1-%d", maxNotifListPage)
	}

	spec := searchSpec{fields: []string{"notificationid", "id", "messagecontent", "content"}}

	return runSearch(b, "SearchNotifications", "Notifications", in, b.notifications, spec,
		func(r *notificationRec) searchSubject {
			text := strings.Join(sortedValues(r.Content), "\n")

			return searchSubject{
				fields: map[string]string{"notificationid": r.ID, "id": r.ID, "messagecontent": text, "content": text},
				tags:   r.Tags,
			}
		},
		func(r *notificationRec) NotificationSearchSummary {
			return NotificationSearchSummary{InstanceID: r.InstanceID, Notification: r.view()}
		})
}

func sortedValues(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, m[k])
	}

	return out
}

// ListUserNotifications lists the unexpired notifications addressed to a user with the user's status.
func (b *Backend) ListUserNotifications(
	inst, userID, token string,
	maxResults int32,
) ([]UserNotificationSummary, string, error) {
	if maxResults > maxUserNotifPage {
		return nil, "", invalidParamErr("maxResults must be 1-%d", maxUserNotifPage)
	}

	all, err := readInstance(b, "ListUserNotifications", inst, func(inst string) ([]UserNotificationSummary, error) {
		u, uerr := b.users.get(inst, userID)
		if uerr != nil {
			return nil, uerr
		}

		recs := b.notifications.in(inst)
		slices.SortStableFunc(
			recs,
			func(a, c *notificationRec) int { return c.CreatedAt.Time.Compare(a.CreatedAt.Time) },
		)

		out := make([]UserNotificationSummary, 0, len(recs))

		for _, r := range recs {
			if r.reaches(u.User.ID) && r.ExpiresAt.After(b.now()) {
				out = append(out, userNotification(r, u.User.ID))
			}
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, token, maxResults)
}

func userNotification(r *notificationRec, userID string) UserNotificationSummary {
	return UserNotificationSummary{
		CreatedAt:          r.CreatedAt,
		ExpiresAt:          r.ExpiresAt,
		Content:            cloneTags(r.Content),
		InstanceID:         r.InstanceID,
		NotificationID:     r.ID,
		NotificationStatus: orDefault(r.UserStatus[userID], notifStatusUnread),
		Priority:           r.Priority,
		RecipientID:        userID,
		Source:             notifSourceCustomer,
	}
}

// UpdateUserNotificationStatus marks a notification READ, UNREAD or HIDDEN for one recipient.
func (b *Backend) UpdateUserNotificationStatus(in *UpdateUserNotificationStatusInput) (*empty, error) {
	if err := oneOf("Status", in.Status, "READ", notifStatusUnread, "HIDDEN"); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateUserNotificationStatus", in.InstanceID, func(inst string) (*empty, error) {
		u, err := b.users.get(inst, in.UserID)
		if err != nil {
			return nil, err
		}

		r, err := b.notifications.get(inst, in.NotificationID)
		if err != nil {
			return nil, err
		}

		if !r.reaches(u.User.ID) {
			return nil, notFoundErr(labelNotification, in.NotificationID)
		}

		if r.UserStatus == nil {
			r.UserStatus = map[string]string{}
		}

		r.UserStatus[u.User.ID] = in.Status

		return &empty{}, nil
	})
}

var pushDeviceTypes = []string{"GCM", "APNS", "APNS_SANDBOX"} //nolint:gochecknoglobals // immutable enum

// CreatePushNotificationRegistration registers a device token for the chat contact.
func (b *Backend) CreatePushNotificationRegistration(
	_ context.Context, in *CreatePushNotificationRegistrationInput,
) (*pushRegistrationCreated, error) {
	cc := in.ContactConfiguration
	if cc == nil || cc.ContactID == "" || in.DeviceToken == "" || in.PinpointAppArn == "" {
		return nil, invalidParamErr("ContactConfiguration, DeviceToken, DeviceType and PinpointAppArn are required")
	}

	if err := firstError(
		oneOf("DeviceType", in.DeviceType, pushDeviceTypes...),
		checkOptional(cc.ParticipantRole, "ContactConfiguration.ParticipantRole", "CUSTOMER"),
		checkPinpointARN(in.PinpointAppArn),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreatePushNotificationRegistration", in.InstanceID,
		func(inst string) (*pushRegistrationCreated, error) {
			c, err := b.contacts.get(inst, cc.ContactID)
			if err != nil {
				return nil, err
			}

			if c.Contact.Channel != channelChat || !c.active() {
				return nil, invalidRequestErr("push notifications need an active chat contact")
			}

			rec := &pushRegistrationRec{
				InstanceID:        inst,
				ID:                newID(),
				PinpointAppArn:    in.PinpointAppArn,
				DeviceToken:       in.DeviceToken,
				DeviceType:        in.DeviceType,
				ContactID:         cc.ContactID,
				ParticipantRole:   orDefault(cc.ParticipantRole, "CUSTOMER"),
				IncludeRawMessage: cc.IncludeRawMessage,
			}
			b.pushRegistrations.put(rec)

			return &pushRegistrationCreated{RegistrationID: rec.ID}, nil
		})
}

func checkPinpointARN(v string) error {
	parts := strings.SplitN(v, ":", 6) //nolint:mnd // arn:partition:service:region:account:resource
	if len(parts) != 6 || parts[0] != "arn" || parts[2] != "mobiletargeting" || !strings.HasPrefix(parts[5], "apps/") {
		return invalidParamErr("PinpointAppArn %q is not a Pinpoint application ARN", v)
	}

	return nil
}

// DeletePushNotificationRegistration removes a registration of a contact.
func (b *Backend) DeletePushNotificationRegistration(in *DeletePushNotificationRegistrationInput) (*empty, error) {
	if in.ContactID == "" {
		return nil, invalidParamErr("contactId is required")
	}

	return writeInstance(b, "DeletePushNotificationRegistration", in.InstanceID, func(inst string) (*empty, error) {
		r, err := b.pushRegistrations.get(inst, in.RegistrationID)
		if err != nil {
			return nil, err
		}

		if r.ContactID != in.ContactID {
			return nil, notFoundErr(labelPushRegistration, in.RegistrationID)
		}

		return nil, b.pushRegistrations.del(inst, in.RegistrationID)
	})
}
