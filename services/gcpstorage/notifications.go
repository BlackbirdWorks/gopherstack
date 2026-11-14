package gcpstorage

import (
	"context"
	"encoding/base64"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	payloadJSONAPIV1 = "JSON_API_V1"
	payloadNone      = "NONE"

	evFinalize       = "OBJECT_FINALIZE"
	evMetadataUpdate = "OBJECT_METADATA_UPDATE"
	evDelete         = "OBJECT_DELETE"
	evArchive        = "OBJECT_ARCHIVE"

	topicURLPrefix         = "//pubsub.googleapis.com/"
	maxNotificationsBucket = 100
	maxNotificationsEvent  = 10
	maxCustomAttributes    = 10
)

// Notifier publishes bucket notifications to Pub/Sub topics ("projects/P/topics/T"); injected at wiring.
type Notifier interface {
	TopicExists(topic string) bool
	Publish(topic string, data []byte, attrs map[string]string) error
}

// Notification is a bucket notificationConfig (storage#notification).
type Notification struct {
	CustomAttributes map[string]string `json:"customAttributes,omitempty"`
	ID               string            `json:"id"`
	Topic            string            `json:"topic"`
	ObjectNamePrefix string            `json:"objectNamePrefix,omitempty"`
	PayloadFormat    string            `json:"payloadFormat"`
	EventTypes       []string          `json:"eventTypes,omitempty"`
}

// SetNotifier wires the Pub/Sub publisher that delivers notificationConfigs.
func (b *InMemoryBackend) SetNotifier(n Notifier) {
	b.mu.Lock("SetNotifier")
	defer b.mu.Unlock()

	b.notifier = n
}

func eventTypeName(t ObjectEventType) string {
	switch t {
	case ObjectFinalized:
		return evFinalize
	case ObjectDeleted:
		return evDelete
	case ObjectArchived:
		return evArchive
	case ObjectMetadataUpdated:
		return evMetadataUpdate
	default:
		return ""
	}
}

func validEventType(s string) bool {
	return slices.Contains([]string{evFinalize, evMetadataUpdate, evDelete, evArchive}, s)
}

func topicPath(topic string) (string, bool) {
	path := strings.TrimPrefix(topic, topicURLPrefix)
	parts := strings.Split(path, "/")

	if len(parts) != 4 || parts[0] != "projects" || parts[2] != "topics" || parts[1] == "" || parts[3] == "" {
		return "", false
	}

	return path, true
}

func (n Notification) covers(evType, object string) bool {
	if !strings.HasPrefix(object, n.ObjectNamePrefix) {
		return false
	}

	return len(n.EventTypes) == 0 || slices.Contains(n.EventTypes, evType)
}

func (n Notification) validate() *APIError {
	if n.Topic == "" {
		return errRequired("topic")
	}

	if _, ok := topicPath(n.Topic); !ok {
		return errInvalid("Invalid topic '" + n.Topic + "'; expected //pubsub.googleapis.com/projects/P/topics/T.")
	}

	if n.PayloadFormat != payloadJSONAPIV1 && n.PayloadFormat != payloadNone {
		return errInvalid("Invalid payload_format '" + n.PayloadFormat + "'; must be JSON_API_V1 or NONE.")
	}

	for _, e := range n.EventTypes {
		if !validEventType(e) {
			return errInvalid("Invalid event type '" + e + "'.")
		}
	}

	if len(n.CustomAttributes) > maxCustomAttributes {
		return errInvalid("A notification can have at most 10 custom attributes.")
	}

	return nil
}

// AddNotification validates and stores a notificationConfig; the topic must exist.
func (b *InMemoryBackend) AddNotification(bucket string, n Notification) (Notification, error) {
	if apiErr := n.validate(); apiErr != nil {
		return Notification{}, apiErr
	}

	b.mu.Lock("AddNotification")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return Notification{}, err
	}

	path, _ := topicPath(n.Topic)
	if b.notifier == nil || !b.notifier.TopicExists(path) {
		return Notification{}, errInvalid("Invalid Pub/Sub topic '" + path + "': the topic does not exist.")
	}

	if len(bk.Notifications) >= maxNotificationsBucket {
		return Notification{}, errInvalid("A bucket can have at most 100 notification configurations.")
	}

	evs := n.EventTypes
	if len(evs) == 0 {
		evs = []string{evFinalize, evMetadataUpdate, evDelete, evArchive}
	}

	for _, ev := range evs {
		if bk.countNotifications(ev) >= maxNotificationsEvent {
			return Notification{}, errInvalid("At most 10 notification configurations can fire for the same event.")
		}
	}

	bk.NotifSeq++
	n.ID = strconv.FormatInt(bk.NotifSeq, 10)
	n.Topic = topicURLPrefix + path
	bk.Notifications = append(bk.Notifications, n)
	bk.Metageneration++
	bk.Updated = b.now().UTC()

	return n, nil
}

func (b *Bucket) countNotifications(ev string) int {
	n := 0

	for _, c := range b.Notifications {
		if len(c.EventTypes) == 0 || slices.Contains(c.EventTypes, ev) {
			n++
		}
	}

	return n
}

// ListNotifications returns a bucket's notificationConfigs.
func (b *InMemoryBackend) ListNotifications(bucket string) ([]Notification, error) {
	b.mu.RLock("ListNotifications")
	defer b.mu.RUnlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return nil, err
	}

	return slices.Clone(bk.Notifications), nil
}

// GetNotification returns one notificationConfig.
func (b *InMemoryBackend) GetNotification(bucket, id string) (Notification, error) {
	list, err := b.ListNotifications(bucket)
	if err != nil {
		return Notification{}, err
	}

	for _, n := range list {
		if n.ID == id {
			return n, nil
		}
	}

	return Notification{}, errNotFound("Notification not found: " + id)
}

// DeleteNotification removes a notificationConfig.
func (b *InMemoryBackend) DeleteNotification(bucket, id string) error {
	b.mu.Lock("DeleteNotification")
	defer b.mu.Unlock()

	bk, err := b.bucketLocked(bucket)
	if err != nil {
		return err
	}

	i := slices.IndexFunc(bk.Notifications, func(n Notification) bool { return n.ID == id })
	if i < 0 {
		return errNotFound("Notification not found: " + id)
	}

	bk.Notifications = slices.Delete(slices.Clone(bk.Notifications), i, i+1)
	bk.Metageneration++
	bk.Updated = b.now().UTC()

	return nil
}

func (b *InMemoryBackend) notificationTargets(ev ObjectEvent) (Notifier, []Notification) {
	b.mu.RLock("notificationTargets")
	defer b.mu.RUnlock()

	bk, ok := b.buckets[ev.Object.Bucket]
	if !ok || b.notifier == nil {
		return nil, nil
	}

	var out []Notification

	for _, n := range bk.Notifications {
		if n.covers(eventTypeName(ev.Type), ev.Object.Name) {
			out = append(out, n)
		}
	}

	return b.notifier, out
}

func notificationAttrs(n Notification, bucket string, ev ObjectEvent) map[string]string {
	attrs := map[string]string{}
	maps.Copy(attrs, n.CustomAttributes)

	attrs["notificationConfig"] = "projects/_/buckets/" + bucket + "/notificationConfigs/" + n.ID
	attrs["eventType"] = eventTypeName(ev.Type)
	attrs["payloadFormat"] = n.PayloadFormat
	attrs["bucketId"] = bucket
	attrs["objectId"] = ev.Object.Name
	attrs["objectGeneration"] = itoa64(ev.Object.Generation)
	attrs["eventTime"] = ev.Time.UTC().Format("2006-01-02T15:04:05.000000Z07:00")

	if ev.OverwroteGen != 0 && ev.Type == ObjectFinalized {
		attrs["overwroteGeneration"] = itoa64(ev.OverwroteGen)
	}

	if ev.OverwrittenByGen != 0 && (ev.Type == ObjectDeleted || ev.Type == ObjectArchived) {
		attrs["overwrittenByGeneration"] = itoa64(ev.OverwrittenByGen)
	}

	return attrs
}

func (b *InMemoryBackend) dispatchNotifications(ev ObjectEvent) {
	notifier, targets := b.notificationTargets(ev)
	if notifier == nil || len(targets) == 0 {
		return
	}

	if ev.Type == ObjectDeleted && ev.Object.Deleted.IsZero() {
		ev.Object.Deleted = ev.Time
	}

	for _, n := range targets {
		var data []byte
		if n.PayloadFormat == payloadJSONAPIV1 {
			data = ev.Data()
		}

		path, _ := topicPath(n.Topic)
		if err := notifier.Publish(path, data, notificationAttrs(n, ev.Object.Bucket, ev)); err != nil {
			logger.Load(context.Background()).
				Warn("gcpstorage: notification publish failed", "topic", path, "error", err)
		}
	}
}

func notificationResource(base, bucket string, n Notification) map[string]any {
	res := map[string]any{
		keyKind: "storage#notification", "id": n.ID, "topic": n.Topic, "payload_format": n.PayloadFormat,
		keySelfLink: base + "/storage/v1/b/" + bucket + "/notificationConfigs/" + n.ID,
		keyEtag:     base64.StdEncoding.EncodeToString([]byte(n.ID)),
	}

	if len(n.EventTypes) > 0 {
		res["event_types"] = n.EventTypes
	}

	if len(n.CustomAttributes) > 0 {
		res["custom_attributes"] = n.CustomAttributes
	}

	if n.ObjectNamePrefix != "" {
		res["object_name_prefix"] = n.ObjectNamePrefix
	}

	return res
}

type notificationBody struct {
	CustomAttributes map[string]string `json:"custom_attributes"`
	Topic            string            `json:"topic"`
	ObjectNamePrefix string            `json:"object_name_prefix"`
	PayloadFormat    string            `json:"payload_format"`
	EventTypes       []string          `json:"event_types"`
}

func (h *Handler) insertNotification(c *echo.Context, rt route) error {
	body, err := readBody(c)
	if err != nil {
		return err
	}

	var in notificationBody
	if jsonErr := unmarshalBody(body, &in); jsonErr != nil {
		return errInvalid("Invalid JSON payload received.")
	}

	n, err := h.Backend.AddNotification(rt.bucket, Notification{
		Topic: in.Topic, PayloadFormat: in.PayloadFormat, EventTypes: in.EventTypes,
		CustomAttributes: in.CustomAttributes, ObjectNamePrefix: in.ObjectNamePrefix,
	})
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, notificationResource(baseURL(c.Request()), rt.bucket, n))
}

func (h *Handler) getNotification(c *echo.Context, rt route) error {
	n, err := h.Backend.GetNotification(rt.bucket, rt.entity)
	if err != nil {
		return err
	}

	return c.JSON(http.StatusOK, notificationResource(baseURL(c.Request()), rt.bucket, n))
}

func (h *Handler) listNotifications(c *echo.Context, rt route) error {
	list, err := h.Backend.ListNotifications(rt.bucket)
	if err != nil {
		return err
	}

	base := baseURL(c.Request())
	items := make([]map[string]any, 0, len(list))

	for _, n := range list {
		items = append(items, notificationResource(base, rt.bucket, n))
	}

	return c.JSON(http.StatusOK, map[string]any{keyKind: "storage#notifications", keyItems: items})
}

func (h *Handler) deleteNotification(c *echo.Context, rt route) error {
	if err := h.Backend.DeleteNotification(rt.bucket, rt.entity); err != nil {
		return err
	}

	return c.NoContent(http.StatusNoContent)
}
