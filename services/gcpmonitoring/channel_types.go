package gcpmonitoring

import (
	"net/url"
	"regexp"
	"slices"

	"cloud.google.com/go/monitoring/apiv3/v2/monitoringpb"
	"google.golang.org/genproto/googleapis/api"
	"google.golang.org/genproto/googleapis/api/label"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	typeEmail        = "email"
	typeSMS          = "sms"
	typePubSub       = "pubsub"
	typeWebhookToken = "webhook_tokenauth" //nolint:gosec // channel type name, not a credential
	typeWebhookBasic = "webhook_basicauth"
	typeSlack        = "slack"
	typePagerDuty    = "pagerduty"
	labelURL         = "url"
	labelTopic       = "topic"
	labelAuthToken   = "auth_token"
	labelPassword    = "password"
)

var topicRe = regexp.MustCompile(`^projects/[^/]+/topics/[^/]+$`)

type channelKind struct {
	typ, display, desc string
	required           []string
	optional           []string
	sensitive          []string
	needsVerify        bool
}

//nolint:gochecknoglobals // immutable catalog of notification channel types
var channelKinds = []channelKind{
	{typ: typeEmail, display: "Email", desc: "Send notifications to an email address.",
		required: []string{"email_address"}, needsVerify: true},
	{typ: typePagerDuty, display: "PagerDuty Services", desc: "Send notifications to PagerDuty.",
		optional: []string{"service_key"}, sensitive: []string{"service_key"}},
	{typ: typePubSub, display: "Pub/Sub", desc: "Publish notifications to a Pub/Sub topic.",
		required: []string{labelTopic}},
	{typ: typeSlack, display: "Slack", desc: "Send notifications to a Slack channel.",
		required: []string{"channel_name"}, sensitive: []string{labelAuthToken}},
	{typ: typeSMS, display: "SMS", desc: "Send notifications by text message.",
		required: []string{"number"}, needsVerify: true},
	{
		typ:       typeWebhookBasic,
		display:   "Webhooks with Basic Auth",
		desc:      "Post notifications to a webhook with basic authentication.",
		required:  []string{labelURL},
		optional:  []string{"username"},
		sensitive: []string{labelPassword},
	},
	{
		typ:       typeWebhookToken,
		display:   "Webhooks with Token Auth",
		desc:      "Post notifications to a webhook with token authentication.",
		required:  []string{labelURL},
		sensitive: []string{labelAuthToken},
	},
}

func kindOf(typ string) (channelKind, bool) {
	i := slices.IndexFunc(channelKinds, func(k channelKind) bool { return k.typ == typ })
	if i < 0 {
		return channelKind{}, false
	}

	return channelKinds[i], true
}

func (k channelKind) allows(key string) bool {
	return slices.Contains(k.required, key) || slices.Contains(k.optional, key) || slices.Contains(k.sensitive, key)
}

func (k channelKind) isSensitive(key string) bool { return slices.Contains(k.sensitive, key) }

func channelDescriptorName(project, typ string) string {
	return projectScope + project + "/" + collChannelDescriptors + "/" + typ
}

func (k channelKind) descriptor(project string) *monitoringpb.NotificationChannelDescriptor {
	d := &monitoringpb.NotificationChannelDescriptor{
		Name: channelDescriptorName(project, k.typ), Type: k.typ, DisplayName: k.display, Description: k.desc,
		LaunchStage: api.LaunchStage_GA,
	}

	var labels []*label.LabelDescriptor

	for _, key := range slices.Concat(k.required, k.optional, k.sensitive) {
		if slices.ContainsFunc(labels, func(l *label.LabelDescriptor) bool { return l.GetKey() == key }) {
			continue
		}

		labels = append(labels, &label.LabelDescriptor{
			Key: key, ValueType: label.LabelDescriptor_STRING, Description: key,
		})
	}

	d.Labels = labels

	return d
}

// validateChannelLabels checks required keys and value shapes; keys must be known to the channel type.
func validateChannelLabels(k channelKind, labels map[string]string) error {
	for key := range labels {
		if !k.allows(key) {
			return errInvalid("Field notification_channel.labels had an invalid value of %q: "+
				"unknown label for channel type %s.", key, k.typ)
		}
	}

	for _, key := range k.required {
		if labels[key] == "" {
			return errInvalid("Field notification_channel.labels had an invalid value: "+
				"label %q is required for channel type %s.", key, k.typ)
		}
	}

	switch k.typ {
	case typePubSub:
		if !topicRe.MatchString(labels[labelTopic]) {
			return errInvalid("Field notification_channel.labels.topic had an invalid value of %q: "+
				"expected projects/[PROJECT]/topics/[TOPIC].", labels[labelTopic])
		}
	case typeWebhookBasic, typeWebhookToken:
		u, err := url.Parse(labels[labelURL])
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return errInvalid("Field notification_channel.labels.url had an invalid value of %q: "+
				"expected an http or https URL.", labels[labelURL])
		}
	}

	return nil
}

// ListNotificationChannelDescriptors pages the supported channel types.
func (b *InMemoryBackend) ListNotificationChannelDescriptors(
	req *monitoringpb.ListNotificationChannelDescriptorsRequest,
) ([]*monitoringpb.NotificationChannelDescriptor, string, error) {
	project, err := projectOf(req.GetName())
	if err != nil {
		return nil, "", err
	}

	if err = pageTokenOK(req.GetPageToken()); err != nil {
		return nil, "", err
	}

	all := make([]*monitoringpb.NotificationChannelDescriptor, len(channelKinds))
	for i, k := range channelKinds {
		all[i] = k.descriptor(project)
	}

	pg := page.New(all, req.GetPageToken(), clampPage(req.GetPageSize()), defaultPage)

	return pg.Data, pg.Next, nil
}

// GetNotificationChannelDescriptor returns one channel type.
func (b *InMemoryBackend) GetNotificationChannelDescriptor(
	name string,
) (*monitoringpb.NotificationChannelDescriptor, error) {
	project, typ, err := splitName(name, collChannelDescriptors)
	if err != nil {
		return nil, err
	}

	k, ok := kindOf(typ)
	if !ok {
		return nil, errNotFound("Notification channel descriptor %s does not exist.", name)
	}

	return k.descriptor(project), nil
}
