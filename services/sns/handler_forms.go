package sns

import (
	"fmt"
	"net/url"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// parseIntParam reads a query or form integer parameter by name; returns defaultVal on missing or parse error.
func parseIntParam(c *echo.Context, name string, defaultVal int) int {
	s := c.Request().FormValue(name)
	if s == "" {
		return defaultVal
	}

	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}

	return n
}

// attrsToEntries converts a string map to sorted XMLAttributeEntry slice.
func attrsToEntries(attrs map[string]string) []XMLAttributeEntry {
	keys := collections.SortedKeys(attrs)

	entries := make([]XMLAttributeEntry, len(keys))
	for i, k := range keys {
		entries[i] = XMLAttributeEntry{Key: k, Value: attrs[k]}
	}

	return entries
}

// toXMLSubscriptions converts Subscription slice to XMLSubscription slice.
// Pending (unconfirmed) subscriptions use pendingConfirmationARN ("pending
// confirmation") as their ARN, matching the AWS SNS ListSubscriptions /
// ListSubscriptionsByTopic behaviour.
func toXMLSubscriptions(subs []Subscription) []XMLSubscription {
	result := make([]XMLSubscription, len(subs))
	for i, s := range subs {
		subArn := s.SubscriptionArn
		if s.PendingConfirmation {
			subArn = pendingConfirmationARN
		}

		result[i] = XMLSubscription{
			TopicArn:        s.TopicArn,
			Protocol:        s.Protocol,
			SubscriptionArn: subArn,
			Owner:           s.Owner,
			Endpoint:        s.Endpoint,
		}
	}

	return result
}

// extractFormAttributes reads Attributes.entry.N.key/value pairs from the form.
func extractFormAttributes(c *echo.Context) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		key := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return attrs
		}

		val := c.Request().FormValue(fmt.Sprintf("Attributes.entry.%d.value", i))
		attrs[key] = val
	}
}

// extractFilterPolicy reads the FilterPolicy attribute from form Attributes entries.
func extractFilterPolicy(form url.Values) string {
	for i := 1; ; i++ {
		key := form.Get(fmt.Sprintf("Attributes.entry.%d.key", i))
		if key == "" {
			return ""
		}

		if key == attrFilterPolicy {
			return form.Get(fmt.Sprintf("Attributes.entry.%d.value", i))
		}
	}
}

// extractMessageAttributes reads MessageAttributes.entry.N.Name/Value pairs from the form.
func extractMessageAttributes(form url.Values) map[string]MessageAttribute {
	return extractMessageAttributesWithPrefix(form, "MessageAttributes.")
}

// extractMessageAttributesWithPrefix reads MessageAttributes from form values using the
// given prefix (e.g. "MessageAttributes." or "PublishBatchRequestEntries.member.1.").
func extractMessageAttributesWithPrefix(
	form url.Values,
	prefix string,
) map[string]MessageAttribute {
	attrs := make(map[string]MessageAttribute)

	for i := 1; ; i++ {
		name := form.Get(fmt.Sprintf("%sentry.%d.Name", prefix, i))
		if name == "" {
			return attrs
		}

		attrs[name] = MessageAttribute{
			DataType:    form.Get(fmt.Sprintf("%sentry.%d.Value.DataType", prefix, i)),
			StringValue: form.Get(fmt.Sprintf("%sentry.%d.Value.StringValue", prefix, i)),
		}
	}
}
