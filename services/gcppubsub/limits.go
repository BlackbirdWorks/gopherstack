package gcppubsub

import "strings"

// Fixed resource limits from docs.cloud.google.com/pubsub/quotas.
const (
	maxTopicsPerProject    = 10000
	maxSubsPerProject      = 10000
	maxSnapshotsPerProject = 5000
	maxSchemasPerProject   = 10000
	maxAttrsPerMessage     = 100
	maxAttrKeyBytes        = 256
	maxAttrValueBytes      = 1024
	maxAckRequestBytes     = 512 << 10
	maxSchemaDefBytes      = 300 << 10
	maxSchemaRevisions     = 20
	maxPullResponseBytes   = 10 << 20
)

type resourceLimits struct{ topics, subs, snapshots, schemas int }

func defaultResourceLimits() resourceLimits {
	return resourceLimits{
		topics: maxTopicsPerProject, subs: maxSubsPerProject,
		snapshots: maxSnapshotsPerProject, schemas: maxSchemasPerProject,
	}
}

func countPrefix[V any](m map[string]V, prefix string) int {
	n := 0

	for name := range m {
		if strings.HasPrefix(name, prefix) {
			n++
		}
	}

	return n
}

func errLimit(kind, project string, limit int) error {
	return errExhausted("project %s has reached the limit of %d %s", project, limit, kind)
}

func checkMessageLimits(attrs map[string]string) error {
	if len(attrs) > maxAttrsPerMessage {
		return errInvalid("Message has %d attributes; the maximum is %d.", len(attrs), maxAttrsPerMessage)
	}

	for k, v := range attrs {
		if len(k) > maxAttrKeyBytes {
			return errInvalid("Attribute key exceeds %d bytes.", maxAttrKeyBytes)
		}

		if len(v) > maxAttrValueBytes {
			return errInvalid("Attribute value for key %q exceeds %d bytes.", k, maxAttrValueBytes)
		}
	}

	return nil
}

func checkAckRequestSize(ids []string) error {
	total := 0

	for _, id := range ids {
		total += len(id)
	}

	if total > maxAckRequestBytes {
		return errInvalid("Request payload size exceeds the limit: %d bytes.", maxAckRequestBytes)
	}

	return nil
}
