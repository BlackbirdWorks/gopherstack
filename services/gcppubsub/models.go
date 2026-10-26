package gcppubsub

import (
	"strings"
	"time"

	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
)

const (
	projectsSeg  = "projects"
	deletedTopic = "_deleted-topic_"

	defaultAckDeadline     = 10
	minAckDeadline         = 10
	maxAckDeadline         = 600
	defaultRetention       = 7 * 24 * time.Hour
	minSubRetention        = 10 * time.Minute
	maxRetention           = 31 * 24 * time.Hour
	minTopicRetention      = 10 * time.Minute
	defaultExpiration      = 31 * 24 * time.Hour
	minExpiration          = 24 * time.Hour
	snapshotLifetime       = 7 * 24 * time.Hour
	maxLabels              = 64
	minDeliveryAttempts    = 5
	maxDeliveryAttempts    = 100
	maxRetryBackoff        = 600 * time.Second
	defaultMinRetryBackoff = 10 * time.Second
	maxPublishBatch        = 1000
	maxPublishBytes        = 10 << 20
	maxIDLen               = 255
	minIDLen               = 3
	defaultPageSize        = 100
	maxPageSize            = 1000
	ackIDLen               = 36
)

// message is one published message; subscriptions share the pointer.
type message struct {
	PublishTime time.Time         `json:"publishTime"`
	Attrs       map[string]string `json:"attributes,omitempty"`
	ID          string            `json:"id"`
	OrderingKey string            `json:"orderingKey,omitempty"`
	Data        []byte            `json:"data,omitempty"`
}

// subMsg is a message's delivery state within one subscription.
type subMsg struct {
	NotBefore  time.Time `json:"notBefore"`
	Msg        *message  `json:"msg"`
	leaseUntil time.Time
	ackID      string
	owner      string
	Attempts   int32 `json:"attempts,omitempty"`
	Acked      bool  `json:"acked,omitempty"`
}

type topic struct {
	pb       *pubsubpb.Topic
	retained []*message
}

type subscription struct {
	lastActive time.Time
	pb         *pubsubpb.Subscription
	filter     filterExpr
	acks       map[string]*subMsg
	wake       chan struct{}
	msgs       []*subMsg
	acked      int
	pushing    bool
}

type snapshot struct {
	created time.Time
	pb      *pubsubpb.Snapshot
	unacked map[string]struct{}
}

func parseID(name, coll string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != projectsSeg || parts[2] != coll || parts[1] == "" || parts[3] == "" {
		return "", false
	}

	return parts[3], true
}

func validID(id string) bool {
	if len(id) < minIDLen || len(id) > maxIDLen || strings.HasPrefix(id, "goog") {
		return false
	}

	for i := range len(id) {
		c := id[i]
		letter := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'

		if i == 0 && !letter {
			return false
		}

		digit := c >= '0' && c <= '9'
		if !letter && !digit && strings.IndexByte("-_.~+%", c) < 0 {
			return false
		}
	}

	return true
}

func checkName(name, coll string) error {
	id, ok := parseID(name, coll)
	if !ok || !validID(id) {
		return errInvalid("Invalid resource name given (name=%s). Refer to "+
			"https://cloud.google.com/pubsub/docs/pubsub-basics#resource_names for more information.", name)
	}

	return nil
}

func projectOf(name string) string {
	rest, ok := strings.CutPrefix(name, "projects/")
	if !ok {
		return ""
	}

	project, _, _ := strings.Cut(rest, "/")

	return project
}

func checkProject(p string) error {
	parts := strings.Split(p, "/")
	if len(parts) != 2 || parts[0] != "projects" || parts[1] == "" {
		return errInvalid("Invalid project name %q. Expected projects/{project}.", p)
	}

	return nil
}
