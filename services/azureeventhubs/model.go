package azureeventhubs

import (
	"time"
)

// Bounds and defaults.
const (
	DefaultNamespace      = "ehemulatorns"
	DefaultKeyName        = "RootManageSharedAccessKey"
	DefaultKeyValue       = "Kq0nF1bPjX5uV9c2yR7wZtHdLsGm3aEoQ8xT4iNvB6w="
	DefaultConsumerGroup  = "$Default"
	DefaultPartitionCount = 4
	MaxPartitionCount     = 1024
	DefaultRetention      = 24 * time.Hour
	MaxRetention          = 90 * 24 * time.Hour
	MaxEventBytes         = 1 << 20
	maxEventsPerPartition = 100000
	maxNameLen            = 256
	maxReceiversPerGroup  = 5
	maxEntitiesPerParent  = 1000
)

// Rights an authorization rule can grant.
const (
	RightListen = "Listen"
	RightSend   = "Send"
	RightManage = "Manage"
)

// HubConfig is the configurable part of an event hub.
type HubConfig struct {
	PartitionCount int           `json:"partitionCount"`
	Retention      time.Duration `json:"retention"`
}

type event struct {
	Enqueued time.Time `json:"enqueued"`
	Key      string    `json:"key,omitempty"`
	Raw      []byte    `json:"raw"`
	Seq      int64     `json:"seq"`
	Offset   int64     `json:"offset"`
}

type partition struct {
	LastTime   time.Time `json:"lastTime"`
	notify     chan struct{}
	Events     []*event `json:"events"`
	NextSeq    int64    `json:"nextSeq"`
	LastOffset int64    `json:"lastOffset"`
}

type hub struct {
	CreatedAt      time.Time            `json:"createdAt"`
	ConsumerGroups map[string]time.Time `json:"consumerGroups"`
	Name           string               `json:"name"`
	Partitions     []*partition         `json:"partitions"`
	Config         HubConfig            `json:"config"`
	RoundRobin     int                  `json:"roundRobin"`
}

type rule struct {
	Name         string   `json:"name"`
	Hub          string   `json:"hub,omitempty"`
	PrimaryKey   string   `json:"primaryKey"`
	SecondaryKey string   `json:"secondaryKey"`
	Rights       []string `json:"rights"`
}

type namespace struct {
	CreatedAt time.Time        `json:"createdAt"`
	Hubs      map[string]*hub  `json:"hubs"`
	Rules     map[string]*rule `json:"rules"`
	Name      string           `json:"name"`
}

// Rule is the exported view of an authorization rule.
type Rule struct {
	Name         string
	PrimaryKey   string
	SecondaryKey string
	Rights       []string
}

// HubInfo is the exported view of an event hub.
type HubInfo struct {
	CreatedAt    time.Time
	Name         string
	Namespace    string
	PartitionIDs []string
	Config       HubConfig
}

// PartitionInfo is the runtime state of one partition.
type PartitionInfo struct {
	LastEnqueuedTime time.Time
	Hub              string
	Partition        string
	BeginSequence    int64
	LastSequence     int64
	LastOffset       int64
	Empty            bool
}

// NewEvent is one event to append: the encoded AMQP message minus the
// broker-owned annotations, and its partition key.
type NewEvent struct {
	Key string
	Raw []byte
}

// HubRef addresses an event hub within a namespace.
type HubRef struct {
	Namespace string
	Hub       string
}
