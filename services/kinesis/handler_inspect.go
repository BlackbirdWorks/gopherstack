package kinesis

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const (
	inspectPrefix        = "/_gopherstack/kinesis/"
	inspectConsumersPath = inspectPrefix + "consumers/"
)

// activeSubscription is one in-flight SubscribeToShard stream.
type activeSubscription struct {
	startedAt time.Time
	expiresAt time.Time
	shardID   string
	position  string
}

// subscriptionTracker records the SubscribeToShard streams currently open,
// keyed by consumer ARN, for the local inspection API.
type subscriptionTracker struct {
	subs map[string]map[int]activeSubscription
	mu   sync.Mutex
	next int
}

func (t *subscriptionTracker) add(consumerARN string, sub activeSubscription) func() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.subs == nil {
		t.subs = map[string]map[int]activeSubscription{}
	}

	if t.subs[consumerARN] == nil {
		t.subs[consumerARN] = map[int]activeSubscription{}
	}

	t.next++
	id := t.next
	t.subs[consumerARN][id] = sub

	return func() {
		t.mu.Lock()
		defer t.mu.Unlock()

		delete(t.subs[consumerARN], id)

		if len(t.subs[consumerARN]) == 0 {
			delete(t.subs, consumerARN)
		}
	}
}

func (t *subscriptionTracker) list(consumerARN string) []activeSubscription {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]activeSubscription, 0, len(t.subs[consumerARN]))
	for _, s := range t.subs[consumerARN] {
		out = append(out, s)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].startedAt.Before(out[j].startedAt) })

	return out
}

type inspectInspector interface {
	InspectStreamShards(ctx context.Context, name string) (*InspectedStream, error)
	InspectShardRecords(ctx context.Context, name, shardID, fromSeq string, limit int) (*InspectedRecords, error)
	InspectConsumer(ctx context.Context, consumerARN string) (*Consumer, error)
}

type inspectShardJSON struct {
	ShardID               string           `json:"ShardId"`
	ParentShardID         string           `json:"ParentShardId,omitempty"`
	AdjacentParentShardID string           `json:"AdjacentParentShardId,omitempty"`
	HashKeyRange          jsonHashKeyRange `json:"HashKeyRange"`
	SequenceNumberRange   jsonSeqNumRange  `json:"SequenceNumberRange"`
	ChildShardIDs         []string         `json:"ChildShardIds"`
	RecordCount           int              `json:"RecordCount"`
	Closed                bool             `json:"Closed"`
}

type inspectStreamJSON struct {
	StreamName string             `json:"StreamName"`
	StreamARN  string             `json:"StreamARN"`
	Status     string             `json:"StreamStatus"`
	Shards     []inspectShardJSON `json:"Shards"`
}

type inspectRecordJSON struct {
	SequenceNumber              string `json:"SequenceNumber"`
	PartitionKey                string `json:"PartitionKey"`
	ApproximateArrivalTimestamp string `json:"ApproximateArrivalTimestamp"`
	Data                        []byte `json:"Data"`
}

type inspectRecordsJSON struct {
	ShardID string              `json:"ShardId"`
	Records []inspectRecordJSON `json:"Records"`
	Count   int                 `json:"Count"`
	Closed  bool                `json:"Closed"`
}

type inspectSubscriptionJSON struct {
	ShardID      string `json:"ShardId"`
	StartingType string `json:"StartingPositionType"`
	StartedAt    string `json:"StartedAt"`
	ExpiresAt    string `json:"ExpiresAt"`
}

type inspectConsumerJSON struct {
	ConsumerName      string                    `json:"ConsumerName"`
	ConsumerARN       string                    `json:"ConsumerARN"`
	ConsumerStatus    string                    `json:"ConsumerStatus"`
	StreamARN         string                    `json:"StreamARN"`
	CreationTimestamp string                    `json:"ConsumerCreationTimestamp"`
	Subscriptions     []inspectSubscriptionJSON `json:"Subscriptions"`
}

// RegisterInspectionRoutes mounts the read-only, emulator-only inspection API
// under /_gopherstack/kinesis/.
func (h *Handler) RegisterInspectionRoutes(e *echo.Echo) {
	e.GET(inspectPrefix+"streams/:name/shards", h.ServeInspectShards)
	e.GET(inspectPrefix+"streams/:name/shards/:shardId/records", h.ServeInspectRecords)
	e.GET(inspectConsumersPath+"*", h.ServeInspectConsumer)
}

func (h *Handler) inspectContext(c *echo.Context) context.Context {
	region := c.QueryParam("region")
	if region == "" {
		region = httputils.ExtractRegionFromRequest(c.Request(), h.defaultRegion())
	}

	return contextWithRegion(c.Request().Context(), region)
}

func inspectError(c *echo.Context, err error) error {
	var status int

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, ErrInvalidArgument):
		status = http.StatusBadRequest
	default:
		status = http.StatusInternalServerError
	}

	return c.JSON(status, map[string]string{"error": err.Error()})
}

func (h *Handler) inspector(c *echo.Context) (inspectInspector, bool) {
	ins, ok := h.Backend.(inspectInspector)
	if !ok {
		_ = c.JSON(http.StatusNotImplemented, map[string]string{"error": "backend does not support inspection"})
	}

	return ins, ok
}

// ServeInspectShards serves GET /_gopherstack/kinesis/streams/<name>/shards.
func (h *Handler) ServeInspectShards(c *echo.Context) error {
	ins, ok := h.inspector(c)
	if !ok {
		return nil
	}

	st, err := ins.InspectStreamShards(h.inspectContext(c), c.Param("name"))
	if err != nil {
		return inspectError(c, err)
	}

	out := inspectStreamJSON{
		StreamName: st.StreamName,
		StreamARN:  st.StreamARN,
		Status:     st.Status,
		Shards:     make([]inspectShardJSON, 0, len(st.Shards)),
	}

	for _, s := range st.Shards {
		out.Shards = append(out.Shards, inspectShardJSON{
			ShardID:               s.ShardID,
			ParentShardID:         s.ParentShardID,
			AdjacentParentShardID: s.AdjacentParentShardID,
			HashKeyRange: jsonHashKeyRange{
				StartingHashKey: s.HashKeyRangeStart,
				EndingHashKey:   s.HashKeyRangeEnd,
			},
			SequenceNumberRange: jsonSeqNumRange{
				StartingSequenceNumber: s.SequenceNumberRangeStart,
				EndingSequenceNumber:   s.SequenceNumberRangeEnd,
			},
			ChildShardIDs: s.ChildShardIDs,
			RecordCount:   s.RecordCount,
			Closed:        s.Closed,
		})
	}

	return c.JSON(http.StatusOK, out)
}

// ServeInspectRecords serves GET /_gopherstack/kinesis/streams/<name>/shards/<id>/records.
func (h *Handler) ServeInspectRecords(c *echo.Context) error {
	ins, ok := h.inspector(c)
	if !ok {
		return nil
	}

	limit := 0

	if raw := c.QueryParam("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			return inspectError(c, ErrInvalidArgument)
		}

		limit = n
	}

	res, err := ins.InspectShardRecords(
		h.inspectContext(c), c.Param("name"), c.Param("shardId"), c.QueryParam("from"), limit,
	)
	if err != nil {
		return inspectError(c, err)
	}

	out := inspectRecordsJSON{
		ShardID: res.ShardID,
		Closed:  res.Closed,
		Count:   len(res.Records),
		Records: make([]inspectRecordJSON, 0, len(res.Records)),
	}

	for _, r := range res.Records {
		out.Records = append(out.Records, inspectRecordJSON{
			SequenceNumber:              r.SequenceNumber,
			PartitionKey:                r.PartitionKey,
			ApproximateArrivalTimestamp: r.ApproximateArrivalTimestamp.UTC().Format(time.RFC3339Nano),
			Data:                        r.Data,
		})
	}

	return c.JSON(http.StatusOK, out)
}

// ServeInspectConsumer serves GET /_gopherstack/kinesis/consumers/<consumer-arn>.
func (h *Handler) ServeInspectConsumer(c *echo.Context) error {
	ins, ok := h.inspector(c)
	if !ok {
		return nil
	}

	arn := strings.TrimPrefix(c.Request().URL.Path, inspectConsumersPath)

	consumer, err := ins.InspectConsumer(h.inspectContext(c), arn)
	if err != nil {
		return inspectError(c, err)
	}

	subs := h.subscriptions.list(consumer.ConsumerARN)
	out := inspectConsumerJSON{
		ConsumerName:      consumer.ConsumerName,
		ConsumerARN:       consumer.ConsumerARN,
		ConsumerStatus:    consumer.ConsumerStatus,
		StreamARN:         consumer.StreamARN,
		CreationTimestamp: consumer.ConsumerCreationTimestamp.UTC().Format(time.RFC3339Nano),
		Subscriptions:     make([]inspectSubscriptionJSON, 0, len(subs)),
	}

	for _, s := range subs {
		out.Subscriptions = append(out.Subscriptions, inspectSubscriptionJSON{
			ShardID:      s.shardID,
			StartingType: s.position,
			StartedAt:    s.startedAt.UTC().Format(time.RFC3339Nano),
			ExpiresAt:    s.expiresAt.UTC().Format(time.RFC3339Nano),
		})
	}

	return c.JSON(http.StatusOK, out)
}
