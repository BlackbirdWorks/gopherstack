package kinesis

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math"
	"net/http"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type jsonRegisterStreamConsumerReq struct {
	Tags         map[string]string `json:"Tags,omitempty"`
	StreamARN    string            `json:"StreamARN"`
	ConsumerName string            `json:"ConsumerName"`
}

// jsonConsumer mirrors types.Consumer (deserializers.go:6279-6349): it has no
// StreamARN member -- that field only exists on types.ConsumerDescription,
// returned by DescribeStreamConsumer. RegisterStreamConsumer and
// ListStreamConsumers both wire types.Consumer.
type jsonConsumer struct {
	ConsumerName              string  `json:"ConsumerName"`
	ConsumerARN               string  `json:"ConsumerARN"`
	ConsumerStatus            string  `json:"ConsumerStatus"`
	ConsumerCreationTimestamp float64 `json:"ConsumerCreationTimestamp"`
}

// jsonConsumerDescription mirrors types.ConsumerDescription
// (deserializers.go:6353-6432), which adds StreamARN on top of jsonConsumer's
// fields.
type jsonConsumerDescription struct {
	ConsumerName              string  `json:"ConsumerName"`
	ConsumerARN               string  `json:"ConsumerARN"`
	ConsumerStatus            string  `json:"ConsumerStatus"`
	StreamARN                 string  `json:"StreamARN"`
	ConsumerCreationTimestamp float64 `json:"ConsumerCreationTimestamp"`
}

type jsonRegisterStreamConsumerResp struct {
	Consumer jsonConsumer `json:"Consumer"`
}

type jsonDescribeStreamConsumerReq struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerARN  string `json:"ConsumerARN"`
	ConsumerName string `json:"ConsumerName"`
}

type jsonDescribeStreamConsumerResp struct {
	ConsumerDescription jsonConsumerDescription `json:"ConsumerDescription"`
}

type jsonListStreamConsumersReq struct {
	StreamARN  string `json:"StreamARN"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type jsonListStreamConsumersResp struct {
	NextToken string         `json:"NextToken,omitempty"`
	Consumers []jsonConsumer `json:"Consumers"`
}

type jsonDeregisterStreamConsumerReq struct {
	StreamARN    string `json:"StreamARN"`
	ConsumerARN  string `json:"ConsumerARN"`
	ConsumerName string `json:"ConsumerName"`
}

type jsonStartingPosition struct {
	Timestamp      *float64 `json:"Timestamp,omitempty"`
	Type           string   `json:"Type"`
	SequenceNumber string   `json:"SequenceNumber,omitempty"`
}

type jsonSubscribeToShardReq struct {
	StartingPosition jsonStartingPosition `json:"StartingPosition"`
	ConsumerARN      string               `json:"ConsumerARN"`
	ShardID          string               `json:"ShardId"`
}

type jsonSubscribeToShardEvent struct {
	ContinuationSequenceNumber string       `json:"ContinuationSequenceNumber"`
	Records                    []jsonRecord `json:"Records"`
	MillisBehindLatest         int64        `json:"MillisBehindLatest"`
}

// toJSONConsumer converts a Consumer to its JSON representation (types.Consumer
// shape -- no StreamARN). Used by RegisterStreamConsumer/ListStreamConsumers.
func toJSONConsumer(c Consumer) jsonConsumer {
	return jsonConsumer{
		ConsumerName:              c.ConsumerName,
		ConsumerARN:               c.ConsumerARN,
		ConsumerStatus:            c.ConsumerStatus,
		ConsumerCreationTimestamp: float64(c.ConsumerCreationTimestamp.UnixMilli()) / millisPerSecond,
	}
}

// toJSONConsumerDescription converts a Consumer to its JSON representation
// (types.ConsumerDescription shape -- includes StreamARN). Used by
// DescribeStreamConsumer.
func toJSONConsumerDescription(c Consumer) jsonConsumerDescription {
	return jsonConsumerDescription{
		ConsumerName:              c.ConsumerName,
		ConsumerARN:               c.ConsumerARN,
		ConsumerStatus:            c.ConsumerStatus,
		ConsumerCreationTimestamp: float64(c.ConsumerCreationTimestamp.UnixMilli()) / millisPerSecond,
		StreamARN:                 c.StreamARN,
	}
}

func (h *Handler) handleRegisterStreamConsumer(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonRegisterStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.RegisterStreamConsumer(ctx, &RegisterStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerName: req.ConsumerName,
		Tags:         req.Tags,
	})
	if err != nil {
		return nil, err
	}

	return jsonRegisterStreamConsumerResp{Consumer: toJSONConsumer(out.Consumer)}, nil
}

func (h *Handler) handleDescribeStreamConsumer(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDescribeStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	out, err := h.Backend.DescribeStreamConsumer(ctx, &DescribeStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerARN:  req.ConsumerARN,
		ConsumerName: req.ConsumerName,
	})
	if err != nil {
		return nil, err
	}

	return jsonDescribeStreamConsumerResp{ConsumerDescription: toJSONConsumerDescription(out.ConsumerDescription)}, nil
}

func (h *Handler) handleListStreamConsumers(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonListStreamConsumersReq
	_ = json.Unmarshal(body, &req)

	out, err := h.Backend.ListStreamConsumers(ctx, &ListStreamConsumersInput{
		StreamARN:  req.StreamARN,
		NextToken:  req.NextToken,
		MaxResults: req.MaxResults,
	})
	if err != nil {
		return nil, err
	}

	consumers := make([]jsonConsumer, len(out.Consumers))
	for i, c := range out.Consumers {
		consumers[i] = toJSONConsumer(c)
	}

	return jsonListStreamConsumersResp{Consumers: consumers, NextToken: out.NextToken}, nil
}

func (h *Handler) handleDeregisterStreamConsumer(
	ctx context.Context,
	_ *http.Request,
	body []byte,
) (any, error) {
	var req jsonDeregisterStreamConsumerReq
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, ErrInvalidArgument
	}

	if err := h.Backend.DeregisterStreamConsumer(ctx, &DeregisterStreamConsumerInput{
		StreamARN:    req.StreamARN,
		ConsumerARN:  req.ConsumerARN,
		ConsumerName: req.ConsumerName,
	}); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

// --- AWS Event Stream encoding for SubscribeToShard ---

// eventStreamHeaderValueTypeString is the AWS event stream type byte for string values.
const eventStreamHeaderValueTypeString = 7

// eventStreamPreludeLen is the number of bytes in an event stream prelude.
const eventStreamPreludeLen = 12

// eventStreamHeaderValueLenBytes is the number of bytes used to encode a header value length.
const eventStreamHeaderValueLenBytes = 2

// eventStreamMsgCRCLen is the number of bytes used for the message CRC field.
const eventStreamMsgCRCLen = 4

// buildEventStreamHeaders encodes the given slice of header name/value pairs as AWS
// event stream binary headers. Headers are encoded in the order provided in the slice.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	var buf bytes.Buffer

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		nameLen := len(name)
		if nameLen > math.MaxUint8 {
			continue
		}

		buf.WriteByte(byte(nameLen))
		buf.WriteString(name)
		buf.WriteByte(eventStreamHeaderValueTypeString)
		vlen := make([]byte, eventStreamHeaderValueLenBytes)
		//nolint:gosec // header value length fits in uint16 by AWS event stream protocol definition
		binary.BigEndian.PutUint16(vlen, uint16(len(value)))
		buf.Write(vlen)
		buf.WriteString(value)
	}

	return buf.Bytes()
}

// encodeEventStreamMsg encodes a single AWS event stream binary message.
// Format: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
func encodeEventStreamMsg(hdrs [][2]string, payload []byte) []byte {
	hdrBytes := buildEventStreamHeaders(hdrs)
	headerLen := len(hdrBytes)
	payloadLen := len(payload)
	// prelude (12 bytes) + headers + payload + message CRC (4 bytes)
	// Guard against integer overflow when calculating totalLen.
	totalLen := uint64(eventStreamPreludeLen) + uint64(headerLen) + uint64(payloadLen) + uint64(eventStreamMsgCRCLen)
	if totalLen > math.MaxInt32 {
		return nil
	}

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	//nolint:gosec // headerLen is bounded by AWS event stream protocol constraints
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen))

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:eventStreamPreludeLen], preludeCRC)

	copy(buf[eventStreamPreludeLen:eventStreamPreludeLen+headerLen], hdrBytes)
	copy(buf[eventStreamPreludeLen+headerLen:eventStreamPreludeLen+headerLen+payloadLen], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0 : eventStreamPreludeLen+headerLen+payloadLen])
	binary.BigEndian.PutUint32(buf[eventStreamPreludeLen+headerLen+payloadLen:], msgCRC)

	return buf
}

// subscribeToShardStreamDuration is how long a SubscribeToShard stream stays open (~5 min).
const subscribeToShardStreamDuration = 5 * time.Minute

// subscribeToShardPollInterval is the poll interval between record checks.
const subscribeToShardPollInterval = 200 * time.Millisecond

// subscribeToShardMaxIdlePolls is the number of consecutive empty polls before the stream
// is closed gracefully.  AWS clients re-subscribe after a stream closes, so closing on
// idle is safe.  Keeping this small (3 × 200 ms = 600 ms) ensures tests complete quickly.
const subscribeToShardMaxIdlePolls = 3

// handleSubscribeToShardHTTP handles the SubscribeToShard operation using the AWS event stream
// binary protocol. It keeps the response stream open for up to 5 minutes, pushing records as
// they arrive via periodic polling with chunked flushing.
func (h *Handler) handleSubscribeToShardHTTP(c *echo.Context) error {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.defaultRegion())
	ctx := contextWithRegion(c.Request().Context(), region)
	log := logger.Load(ctx)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "SubscribeToShard: failed to read body", "error", err)

		return h.handleError(ctx, c, "SubscribeToShard", err)
	}

	var req jsonSubscribeToShardReq
	if err = json.Unmarshal(body, &req); err != nil {
		return h.handleError(ctx, c, "SubscribeToShard", ErrInvalidArgument)
	}

	sp := StartingPosition{
		Type:           req.StartingPosition.Type,
		SequenceNumber: req.StartingPosition.SequenceNumber,
	}

	if req.StartingPosition.Timestamp != nil {
		ts := time.UnixMilli(int64(*req.StartingPosition.Timestamp * millisPerSecond))
		sp.Timestamp = &ts
	}

	// Validate consumer/shard before opening the stream.
	if _, err = h.Backend.SubscribeToShard(ctx, &SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: sp,
	}); err != nil {
		return h.handleError(ctx, c, "SubscribeToShard", err)
	}

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)

	flusher, canFlush := c.Response().(http.Flusher)

	// Send initial-response so the SDK event-stream middleware unblocks.
	initialMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "initial-response"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, []byte("{}"))
	if _, writeErr := c.Response().Write(initialMsg); writeErr != nil {
		return writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	deadline := time.Now().Add(subscribeToShardStreamDuration)
	ticker := time.NewTicker(subscribeToShardPollInterval)
	defer ticker.Stop()

	curSP := sp
	idlePolls := 0

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if time.Now().After(deadline) {
				return nil
			}

			if stop, next := h.advanceShardCursor(ctx, req, curSP, c.Response(), flusher, canFlush, &idlePolls); stop {
				return nil
			} else if next != nil {
				curSP = *next
			}
		}
	}
}

// advanceShardCursor calls pollSubscribeToShardTick and returns (stop=true, nil) when the
// stream should close, or (false, nextSP) when it should continue (nextSP may be nil).
func (h *Handler) advanceShardCursor(
	ctx context.Context,
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	idlePolls *int,
) (bool, *StartingPosition) {
	done, next, tickErr := h.pollSubscribeToShardTick(ctx, req, curSP, w, flusher, canFlush, idlePolls)
	if tickErr != nil || done {
		return true, nil
	}

	return false, next
}

// pollSubscribeToShardTick performs one poll tick for handleSubscribeToShardHTTP.
// Returns (true, nil, err) when the stream should close (poll error or idle limit reached),
// (false, nextSP, nil) when records were delivered (nextSP non-nil means cursor advanced),
// and (false, nil, err) on a write error.
func (h *Handler) pollSubscribeToShardTick(
	ctx context.Context,
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	idlePolls *int,
) (bool, *StartingPosition, error) {
	out, pollErr := h.Backend.SubscribeToShard(ctx, &SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: curSP,
	})
	if pollErr != nil {
		return true, nil, pollErr
	}

	if len(out.Event.Records) == 0 {
		*idlePolls++
		if *idlePolls >= subscribeToShardMaxIdlePolls {
			return true, nil, nil
		}

		return false, nil, nil
	}
	*idlePolls = 0

	records := make([]jsonRecord, len(out.Event.Records))
	for i, r := range out.Event.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisPerSecond,
		}
	}

	eventPayload, marshalErr := json.Marshal(jsonSubscribeToShardEvent{
		Records:                    records,
		ContinuationSequenceNumber: out.Event.ContinuationSequenceNumber,
		MillisBehindLatest:         out.Event.MillisBehindLatest,
	})
	if marshalErr != nil {
		return false, nil, marshalErr
	}

	eventMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "SubscribeToShardEvent"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, eventPayload)

	if _, writeErr := w.Write(eventMsg); writeErr != nil {
		return false, nil, writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	if out.Event.ContinuationSequenceNumber != "" {
		sp := StartingPosition{
			Type:           iteratorTypeAfterSequenceNumber,
			SequenceNumber: out.Event.ContinuationSequenceNumber,
		}

		return false, &sp, nil
	}

	return false, nil, nil
}
