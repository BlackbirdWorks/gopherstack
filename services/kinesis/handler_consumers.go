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

// defaultSubscribeToShardStreamDuration is how long a SubscribeToShard
// stream stays open by default: "Kinesis Data Streams then starts pushing
// the records from that shard to you ... over an HTTP/2 connection. The
// connection remains open for up to 5 minutes."
// (docs.aws.amazon.com/streams/latest/dev/building-enhanced-consumers-api.html).
// Overridable per Handler; see WithSubscribeToShardTiming (handler.go).
const defaultSubscribeToShardStreamDuration = 5 * time.Minute

// defaultSubscribeToShardPollInterval is how often the emulator checks the
// shard for new records while a SubscribeToShard stream is open. Real AWS
// pushes as data arrives rather than polling; this is the polling-emulation
// tick, deliberately short so newly Put records are delivered promptly.
const defaultSubscribeToShardPollInterval = 200 * time.Millisecond

// defaultSubscribeToShardHeartbeatInterval is how often an empty
// SubscribeToShardEvent (a heartbeat) is sent while idle, instead of
// closing the stream. API_SubscribeToShardEvent.html documents
// ContinuationSequenceNumber as required even with no records ("captures
// your shard progress even when no data is written to the shard"), which
// implies periodic empty events keep the connection alive for the full
// 5-minute window -- but neither that page nor
// building-enhanced-consumers-api.html states an exact interval. This value
// is a disclosed inference (see PARITY.md), not a verified AWS constant.
const defaultSubscribeToShardHeartbeatInterval = 5 * time.Second

// handleSubscribeToShardHTTP handles the SubscribeToShard operation using the AWS event stream
// binary protocol. It keeps the response stream open for up to 5 minutes, pushing records as
// they arrive via periodic polling with chunked flushing.
func (h *Handler) handleSubscribeToShardHTTP(c *echo.Context) error {
	region := httputils.ExtractRegionFromRequest(c.Request(), h.defaultRegion())
	ctx := contextWithRegion(c.Request().Context(), region)

	req, sp, ok, err := h.parseSubscribeToShardRequest(ctx, c)
	if !ok {
		return err
	}

	flusher, canFlush, err := h.openSubscribeToShardStream(c)
	if err != nil {
		return err
	}

	return h.runSubscribeToShardStream(ctx, c, req, sp, flusher, canFlush)
}

// parseSubscribeToShardRequest reads and JSON-decodes the request body,
// builds the initial StartingPosition, and validates the consumer/shard
// against the backend before any streaming response is written. When ok is
// false, an error response has already been written via h.handleError (err
// is what the caller should return to satisfy the echo handler signature,
// which may itself be nil).
func (h *Handler) parseSubscribeToShardRequest(
	ctx context.Context,
	c *echo.Context,
) (jsonSubscribeToShardReq, StartingPosition, bool, error) {
	log := logger.Load(ctx)

	var req jsonSubscribeToShardReq

	body, readErr := httputils.ReadBody(c.Request())
	if readErr != nil {
		log.ErrorContext(ctx, "SubscribeToShard: failed to read body", "error", readErr)

		return req, StartingPosition{}, false, h.handleError(ctx, c, "SubscribeToShard", readErr)
	}

	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return req, StartingPosition{}, false, h.handleError(ctx, c, "SubscribeToShard", ErrInvalidArgument)
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
	if _, subErr := h.Backend.SubscribeToShard(ctx, &SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: sp,
	}); subErr != nil {
		return req, sp, false, h.handleError(ctx, c, "SubscribeToShard", subErr)
	}

	return req, sp, true, nil
}

// openSubscribeToShardStream writes the event-stream response headers and
// the initial-response frame the SDK's event-stream middleware waits for to
// unblock, returning the response's http.Flusher (if any) for the caller's
// streaming loop.
func (h *Handler) openSubscribeToShardStream(c *echo.Context) (http.Flusher, bool, error) {
	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)

	flusher, canFlush := c.Response().(http.Flusher)

	initialMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "initial-response"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, []byte("{}"))
	if _, writeErr := c.Response().Write(initialMsg); writeErr != nil {
		return flusher, canFlush, writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	return flusher, canFlush, nil
}

// runSubscribeToShardStream drives the event-stream response until the
// stream's deadline elapses (h.subscribeToShardStreamDuration) or ctx is
// cancelled: it polls the backend every h.subscribeToShardPollInterval,
// delivering new records immediately and sending a heartbeat
// SubscribeToShardEvent (empty Records) once
// h.subscribeToShardHeartbeatInterval has elapsed since the last frame.
func (h *Handler) runSubscribeToShardStream(
	ctx context.Context,
	c *echo.Context,
	req jsonSubscribeToShardReq,
	sp StartingPosition,
	flusher http.Flusher,
	canFlush bool,
) error {
	deadline := time.Now().Add(h.subscribeToShardStreamDuration)
	curSP := sp
	lastEventAt := time.Now()

	// Check immediately: a consumer commonly subscribes (e.g. TRIM_HORIZON)
	// after data was already written, and delivering it here avoids making
	// that first event wait on the poll interval's next tick. heartbeatDue
	// is false on this first check -- an idle subscription's first
	// heartbeat waits for the normal cadence, matching the ticker path
	// below, rather than firing instantly at t=0.
	stop, next, sentEvent := h.advanceShardCursor(ctx, req, curSP, c.Response(), flusher, canFlush, false)
	if stop {
		return nil
	}
	if next != nil {
		curSP = *next
	}
	if sentEvent {
		lastEventAt = time.Now()
	}

	ticker := time.NewTicker(h.subscribeToShardPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			if now.After(deadline) {
				return nil
			}

			heartbeatDue := now.Sub(lastEventAt) >= h.subscribeToShardHeartbeatInterval

			tickStop, tickNext, tickSentEvent := h.advanceShardCursor(
				ctx,
				req,
				curSP,
				c.Response(),
				flusher,
				canFlush,
				heartbeatDue,
			)
			if tickStop {
				return nil
			}
			if tickNext != nil {
				curSP = *tickNext
			}
			if tickSentEvent {
				lastEventAt = now
			}
		}
	}
}

// advanceShardCursor calls pollSubscribeToShardTick and returns (stop=true) when the
// stream should close (a backend or write error), or (false, nextSP, sentEvent)
// when it should continue (nextSP may be nil; sentEvent reports whether a
// SubscribeToShardEvent frame -- data or heartbeat -- was actually written).
func (h *Handler) advanceShardCursor(
	ctx context.Context,
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	heartbeatDue bool,
) (bool, *StartingPosition, bool) {
	next, sentEvent, err := h.pollSubscribeToShardTick(ctx, req, curSP, w, flusher, canFlush, heartbeatDue)
	if err != nil {
		return true, nil, false
	}

	return false, next, sentEvent
}

// pollSubscribeToShardTick performs one poll tick for handleSubscribeToShardHTTP.
// When the shard has no new records, a SubscribeToShardEvent frame is only
// written if heartbeatDue is set -- otherwise this tick is a silent no-op,
// so heartbeats fire on their own cadence (subscribeToShardHeartbeatInterval)
// independent of the faster poll tick used to detect new data promptly.
// Returns the advanced StartingPosition (nil if unchanged), whether a frame
// was written, and any backend/write error (which always closes the stream).
func (h *Handler) pollSubscribeToShardTick(
	ctx context.Context,
	req jsonSubscribeToShardReq,
	curSP StartingPosition,
	w http.ResponseWriter,
	flusher http.Flusher,
	canFlush bool,
	heartbeatDue bool,
) (*StartingPosition, bool, error) {
	out, pollErr := h.Backend.SubscribeToShard(ctx, &SubscribeToShardInput{
		ConsumerARN:      req.ConsumerARN,
		ShardID:          req.ShardID,
		StartingPosition: curSP,
	})
	if pollErr != nil {
		return nil, false, pollErr
	}

	if len(out.Event.Records) == 0 && !heartbeatDue {
		return nil, false, nil
	}

	records := make([]jsonRecord, len(out.Event.Records))
	for i, r := range out.Event.Records {
		records[i] = jsonRecord{
			Data:                        r.Data,
			PartitionKey:                r.PartitionKey,
			SequenceNumber:              r.SequenceNumber,
			EncryptionType:              r.EncryptionType,
			ApproximateArrivalTimestamp: float64(r.ApproximateArrivalTimestamp.UnixMilli()) / millisPerSecond,
		}
	}

	eventPayload, marshalErr := json.Marshal(jsonSubscribeToShardEvent{
		Records:                    records,
		ContinuationSequenceNumber: out.Event.ContinuationSequenceNumber,
		MillisBehindLatest:         out.Event.MillisBehindLatest,
	})
	if marshalErr != nil {
		return nil, false, marshalErr
	}

	eventMsg := encodeEventStreamMsg([][2]string{
		{":event-type", "SubscribeToShardEvent"},
		{":message-type", "event"},
		{":content-type", "application/json"},
	}, eventPayload)

	if _, writeErr := w.Write(eventMsg); writeErr != nil {
		return nil, false, writeErr
	}
	if canFlush {
		flusher.Flush()
	}

	if out.Event.ContinuationSequenceNumber != "" {
		sp := StartingPosition{
			Type:           iteratorTypeAfterSequenceNumber,
			SequenceNumber: out.Event.ContinuationSequenceNumber,
		}

		return &sp, true, nil
	}

	return nil, true, nil
}
