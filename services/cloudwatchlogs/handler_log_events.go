package cloudwatchlogs

import (
	"context"
	"encoding/json"
	"fmt"
)

type putLogEventsInput struct {
	LogGroupName  string          `json:"logGroupName"`
	LogStreamName string          `json:"logStreamName"`
	SequenceToken string          `json:"sequenceToken,omitempty"`
	LogEvents     []InputLogEvent `json:"logEvents"`
}

type getLogEventsInput struct {
	StartTime     *int64 `json:"startTime"`
	EndTime       *int64 `json:"endTime"`
	LogGroupName  string `json:"logGroupName"`
	LogStreamName string `json:"logStreamName"`
	NextToken     string `json:"nextToken"`
	Limit         int    `json:"limit"`
	StartFromHead bool   `json:"startFromHead"`
}

type filterLogEventsInput struct {
	StartTime           *int64   `json:"startTime"`
	EndTime             *int64   `json:"endTime"`
	LogGroupName        string   `json:"logGroupName"`
	FilterPattern       string   `json:"filterPattern"`
	NextToken           string   `json:"nextToken"`
	LogStreamNamePrefix string   `json:"logStreamNamePrefix"`
	LogStreamNames      []string `json:"logStreamNames"`
	Limit               int      `json:"limit"`
}

type putLogEventsOutput struct {
	RejectedLogEventsInfo *RejectedLogEventsInfo `json:"rejectedLogEventsInfo,omitempty"`
	NextSequenceToken     string                 `json:"nextSequenceToken"`
}

type getLogEventsOutput struct {
	NextForwardToken  string           `json:"nextForwardToken"`
	NextBackwardToken string           `json:"nextBackwardToken"`
	Events            []OutputLogEvent `json:"events"`
}

type filterLogEventsOutput struct {
	NextToken          string              `json:"nextToken,omitempty"`
	Events             []FilteredLogEvent  `json:"events"`
	SearchedLogStreams []SearchedLogStream `json:"searchedLogStreams"`
}

// --- GetLogRecord ---.
type getLogRecordInput struct {
	LogRecordPointer string `json:"logRecordPointer"`
}

type getLogRecordOutput struct {
	LogRecord map[string]string `json:"logRecord"`
}

func (h *Handler) logEventActions() map[string]actionFn {
	return map[string]actionFn{
		"PutLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input putLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			result, err := h.Backend.PutLogEvents(
				ctx,
				input.LogGroupName,
				input.LogStreamName,
				input.SequenceToken,
				input.LogEvents,
			)
			if err != nil {
				return nil, err
			}

			return &putLogEventsOutput{
				NextSequenceToken:     result.NextSequenceToken,
				RejectedLogEventsInfo: result.RejectedLogEventsInfo,
			}, nil
		},
		"GetLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input getLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, fwd, bwd, err := h.Backend.GetLogEvents(ctx,
				input.LogGroupName, input.LogStreamName, input.StartTime, input.EndTime,
				input.Limit, input.NextToken, input.StartFromHead)
			if err != nil {
				return nil, err
			}

			return &getLogEventsOutput{
				Events:            evts,
				NextForwardToken:  fwd,
				NextBackwardToken: bwd,
			}, nil
		},
		"FilterLogEvents": func(ctx context.Context, b []byte) (any, error) {
			var input filterLogEventsInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			evts, next, searched, err := h.Backend.FilterLogEvents(ctx, FilterLogEventsParams{
				GroupName:           input.LogGroupName,
				StreamNames:         input.LogStreamNames,
				LogStreamNamePrefix: input.LogStreamNamePrefix,
				FilterPattern:       input.FilterPattern,
				StartTime:           input.StartTime,
				EndTime:             input.EndTime,
				Limit:               input.Limit,
				NextToken:           input.NextToken,
			})
			if err != nil {
				return nil, err
			}

			return &filterLogEventsOutput{
				Events:             evts,
				SearchedLogStreams: searched,
				NextToken:          next,
			}, nil
		},
	}
}

func (h *Handler) handleGetLogRecord(ctx context.Context, b []byte) (any, error) {
	var input getLogRecordInput
	if err := json.Unmarshal(b, &input); err != nil {
		return nil, err
	}
	record, err := h.Backend.GetLogRecord(ctx, input.LogRecordPointer)
	if err != nil {
		return nil, err
	}

	return &getLogRecordOutput{LogRecord: record}, nil
}

// handleGetLogFields returns the set of field names discovered from the log
// events stored for the given log group. The AWS SDK models this operation with
// a dataSourceName/dataSourceType pair; the data source name is interpreted as
// the log group (name or ARN identifier). The log group must exist; otherwise a
// ResourceNotFoundException is returned. The response uses the AWS logFields
// shape (a list of {logFieldName} items), derived from real stored events.
func (h *Handler) handleGetLogFields(ctx context.Context, body []byte) (any, error) {
	var input struct {
		DataSourceName     string `json:"dataSourceName"`
		DataSourceType     string `json:"dataSourceType"`
		LogGroupIdentifier string `json:"logGroupIdentifier"`
		LogGroupName       string `json:"logGroupName"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	name := input.LogGroupName
	if name == "" {
		name = normalizeLogGroupIdentifier(input.LogGroupIdentifier)
	}
	if name == "" {
		name = normalizeLogGroupIdentifier(input.DataSourceName)
	}
	if name == "" {
		return nil, fmt.Errorf("%w: dataSourceName is required", ErrValidation)
	}
	if input.DataSourceType == "" {
		return nil, fmt.Errorf("%w: dataSourceType is required", ErrValidation)
	}

	b := cwlBackend(h)
	if b == nil {
		return map[string]any{"logFields": []any{}}, nil
	}

	fields, err := b.DiscoverLogFields(ctx, name)
	if err != nil {
		return nil, err
	}

	logFields := make([]map[string]any, 0, len(fields))
	for _, f := range fields {
		logFields = append(logFields, map[string]any{"logFieldName": f})
	}

	return map[string]any{"logFields": logFields}, nil
}

// handleGetLogObject resolves a stored log event from its log record pointer and
// returns the record fields. The pointer is the same base64-encoded pointer
// returned by GetLogRecord; an unresolvable pointer yields a
// ResourceNotFoundException (or InvalidParameterException for malformed input).
func (h *Handler) handleGetLogObject(ctx context.Context, body []byte) (any, error) {
	var input struct {
		LogObjectPointer string `json:"logObjectPointer"`
		LogRecordPointer string `json:"logRecordPointer"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	pointer := input.LogObjectPointer
	if pointer == "" {
		pointer = input.LogRecordPointer
	}
	if pointer == "" {
		return nil, fmt.Errorf("%w: logObjectPointer is required", ErrValidation)
	}

	b := cwlBackend(h)
	if b == nil {
		return nil, fmt.Errorf("%w: log object %s not found", ErrLogStreamNotFound, pointer)
	}

	record, err := b.GetLogRecord(ctx, pointer)
	if err != nil {
		return nil, err
	}

	return map[string]any{"fieldStream": record}, nil
}

// putBearerTokenAuthenticationInput mirrors PutBearerTokenAuthenticationInput
// (api_op_PutBearerTokenAuthentication.go:33-58). No token material is ever
// present on this request -- BearerTokenAuthenticationEnabled only turns the
// feature on/off for the log group; the token itself is carried on later,
// separately authenticated requests, never here.
type putBearerTokenAuthenticationInput struct {
	BearerTokenAuthenticationEnabled *bool  `json:"bearerTokenAuthenticationEnabled"`
	LogGroupIdentifier               string `json:"logGroupIdentifier"`
}

func (h *Handler) handlePutBearerTokenAuthentication(
	ctx context.Context, body []byte,
) (any, error) {
	var in putBearerTokenAuthenticationInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON: %w", ErrValidation, err)
	}

	if in.LogGroupIdentifier == "" {
		return nil, fmt.Errorf("%w: logGroupIdentifier is required", ErrValidation)
	}

	if in.BearerTokenAuthenticationEnabled == nil {
		return nil, fmt.Errorf("%w: bearerTokenAuthenticationEnabled is required", ErrValidation)
	}

	b := cwlBackend(h)
	if b == nil {
		return nil, fmt.Errorf("%w: Log group %s not found", ErrLogGroupNotFound, in.LogGroupIdentifier)
	}

	if err := b.PutBearerTokenAuthentication(
		ctx, in.LogGroupIdentifier, *in.BearerTokenAuthenticationEnabled,
	); err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

// handleStartLiveTail is validation-only. StartLiveTail is a streaming (HTTP/2
// event-stream) operation that cannot be meaningfully emulated over the standard
// unary JSON response, so rather than returning a silent empty success this
// handler validates the supplied log group ARNs/identifiers and returns
// ResourceNotFoundException when any does not exist. A valid request returns an
// empty (but well-formed) responseStream.
func (h *Handler) handleStartLiveTail(ctx context.Context, body []byte) (any, error) {
	var input struct {
		LogGroupIdentifiers []string `json:"logGroupIdentifiers"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	if len(input.LogGroupIdentifiers) == 0 {
		return nil, fmt.Errorf("%w: logGroupIdentifiers is required", ErrValidation)
	}

	if b := cwlBackend(h); b != nil {
		if err := b.ValidateLiveTailLogGroups(ctx, input.LogGroupIdentifiers); err != nil {
			return nil, err
		}
	}

	return map[string]any{"responseStream": map[string]any{}}, nil
}
