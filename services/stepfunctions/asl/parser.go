// Package asl implements an interpreter for the Amazon States Language (ASL)
// used by AWS Step Functions to define state machine workflows.
package asl

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrParseError is returned when the state machine definition cannot be parsed.
var ErrParseError = errors.New("parse error")

// StateMachine represents a parsed ASL state machine definition.
type StateMachine struct {
	States  map[string]*State `json:"States"`
	Comment string            `json:"Comment,omitempty"`
	StartAt string            `json:"StartAt"`
}

// ItemBatcher configures batching for a Map state's Distributed Map.
type ItemBatcher struct {
	// MaxItemsPerBatch is the maximum number of items per batch. Zero means no limit.
	MaxItemsPerBatch int `json:"MaxItemsPerBatch,omitempty"`
	// MaxInputBytesPerBatch is the max total JSON bytes per batch. Zero means no limit.
	MaxInputBytesPerBatch int `json:"MaxInputBytesPerBatch,omitempty"`
}

// ItemReader configures reading items from S3 for a Map state's Distributed Map.
type ItemReader struct {
	Parameters   map[string]any `json:"Parameters,omitempty"`
	ReaderConfig *ReaderConfig  `json:"ReaderConfig,omitempty"`
	Resource     string         `json:"Resource,omitempty"`
}

// ReaderConfig describes how the ItemReader should interpret S3 object data.
// InputType: "JSON" (default), "JSONL", or "CSV".
// CSVHeaderLocation: "FIRST_ROW" or "GIVEN".
// CSVHeaders: explicit headers when CSVHeaderLocation == "GIVEN".
// MaxItems: optional cap on number of items returned (0 = unlimited).
type ReaderConfig struct {
	InputType         string   `json:"InputType,omitempty"`
	CSVHeaderLocation string   `json:"CSVHeaderLocation,omitempty"`
	CSVHeaders        []string `json:"CSVHeaders,omitempty"`
	MaxItems          int      `json:"MaxItems,omitempty"`
}

// State represents a single state in the state machine.
type State struct {
	Iterator      *StateMachine   `json:"Iterator,omitempty"`
	ItemProcessor *StateMachine   `json:"ItemProcessor,omitempty"`
	ItemBatcher   *ItemBatcher    `json:"ItemBatcher,omitempty"`
	ItemReader    *ItemReader     `json:"ItemReader,omitempty"`
	ItemSelector  json.RawMessage `json:"ItemSelector,omitempty"`
	SecondsPath   string          `json:"SecondsPath,omitempty"`
	TimestampPath string          `json:"TimestampPath,omitempty"`
	ItemsPath     string          `json:"ItemsPath,omitempty"`
	// ToleratedFailureCount/Percentage (and their *Path variants) bound how many
	// Map iterations may fail before the Map state itself fails with
	// States.ExceedToleratedFailureThreshold. AWS supports these only for
	// Distributed Map, but the emulator applies them uniformly since Map
	// processing mode is not otherwise distinguished. When both a count and a
	// percentage are set, the Map fails when EITHER threshold is crossed.
	ToleratedFailureCountPath      string          `json:"ToleratedFailureCountPath,omitempty"`
	ToleratedFailurePercentagePath string          `json:"ToleratedFailurePercentagePath,omitempty"`
	ToleratedFailureCount          *int            `json:"ToleratedFailureCount,omitempty"`
	ToleratedFailurePercentage     *float64        `json:"ToleratedFailurePercentage,omitempty"`
	InputPath                      string          `json:"InputPath,omitempty"`
	OutputPath                     string          `json:"OutputPath,omitempty"`
	ResultPath                     string          `json:"ResultPath,omitempty"`
	Type                           string          `json:"Type"`
	Error                          string          `json:"Error,omitempty"`
	Cause                          string          `json:"Cause,omitempty"`
	Comment                        string          `json:"Comment,omitempty"`
	Next                           string          `json:"Next,omitempty"`
	Default                        string          `json:"Default,omitempty"`
	Timestamp                      string          `json:"Timestamp,omitempty"`
	Resource                       string          `json:"Resource,omitempty"`
	Retry                          []Retrier       `json:"Retry,omitempty"`
	Catch                          []Catcher       `json:"Catch,omitempty"`
	Choices                        []ChoiceRule    `json:"Choices,omitempty"`
	Result                         json.RawMessage `json:"Result,omitempty"`
	Branches                       []Branch        `json:"Branches,omitempty"`
	Parameters                     json.RawMessage `json:"Parameters,omitempty"`
	ResultSelector                 json.RawMessage `json:"ResultSelector,omitempty"`
	TimeoutSeconds                 int             `json:"TimeoutSeconds,omitempty"`
	HeartbeatSeconds               int             `json:"HeartbeatSeconds,omitempty"`
	Seconds                        int             `json:"Seconds,omitempty"`
	MaxConcurrency                 int             `json:"MaxConcurrency,omitempty"`
	End                            bool            `json:"End,omitempty"`
}

// Retrier defines retry behavior for a Task state on error.
//
// JitterStrategy controls whether the computed backoff delay is randomized:
// "FULL" randomizes the delay uniformly between 0 and the computed value;
// "NONE" (the AWS default when omitted) uses the computed delay as-is.
// MaxDelaySeconds, when set, caps the delay between retry attempts.
type Retrier struct {
	IntervalSeconds *int     `json:"IntervalSeconds,omitempty"`
	MaxAttempts     *int     `json:"MaxAttempts,omitempty"`
	MaxDelaySeconds *int     `json:"MaxDelaySeconds,omitempty"`
	JitterStrategy  string   `json:"JitterStrategy,omitempty"`
	ErrorEquals     []string `json:"ErrorEquals"`
	BackoffRate     float64  `json:"BackoffRate,omitempty"`
}

// Catcher defines catch behavior for a Task state on error.
type Catcher struct {
	Next        string   `json:"Next"`
	ResultPath  string   `json:"ResultPath,omitempty"`
	ErrorEquals []string `json:"ErrorEquals"`
}

// Branch represents a parallel branch (or iterator root).
type Branch struct {
	States  map[string]*State `json:"States"`
	StartAt string            `json:"StartAt"`
	Comment string            `json:"Comment,omitempty"`
}

// ChoiceRule represents a single condition/transition in a Choice state.
type ChoiceRule struct {
	// Numeric comparisons
	NumericEquals                *float64 `json:"NumericEquals,omitempty"`
	NumericLessThan              *float64 `json:"NumericLessThan,omitempty"`
	NumericGreaterThan           *float64 `json:"NumericGreaterThan,omitempty"`
	NumericLessThanEquals        *float64 `json:"NumericLessThanEquals,omitempty"`
	NumericGreaterThanEquals     *float64 `json:"NumericGreaterThanEquals,omitempty"`
	NumericEqualsPath            *string  `json:"NumericEqualsPath,omitempty"`
	NumericLessThanPath          *string  `json:"NumericLessThanPath,omitempty"`
	NumericGreaterThanPath       *string  `json:"NumericGreaterThanPath,omitempty"`
	NumericLessThanEqualsPath    *string  `json:"NumericLessThanEqualsPath,omitempty"`
	NumericGreaterThanEqualsPath *string  `json:"NumericGreaterThanEqualsPath,omitempty"`

	// String comparisons
	StringEquals                *string `json:"StringEquals,omitempty"`
	StringLessThan              *string `json:"StringLessThan,omitempty"`
	StringGreaterThan           *string `json:"StringGreaterThan,omitempty"`
	StringLessThanEquals        *string `json:"StringLessThanEquals,omitempty"`
	StringGreaterThanEquals     *string `json:"StringGreaterThanEquals,omitempty"`
	StringEqualsPath            *string `json:"StringEqualsPath,omitempty"`
	StringLessThanPath          *string `json:"StringLessThanPath,omitempty"`
	StringGreaterThanPath       *string `json:"StringGreaterThanPath,omitempty"`
	StringLessThanEqualsPath    *string `json:"StringLessThanEqualsPath,omitempty"`
	StringGreaterThanEqualsPath *string `json:"StringGreaterThanEqualsPath,omitempty"`
	StringMatches               *string `json:"StringMatches,omitempty"`

	// Timestamp comparisons (ISO 8601 / RFC3339 strings)
	TimestampEquals                *string `json:"TimestampEquals,omitempty"`
	TimestampLessThan              *string `json:"TimestampLessThan,omitempty"`
	TimestampGreaterThan           *string `json:"TimestampGreaterThan,omitempty"`
	TimestampLessThanEquals        *string `json:"TimestampLessThanEquals,omitempty"`
	TimestampGreaterThanEquals     *string `json:"TimestampGreaterThanEquals,omitempty"`
	TimestampEqualsPath            *string `json:"TimestampEqualsPath,omitempty"`
	TimestampLessThanPath          *string `json:"TimestampLessThanPath,omitempty"`
	TimestampGreaterThanPath       *string `json:"TimestampGreaterThanPath,omitempty"`
	TimestampLessThanEqualsPath    *string `json:"TimestampLessThanEqualsPath,omitempty"`
	TimestampGreaterThanEqualsPath *string `json:"TimestampGreaterThanEqualsPath,omitempty"`

	// Boolean comparison
	BooleanEquals     *bool   `json:"BooleanEquals,omitempty"`
	BooleanEqualsPath *string `json:"BooleanEqualsPath,omitempty"`

	// Existence and type checks
	IsNull      *bool `json:"IsNull,omitempty"`
	IsPresent   *bool `json:"IsPresent,omitempty"`
	IsString    *bool `json:"IsString,omitempty"`
	IsNumeric   *bool `json:"IsNumeric,omitempty"`
	IsBoolean   *bool `json:"IsBoolean,omitempty"`
	IsTimestamp *bool `json:"IsTimestamp,omitempty"`

	// Logical operators
	Not      *ChoiceRule  `json:"Not,omitempty"`
	Variable string       `json:"Variable,omitempty"`
	Next     string       `json:"Next,omitempty"`
	And      []ChoiceRule `json:"And,omitempty"`
	Or       []ChoiceRule `json:"Or,omitempty"`
}

// Parse parses an ASL state machine definition from JSON.
func Parse(definition string) (*StateMachine, error) {
	var sm StateMachine
	if err := json.Unmarshal([]byte(definition), &sm); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrParseError, err)
	}

	if sm.StartAt == "" {
		return nil, fmt.Errorf("%w: StartAt is required", ErrParseError)
	}

	if len(sm.States) == 0 {
		return nil, fmt.Errorf("%w: States is required", ErrParseError)
	}

	if _, ok := sm.States[sm.StartAt]; !ok {
		return nil, fmt.Errorf("%w: StartAt state %q not found in States", ErrParseError, sm.StartAt)
	}

	return &sm, nil
}
