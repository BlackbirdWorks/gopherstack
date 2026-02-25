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

// State represents a single state in the state machine.
type State struct {
	Iterator       *StateMachine   `json:"Iterator,omitempty"`
	SecondsPath    string          `json:"SecondsPath,omitempty"`
	TimestampPath  string          `json:"TimestampPath,omitempty"`
	ItemsPath      string          `json:"ItemsPath,omitempty"`
	InputPath      string          `json:"InputPath,omitempty"`
	OutputPath     string          `json:"OutputPath,omitempty"`
	ResultPath     string          `json:"ResultPath,omitempty"`
	Type           string          `json:"Type"`
	Error          string          `json:"Error,omitempty"`
	Cause          string          `json:"Cause,omitempty"`
	Comment        string          `json:"Comment,omitempty"`
	Next           string          `json:"Next,omitempty"`
	Default        string          `json:"Default,omitempty"`
	Timestamp      string          `json:"Timestamp,omitempty"`
	Resource       string          `json:"Resource,omitempty"`
	Retry          []Retrier       `json:"Retry,omitempty"`
	Catch          []Catcher       `json:"Catch,omitempty"`
	Choices        []ChoiceRule    `json:"Choices,omitempty"`
	Result         json.RawMessage `json:"Result,omitempty"`
	Branches       []Branch        `json:"Branches,omitempty"`
	Parameters     json.RawMessage `json:"Parameters,omitempty"`
	TimeoutSeconds int             `json:"TimeoutSeconds,omitempty"`
	Seconds        int             `json:"Seconds,omitempty"`
	MaxConcurrency int             `json:"MaxConcurrency,omitempty"`
	End            bool            `json:"End,omitempty"`
}

// Retrier defines retry behavior for a Task state on error.
type Retrier struct {
	ErrorEquals     []string `json:"ErrorEquals"`
	IntervalSeconds int      `json:"IntervalSeconds,omitempty"`
	MaxAttempts     int      `json:"MaxAttempts,omitempty"`
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
	NumericLessThan    *float64     `json:"NumericLessThan,omitempty"`
	IsNull             *bool        `json:"IsNull,omitempty"`
	StringEqualsPath   *string      `json:"StringEqualsPath,omitempty"`
	StringLessThan     *string      `json:"StringLessThan,omitempty"`
	StringGreaterThan  *string      `json:"StringGreaterThan,omitempty"`
	NumericEquals      *float64     `json:"NumericEquals,omitempty"`
	StringEquals       *string      `json:"StringEquals,omitempty"`
	BooleanEquals      *bool        `json:"BooleanEquals,omitempty"`
	Not                *ChoiceRule  `json:"Not,omitempty"`
	IsPresent          *bool        `json:"IsPresent,omitempty"`
	NumericGreaterThan *float64     `json:"NumericGreaterThan,omitempty"`
	Variable           string       `json:"Variable,omitempty"`
	Next               string       `json:"Next,omitempty"`
	And                []ChoiceRule `json:"And,omitempty"`
	Or                 []ChoiceRule `json:"Or,omitempty"`
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
