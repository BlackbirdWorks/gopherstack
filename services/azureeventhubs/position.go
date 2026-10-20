package azureeventhubs

import (
	"fmt"
	"regexp"
	"strconv"
)

// SelectorFilterName is the AMQP filter key Event Hubs receivers use.
const SelectorFilterName = "apache.org:selector-filter:string"

// SelectorFilterCode is the numeric descriptor of the selector filter.
const SelectorFilterCode uint64 = 0x00000468C00000004

type selectorKind int

const (
	selEarliest selectorKind = iota
	selOffset
	selSequence
	selEnqueued
)

// Selector is a parsed start position (offset, sequence number or enqueued time).
type Selector struct {
	Value     int64
	Kind      selectorKind
	Latest    bool
	Inclusive bool
}

var selectorRe = regexp.MustCompile(
	`^\s*amqp\.annotation\.x-opt-(offset|sequence-number|enqueued-time)\s*(>=|>)\s*'([^']{0,64})'\s*$`)

// ParseSelector parses an `amqp.annotation.x-opt-* > 'v'` filter expression.
func ParseSelector(expr string) (Selector, error) {
	m := selectorRe.FindStringSubmatch(expr)
	if m == nil {
		return Selector{}, ErrInvalidSelector
	}

	sel := Selector{Inclusive: m[2] == ">="}

	switch m[1] {
	case "offset":
		sel.Kind = selOffset
		if m[3] == "@latest" {
			sel.Latest = true

			return sel, nil
		}
	case "sequence-number":
		sel.Kind = selSequence
	default:
		sel.Kind = selEnqueued
	}

	v, err := strconv.ParseInt(m[3], 10, 64)
	if err != nil {
		return Selector{}, fmt.Errorf("%w: %q", ErrInvalidSelector, m[3])
	}

	sel.Value = v

	return sel, nil
}

func (s Selector) passes(v int64) bool {
	if s.Inclusive {
		return v >= s.Value
	}

	return v > s.Value
}

func (s Selector) start(p *partition) int64 {
	if s.Latest {
		return p.NextSeq
	}

	switch s.Kind {
	case selOffset:
		return s.firstMatch(p, func(e *event) int64 { return e.Offset })
	case selEnqueued:
		return s.firstMatch(p, func(e *event) int64 { return e.Enqueued.UnixMilli() })
	case selSequence:
		return s.sequenceStart(p)
	case selEarliest:
	}

	return max(p.begin(), 0)
}

func (s Selector) firstMatch(p *partition, val func(*event) int64) int64 {
	for _, e := range p.Events {
		if s.passes(val(e)) {
			return e.Seq
		}
	}

	return p.NextSeq
}

func (s Selector) sequenceStart(p *partition) int64 {
	target := s.Value
	if !s.Inclusive {
		target++
	}

	return min(max(target, max(p.begin(), 0)), p.NextSeq)
}
