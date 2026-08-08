package dlm

import (
	"maps"
	"slices"
	"strings"
	"time"
)

const (
	policyIDPrefix = "policy-"
	stateEnabled   = "ENABLED"
	stateDisabled  = "DISABLED"
)

// storedPolicy holds a lifecycle policy with all persisted fields.
// time.Time fields are first so their non-pointer prefix reduces GC pointer bytes.
type storedPolicy struct {
	DateCreated      time.Time         `json:"dateCreated"`
	DateModified     time.Time         `json:"dateModified"`
	Tags             map[string]string `json:"tags"`
	PolicyDetails    map[string]any    `json:"policyDetails,omitempty"`
	Description      string            `json:"description"`
	ExecutionRoleARN string            `json:"executionRoleArn"`
	PolicyArn        string            `json:"policyArn"`
	PolicyID         string            `json:"policyId"`
	State            string            `json:"state"`
	// StatusMessage is real AWS's description of why a policy is in ERROR
	// state. This backend's state machine never produces ERROR (only
	// ENABLED/DISABLED, see stateEnabled/stateDisabled), so this always
	// stays empty -- present for wire completeness (gopherstack-x009).
	StatusMessage string `json:"statusMessage,omitempty"`
}

func (p *storedPolicy) toPolicy() *Policy {
	tags := make(map[string]string)
	maps.Copy(tags, p.Tags)

	return &Policy{
		DateCreated:      p.DateCreated,
		DateModified:     p.DateModified,
		Description:      p.Description,
		ExecutionRoleARN: p.ExecutionRoleARN,
		PolicyArn:        p.PolicyArn,
		PolicyID:         p.PolicyID,
		State:            p.State,
		StatusMessage:    p.StatusMessage,
		Tags:             tags,
		PolicyDetails:    p.PolicyDetails,
	}
}

func (p *storedPolicy) toSummary() *PolicySummary {
	tags := make(map[string]string)
	maps.Copy(tags, p.Tags)

	return &PolicySummary{
		PolicyID:    p.PolicyID,
		Description: p.Description,
		State:       p.State,
		Tags:        tags,
		PolicyType:  policyDetailsPolicyType(p.PolicyDetails),
	}
}

// tagPair is a decoded "key=value" filter entry, or a {Key,Value} tag
// extracted from a stored PolicyDetails document.
type tagPair struct {
	Key   string
	Value string
}

// policyDetailsPolicyType returns the PolicyType carried in a stored
// PolicyDetails document, defaulting to EBS_SNAPSHOT_MANAGEMENT to match AWS
// behavior when PolicyType is unspecified.
func policyDetailsPolicyType(details map[string]any) string {
	if pt, ok := details["PolicyType"].(string); ok && pt != "" {
		return pt
	}

	return "EBS_SNAPSHOT_MANAGEMENT"
}

// policyDetailsStringSlice reads a []string-shaped field out of a decoded
// PolicyDetails document (stored as map[string]any, so list elements arrive
// as []any of string).
func policyDetailsStringSlice(details map[string]any, key string) []string {
	raw, _ := details[key].([]any)
	out := make([]string, 0, len(raw))

	for _, v := range raw {
		if s, ok := v.(string); ok {
			out = append(out, s)
		}
	}

	return out
}

// policyDetailsTagPairs reads a []{Key,Value}-shaped field (e.g. TargetTags,
// or a schedule's TagsToAdd) out of a decoded PolicyDetails/schedule
// document.
func policyDetailsTagPairs(doc map[string]any, key string) []tagPair {
	raw, _ := doc[key].([]any)
	out := make([]tagPair, 0, len(raw))

	for _, v := range raw {
		m, ok := v.(map[string]any)
		if !ok {
			continue
		}

		k, _ := m["Key"].(string)
		val, _ := m["Value"].(string)
		out = append(out, tagPair{Key: k, Value: val})
	}

	return out
}

// policyDetailsScheduleTagsToAdd gathers the TagsToAdd of every schedule in a
// stored PolicyDetails document.
func policyDetailsScheduleTagsToAdd(details map[string]any) []tagPair {
	schedules, _ := details["Schedules"].([]any)

	var out []tagPair

	for _, s := range schedules {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}

		out = append(out, policyDetailsTagPairs(sm, "TagsToAdd")...)
	}

	return out
}

// parseTagPairs decodes "key=value" query filter strings (the wire format
// documented for the targetTags/tagsToAdd GetLifecyclePolicies query
// parameters) into tagPairs. Entries without an "=" are skipped.
func parseTagPairs(raw []string) []tagPair {
	out := make([]tagPair, 0, len(raw))

	for _, s := range raw {
		k, v, ok := strings.Cut(s, "=")
		if !ok {
			continue
		}

		out = append(out, tagPair{Key: k, Value: v})
	}

	return out
}

// matchesResourceTypes reports whether want is empty, or any entry in want
// case-insensitively matches an entry of details' ResourceTypes.
func matchesResourceTypes(details map[string]any, want []string) bool {
	if len(want) == 0 {
		return true
	}

	got := policyDetailsStringSlice(details, "ResourceTypes")
	for _, w := range want {
		for _, g := range got {
			if strings.EqualFold(g, w) {
				return true
			}
		}
	}

	return false
}

// matchesAnyTagPair reports whether want is empty, or any pair in want is
// present in have.
func matchesAnyTagPair(have, want []tagPair) bool {
	if len(want) == 0 {
		return true
	}

	for _, w := range want {
		if slices.Contains(have, w) {
			return true
		}
	}

	return false
}
