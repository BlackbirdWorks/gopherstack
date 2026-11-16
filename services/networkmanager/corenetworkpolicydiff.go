package networkmanager

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"reflect"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

// This file implements the real ADD/MODIFY/REMOVE structural diff
// GetCoreNetworkChangeSet/GetCoreNetworkChangeEvents compute between the
// LIVE core network policy and a submitted policy version, over the policy
// document's real top-level sections: "segments", "network-function-groups",
// "segment-actions", "attachment-policies", and "core-network-configuration"
// (the AWS Cloud WAN policy-document schema's own top-level keys).
//
// Document-level changes live here; per-attachment changes (ATTACHMENT_MAPPING and the route and routing policy
// types) are in corenetworkattachmentdiff.go and corenetworkroutingpolicydiff.go.

type namedSection struct {
	key string
	raw jsontext.Value
}

// policyDocSections is the subset of the real Cloud WAN policy-document
// schema's top-level keys this diff engine reads.
type policyDocSections struct {
	CoreNetworkConfiguration jsontext.Value   `json:"core-network-configuration"`
	Segments                 []jsontext.Value `json:"segments"`
	NetworkFunctionGroups    []jsontext.Value `json:"network-function-groups"`
	SegmentActions           []jsontext.Value `json:"segment-actions"`
	AttachmentPolicies       []jsontext.Value `json:"attachment-policies"`
	RoutingPolicies          []jsontext.Value `json:"routing-policies"`
	AttachmentRoutingRules   []jsontext.Value `json:"attachment-routing-policy-rules"`
}

func parsePolicyDocSections(doc string) policyDocSections {
	if doc == "" {
		return policyDocSections{}
	}

	var s policyDocSections
	// Already validated JSON at Put time (PutCoreNetworkPolicy/
	// CreateCoreNetwork) -- an error here means an empty/absent document,
	// which decodes to the zero value, itself a valid "nothing to diff"
	// input to diffNamedSections.
	_ = json.Unmarshal([]byte(doc), &s, jsonx.V1())

	return s
}

func namedByKey(items []jsontext.Value, keyField string) map[string]namedSection {
	out := make(map[string]namedSection, len(items))

	for _, item := range items {
		var fields map[string]jsontext.Value
		if err := json.Unmarshal(item, &fields, jsonx.V1()); err != nil {
			continue
		}

		var key string
		if err := json.Unmarshal(fields[keyField], &key, jsonx.V1()); err != nil {
			continue
		}

		if key != "" {
			out[key] = namedSection{raw: item, key: key}
		}
	}

	return out
}

// diffSectionEntries computes an ADD/MODIFY/REMOVE CoreNetworkChange per
// key present in either oldItems or newItems, keyed by keyField (e.g.
// "name", "rule-number").
func diffSectionEntries(
	oldItems, newItems []jsontext.Value,
	keyField, changeType, pathPrefix string,
	values func(key string, raw jsontext.Value) *CoreNetworkChangeValues,
) []CoreNetworkChange {
	oldByKey := namedByKey(oldItems, keyField)
	newByKey := namedByKey(newItems, keyField)

	keys := make(map[string]bool, len(oldByKey)+len(newByKey))
	for k := range oldByKey {
		keys[k] = true
	}

	for k := range newByKey {
		keys[k] = true
	}

	changes := make([]CoreNetworkChange, 0, len(keys))

	for key := range keys {
		oldEntry, oldOK := oldByKey[key]
		newEntry, newOK := newByKey[key]

		change := CoreNetworkChange{
			Identifier: key, IdentifierPath: pathPrefix + "/" + key, Type: changeType,
		}

		switch {
		case !oldOK:
			change.Action = "ADD"
			change.NewValues = withRawJSON(values(key, newEntry.raw), string(newEntry.raw))
		case !newOK:
			change.Action = "REMOVE"
			change.PreviousValues = withRawJSON(values(key, oldEntry.raw), string(oldEntry.raw))
		case !jsonEqual(oldEntry.raw, newEntry.raw):
			change.Action = "MODIFY"
			change.PreviousValues = withRawJSON(values(key, oldEntry.raw), string(oldEntry.raw))
			change.NewValues = withRawJSON(values(key, newEntry.raw), string(newEntry.raw))
		default:
			continue
		}

		changes = append(changes, change)
	}

	return changes
}

func withRawJSON(v *CoreNetworkChangeValues, raw string) *CoreNetworkChangeValues {
	if v == nil {
		v = &CoreNetworkChangeValues{}
	}

	v.RawJSON = raw

	return v
}

func jsonEqual(a, b jsontext.Value) bool {
	var av, bv any
	if json.Unmarshal(a, &av, jsonx.V1()) != nil || json.Unmarshal(b, &bv, jsonx.V1()) != nil {
		return string(a) == string(b)
	}

	return reflect.DeepEqual(av, bv)
}

// coreNetworkConfigurationIdentifier/coreNetworkConfigurationChangeType are
// diffCoreNetworkConfiguration's fixed Identifier/IdentifierPath/Type --
// unlike every other section, "core-network-configuration" has no natural
// per-entry key, so every change it produces shares the same three values.
const (
	coreNetworkConfigurationIdentifier = "core-network-configuration"
	coreNetworkConfigurationChangeType = "CORE_NETWORK_CONFIGURATION"
)

// diffCoreNetworkConfiguration compares the single "core-network-configuration"
// object, which has no natural key -- a single MODIFY/ADD/REMOVE covers it.
func diffCoreNetworkConfiguration(oldRaw, newRaw jsontext.Value) []CoreNetworkChange {
	base := CoreNetworkChange{
		Identifier: coreNetworkConfigurationIdentifier, IdentifierPath: coreNetworkConfigurationChangeType,
		Type: coreNetworkConfigurationChangeType,
	}

	switch {
	case len(oldRaw) == 0 && len(newRaw) == 0:
		return nil
	case len(oldRaw) == 0:
		base.Action = "ADD"
		base.NewValues = &CoreNetworkChangeValues{RawJSON: string(newRaw)}
	case len(newRaw) == 0:
		base.Action = "REMOVE"
		base.PreviousValues = &CoreNetworkChangeValues{RawJSON: string(oldRaw)}
	case !jsonEqual(oldRaw, newRaw):
		base.Action = "MODIFY"
		base.PreviousValues = &CoreNetworkChangeValues{RawJSON: string(oldRaw)}
		base.NewValues = &CoreNetworkChangeValues{RawJSON: string(newRaw)}
	default:
		return nil
	}

	return []CoreNetworkChange{base}
}

// diffCoreNetworkPolicy computes the real structural diff between two
// policy documents -- see this file's doc comment for scope.
func diffCoreNetworkPolicy(oldDoc, newDoc string) []CoreNetworkChange {
	oldSections := parsePolicyDocSections(oldDoc)
	newSections := parsePolicyDocSections(newDoc)

	var changes []CoreNetworkChange

	changes = append(changes, diffSectionEntries(
		oldSections.Segments, newSections.Segments, "name", "CORE_NETWORK_SEGMENT", "CORE_NETWORK_SEGMENT",
		func(key string, _ jsontext.Value) *CoreNetworkChangeValues {
			return &CoreNetworkChangeValues{SegmentName: key}
		},
	)...)

	changes = append(changes, diffSectionEntries(
		oldSections.NetworkFunctionGroups, newSections.NetworkFunctionGroups, "name", "NETWORK_FUNCTION_GROUP",
		"NETWORK_FUNCTION_GROUP",
		func(key string, _ jsontext.Value) *CoreNetworkChangeValues {
			return &CoreNetworkChangeValues{NetworkFunctionGroupName: key}
		},
	)...)

	changes = append(changes, diffSectionEntries(
		oldSections.SegmentActions, newSections.SegmentActions, "segment", "SEGMENT_ACTIONS_CONFIGURATION",
		"SEGMENT_ACTIONS_CONFIGURATION",
		func(key string, _ jsontext.Value) *CoreNetworkChangeValues {
			return &CoreNetworkChangeValues{SegmentName: key}
		},
	)...)

	changes = append(changes, diffSectionEntries(
		oldSections.AttachmentPolicies, newSections.AttachmentPolicies, "rule-number",
		"ATTACHMENT_POLICIES_CONFIGURATION", "ATTACHMENT_POLICIES_CONFIGURATION",
		func(string, jsontext.Value) *CoreNetworkChangeValues { return nil },
	)...)

	changes = append(changes, diffCoreNetworkConfiguration(oldSections.CoreNetworkConfiguration,
		newSections.CoreNetworkConfiguration)...)
	changes = append(changes, diffEdges(oldSections.CoreNetworkConfiguration, newSections.CoreNetworkConfiguration)...)
	changes = append(changes, diffStaticRoutes(oldSections.SegmentActions, newSections.SegmentActions)...)
	changes = append(changes, diffRoutingPolicies(&oldSections, &newSections)...)

	return changes
}

// changeEventStatusFor maps a ChangeSetState to the real ChangeStatus every
// change in that changeset shares -- this backend tracks one ChangeSetState
// per policy version, not granular per-IdentifierPath progress, so every
// event for a given changeset reports the same status (a documented
// coarseness, not fabricated variance).
func changeEventStatusFor(changeSetState string) string {
	switch changeSetState {
	case changeSetStateExecuting:
		return "IN_PROGRESS"
	case changeSetStateExecutionSucceeded:
		return "COMPLETE"
	default:
		return "NOT_STARTED"
	}
}

func changesToEvents(changes []CoreNetworkChange, changeSetState string) []CoreNetworkChangeEvent {
	status := changeEventStatusFor(changeSetState)
	events := make([]CoreNetworkChangeEvent, 0, len(changes))
	eventTime := nowUTC()

	for _, c := range changes {
		values := c.NewValues
		if values == nil {
			values = c.PreviousValues
		}

		events = append(events, CoreNetworkChangeEvent{
			Action: c.Action, IdentifierPath: c.IdentifierPath, Status: status, Type: c.Type,
			Values: values, EventTime: eventTime,
		})
	}

	return events
}
