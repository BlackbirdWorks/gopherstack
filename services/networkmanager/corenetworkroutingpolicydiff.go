package networkmanager

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	changeTypeRoutingPolicy        = "ROUTING_POLICY"
	changeTypeRoutingSegmentAssoc  = "ROUTING_POLICY_SEGMENT_ASSOCIATION"
	changeTypeRoutingEdgeAssoc     = "ROUTING_POLICY_EDGE_ASSOCIATION"
	changeTypeRoutingAttachAssoc   = "ROUTING_POLICY_ATTACHMENT_ASSOCIATION"
	segmentActionShare             = "share"
	segmentActionAssociateRouting  = "associate-routing-policy"
	routingPolicyLabelConditionKey = "routing-policy-label"
)

func diffRoutingPolicies(oldS, newS *policyDocSections) []CoreNetworkChange {
	changes := diffSectionEntries(
		oldS.RoutingPolicies,
		newS.RoutingPolicies,
		"routing-policy-name",
		changeTypeRoutingPolicy,
		changeTypeRoutingPolicy,
		func(_ string, raw jsontext.Value) *CoreNetworkChangeValues {
			var p struct {
				Direction string `json:"routing-policy-direction"`
			}

			_ = json.Unmarshal(raw, &p, jsonx.V1())

			return &CoreNetworkChangeValues{RoutingPolicy: string(raw), RoutingPolicyDirection: p.Direction}
		},
	)

	changes = append(changes, diffSectionEntries(
		shareAssociationItems(oldS.SegmentActions), shareAssociationItems(newS.SegmentActions), "key",
		changeTypeRoutingSegmentAssoc, changeTypeRoutingSegmentAssoc, associationValues,
	)...)

	changes = append(changes, diffSectionEntries(
		edgeAssociationItems(oldS.SegmentActions), edgeAssociationItems(newS.SegmentActions), "key",
		changeTypeRoutingEdgeAssoc, changeTypeRoutingEdgeAssoc, associationValues,
	)...)

	return changes
}

type associationItem struct {
	Key     string   `json:"key"`
	Segment string   `json:"segment"`
	Edge    string   `json:"edge,omitempty"`
	Peer    string   `json:"peer,omitempty"`
	Names   []string `json:"names"`
	Shared  []string `json:"shared,omitempty"`
}

func associationValues(_ string, raw jsontext.Value) *CoreNetworkChangeValues {
	var it associationItem
	if json.Unmarshal(raw, &it, jsonx.V1()) != nil {
		return nil
	}

	v := &CoreNetworkChangeValues{
		SegmentName:              it.Segment,
		RoutingPolicyAssociation: []RoutingPolicyAssociation{{RoutingPolicyNames: it.Names, SharedSegments: it.Shared}},
	}

	if it.Edge != "" {
		v.EdgeLocations, v.PeerEdgeLocations = []string{it.Edge}, []string{it.Peer}
	}

	return v
}

func shareAssociationItems(actions []jsontext.Value) []jsontext.Value {
	var out []jsontext.Value

	for _, raw := range actions {
		var a struct {
			Action    string   `json:"action"`
			Segment   string   `json:"segment"`
			ShareWith any      `json:"share-with"`
			Names     []string `json:"routing-policy-names"`
		}

		if json.Unmarshal(raw, &a, jsonx.V1()) != nil || a.Action != segmentActionShare || len(a.Names) == 0 {
			continue
		}

		var shared []string

		switch sw := a.ShareWith.(type) {
		case string:
			shared = []string{sw}
		case []any:
			for _, s := range sw {
				if str, ok := s.(string); ok {
					shared = append(shared, str)
				}
			}
		}

		out = append(out, marshalAssociation(associationItem{
			Key: a.Segment + "/" + strings.Join(shared, ","), Segment: a.Segment, Names: a.Names, Shared: shared,
		}))
	}

	return out
}

func edgeAssociationItems(actions []jsontext.Value) []jsontext.Value {
	var out []jsontext.Value

	for _, raw := range actions {
		var a struct {
			Action  string `json:"action"`
			Segment string `json:"segment"`
			Assoc   struct {
				Edge  string   `json:"edge-location"`
				Peer  string   `json:"peer-edge-location"`
				Names []string `json:"routing-policy-names"`
			} `json:"edge-location-association"`
		}

		if json.Unmarshal(raw, &a, jsonx.V1()) != nil || a.Action != segmentActionAssociateRouting {
			continue
		}

		out = append(out, marshalAssociation(associationItem{
			Key: a.Segment + "/" + a.Assoc.Edge + "/" + a.Assoc.Peer, Segment: a.Segment,
			Edge: a.Assoc.Edge, Peer: a.Assoc.Peer, Names: a.Assoc.Names,
		}))
	}

	return out
}

func marshalAssociation(it associationItem) jsontext.Value {
	raw, _ := json.Marshal(it, jsonx.V1())

	return raw
}

type attachmentRoutingRule struct {
	Action struct {
		Policies []string `json:"associate-routing-policies"`
	} `json:"action"`
	Conditions []attachmentCondition `json:"conditions"`
	Edges      []string              `json:"edge-locations"`
	RuleNumber int32                 `json:"rule-number"`
}

func parseAttachmentRoutingRules(raws []jsontext.Value) []attachmentRoutingRule {
	var rules []attachmentRoutingRule

	for _, raw := range raws {
		var r attachmentRoutingRule
		if json.Unmarshal(raw, &r, jsonx.V1()) == nil {
			rules = append(rules, r)
		}
	}

	sort.Slice(rules, func(i, j int) bool { return rules[i].RuleNumber < rules[j].RuleNumber })

	return rules
}

// routingPoliciesFor returns the routing policies the first matching rule associates with a's routing policy label.
func routingPoliciesFor(rules []attachmentRoutingRule, a *Attachment) []string {
	if a.RoutingPolicyLabel == "" {
		return nil
	}

	for i := range rules {
		r := &rules[i]
		if len(r.Edges) > 0 && !slices.Contains(r.Edges, a.EdgeLocation) {
			continue
		}

		for _, c := range r.Conditions {
			if c.Type == routingPolicyLabelConditionKey && c.Value == a.RoutingPolicyLabel {
				return r.Action.Policies
			}
		}
	}

	return nil
}

// attachmentRoutingChanges reports the per-attachment routing policy associations that differ between the docs.
func attachmentRoutingChanges(oldDoc, newDoc string, live []*Attachment) []CoreNetworkChange {
	oldRules := parseAttachmentRoutingRules(parsePolicyDocSections(oldDoc).AttachmentRoutingRules)
	newRules := parseAttachmentRoutingRules(parsePolicyDocSections(newDoc).AttachmentRoutingRules)

	var changes []CoreNetworkChange

	for _, a := range live {
		oldNames, newNames := routingPoliciesFor(oldRules, a), routingPoliciesFor(newRules, a)
		if slices.Equal(oldNames, newNames) {
			continue
		}

		vals := func(names []string) *CoreNetworkChangeValues {
			if len(names) == 0 {
				return nil
			}

			return &CoreNetworkChangeValues{
				AttachmentID:             a.AttachmentID,
				RoutingPolicyAssociation: []RoutingPolicyAssociation{{RoutingPolicyNames: names}},
			}
		}

		c := CoreNetworkChange{
			Identifier: a.AttachmentID, IdentifierPath: changeTypeRoutingAttachAssoc + "/" + a.AttachmentID,
			Type: changeTypeRoutingAttachAssoc, PreviousValues: vals(oldNames), NewValues: vals(newNames),
		}

		switch {
		case len(oldNames) == 0:
			c.Action = actionAdd
		case len(newNames) == 0:
			c.Action = actionRemove
		default:
			c.Action = actionModify
		}

		changes = append(changes, c)
	}

	return changes
}
