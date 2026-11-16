package networkmanager

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	changeTypeEdge             = "CORE_NETWORK_EDGE"
	changeTypeAttachmentMap    = "ATTACHMENT_MAPPING"
	changeTypeRoutePropagation = "ATTACHMENT_ROUTE_PROPAGATION"
	changeTypeRouteStatic      = "ATTACHMENT_ROUTE_STATIC"
	actionAdd                  = "ADD"
	actionModify               = "MODIFY"
	actionRemove               = "REMOVE"
	associationTag             = "tag"
	conditionAnd               = "and"
)

type edgeConfig struct {
	ASN              *int64   `json:"asn"`
	Location         string   `json:"location"`
	InsideCidrBlocks []string `json:"inside-cidr-blocks"`
}

// diffEdges reports ADD/MODIFY/REMOVE per edge location of core-network-configuration.
func diffEdges(oldCfg, newCfg jsontext.Value) []CoreNetworkChange {
	return diffSectionEntries(
		edgeItems(oldCfg), edgeItems(newCfg), "location", changeTypeEdge, changeTypeEdge,
		func(key string, raw jsontext.Value) *CoreNetworkChangeValues {
			var e edgeConfig
			_ = json.Unmarshal(raw, &e, jsonx.V1())

			return &CoreNetworkChangeValues{
				EdgeLocations: []string{key}, Asn: e.ASN, InsideCidrBlocks: e.InsideCidrBlocks,
			}
		},
	)
}

func edgeItems(cfg jsontext.Value) []jsontext.Value {
	if len(cfg) == 0 {
		return nil
	}

	var parsed struct {
		EdgeLocations []jsontext.Value `json:"edge-locations"`
	}

	_ = json.Unmarshal(cfg, &parsed, jsonx.V1())

	return parsed.EdgeLocations
}

type segmentAction struct {
	Action       string `json:"action"`
	Segment      string `json:"segment"`
	Destinations []any  `json:"destinations"`
	Cidrs        []any  `json:"destination-cidr-blocks"`
}

// diffStaticRoutes reports ADD/MODIFY/REMOVE per (segment, cidr) of the create-route segment actions.
func diffStaticRoutes(oldActions, newActions []jsontext.Value) []CoreNetworkChange {
	return diffSectionEntries(
		staticRouteItems(oldActions), staticRouteItems(newActions), "key", changeTypeRouteStatic, changeTypeRouteStatic,
		func(_ string, raw jsontext.Value) *CoreNetworkChangeValues {
			var r struct {
				Segment     string `json:"segment"`
				Cidr        string `json:"cidr"`
				Destination string `json:"destination"`
			}

			_ = json.Unmarshal(raw, &r, jsonx.V1())

			return &CoreNetworkChangeValues{SegmentName: r.Segment, Cidr: r.Cidr, DestinationIdentifier: r.Destination}
		},
	)
}

func staticRouteItems(actions []jsontext.Value) []jsontext.Value {
	var out []jsontext.Value

	for _, raw := range actions {
		var a segmentAction
		if json.Unmarshal(raw, &a, jsonx.V1()) != nil || a.Action != "create-route" {
			continue
		}

		dests := make([]string, 0, len(a.Destinations))
		for _, d := range a.Destinations {
			if s, ok := d.(string); ok {
				dests = append(dests, s)
			}
		}

		for _, c := range a.Cidrs {
			cidr, ok := c.(string)
			if !ok {
				continue
			}

			item, err := json.Marshal(map[string]string{
				"key":         a.Segment + "/" + cidr,
				"segment":     a.Segment,
				"cidr":        cidr,
				"destination": strings.Join(dests, ","),
			}, jsonx.V1())
			if err == nil {
				out = append(out, item)
			}
		}
	}

	return out
}

type attachmentCondition struct {
	Type     string `json:"type"`
	Operator string `json:"operator"`
	Key      string `json:"key"`
	Value    string `json:"value"`
}

type attachmentRule struct {
	Action struct {
		Method  string `json:"association-method"`
		Segment string `json:"segment"`
		TagKey  string `json:"tag-value-of-key"`
	} `json:"action"`
	Logic      string                `json:"condition-logic"`
	Conditions []attachmentCondition `json:"conditions"`
	RuleNumber int32                 `json:"rule-number"`
}

func parseAttachmentRules(doc string) []attachmentRule {
	var rules []attachmentRule

	for _, raw := range parsePolicyDocSections(doc).AttachmentPolicies {
		var r attachmentRule
		if json.Unmarshal(raw, &r, jsonx.V1()) == nil {
			rules = append(rules, r)
		}
	}

	sort.Slice(rules, func(i, j int) bool { return rules[i].RuleNumber < rules[j].RuleNumber })

	return rules
}

// attachmentSegment returns the segment the first matching attachment-policy rule assigns to a.
func attachmentSegment(rules []attachmentRule, a *Attachment) (string, bool) {
	for i := range rules {
		r := &rules[i]
		if !ruleMatches(r, a) {
			continue
		}

		if r.Action.Method == associationTag {
			if seg, ok := a.Tags.Get(r.Action.TagKey); ok && seg != "" {
				return seg, true
			}

			continue
		}

		if r.Action.Segment != "" {
			return r.Action.Segment, true
		}
	}

	return "", false
}

func ruleMatches(r *attachmentRule, a *Attachment) bool {
	if len(r.Conditions) == 0 {
		return false
	}

	for _, c := range r.Conditions {
		ok := conditionMatches(c, a)
		if strings.EqualFold(r.Logic, conditionAnd) && !ok {
			return false
		}

		if !strings.EqualFold(r.Logic, conditionAnd) && ok {
			return true
		}
	}

	return strings.EqualFold(r.Logic, conditionAnd)
}

func conditionMatches(c attachmentCondition, a *Attachment) bool {
	switch c.Type {
	case "tag-name":
		return a.Tags.HasTag(c.Key)
	case "any":
		return true
	case "tag-exists":
		return a.Tags.HasTag(c.Key)
	case "tag-value":
		v, ok := a.Tags.Get(c.Key)

		return ok && compareOperator(c.Operator, v, c.Value)
	case "account-id", "account":
		return compareOperator(c.Operator, a.OwnerAccountID, c.Value)
	case "resource-id":
		return compareOperator(c.Operator, resourceIDOf(a.ResourceArn), c.Value)
	case "region":
		return compareOperator(c.Operator, attachmentRegion(a), c.Value)
	case "attachment-type":
		return compareOperator(c.Operator, strings.ToLower(strings.ReplaceAll(a.AttachmentType, "_", "-")), c.Value)
	}

	return false
}

const arnFields = 6

func resourceIDOf(resourceArn string) string {
	parts := strings.SplitN(resourceArn, ":", arnFields)
	if len(parts) < arnFields {
		return resourceArn
	}

	if _, id, ok := strings.Cut(parts[5], "/"); ok {
		return id
	}

	return parts[5]
}

func attachmentRegion(a *Attachment) string {
	if parts := strings.SplitN(a.ResourceArn, ":", arnFields); len(parts) == arnFields && parts[3] != "" {
		return parts[3]
	}

	return a.EdgeLocation
}

func compareOperator(op, have, want string) bool {
	switch op {
	case "not-equals":
		return have != want
	case "contains":
		return strings.Contains(have, want)
	case "begins-with":
		return strings.HasPrefix(have, want)
	default:
		return have == want
	}
}

// attachmentChangesLocked reports the ATTACHMENT_MAPPING and ATTACHMENT_ROUTE_PROPAGATION changes newDoc's
// attachment policies make to the core network's live attachments. Caller holds b.mu.
func (b *InMemoryBackend) attachmentChangesLocked(coreNetworkID, oldDoc, newDoc string) []CoreNetworkChange {
	rules := parseAttachmentRules(newDoc)

	var live []*Attachment

	for _, a := range b.attachments.All() {
		if a.CoreNetworkID == coreNetworkID && a.State != attachmentStateDeleting {
			live = append(live, a)
		}
	}

	sort.Slice(live, func(i, j int) bool { return live[i].AttachmentID < live[j].AttachmentID })

	var changes []CoreNetworkChange

	for _, a := range live {
		next, mapped := attachmentSegment(rules, a)

		switch {
		case mapped && next != a.SegmentName:
			action := actionAdd
			if a.SegmentName != "" {
				action = actionModify
			}

			changes = append(changes, mappingChange(action, a.AttachmentID, a.SegmentName, next)...)
		case !mapped && a.SegmentName != "" && a.AttachmentPolicyRuleNumber > 0:
			changes = append(changes, mappingChange(actionRemove, a.AttachmentID, a.SegmentName, "")...)
		}
	}

	changes = append(changes, attachmentRoutingChanges(oldDoc, newDoc, live)...)

	return changes
}

func mappingChange(action, id, from, to string) []CoreNetworkChange {
	vals := func(seg string) *CoreNetworkChangeValues {
		if seg == "" {
			return nil
		}

		return &CoreNetworkChangeValues{AttachmentID: id, SegmentName: seg}
	}

	out := []CoreNetworkChange{
		{
			Action:         action,
			Identifier:     id,
			IdentifierPath: changeTypeAttachmentMap + "/" + id,
			Type:           changeTypeAttachmentMap,
			PreviousValues: vals(from),
			NewValues:      vals(to),
		},
	}

	if from != "" {
		out = append(out, propagationChange(actionRemove, id, from))
	}

	if to != "" {
		out = append(out, propagationChange(actionAdd, id, to))
	}

	return out
}

func propagationChange(action, id, segment string) CoreNetworkChange {
	v := &CoreNetworkChangeValues{AttachmentID: id, SegmentName: segment}
	c := CoreNetworkChange{
		Action: action, Identifier: id, IdentifierPath: changeTypeRoutePropagation + "/" + id + "/" + segment,
		Type: changeTypeRoutePropagation,
	}

	if action == actionRemove {
		c.PreviousValues = v
	} else {
		c.NewValues = v
	}

	return c
}
