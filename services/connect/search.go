package connect

import (
	"encoding/json"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// TimeRange bounds a contact search.
type TimeRange struct {
	StartTime *awstime.EpochTime `json:"StartTime"`
	EndTime   *awstime.EpochTime `json:"EndTime"`
	Type      string             `json:"Type"`
}

// SearchableAttribute matches contact attributes by key.
type SearchableAttribute struct {
	Key    string   `json:"Key"`
	Values []string `json:"Values"`
}

// SearchableAttributes combines attribute criteria.
type SearchableAttributes struct {
	MatchType string                `json:"MatchType"`
	Criteria  []SearchableAttribute `json:"Criteria"`
}

// NameCriteria matches contact names.
type NameCriteria struct {
	MatchType  string   `json:"MatchType"`
	SearchText []string `json:"SearchText"`
}

// ContactSearchCriteria is the supported subset of SearchCriteria; other members are rejected.
type ContactSearchCriteria struct {
	SearchableContactAttributes *SearchableAttributes `json:"SearchableContactAttributes"`
	Name                        *NameCriteria         `json:"Name"`
	AgentIDs                    []string              `json:"AgentIds"`
	Channels                    []string              `json:"Channels"`
	InitiationMethods           []string              `json:"InitiationMethods"`
	QueueIDs                    []string              `json:"QueueIds"`
}

var timeRangeTypes = []string{ //nolint:gochecknoglobals // immutable lookup table
	tsInitiation,
	tsScheduled,
	tsConnected,
	tsDisconnect,
	"ENQUEUE_TIMESTAMP",
}

var sortFields = []string{ //nolint:gochecknoglobals // immutable lookup table
	tsInitiation, tsScheduled, tsConnected, tsDisconnect,
	"INITIATION_METHOD", "CHANNEL",
}

var unsupportedCriteria = []string{ //nolint:gochecknoglobals // immutable lookup table
	"ActiveRegions", "AdditionalTimeRange", "AgentHierarchyGroups", "AiAgents", "ContactAnalysis", "ContactTags",
	"RoutingCriteria", "SearchableSegmentAttributes",
}

// SearchContactsInput is the SearchContacts request.
type SearchContactsInput struct {
	Sort *struct {
		FieldName string `json:"FieldName"`
		Order     string `json:"Order"`
	}
	TimeRange      TimeRange
	InstanceID     string `json:"InstanceId"`
	NextToken      string
	SearchCriteria json.RawMessage
	MaxResults     int32
}

func parseCriteria(raw json.RawMessage) (*ContactSearchCriteria, error) {
	var crit ContactSearchCriteria

	if len(raw) == 0 {
		return &crit, nil
	}

	if err := json.Unmarshal(raw, &crit); err != nil {
		return nil, invalidParamErr("SearchCriteria is not valid")
	}

	var members map[string]json.RawMessage
	if err := json.Unmarshal(raw, &members); err != nil {
		return nil, invalidParamErr("SearchCriteria is not valid")
	}

	for _, k := range unsupportedCriteria {
		if v, ok := members[k]; ok && string(v) != jsonNull {
			return nil, invalidRequestErr("search criterion %s is not supported", k)
		}
	}

	return &crit, nil
}

func (cr *ContactSearchCriteria) matches(c *contactRec) bool {
	switch {
	case len(cr.AgentIDs) > 0 && !slices.Contains(cr.AgentIDs, c.agentID()):
		return false
	case len(cr.Channels) > 0 && !slices.Contains(cr.Channels, c.Contact.Channel):
		return false
	case len(cr.InitiationMethods) > 0 && !slices.Contains(cr.InitiationMethods, c.Contact.InitiationMethod):
		return false
	case len(cr.QueueIDs) > 0 && !slices.Contains(cr.QueueIDs, c.queueID()):
		return false
	case cr.Name != nil && !matchText(cr.Name, c.Contact.Name):
		return false
	case cr.SearchableContactAttributes != nil && !matchAttributes(cr.SearchableContactAttributes, c.Contact.Attributes):
		return false
	}

	return true
}

func matchText(n *NameCriteria, name string) bool {
	hit := func(t string) bool {
		if n.MatchType == "MATCH_EXACT" {
			return strings.EqualFold(name, t)
		}

		return strings.Contains(strings.ToLower(name), strings.ToLower(t))
	}

	switch n.MatchType {
	case "MATCH_ALL":
		return len(n.SearchText) == 0 || allOf(n.SearchText, hit)
	case "MATCH_NONE":
		return !slices.ContainsFunc(n.SearchText, hit)
	default:
		return len(n.SearchText) == 0 || slices.ContainsFunc(n.SearchText, hit)
	}
}

func allOf(s []string, f func(string) bool) bool {
	for _, v := range s {
		if !f(v) {
			return false
		}
	}

	return true
}

func matchAttributes(sa *SearchableAttributes, attrs map[string]string) bool {
	hit := func(c SearchableAttribute) bool {
		v, ok := attrs[c.Key]

		return ok && (len(c.Values) == 0 || slices.Contains(c.Values, v))
	}

	if sa.MatchType == "MATCH_ALL" {
		return allOfAttr(sa.Criteria, hit)
	}

	return slices.ContainsFunc(sa.Criteria, hit)
}

func allOfAttr(s []SearchableAttribute, f func(SearchableAttribute) bool) bool {
	for _, v := range s {
		if !f(v) {
			return false
		}
	}

	return true
}

func (c *contactRec) timeFor(kind string) *awstime.EpochTime {
	switch kind {
	case tsScheduled:
		return c.Contact.ScheduledTimestamp
	case tsDisconnect:
		return c.Contact.DisconnectTimestamp
	case tsConnected:
		if c.Contact.AgentInfo != nil {
			return c.Contact.AgentInfo.ConnectedToAgentTimestamp
		}

		return nil
	case "ENQUEUE_TIMESTAMP":
		if c.Contact.QueueInfo != nil {
			return c.Contact.QueueInfo.EnqueueTimestamp
		}

		return nil
	default:
		return c.Contact.InitiationTimestamp
	}
}

// searchPlan is the validated form of a SearchContacts request.
type searchPlan struct {
	crit         *ContactSearchCriteria
	field, order string
}

func planSearch(in *SearchContactsInput) (*searchPlan, error) {
	if err := oneOf("TimeRange.Type", in.TimeRange.Type, timeRangeTypes...); err != nil {
		return nil, err
	}

	if in.TimeRange.StartTime == nil || in.TimeRange.EndTime == nil ||
		!in.TimeRange.StartTime.Before(in.TimeRange.EndTime.Time) {
		return nil, invalidParamErr("TimeRange needs a StartTime before its EndTime")
	}

	crit, err := parseCriteria(in.SearchCriteria)
	if err != nil {
		return nil, err
	}

	plan := &searchPlan{crit: crit, field: timeRangeTypes[0], order: "ASCENDING"}
	if in.Sort != nil {
		plan.field, plan.order = in.Sort.FieldName, in.Sort.Order

		if err = oneOf("Sort.FieldName", plan.field, sortFields...); err != nil {
			return nil, err
		}

		if err = oneOf("Sort.Order", plan.order, "ASCENDING", "DESCENDING"); err != nil {
			return nil, err
		}
	}

	return plan, nil
}

// SearchContacts finds contacts of an instance by time range and the supported criteria.
func (b *Backend) SearchContacts(in *SearchContactsInput) (*searchContactsOut, error) {
	plan, err := planSearch(in)
	if err != nil {
		return nil, err
	}

	crit, field, order := plan.crit, plan.field, plan.order

	b.mu.RLock("SearchContacts")

	r, err := b.instanceLocked(in.InstanceID)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	var hits []*contactRec

	for _, c := range b.contacts.in(r.Instance.ID) {
		t := c.timeFor(in.TimeRange.Type)
		if t != nil && !t.Before(in.TimeRange.StartTime.Time) && t.Before(in.TimeRange.EndTime.Time) &&
			crit.matches(c) {
			hits = append(hits, c)
		}
	}

	sortContacts(hits, field, order)

	all := make([]ContactSearchSummary, len(hits))
	for i, c := range hits {
		all[i] = summarizeContact(c)
	}

	b.mu.RUnlock()

	items, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &searchContactsOut{Contacts: orEmpty(items), NextToken: next, TotalCount: int64(len(all))}, nil
}

// ContactSearchSummary is the SearchContacts list shape of a contact.
type ContactSearchSummary struct {
	InitiationTimestamp *awstime.EpochTime `json:"InitiationTimestamp,omitempty"`
	DisconnectTimestamp *awstime.EpochTime `json:"DisconnectTimestamp,omitempty"`
	ScheduledTimestamp  *awstime.EpochTime `json:"ScheduledTimestamp,omitempty"`
	QueueInfo           *ContactQueueInfo  `json:"QueueInfo,omitempty"`
	AgentInfo           *ContactAgentInfo  `json:"AgentInfo,omitempty"`
	Tags                map[string]string  `json:"Tags,omitempty"`
	ID                  string             `json:"Id"`
	Arn                 string             `json:"Arn"`
	Channel             string             `json:"Channel"`
	InitialContactID    string             `json:"InitialContactId"`
	InitiationMethod    string             `json:"InitiationMethod"`
	Name                string             `json:"Name,omitempty"`
	PreviousContactID   string             `json:"PreviousContactId,omitempty"`
	SegmentAttributes   json.RawMessage    `json:"SegmentAttributes,omitempty"`
}

func summarizeContact(c *contactRec) ContactSearchSummary {
	k := c.Contact

	return ContactSearchSummary{
		InitiationTimestamp: k.InitiationTimestamp, DisconnectTimestamp: k.DisconnectTimestamp,
		ScheduledTimestamp: k.ScheduledTimestamp, QueueInfo: k.QueueInfo, AgentInfo: k.AgentInfo,
		Tags: cloneTags(k.Tags), SegmentAttributes: k.SegmentAttributes, Arn: k.Arn, Channel: k.Channel, ID: k.ID,
		InitialContactID: k.InitialContactID, InitiationMethod: k.InitiationMethod, Name: k.Name,
		PreviousContactID: k.PreviousContactID,
	}
}
