package cloudwatchlogs

import (
	"encoding/json/v2"
	"regexp"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	policyTypeSubscriptionFilter = "SUBSCRIPTION_FILTER_POLICY"
	policyTypeTransformer        = "TRANSFORMER_POLICY"
	policyTypeFieldIndex         = "FIELD_INDEX_POLICY"
)

var selectionPattern = regexp.MustCompile(`^\s*(LogGroupName|LogGroupNamePrefix)\s*(=|NOT\s+IN|IN)\s*(.*?)\s*$`)

// groupSelector is a parsed PutAccountPolicy selectionCriteria over log group names.
type groupSelector struct {
	field  string
	values []string
	all    bool
	not    bool
	valid  bool
}

// parseSelection reads `LogGroupName [NOT] IN [...]`, `LogGroupNamePrefix [NOT] IN [...]` and
// `LogGroupNamePrefix="x"`; an empty string selects every group, anything else selects none.
func parseSelection(s string) groupSelector {
	if strings.TrimSpace(s) == "" {
		return groupSelector{all: true, valid: true}
	}

	m := selectionPattern.FindStringSubmatch(s)
	if m == nil {
		return groupSelector{}
	}

	sel := groupSelector{field: m[1], not: strings.HasPrefix(m[2], "NOT"), valid: true}

	var list []string
	if json.Unmarshal([]byte(m[3]), &list, jsonx.V1()) == nil && list != nil {
		sel.values = list

		return sel
	}

	var one string
	if strings.HasPrefix(m[3], `"`) && json.Unmarshal([]byte(m[3]), &one, jsonx.V1()) == nil {
		sel.values = []string{one}

		return sel
	}

	return groupSelector{}
}

func (g groupSelector) matches(name string) bool {
	if !g.valid {
		return false
	}

	if g.all {
		return true
	}

	hit := false

	for _, v := range g.values {
		if (g.field == "LogGroupName" && v == name) || (g.field == "LogGroupNamePrefix" && strings.HasPrefix(name, v)) {
			hit = true

			break
		}
	}

	return hit != g.not
}

// accountPoliciesFor lists the account policies of policyType whose selection criteria cover groupName, by
// policy name. Caller holds b.mu.
func (b *InMemoryBackend) accountPoliciesFor(policyType, groupName string) []*AccountPolicy {
	var out []*AccountPolicy

	for _, p := range b.accountPolicies.All() {
		if p.PolicyType == policyType && parseSelection(p.SelectionCriteria).matches(groupName) {
			out = append(out, p)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].PolicyName < out[j].PolicyName })

	return out
}

type subscriptionPolicyDoc struct {
	DestinationArn string `json:"DestinationArn"`
	RoleArn        string `json:"RoleArn"`
	FilterPattern  string `json:"FilterPattern"`
	Distribution   string `json:"Distribution"`
}

// accountSubscriptionFilters turns the account SUBSCRIPTION_FILTER_POLICY policies covering groupName into
// filters. Caller holds b.mu.
func (b *InMemoryBackend) accountSubscriptionFilters(region, groupName string) []*SubscriptionFilter {
	var out []*SubscriptionFilter

	for _, p := range b.accountPoliciesFor(policyTypeSubscriptionFilter, groupName) {
		var doc subscriptionPolicyDoc
		if json.Unmarshal([]byte(p.PolicyDocument), &doc, jsonx.V1()) != nil || doc.DestinationArn == "" {
			continue
		}

		out = append(out, &SubscriptionFilter{
			FilterName: p.PolicyName, LogGroupName: groupName, FilterPattern: doc.FilterPattern,
			DestinationArn: doc.DestinationArn, RoleArn: doc.RoleArn, Distribution: doc.Distribution,
			CreationTime: p.LastUpdatedTime, region: region,
		})
	}

	return out
}

// accountTransformerProcessors returns the processors of the first account TRANSFORMER_POLICY covering
// groupName. Caller holds b.mu.
func (b *InMemoryBackend) accountTransformerProcessors(groupName string) ([]map[string]any, bool) {
	for _, p := range b.accountPoliciesFor(policyTypeTransformer, groupName) {
		var procs []map[string]any
		if json.Unmarshal([]byte(p.PolicyDocument), &procs, jsonx.V1()) == nil && len(procs) > 0 {
			return procs, true
		}
	}

	return nil, false
}
