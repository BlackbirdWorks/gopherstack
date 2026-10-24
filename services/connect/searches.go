package connect

import (
	"cmp"
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

// SearchRequest is the body shared by the Search* operations.
type SearchRequest struct {
	InstanceID     string `json:"InstanceId"`
	NextToken      string
	SearchCriteria json.RawMessage
	SearchFilter   json.RawMessage
	ResourceTypes  []string
	MaxResults     int32
}

// SearchResult is the response body of a Search* operation.
type SearchResult = map[string]any

// runSearch filters the records of one family and pages the matches.
func runSearch[V, O any](
	b *Backend, op, key string, in *SearchRequest, f family[V], spec searchSpec,
	subj func(*V) searchSubject, out func(*V) O,
) (*SearchResult, error) {
	crit, filt, err := parseSearch(in.SearchCriteria, in.SearchFilter, spec)
	if err != nil {
		return nil, err
	}

	all, err := readInstance(b, op, in.InstanceID, func(inst string) ([]O, error) {
		var res []O

		for _, v := range f.in(inst) {
			if s := subj(v); crit.matches(&s) && filt.matches(&s) {
				res = append(res, out(v))
			}
		}

		return res, nil
	})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	body := SearchResult{key: orEmpty(page), "ApproximateTotalCount": int64(len(all))}
	if next != "" {
		body["NextToken"] = next
	}

	return &body, nil
}

func nameDesc(name, desc string) map[string]string {
	return map[string]string{fieldName: name, fieldDescription: desc}
}

func nameDescSpec() searchSpec { return searchSpec{fields: []string{fieldName, fieldDescription}} }

// SearchQueues searches queues.
func (b *Backend) SearchQueues(in *SearchRequest) (*SearchResult, error) {
	spec := nameDescSpec()

	return runSearch(b, "SearchQueues", "Queues", in, b.queues, spec,
		func(q *queueRec) searchSubject {
			return searchSubject{
				fields: nameDesc(q.Queue.Name, q.Queue.Description), tags: q.Queue.Tags,
				conds: map[string]string{"queuetype": "STANDARD"},
			}
		},
		func(q *queueRec) Queue {
			return withTags(q.Queue, func(x *Queue) *map[string]string { return &x.Tags })
		})
}

// withTags returns a copy of v whose tag map is cloned.
func withTags[T any](v T, tags func(*T) *map[string]string) T {
	t := tags(&v)
	*t = cloneTags(*t)

	return v
}

// SearchHoursOfOperations searches schedules.
func (b *Backend) SearchHoursOfOperations(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName, fieldDescription, "timezone"}}

	return runSearch(b, "SearchHoursOfOperations", "HoursOfOperations", in, b.hours, spec,
		func(h *hoursRec) searchSubject {
			f := nameDesc(h.Hours.Name, h.Hours.Description)
			f["timezone"] = h.Hours.TimeZone

			return searchSubject{fields: f, tags: h.Hours.Tags}
		},
		func(h *hoursRec) HoursOfOperation {
			out := withTags(h.Hours, func(x *HoursOfOperation) *map[string]string { return &x.Tags })
			out.ParentHoursOfOperations = b.hoursIdentifiers(h.InstanceID, h.Parents)

			return out
		})
}

// SearchHoursOfOperationOverrides searches schedule overrides.
func (b *Backend) SearchHoursOfOperationOverrides(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchHoursOfOperationOverrides", "HoursOfOperationOverrides", in, b.hoursOverrides,
		nameDescSpec(),
		func(o *hoursOverrideRec) searchSubject {
			var tags map[string]string
			if h, ok := b.hours.table.Get(o.InstanceID + "/" + o.Override.HoursOfOperationID); ok {
				tags = h.Hours.Tags
			}

			return searchSubject{
				fields: nameDesc(o.Override.Name, o.Override.Description),
				tags:   tags,
				dates: map[string]string{
					"effectivefrom": o.Override.EffectiveFrom,
					"effectivetill": o.Override.EffectiveTill,
				},
			}
		},
		func(o *hoursOverrideRec) HoursOfOperationOverride { return o.Override })
}

// SearchPrompts searches prompts.
func (b *Backend) SearchPrompts(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchPrompts", "Prompts", in, b.prompts, nameDescSpec(),
		func(p *promptRec) searchSubject {
			return searchSubject{fields: nameDesc(p.Prompt.Name, p.Prompt.Description), tags: p.Prompt.Tags}
		},
		func(p *promptRec) Prompt {
			return withTags(p.Prompt, func(x *Prompt) *map[string]string { return &x.Tags })
		})
}

// SearchQuickConnects searches quick connects.
func (b *Backend) SearchQuickConnects(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchQuickConnects", "QuickConnects", in, b.quickConnects, nameDescSpec(),
		func(q *quickConnectRec) searchSubject {
			return searchSubject{fields: nameDesc(q.Connect.Name, q.Connect.Description), tags: q.Connect.Tags}
		},
		func(q *quickConnectRec) QuickConnect {
			return withTags(q.Connect, func(x *QuickConnect) *map[string]string { return &x.Tags })
		})
}

// SearchAgentStatuses searches agent statuses.
func (b *Backend) SearchAgentStatuses(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchAgentStatuses", "AgentStatuses", in, b.agentStatuses, nameDescSpec(),
		func(a *agentStatusRec) searchSubject {
			return searchSubject{fields: nameDesc(a.Status.Name, a.Status.Description), tags: a.Status.Tags}
		},
		func(a *agentStatusRec) AgentStatus {
			return withTags(a.Status, func(x *AgentStatus) *map[string]string { return &x.Tags })
		})
}

// SearchRoutingProfiles searches routing profiles.
func (b *Backend) SearchRoutingProfiles(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchRoutingProfiles", "RoutingProfiles", in, b.routingProfiles, nameDescSpec(),
		func(p *routingProfileRec) searchSubject {
			return searchSubject{fields: nameDesc(p.Profile.Name, p.Profile.Description), tags: p.Profile.Tags}
		},
		func(p *routingProfileRec) RoutingProfile { return b.routingProfileView(p) })
}

func (b *Backend) routingProfileView(p *routingProfileRec) RoutingProfile {
	out := withTags(p.Profile, func(x *RoutingProfile) *map[string]string { return &x.Tags })
	out.AssociatedQueueIDs = queueIDsOf(p.Queues)
	out.AssociatedManualAssignmentQueueIDs = queueIDsOf(p.Manual)
	out.NumberOfAssociatedQueues = int64(len(out.AssociatedQueueIDs))
	out.NumberOfAssociatedManualAssignmentQueues = int64(len(out.AssociatedManualAssignmentQueueIDs))

	for _, u := range b.users.in(p.InstanceID) {
		if u.User.RoutingProfileID == p.Profile.RoutingProfileID {
			out.NumberOfAssociatedUsers++
		}
	}

	return out
}

// SearchContactFlows searches contact flows.
func (b *Backend) SearchContactFlows(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchContactFlows", "ContactFlows", in, b.flows, nameDescSpec(),
		func(f *flowRec) searchSubject {
			return searchSubject{
				fields: nameDesc(f.Flow.Name, f.Flow.Description), tags: f.Flow.Tags, flowType: f.Flow.Type,
				conds: map[string]string{fieldState: f.Flow.State, fieldStatus: f.Flow.Status, fieldType: f.Flow.Type},
			}
		},
		func(f *flowRec) ContactFlow {
			return withTags(f.Flow, func(x *ContactFlow) *map[string]string { return &x.Tags })
		})
}

// SearchContactFlowModules searches contact flow modules.
func (b *Backend) SearchContactFlowModules(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchContactFlowModules", "ContactFlowModules", in, b.modules, nameDescSpec(),
		func(m *moduleRec) searchSubject {
			return searchSubject{
				fields: nameDesc(m.Module.Name, m.Module.Description), tags: m.Module.Tags,
				conds: map[string]string{fieldState: m.Module.State, fieldStatus: m.Module.Status},
			}
		},
		func(m *moduleRec) ContactFlowModule {
			return withTags(m.Module, func(x *ContactFlowModule) *map[string]string { return &x.Tags })
		})
}

type securityProfileSummary struct {
	Tags        map[string]string `json:"Tags,omitempty"`
	Arn         string            `json:"Arn"`
	Description string            `json:"Description,omitempty"`
	ID          string            `json:"Id"`
	Name        string            `json:"SecurityProfileName"`
}

// SearchSecurityProfiles searches security profiles.
func (b *Backend) SearchSecurityProfiles(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName, fieldDescription, "securityprofilename"}}

	return runSearch(b, "SearchSecurityProfiles", "SecurityProfiles", in, b.securityProfiles, spec,
		func(p *securityProfileRec) searchSubject {
			f := nameDesc(p.Profile.SecurityProfileName, p.Profile.Description)
			f["securityprofilename"] = p.Profile.SecurityProfileName

			return searchSubject{fields: f, tags: p.Profile.Tags}
		},
		func(p *securityProfileRec) securityProfileSummary {
			return securityProfileSummary{
				Tags: cloneTags(p.Profile.Tags), Arn: p.Profile.Arn, Description: p.Profile.Description,
				ID: p.Profile.ID, Name: p.Profile.SecurityProfileName,
			}
		})
}

// SearchUserHierarchyGroups searches hierarchy groups.
func (b *Backend) SearchUserHierarchyGroups(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName}}

	return runSearch(b, "SearchUserHierarchyGroups", "UserHierarchyGroups", in, b.hierarchyGroups, spec,
		func(g *hierarchyGroupRec) searchSubject {
			return searchSubject{fields: map[string]string{fieldName: g.Group.Name}, tags: g.Group.Tags}
		},
		func(g *hierarchyGroupRec) HierarchyGroup {
			out := withTags(g.Group, func(x *HierarchyGroup) *map[string]string { return &x.Tags })
			out.HierarchyPath = b.groupPathLocked(g.InstanceID, g)

			return out
		})
}

// groupChain lists a hierarchy group and its ancestors, nearest first.
func (b *Backend) groupChain(inst, id string) []string {
	var chain []string

	for id != "" && !slices.Contains(chain, id) {
		g, err := b.hierarchyGroups.get(inst, id)
		if err != nil {
			break
		}

		chain = append(chain, id)
		id = g.ParentID
	}

	return chain
}

// SearchUsers searches users.
func (b *Backend) SearchUsers(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{
		"username", "firstname", "lastname", "email", "secondaryemail", "mobile",
	}}

	return runSearch(b, "SearchUsers", "Users", in, b.users, spec,
		func(u *userRec) searchSubject {
			f := map[string]string{"username": u.User.Username}
			if id := u.User.IdentityInfo; id != nil {
				f["firstname"], f["lastname"], f["email"] = id.FirstName, id.LastName, id.Email
				f["secondaryemail"], f["mobile"] = id.SecondaryEmail, id.Mobile
			}

			return searchSubject{
				fields: f, tags: u.User.Tags, hierarchy: b.groupChain(u.InstanceID, u.User.HierarchyGroupID),
				profs: userProficiencies(u),
			}
		},
		func(u *userRec) User {
			out := withTags(u.User, func(x *User) *map[string]string { return &x.Tags })
			out.SecurityProfileIDs = slices.Clone(out.SecurityProfileIDs)

			return out
		})
}

// SearchPredefinedAttributes searches predefined attributes.
func (b *Backend) SearchPredefinedAttributes(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName}}

	return runSearch(b, "SearchPredefinedAttributes", "PredefinedAttributes", in, b.predefined, spec,
		func(p *predefinedRec) searchSubject {
			return searchSubject{fields: map[string]string{fieldName: p.Attribute.Name}}
		},
		func(p *predefinedRec) PredefinedAttribute { return p.Attribute })
}

// SearchViews searches views.
func (b *Backend) SearchViews(in *SearchRequest) (*SearchResult, error) {
	return runSearch(b, "SearchViews", "Views", in, b.views, nameDescSpec(),
		func(v *viewRec) searchSubject {
			return searchSubject{
				fields: nameDesc(v.View.Name, v.View.Description), tags: v.View.Tags,
				conds: map[string]string{"viewstatus": v.View.Status, "viewtype": v.View.Type},
			}
		},
		func(v *viewRec) View { return withTags(v.View, func(x *View) *map[string]string { return &x.Tags }) })
}

type ruleSearchSummary struct {
	Rule
	ActionSummaries []ActionSummary `json:"ActionSummaries"`
}

// SearchRules searches rules.
func (b *Backend) SearchRules(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{fieldName, "eventsourcename", "publishstatus"}}

	return runSearch(b, "SearchRules", "Rules", in, b.rules, spec,
		func(r *ruleRec) searchSubject {
			return searchSubject{
				fields: map[string]string{
					fieldName: r.Rule.Name, "eventsourcename": r.Rule.TriggerEventSource.EventSourceName,
					"publishstatus": r.Rule.PublishStatus,
				},
				tags: r.Rule.Tags,
			}
		},
		func(r *ruleRec) ruleSearchSummary {
			out := withTags(r.Rule, func(x *Rule) *map[string]string { return &x.Tags })

			return ruleSearchSummary{Rule: out, ActionSummaries: ruleSummary(&out).ActionSummaries}
		})
}

type taggedResource struct {
	tags         map[string]string
	resourceType string
}

// taggedResources lists the tags of every taggable resource of an instance.
func (b *Backend) taggedResources(inst string) []taggedResource {
	sources := b.taggedSources()
	out := make([]taggedResource, 0, len(sources))

	for _, t := range sources {
		out = append(out, t(inst)...)
	}

	return out
}

func tagsOfFamily[V any](f family[V]) func(string) []taggedResource {
	return func(inst string) []taggedResource {
		recs := f.in(inst)
		out := make([]taggedResource, 0, len(recs))

		for _, v := range recs {
			rt := arnResourceType(f.arnOf(v))
			out = append(out, taggedResource{tags: *f.tagsOf(v), resourceType: rt})
		}

		return out
	}
}

// arnResourceType extracts "queue" from arn:...:instance/{id}/queue/{id}.
func arnResourceType(resourceARN string) string {
	parts := strings.Split(resourceARN, "/")
	if len(parts) < 3 { //nolint:mnd // instance/{id}/{type}/{id}
		return ""
	}

	return parts[2]
}

func (b *Backend) taggedSources() []func(string) []taggedResource {
	return []func(string) []taggedResource{
		tagsOfFamily(b.users), tagsOfFamily(b.routingProfiles), tagsOfFamily(b.securityProfiles),
		tagsOfFamily(b.hierarchyGroups), tagsOfFamily(b.queues), tagsOfFamily(b.hours), tagsOfFamily(b.quickConnects),
		tagsOfFamily(b.flows), tagsOfFamily(b.modules), tagsOfFamily(b.agentStatuses), tagsOfFamily(b.prompts),
		tagsOfFamily(b.contacts), tagsOfFamily(b.integrations), tagsOfFamily(b.rules), tagsOfFamily(b.taskTemplates),
		tagsOfFamily(b.vocabularies), tagsOfFamily(b.views), tagsOfFamily(b.evalForms), tagsOfFamily(b.dataTables),
	}
}

type resourceTagsCriteria struct {
	TagSearchCondition *struct {
		TagKey                 string `json:"tagKey"`
		TagValue               string `json:"tagValue"`
		TagKeyComparisonType   string `json:"tagKeyComparisonType"`
		TagValueComparisonType string `json:"tagValueComparisonType"`
	} `json:"TagSearchCondition"`
}

// TagSet is one distinct tag found by SearchResourceTags.
type TagSet struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func parseTagSearch(raw json.RawMessage) (func(k, v string) bool, error) {
	var c resourceTagsCriteria
	if len(raw) > 0 && string(raw) != jsonNull {
		if err := json.Unmarshal(raw, &c); err != nil {
			return nil, invalidParamErr("SearchCriteria is not valid")
		}
	}

	t := c.TagSearchCondition
	if t == nil {
		return func(string, string) bool { return true }, nil
	}

	for _, cmp := range []string{t.TagKeyComparisonType, t.TagValueComparisonType} {
		if cmp != "" {
			if err := oneOf("ComparisonType", cmp, stringComparisons()...); err != nil {
				return nil, err
			}
		}
	}

	key := &stringCond{Value: t.TagKey, ComparisonType: t.TagKeyComparisonType}
	val := &stringCond{Value: t.TagValue, ComparisonType: t.TagValueComparisonType}

	return func(k, v string) bool {
		return (t.TagKey == "" || compareString(key, k)) && (t.TagValue == "" || compareString(val, v))
	}, nil
}

// SearchResourceTags returns the distinct tag key and value pairs of an instance's resources.
func (b *Backend) SearchResourceTags(in *SearchRequest) (*SearchResult, error) {
	match, err := parseTagSearch(in.SearchCriteria)
	if err != nil {
		return nil, err
	}

	all, err := readInstance(b, "SearchResourceTags", in.InstanceID, func(inst string) ([]TagSet, error) {
		seen := map[TagSet]bool{}

		for _, r := range b.taggedResources(inst) {
			if len(in.ResourceTypes) > 0 && !slices.Contains(in.ResourceTypes, r.resourceType) {
				continue
			}

			for k, v := range r.tags {
				if match(k, v) {
					seen[TagSet{Key: k, Value: v}] = true
				}
			}
		}

		out := slices.Collect(maps.Keys(seen))
		slices.SortFunc(
			out,
			func(a, c TagSet) int { return cmp.Or(cmp.Compare(a.Key, c.Key), cmp.Compare(a.Value, c.Value)) },
		)

		return out, nil
	})
	if err != nil {
		return nil, err
	}

	page, next, err := paged(all, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	body := SearchResult{"Tags": orEmpty(page)}
	if next != "" {
		body["NextToken"] = next
	}

	return &body, nil
}
