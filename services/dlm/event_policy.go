package dlm

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	policyTypeEventBased = "EVENT_BASED_POLICY"
	eventSourceManaged   = "MANAGED_CWE"
	eventTypeShare       = "shareSnapshot"
	maxEventRegexLen     = 500
	maxEventActions      = 10
	maxEventCopies       = 10
	maxEventManaged      = 1000
)

// eventRule is the decoded EventSource and CrossRegionCopy actions of an event-based policy.
type eventRule struct {
	re     *regexp.Regexp
	owners []string
	copies []eventCopy
}

type eventCopy struct {
	rule   map[string]any
	action string
	target string
}

func eventSource(details map[string]any) (map[string]any, bool) {
	src, ok := details["EventSource"].(map[string]any)
	if !ok {
		return nil, false
	}

	params, ok := src["Parameters"].(map[string]any)

	return params, ok
}

func parseEventRule(details map[string]any) (*eventRule, error) {
	src, _ := details["EventSource"].(map[string]any)
	if t, _ := src["Type"].(string); t != eventSourceManaged {
		return nil, invalidRequest("EventSource.Type must be %s", eventSourceManaged)
	}

	params, _ := eventSource(details)
	if et, _ := params["EventType"].(string); et != eventTypeShare {
		return nil, invalidRequest("EventSource.Parameters.EventType must be %s", eventTypeShare)
	}

	expr, _ := params["DescriptionRegex"].(string)
	if expr == "" || len(expr) > maxEventRegexLen {
		return nil, invalidRequest("EventSource.Parameters.DescriptionRegex must be 1-%d characters", maxEventRegexLen)
	}

	re, err := regexp.Compile(expr)
	if err != nil {
		return nil, invalidRequest("EventSource.Parameters.DescriptionRegex is not a valid regular expression")
	}

	owners := policyDetailsStringSlice(params, "SnapshotOwner")
	if len(owners) == 0 {
		return nil, invalidRequest("EventSource.Parameters.SnapshotOwner must list at least one account")
	}

	copies, err := parseEventActions(details)
	if err != nil {
		return nil, err
	}

	return &eventRule{re: re, owners: owners, copies: copies}, nil
}

func parseEventActions(details map[string]any) ([]eventCopy, error) {
	actions := asSlice(details["Actions"])
	if len(actions) > maxEventActions {
		return nil, invalidRequest("a policy can have at most %d actions", maxEventActions)
	}

	var out []eventCopy

	for _, a := range actions {
		am, _ := a.(map[string]any)

		name, _ := am["Name"].(string)
		if name == "" {
			return nil, invalidRequest("Actions[].Name is required")
		}

		list := asSlice(am["CrossRegionCopy"])
		if len(list) > maxEventCopies {
			return nil, invalidRequest("an action can have at most %d CrossRegionCopy entries", maxEventCopies)
		}

		for _, c := range list {
			cm, _ := c.(map[string]any)

			target, _ := cm["Target"].(string)
			if target == "" {
				return nil, invalidRequest("Actions[].CrossRegionCopy[].Target is required")
			}

			out = append(out, eventCopy{action: name, target: target, rule: cm})
		}
	}

	return out, nil
}

func validateEventPolicy(details map[string]any) error {
	_, err := parseEventRule(details)

	return err
}

func (r *eventRule) matches(evt ec2backend.SnapshotSharedEvent, account string) bool {
	return slices.Contains(evt.SharedWith, account) && slices.Contains(r.owners, evt.OwnerID) &&
		r.re.MatchString(evt.Description)
}

// HandleSnapshotShared runs every ENABLED event-based policy whose EventSource matches evt.
func (b *InMemoryBackend) HandleSnapshotShared(ctx context.Context, now time.Time, evt ec2backend.SnapshotSharedEvent) {
	b.mu.RLock("HandleSnapshotShared")

	compute := b.compute

	type job struct {
		rule     *eventRule
		policyID string
	}

	var jobs []job

	managedBy := map[string]int{}
	for _, m := range b.managed.All() {
		managedBy[m.PolicyID]++
	}

	for _, p := range b.policies.All() {
		if p.State != stateEnabled || policyDetailsPolicyType(p.PolicyDetails) != policyTypeEventBased {
			continue
		}

		rule, err := parseEventRule(p.PolicyDetails)
		if err != nil || !rule.matches(evt, b.accountID) || managedBy[p.PolicyID] >= maxEventManaged {
			continue
		}

		jobs = append(jobs, job{policyID: p.PolicyID, rule: rule})
	}

	b.mu.RUnlock()

	if compute == nil {
		return
	}

	sort.Slice(jobs, func(i, j int) bool { return jobs[i].policyID < jobs[j].policyID })

	for _, j := range jobs {
		for _, c := range j.rule.copies {
			b.runEventCopy(ctx, compute, j.policyID, c, evt, now.UTC())
		}
	}
}

func (b *InMemoryBackend) runEventCopy(
	ctx context.Context, compute ComputeService, policyID string, c eventCopy,
	evt ec2backend.SnapshotSharedEvent, now time.Time,
) {
	if strings.HasPrefix(c.target, "arn:") {
		return
	}

	enc, _ := ruleMap(c.rule, "EncryptionConfiguration")["Encrypted"].(bool)
	cmk, _ := ruleMap(c.rule, "EncryptionConfiguration")["CmkArn"].(string)
	desc := fmt.Sprintf("Created for policy: %s event: %s source: %s", policyID, eventTypeShare, evt.SnapshotID)
	tags := map[string]string{
		tagPolicyID: policyID, tagScheduleName: c.action, tagTimestamp: now.Format(time.RFC3339), tagManaged: valueTrue,
	}

	id, err := compute.CopySnapshot(evt.Region, c.target, evt.SnapshotID, desc, enc, cmk, tags)
	if id == "" {
		logger.Load(ctx).WarnContext(ctx, "dlm: event copy failed",
			"policy", policyID, "snapshot", evt.SnapshotID, "error", err)

		return
	}

	cp := &managedResource{
		ID: id, Kind: kindSnapshot, PolicyID: policyID, Schedule: c.action, TargetID: evt.SnapshotID,
		CreatedAt: now, Region: c.target, CopyOf: evt.SnapshotID,
	}

	if rr := ruleMap(c.rule, "RetainRule"); rr != nil {
		if at, ok := addInterval(rr["Interval"], rr["IntervalUnit"], now); ok {
			cp.ExpiresAt = at
		}
	}

	if err != nil {
		b.warn(ctx, "tag event copy failed", cp, err)
	}

	b.mu.Lock("HandleSnapshotShared")
	b.managed.Put(cp)
	b.mu.Unlock()
}

// runEventPolicy expires the policy's cross-region copies whose RetainRule has elapsed.
func (b *InMemoryBackend) runEventPolicy(ctx context.Context, p *storedPolicy, now time.Time) {
	if p.State != stateEnabled {
		return
	}

	var members []*managedResource

	for _, m := range b.managed.All() {
		if m.PolicyID == p.PolicyID {
			members = append(members, m)
		}
	}

	for _, m := range members {
		if !b.compute.Exists(m.regionOr(b.region), m.Kind, m.ID) {
			b.managed.Delete(m.ID)
		}
	}

	members = slices.DeleteFunc(members, func(m *managedResource) bool {
		_, ok := b.managed.Get(m.ID)

		return !ok
	})

	b.expireCopies(ctx, members, now)
}

// OnSnapshotShared routes an EC2 snapshot-shared event to the event-based policies of the snapshot's region.
func (h *Handler) OnSnapshotShared(ctx context.Context, evt ec2backend.SnapshotSharedEvent) {
	if mem, ok := h.BackendFor(evt.Region).(*InMemoryBackend); ok {
		mem.HandleSnapshotShared(ctx, time.Now().UTC(), evt)
	}
}
