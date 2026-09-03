package networkmanager

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// This file implements PARITY.md families L (Core Networks, 5 ops), M (the
// versioned Core Network Policy lifecycle + change-set state machine, 8
// ops), N (Core Network Prefix List Associations, 3 ops), O (Core Network
// Routing Information, 1 op), and P (Attachment Routing Policy labels, 3
// ops) -- 20 ops total.
//
// Policy lifecycle: CoreNetworkPolicyHistory tracks the real LIVE-vs-LATEST
// alias split, the full PolicyVersionId history, ChangeSetState's real
// transitions, and the "can't delete the current LIVE policy" invariant --
// all genuine state-machine bookkeeping over caller-supplied JSON.
// GetCoreNetworkChangeSet/GetCoreNetworkChangeEvents compute a real
// ADD/MODIFY/REMOVE structural diff over the policy JSON's
// segments/network-function-groups/segment-actions/attachment-policies/
// core-network-configuration sections (corenetworkpolicydiff.go) --
// document-level, not correlated against live attachment membership, a
// documented scope reduction from the SDK's full 14-value ChangeType
// granularity, but never a fabricated plausible-looking diff.

// ---- Core Network ----

func (b *InMemoryBackend) CreateCoreNetwork(
	globalNetworkID, description, policyDocument string, tagMap map[string]string,
) (*CoreNetwork, error) {
	b.mu.Lock("CreateCoreNetwork")
	defer b.mu.Unlock()

	if !b.globalNetworkExists(globalNetworkID) {
		// CreateCoreNetwork's own deserializeOpError switch declares no
		// ResourceNotFoundException case -- ValidationException is the real
		// type for an unresolved GlobalNetworkId.
		return nil, validationError(fmt.Sprintf("%s %s not found", resourceGlobalNetwork, globalNetworkID))
	}

	if policyDocument != "" && !json.Valid([]byte(policyDocument)) {
		return nil, coreNetworkPolicyError("PolicyDocument is not valid JSON", []CoreNetworkPolicyError{
			{ErrorCode: "InvalidPolicyDocument", Message: "PolicyDocument must be valid JSON", Path: "/"},
		})
	}

	id := newCoreNetworkID()
	c := &CoreNetwork{
		CoreNetworkID:   id,
		CoreNetworkArn:  b.coreNetworkARN(id),
		GlobalNetworkID: globalNetworkID,
		CreatedAt:       nowUTC(),
		Description:     description,
		State:           coreNetworkStateCreating,
		Tags:            tags.FromMap("networkmanager.corenetwork."+id+".tags", tagMap),
	}
	b.coreNetworks.Put(c)

	hist := &CoreNetworkPolicyHistory{ownerCoreNetworkID: id, Versions: map[int32]*CoreNetworkPolicyVersion{}}

	if policyDocument != "" {
		hist.NextID = 1
		v := &CoreNetworkPolicyVersion{
			CreatedAt: nowUTC(), CoreNetworkID: id, PolicyDocument: policyDocument,
			PolicyVersionID: 1, ChangeSetState: changeSetStateExecutionSucceeded,
		}
		hist.Versions[1] = v
		hist.LiveID = 1
		hist.LatestID = 1
	}

	b.policyHistories.Put(hist)

	scheduleAdvance(b, "CoreNetworkAvailable", b.coreNetworks, id,
		func(v *CoreNetwork) *string { return &v.State }, coreNetworkStateCreating, coreNetworkStateAvailable)

	return c.clone(), nil
}

func (b *InMemoryBackend) UpdateCoreNetwork(id, description string) (*CoreNetwork, error) {
	b.mu.Lock("UpdateCoreNetwork")
	defer b.mu.Unlock()

	c, ok := b.coreNetworks.Get(id)
	if !ok {
		return nil, notFoundError(resourceCoreNetwork, id)
	}

	c.Description = description
	c.State = coreNetworkStateUpdating

	scheduleAdvance(b, "CoreNetworkUpdated", b.coreNetworks, id,
		func(v *CoreNetwork) *string { return &v.State }, coreNetworkStateUpdating, coreNetworkStateAvailable)

	return c.clone(), nil
}

func (b *InMemoryBackend) DeleteCoreNetwork(id string) (*CoreNetwork, error) {
	b.mu.Lock("DeleteCoreNetwork")
	defer b.mu.Unlock()

	c, ok := b.coreNetworks.Get(id)
	if !ok {
		return nil, notFoundError(resourceCoreNetwork, id)
	}

	c.State = coreNetworkStateDeleting
	scheduleRemoval(b, "CoreNetworkDeleted", b.coreNetworks, id)
	scheduleRemoval(b, "CoreNetworkPolicyHistoryDeleted", b.policyHistories, id)

	return c.clone(), nil
}

func (b *InMemoryBackend) GetCoreNetwork(id string) (*CoreNetwork, error) {
	b.mu.RLock("GetCoreNetwork")
	defer b.mu.RUnlock()

	c, ok := b.coreNetworks.Get(id)
	if !ok {
		return nil, notFoundError(resourceCoreNetwork, id)
	}

	return c.clone(), nil
}

func (b *InMemoryBackend) ListCoreNetworks(token string, limit int) page.Page[*CoreNetwork] {
	b.mu.RLock("ListCoreNetworks")
	defer b.mu.RUnlock()

	return page.New(cloneAll(b.coreNetworks.Snapshot(), (*CoreNetwork).clone), token, limit, defaultPageLimit)
}

// coreNetworkExists reports whether id names a known core network.
func (b *InMemoryBackend) coreNetworkExists(id string) bool {
	_, ok := b.coreNetworks.Get(id)

	return ok
}

// ---- Core Network Policy lifecycle ----

// historyLocked returns the policy history for coreNetworkID, creating an
// empty one on first reference (a CoreNetwork created without an initial
// PolicyDocument has no history until the first PutCoreNetworkPolicy).
// Callers must hold b.mu.
func (b *InMemoryBackend) historyLocked(coreNetworkID string) *CoreNetworkPolicyHistory {
	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		h = &CoreNetworkPolicyHistory{
			ownerCoreNetworkID: coreNetworkID,
			Versions:           map[int32]*CoreNetworkPolicyVersion{},
		}
		b.policyHistories.Put(h)
	}

	return h
}

func (b *InMemoryBackend) PutCoreNetworkPolicy(
	coreNetworkID, policyDocument, description string, latestVersionID int32,
) (*CoreNetworkPolicyHistory, *CoreNetworkPolicyVersion, error) {
	b.mu.Lock("PutCoreNetworkPolicy")
	defer b.mu.Unlock()

	if !b.coreNetworkExists(coreNetworkID) {
		return nil, nil, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	if !json.Valid([]byte(policyDocument)) {
		return nil, nil, coreNetworkPolicyError("PolicyDocument is not valid JSON", []CoreNetworkPolicyError{
			{ErrorCode: "InvalidPolicyDocument", Message: "PolicyDocument must be valid JSON", Path: "/"},
		})
	}

	h := b.historyLocked(coreNetworkID)

	if latestVersionID != 0 && latestVersionID != h.LatestID {
		return nil, nil, conflictError(resourcePolicyVersion, coreNetworkID,
			"LatestVersionId does not match the current LATEST policy version")
	}

	h.NextID++
	newID := h.NextID
	v := &CoreNetworkPolicyVersion{
		CreatedAt: nowUTC(), CoreNetworkID: coreNetworkID, Description: description, PolicyDocument: policyDocument,
		PolicyVersionID: newID, ChangeSetState: changeSetStatePendingGeneration,
	}
	h.Versions[newID] = v
	h.LatestID = newID

	scheduleAdvance(b, "CoreNetworkPolicyReadyToExecute", b.policyHistories, coreNetworkID,
		versionChangeSetStatePtr(newID), changeSetStatePendingGeneration, changeSetStateReadyToExecute)

	return h.clone(), v.clone(), nil
}

// versionChangeSetStatePtr returns a statePtr accessor into a specific
// version's ChangeSetState field, for use with scheduleAdvance -- the
// history itself (not any single version) is the table row, so this closes
// over versionID to reach into h.Versions[versionID].
func versionChangeSetStatePtr(versionID int32) func(*CoreNetworkPolicyHistory) *string {
	return func(h *CoreNetworkPolicyHistory) *string {
		v, ok := h.Versions[versionID]
		if !ok {
			// The version was deleted before the timer fired; return a
			// throwaway pointer so scheduleAdvance's compare-and-set is a
			// harmless no-op instead of a nil dereference.
			var discard string

			return &discard
		}

		return &v.ChangeSetState
	}
}

// resolvePolicyVersion resolves alias/explicit versionID to a concrete
// version, per GetCoreNetworkPolicy's semantics (family M): an explicit
// PolicyVersionId wins over Alias; Alias defaults to LATEST when both are
// omitted (PARITY.md flags AWS's own default as unconfirmed -- LATEST is
// this backend's documented, defensible choice).
func resolvePolicyVersion(
	h *CoreNetworkPolicyHistory,
	alias string,
	versionID int32,
) (*CoreNetworkPolicyVersion, string) {
	if versionID != 0 {
		v, ok := h.Versions[versionID]
		if !ok {
			return nil, ""
		}

		return v, aliasFor(h, versionID)
	}

	target := h.LatestID
	if alias == policyAliasLive {
		target = h.LiveID
	}

	if target == 0 {
		return nil, ""
	}

	return h.Versions[target], alias
}

// aliasFor reports which alias (if any) versionID currently holds. A
// version can be both LIVE and LATEST at once (immediately after
// ExecuteCoreNetworkChangeSet runs against the newest version); LIVE is
// reported first since that is the deployed, authoritative state.
func aliasFor(h *CoreNetworkPolicyHistory, versionID int32) string {
	switch versionID {
	case h.LiveID:
		return policyAliasLive
	case h.LatestID:
		return policyAliasLatest
	default:
		return ""
	}
}

func (b *InMemoryBackend) GetCoreNetworkPolicy(
	coreNetworkID, alias string, policyVersionID int32,
) (*CoreNetworkPolicyVersion, string, error) {
	b.mu.RLock("GetCoreNetworkPolicy")
	defer b.mu.RUnlock()

	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		return nil, "", notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	v, resolvedAlias := resolvePolicyVersion(h, alias, policyVersionID)
	if v == nil {
		return nil, "", notFoundError(resourcePolicyVersion, coreNetworkID)
	}

	return v.clone(), resolvedAlias, nil
}

func (b *InMemoryBackend) ListCoreNetworkPolicyVersions(
	coreNetworkID, token string, limit int,
) (page.Page[*CoreNetworkPolicyVersion], map[int32]string, error) {
	b.mu.RLock("ListCoreNetworkPolicyVersions")
	defer b.mu.RUnlock()

	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		if !b.coreNetworkExists(coreNetworkID) {
			return page.Page[*CoreNetworkPolicyVersion]{}, nil, notFoundError(resourceCoreNetwork, coreNetworkID)
		}

		return page.Page[*CoreNetworkPolicyVersion]{}, nil, nil
	}

	ids := make([]int32, 0, len(h.Versions))
	for id := range h.Versions {
		ids = append(ids, id)
	}

	slices.Sort(ids)

	all := make([]*CoreNetworkPolicyVersion, len(ids))
	aliases := make(map[int32]string, len(ids))

	for i, id := range ids {
		all[i] = h.Versions[id].clone()
		aliases[id] = aliasFor(h, id)
	}

	return page.New(all, token, limit, defaultPageLimit), aliases, nil
}

func (b *InMemoryBackend) DeleteCoreNetworkPolicyVersion(
	coreNetworkID string, policyVersionID int32,
) (*CoreNetworkPolicyVersion, string, error) {
	b.mu.Lock("DeleteCoreNetworkPolicyVersion")
	defer b.mu.Unlock()

	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		return nil, "", notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	v, ok := h.Versions[policyVersionID]
	if !ok {
		return nil, "", notFoundError(resourcePolicyVersion, coreNetworkID)
	}

	if policyVersionID == h.LiveID {
		return nil, "", conflictError(resourcePolicyVersion, coreNetworkID, "you can't delete the current LIVE policy")
	}

	alias := aliasFor(h, policyVersionID)
	delete(h.Versions, policyVersionID)

	if h.LatestID == policyVersionID {
		h.LatestID = h.LiveID
	}

	return v.clone(), alias, nil
}

func (b *InMemoryBackend) RestoreCoreNetworkPolicyVersion(
	coreNetworkID string, policyVersionID int32,
) (*CoreNetworkPolicyVersion, error) {
	b.mu.Lock("RestoreCoreNetworkPolicyVersion")
	defer b.mu.Unlock()

	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		return nil, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	old, ok := h.Versions[policyVersionID]
	if !ok {
		return nil, notFoundError(resourcePolicyVersion, coreNetworkID)
	}

	h.NextID++
	newID := h.NextID
	v := &CoreNetworkPolicyVersion{
		CreatedAt: nowUTC(), CoreNetworkID: coreNetworkID, Description: old.Description,
		PolicyDocument: old.PolicyDocument, PolicyVersionID: newID, ChangeSetState: changeSetStatePendingGeneration,
	}
	h.Versions[newID] = v
	h.LatestID = newID

	scheduleAdvance(b, "CoreNetworkPolicyRestoreReadyToExecute", b.policyHistories, coreNetworkID,
		versionChangeSetStatePtr(newID), changeSetStatePendingGeneration, changeSetStateReadyToExecute)

	return v.clone(), nil
}

// GetCoreNetworkChangeSet validates coreNetworkID/policyVersionID and
// returns the real structural diff between the LIVE policy and
// policyVersionID's own document -- see corenetworkpolicydiff.go.
func (b *InMemoryBackend) GetCoreNetworkChangeSet(
	coreNetworkID string, policyVersionID int32,
) ([]CoreNetworkChange, error) {
	b.mu.RLock("GetCoreNetworkChangeSet")
	defer b.mu.RUnlock()

	h, v, err := b.resolvePolicyHistoryAndVersionLocked(coreNetworkID, policyVersionID)
	if err != nil {
		return nil, err
	}

	var liveDoc string
	if live, ok := h.Versions[h.LiveID]; ok {
		liveDoc = live.PolicyDocument
	}

	return diffCoreNetworkPolicy(liveDoc, v.PolicyDocument), nil
}

// GetCoreNetworkChangeEvents validates coreNetworkID/policyVersionID and
// derives per-change EXECUTION progress from the same diff
// GetCoreNetworkChangeSet computes, plus policyVersionID's own
// ChangeSetState -- see corenetworkpolicydiff.go's changesToEvents doc
// comment for the coarseness this implies (one status per changeset, not
// per IdentifierPath).
func (b *InMemoryBackend) GetCoreNetworkChangeEvents(
	coreNetworkID string, policyVersionID int32,
) ([]CoreNetworkChangeEvent, error) {
	b.mu.RLock("GetCoreNetworkChangeEvents")
	defer b.mu.RUnlock()

	h, v, err := b.resolvePolicyHistoryAndVersionLocked(coreNetworkID, policyVersionID)
	if err != nil {
		return nil, err
	}

	var liveDoc string
	if live, ok := h.Versions[h.LiveID]; ok {
		liveDoc = live.PolicyDocument
	}

	changes := diffCoreNetworkPolicy(liveDoc, v.PolicyDocument)

	return changesToEvents(changes, v.ChangeSetState), nil
}

// resolvePolicyHistoryAndVersionLocked looks up coreNetworkID's policy
// history and policyVersionID within it. Callers must hold b.mu (read lock
// suffices).
func (b *InMemoryBackend) resolvePolicyHistoryAndVersionLocked(
	coreNetworkID string, policyVersionID int32,
) (*CoreNetworkPolicyHistory, *CoreNetworkPolicyVersion, error) {
	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		return nil, nil, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	v, ok := h.Versions[policyVersionID]
	if !ok {
		return nil, nil, notFoundError(resourcePolicyVersion, coreNetworkID)
	}

	return h, v, nil
}

func (b *InMemoryBackend) ExecuteCoreNetworkChangeSet(coreNetworkID string, policyVersionID int32) error {
	b.mu.Lock("ExecuteCoreNetworkChangeSet")
	defer b.mu.Unlock()

	h, ok := b.policyHistories.Get(coreNetworkID)
	if !ok {
		return notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	v, ok := h.Versions[policyVersionID]
	if !ok {
		return notFoundError(resourcePolicyVersion, coreNetworkID)
	}

	if v.ChangeSetState != changeSetStateReadyToExecute {
		return conflictError(resourcePolicyVersion, coreNetworkID,
			"change set is not READY_TO_EXECUTE (state="+v.ChangeSetState+")")
	}

	v.ChangeSetState = changeSetStateExecuting

	b.work.After("CoreNetworkChangeSetExecuted", asyncTransitionDelay, func() {
		b.mu.Lock("CoreNetworkChangeSetExecuted-async")
		defer b.mu.Unlock()

		hh, hOK := b.policyHistories.Get(coreNetworkID)
		if !hOK {
			return
		}

		vv, vOK := hh.Versions[policyVersionID]
		if !vOK || vv.ChangeSetState != changeSetStateExecuting {
			return
		}

		vv.ChangeSetState = changeSetStateExecutionSucceeded
		hh.LiveID = policyVersionID
	})

	return nil
}

// ---- Core Network Prefix List Association ----

func (b *InMemoryBackend) CreateCoreNetworkPrefixListAssociation(
	coreNetworkID, prefixListAlias, prefixListArn string,
) (*CoreNetworkPrefixListAssociation, error) {
	b.mu.Lock("CreateCoreNetworkPrefixListAssociation")
	defer b.mu.Unlock()

	if !b.coreNetworkExists(coreNetworkID) {
		return nil, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	a := &CoreNetworkPrefixListAssociation{
		CoreNetworkID:   coreNetworkID,
		PrefixListAlias: prefixListAlias,
		PrefixListArn:   prefixListArn,
	}
	b.prefixListAssocs.Put(a)

	return a.clone(), nil
}

func (b *InMemoryBackend) DeleteCoreNetworkPrefixListAssociation(
	coreNetworkID, prefixListArn string,
) (*CoreNetworkPrefixListAssociation, error) {
	b.mu.Lock("DeleteCoreNetworkPrefixListAssociation")
	defer b.mu.Unlock()

	key := prefixListAssocKey(prefixListArn, coreNetworkID)

	a, ok := b.prefixListAssocs.Get(key)
	if !ok {
		return nil, notFoundError(resourcePrefixListAssn, key)
	}

	b.prefixListAssocs.Delete(key)

	return a.clone(), nil
}

func (b *InMemoryBackend) ListCoreNetworkPrefixListAssociations(
	coreNetworkID, prefixListArn, token string, limit int,
) (page.Page[*CoreNetworkPrefixListAssociation], error) {
	b.mu.RLock("ListCoreNetworkPrefixListAssociations")
	defer b.mu.RUnlock()

	if !b.coreNetworkExists(coreNetworkID) {
		return page.Page[*CoreNetworkPrefixListAssociation]{}, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	all := filterByGlobalNetwork(
		b.prefixListAssocs.Snapshot(),
		coreNetworkID,
		func(a *CoreNetworkPrefixListAssociation) string { return a.CoreNetworkID },
	)
	all = filterOptional(
		all,
		prefixListArn,
		func(a *CoreNetworkPrefixListAssociation) string { return a.PrefixListArn },
	)

	return page.New(cloneAll(all, (*CoreNetworkPrefixListAssociation).clone), token, limit, defaultPageLimit), nil
}

// ---- Core Network Routing Information ----

// ListCoreNetworkRoutingInformation is a real BGP route-table read filtered
// by BGP path attributes (AS path, communities, local preference, MED) --
// honestly derivable ONLY if this emulator tracked real BGP-attribute-
// tagged routes per segment/edge, which it does not (no route-propagation
// engine exists here). This validates its required inputs and returns an
// honest empty list, per PARITY.md's own framing (matching directconnect's
// BGP-peering honesty precedent) rather than inventing plausible AS-path/
// community values.
func (b *InMemoryBackend) ListCoreNetworkRoutingInformation(coreNetworkID, edgeLocation, segmentName string) error {
	b.mu.RLock("ListCoreNetworkRoutingInformation")
	defer b.mu.RUnlock()

	if !b.coreNetworkExists(coreNetworkID) {
		return notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	if edgeLocation == "" || segmentName == "" {
		return validationError("EdgeLocation and SegmentName are required")
	}

	return nil
}

// ---- Attachment Routing Policy labels ----

func (b *InMemoryBackend) PutAttachmentRoutingPolicyLabel(
	coreNetworkID, attachmentID, label string,
) (*AttachmentRoutingPolicyLabel, error) {
	b.mu.Lock("PutAttachmentRoutingPolicyLabel")
	defer b.mu.Unlock()

	att, ok := b.attachments.Get(attachmentID)
	if !ok || att.CoreNetworkID != coreNetworkID {
		return nil, notFoundError(resourceAttachment, attachmentID)
	}

	att.RoutingPolicyLabel = label

	l := &AttachmentRoutingPolicyLabel{
		AttachmentID:       attachmentID,
		CoreNetworkID:      coreNetworkID,
		RoutingPolicyLabel: label,
	}
	b.routingPolicyLabels.Put(l)

	return l.clone(), nil
}

func (b *InMemoryBackend) RemoveAttachmentRoutingPolicyLabel(
	coreNetworkID, attachmentID string,
) (*AttachmentRoutingPolicyLabel, error) {
	b.mu.Lock("RemoveAttachmentRoutingPolicyLabel")
	defer b.mu.Unlock()

	key := routingPolicyLabelKey(coreNetworkID, attachmentID)

	l, ok := b.routingPolicyLabels.Get(key)
	if !ok {
		return nil, notFoundError(resourceRoutingLabel, key)
	}

	b.routingPolicyLabels.Delete(key)

	if att, attOK := b.attachments.Get(attachmentID); attOK {
		att.RoutingPolicyLabel = ""
	}

	return l.clone(), nil
}

func (b *InMemoryBackend) ListAttachmentRoutingPolicyAssociations(
	coreNetworkID, attachmentID, token string, limit int,
) (page.Page[*AttachmentRoutingPolicyLabel], error) {
	b.mu.RLock("ListAttachmentRoutingPolicyAssociations")
	defer b.mu.RUnlock()

	if !b.coreNetworkExists(coreNetworkID) {
		return page.Page[*AttachmentRoutingPolicyLabel]{}, notFoundError(resourceCoreNetwork, coreNetworkID)
	}

	all := filterByGlobalNetwork(
		b.routingPolicyLabels.Snapshot(),
		coreNetworkID,
		func(l *AttachmentRoutingPolicyLabel) string { return l.CoreNetworkID },
	)
	all = filterOptional(all, attachmentID, func(l *AttachmentRoutingPolicyLabel) string { return l.AttachmentID })

	return page.New(cloneAll(all, (*AttachmentRoutingPolicyLabel).clone), token, limit, defaultPageLimit), nil
}
