package batch

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	minArrayJobSize = 2
	maxArrayJobSize = 10000

	dependencyTypeSequential = "SEQUENTIAL"
	dependencyTypeNToN       = "N_TO_N"
)

func arrayChildStatuses() []string {
	return []string{
		jobStatusSubmitted, jobStatusPending, jobStatusRunnable, jobStatusStarting,
		jobStatusRunning, jobStatusSucceeded, jobStatusFailed,
	}
}

func arrayChildID(parentID string, index int32) string {
	return parentID + ":" + strconv.Itoa(int(index))
}

func validateArraySize(ap *ArrayProperties) error {
	if ap == nil {
		return nil
	}

	if ap.Size < minArrayJobSize || ap.Size > maxArrayJobSize {
		return fmt.Errorf("%w: arrayProperties.size must be between %d and %d",
			ErrValidation, minArrayJobSize, maxArrayJobSize)
	}

	return nil
}

func isArrayParent(j *Job) bool {
	return j.ArrayParentID == "" && j.ArrayProperties != nil && j.ArrayProperties.Size > 0
}

// spawnArrayChildrenLocked creates one child job per array index. Caller must hold the write lock.
func (b *InMemoryBackend) spawnArrayChildrenLocked(parent *Job) {
	for i := range parent.ArrayProperties.Size {
		child := *parent
		child.JobID = arrayChildID(parent.JobID, i)
		child.JobARN = parent.JobARN + ":" + strconv.Itoa(int(i))
		child.ArrayParentID = parent.JobID
		child.ArrayProperties = &ArrayProperties{Index: i}
		child.Tags = tagsCloneOrEmpty(parent.Tags)
		child.Parameters = parent.Parameters
		child.Attempts = nil
		b.jobs.Put(&child)
	}
}

// arrayChildrenLocked returns parent's children ordered by index.
func (b *InMemoryBackend) arrayChildrenLocked(parent *Job) []*Job {
	out := make([]*Job, 0, parent.ArrayProperties.Size)

	for i := range parent.ArrayProperties.Size {
		if c, ok := b.jobs.Get(regionKey(parent.region, arrayChildID(parent.JobID, i))); ok {
			out = append(out, c)
		}
	}

	return out
}

func (b *InMemoryBackend) arrayStatusSummaryLocked(parent *Job) map[string]int32 {
	summary := make(map[string]int32, len(arrayChildStatuses()))
	for _, s := range arrayChildStatuses() {
		summary[s] = 0
	}

	for _, c := range b.arrayChildrenLocked(parent) {
		summary[c.Status]++
	}

	return summary
}

// withArrayDetailLocked refreshes a copied parent job's statusSummary from its children.
func (b *InMemoryBackend) withArrayDetailLocked(cp *Job) {
	if !isArrayParent(cp) {
		return
	}

	ap := *cp.ArrayProperties
	ap.StatusSummary = b.arrayStatusSummaryLocked(cp)
	now := time.Now().UnixMilli()
	ap.StatusSummaryLastUpdatedAt = &now
	cp.ArrayProperties = &ap
}

// syncArrayParentLocked derives a parent's status from its children. Caller must hold the write lock.
func (b *InMemoryBackend) syncArrayParentLocked(parent *Job, now int64) {
	if isTerminalJobStatus(parent.Status) {
		return
	}

	summary := b.arrayStatusSummaryLocked(parent)
	terminal := summary[jobStatusSucceeded] + summary[jobStatusFailed]

	switch {
	case terminal == parent.ArrayProperties.Size:
		parent.StoppedAt = &now

		if summary[jobStatusFailed] > 0 {
			parent.Status = jobStatusFailed
			parent.StatusReason = "one or more array child jobs failed"
		} else {
			parent.Status = jobStatusSucceeded
		}
	case summary[jobStatusSubmitted] < parent.ArrayProperties.Size:
		parent.Status = jobStatusPending
	}
}

// cascadeToChildrenLocked applies fn to every non-terminal child of parent.
func (b *InMemoryBackend) cascadeToChildrenLocked(parent *Job, fn func(*Job)) {
	if !isArrayParent(parent) {
		return
	}

	for _, c := range b.arrayChildrenLocked(parent) {
		if !isTerminalJobStatus(c.Status) {
			fn(c)
		}
	}
}

// ListJobChildren lists the children of an array job (arrayJobId) or the nodes of a multi-node job (multiNodeJobId).
func (b *InMemoryBackend) ListJobChildren(
	ctx context.Context,
	parentID, status, nextToken string,
	maxResults int32,
	multiNode bool,
) ([]*Job, string, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListJobChildren")
	defer b.mu.RUnlock()

	parent, ok := b.lookupJobByIDOrARN(region, parentID)
	if !ok {
		return nil, "", nil
	}

	var children []*Job

	switch {
	case multiNode:
		children = b.multiNodeChildrenLocked(parent)
	case isArrayParent(parent):
		children = b.arrayChildrenLocked(parent)
	}

	wantStatus := status
	if wantStatus == "" {
		wantStatus = jobStatusRunning
	}

	filtered := make([]*Job, 0, len(children))

	for _, c := range children {
		if c.Status == wantStatus {
			filtered = append(filtered, c)
		}
	}

	sort.SliceStable(filtered, func(i, j int) bool { return childOrder(filtered[i]) < childOrder(filtered[j]) })

	ids := make([]string, len(filtered))
	for i, c := range filtered {
		ids[i] = c.JobID
	}

	pageIDs, next := paginateMapKeys(ids, nextToken, maxResults)
	byID := make(map[string]*Job, len(filtered))

	for _, c := range filtered {
		byID[c.JobID] = c
	}

	out := make([]*Job, 0, len(pageIDs))

	for _, id := range pageIDs {
		cp := *byID[id]
		cp.Tags = tagsCloneOrEmpty(cp.Tags)
		out = append(out, &cp)
	}

	return out, next, nil
}

func childOrder(j *Job) int32 {
	if j.nodeIndex != nil {
		return *j.nodeIndex
	}

	if j.ArrayProperties != nil {
		return j.ArrayProperties.Index
	}

	return 0
}

// multiNodeChildrenLocked derives one node job per node of a multi-node parent.
func (b *InMemoryBackend) multiNodeChildrenLocked(parent *Job) []*Job {
	np := b.effectiveNodePropertiesLocked(parent)
	if np == nil {
		return nil
	}

	out := make([]*Job, 0, np.NumNodes)

	for i := range np.NumNodes {
		out = append(out, nodeJobFrom(parent, np, i))
	}

	return out
}

func nodeJobFrom(parent *Job, np *NodeProperties, index int32) *Job {
	n := *parent
	n.JobID = parent.JobID + "#" + strconv.Itoa(int(index))
	n.JobARN = parent.JobARN + "#" + strconv.Itoa(int(index))
	n.nodeIndex = &index
	n.nodeCount = np.NumNodes
	n.isMainNode = index == np.MainNode
	n.Attempts = nil

	return &n
}

// splitNodeJobID splits "<jobId>#<index>" into its parent ID and node index.
func splitNodeJobID(id string) (string, int32, bool) {
	base, idx, found := strings.Cut(id, "#")
	if !found {
		return id, 0, false
	}

	n, err := strconv.ParseInt(idx, 10, 32)
	if err != nil || n < 0 {
		return id, 0, false
	}

	return base, int32(n), true
}
