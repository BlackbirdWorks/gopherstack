package opensearch

import (
	"fmt"
	"time"
)

// dataSourceAttachmentKey builds the composite key shared by every data
// source attachment (an application can have at most one attachment per real
// data source ARN, matching AttachDataSource's documented idempotency: "If a
// data source is already attached or pending for the same application, the
// existing attachment is returned").
func dataSourceAttachmentKey(applicationID, dataSourceArn string) string {
	return applicationID + "#" + dataSourceArn
}

func dataSourceAttachmentKeyFn(v *DataSourceAttachment) string {
	return dataSourceAttachmentKey(v.ApplicationID, v.DataSourceArn)
}

// resolveDataSourceRefLocked reports whether dataSourceArn refers to a real,
// currently-tracked resource this backend can attach to an application --
// either an OpenSearch domain or an OpenSearch Serverless collection, the two
// resource kinds AttachDataSource's operation doc names ("The data source can
// be an Amazon OpenSearch Service domain or an Amazon OpenSearch Serverless
// collection") -- and, if found, whether that resource is currently active
// (vs. still in a transient creating/processing window). The caller must
// hold at least a read lock.
func (b *InMemoryBackend) resolveDataSourceRefLocked(dataSourceArn string) (bool, bool) {
	if d := b.findDomainByARN(dataSourceArn); d != nil {
		processing, _, _ := domainProcessing(d, b.clock())

		return true, !processing && !d.Deleted
	}

	for _, c := range b.slCollections.All() {
		if c.Arn != dataSourceArn {
			continue
		}

		cp := *c
		resolveCollectionStatus(&cp, b.clock())

		return true, cp.Status == slCollectionStatusActive
	}

	return false, false
}

// resolveAttachmentStatus settles a PENDING attachment copy's status: to
// ATTACHED once its referenced resource has become active, or to FAILED once
// dsAttachmentFailWindow has elapsed with the resource still not active --
// matching AttachDataSource's documented "Pending attachments that are not
// completed within 24 hours are marked as FAILED". It never mutates stored
// state. The caller must hold at least a read lock.
func (b *InMemoryBackend) resolveAttachmentStatus(att *DataSourceAttachment, now time.Time) {
	if att.Status != dsAttachmentStatusPending {
		return
	}

	if _, active := b.resolveDataSourceRefLocked(att.DataSourceArn); active {
		att.Status = dsAttachmentStatusAttached

		return
	}

	if now.Sub(att.CreatedAt) > dsAttachmentFailWindow {
		att.Status = dsAttachmentStatusFailed
	}
}

// AttachDataSource attaches a real data source (domain or serverless
// collection ARN) to an OpenSearch application. Idempotent: re-attaching the
// same (applicationID, dataSourceArn) pair returns the existing attachment
// rather than creating a duplicate. workspaceConfig/workspaceID are the
// optional WorkspaceConfiguration/WorkspaceId request fields (see
// resolveWorkspaceConfigLocked and the Workspace doc comment in models.go
// for why this can validate/create a workspace but never surfaces one back
// to the caller -- AttachDataSourceOutput has no field for it); both are
// validated on every call, including idempotent replays, since a validation
// error should not be silently skipped just because the attachment itself
// already exists.
func (b *InMemoryBackend) AttachDataSource(
	applicationID, dataSourceArn string,
	workspaceConfig *WorkspaceConfigInput, workspaceID string,
) (*DataSourceAttachment, error) {
	if applicationID == "" {
		return nil, fmt.Errorf("%w: Id is required", ErrInvalidParameter)
	}

	if dataSourceArn == "" {
		return nil, fmt.Errorf("%w: DataSourceArn is required", ErrInvalidParameter)
	}

	b.mu.Lock("AttachDataSource")
	defer b.mu.Unlock()

	if !b.applications.Has(applicationID) {
		return nil, fmt.Errorf("%w: application %s not found", ErrApplicationNotFound, applicationID)
	}

	if err := b.resolveWorkspaceConfigLocked(applicationID, workspaceConfig, workspaceID); err != nil {
		return nil, err
	}

	key := dataSourceAttachmentKey(applicationID, dataSourceArn)
	if existing, ok := b.dataSourceAttachments.Get(key); ok {
		cp := *existing
		b.resolveAttachmentStatus(&cp, b.clock())

		return &cp, nil
	}

	found, active := b.resolveDataSourceRefLocked(dataSourceArn)
	if !found {
		return nil, fmt.Errorf(
			"%w: dataSourceArn %s does not reference a known domain or serverless collection",
			ErrDataSourceNotFound,
			dataSourceArn,
		)
	}

	status := dsAttachmentStatusPending
	if active {
		status = dsAttachmentStatusAttached
	}

	b.dsAttachCounter++
	att := &DataSourceAttachment{
		AttachmentID:  fmt.Sprintf("attach-%d", b.dsAttachCounter),
		ApplicationID: applicationID,
		DataSourceArn: dataSourceArn,
		Status:        status,
		CreatedAt:     b.clock(),
	}
	b.dataSourceAttachments.Put(att)

	cp := *att

	return &cp, nil
}

// DetachDataSource removes an existing attachment. Returns ErrAttachmentNotFound
// if no attachment exists for the (applicationID, dataSourceArn) pair, and
// ErrAttachmentConflict if the attachment is still PENDING (matching
// DetachDataSource's documented ConflictException for a pending attachment).
func (b *InMemoryBackend) DetachDataSource(
	applicationID, dataSourceArn string,
) (*DataSourceAttachment, error) {
	b.mu.Lock("DetachDataSource")
	defer b.mu.Unlock()

	key := dataSourceAttachmentKey(applicationID, dataSourceArn)

	att, ok := b.dataSourceAttachments.Get(key)
	if !ok {
		return nil, fmt.Errorf(
			"%w: no data source attachment for application %s and data source %s",
			ErrAttachmentNotFound,
			applicationID,
			dataSourceArn,
		)
	}

	cp := *att
	b.resolveAttachmentStatus(&cp, b.clock())

	if cp.Status == dsAttachmentStatusPending {
		return nil, fmt.Errorf(
			"%w: attachment for application %s and data source %s is still pending",
			ErrAttachmentConflict,
			applicationID,
			dataSourceArn,
		)
	}

	b.dataSourceAttachments.Delete(key)

	return &cp, nil
}

// DescribeDataSourceAttachment returns the current status and details of a
// specific data source attachment.
func (b *InMemoryBackend) DescribeDataSourceAttachment(
	applicationID, dataSourceArn string,
) (*DataSourceAttachment, error) {
	b.mu.RLock("DescribeDataSourceAttachment")
	defer b.mu.RUnlock()

	att, ok := b.dataSourceAttachments.Get(dataSourceAttachmentKey(applicationID, dataSourceArn))
	if !ok {
		return nil, fmt.Errorf(
			"%w: no data source attachment for application %s and data source %s",
			ErrAttachmentNotFound,
			applicationID,
			dataSourceArn,
		)
	}

	cp := *att
	b.resolveAttachmentStatus(&cp, b.clock())

	return &cp, nil
}

// ListDataSourceAttachments returns every attachment (of any status) for the
// given application.
func (b *InMemoryBackend) ListDataSourceAttachments(applicationID string) []*DataSourceAttachment {
	b.mu.RLock("ListDataSourceAttachments")
	defer b.mu.RUnlock()

	group := b.dataSourceAttachmentsByApp.Get(applicationID)
	now := b.clock()
	out := make([]*DataSourceAttachment, 0, len(group))

	for _, att := range group {
		cp := *att
		b.resolveAttachmentStatus(&cp, now)
		out = append(out, &cp)
	}

	return out
}
