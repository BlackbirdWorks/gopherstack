package ec2

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type copySnapshotResponse struct {
	XMLName    xml.Name        `xml:"CopySnapshotResponse"`
	RequestID  string          `xml:"requestId"`
	SnapshotID string          `xml:"snapshotId"`
	TagSet     []simpleTagItem `xml:"tagSet>item"`
}

type snapshotSetItem struct {
	SnapshotID  string          `xml:"snapshotId"`
	VolumeID    string          `xml:"volumeId"`
	Description string          `xml:"description"`
	State       string          `xml:"state"`
	Progress    string          `xml:"progress"`
	StartTime   string          `xml:"startTime"`
	OwnerID     string          `xml:"ownerId,omitempty"`
	TagSet      []simpleTagItem `xml:"tagSet>item"`
	VolumeSize  int             `xml:"volumeSize"`
	Encrypted   bool            `xml:"encrypted"`
}

type createSnapshotsResponse struct {
	XMLName     xml.Name `xml:"CreateSnapshotsResponse"`
	RequestID   string   `xml:"requestId"`
	SnapshotSet struct {
		Items []snapshotSetItem `xml:"item"`
	} `xml:"snapshotSet"`
}

type snapshotBlockAccessStateResponse struct {
	XMLName   xml.Name `xml:"GetSnapshotBlockPublicAccessStateResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type enableSnapshotBlockPublicAccessResponse struct {
	XMLName   xml.Name `xml:"EnableSnapshotBlockPublicAccessResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type disableSnapshotBlockPublicAccessResponse struct {
	XMLName   xml.Name `xml:"DisableSnapshotBlockPublicAccessResponse"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

type snapshotTierStatusItem struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	StorageTier string `xml:"storageTier"`
}

type describeSnapshotTierStatusResponse struct {
	XMLName               xml.Name `xml:"DescribeSnapshotTierStatusResponse"`
	RequestID             string   `xml:"requestId"`
	NextToken             string   `xml:"nextToken,omitempty"`
	SnapshotTierStatusSet struct {
		Items []snapshotTierStatusItem `xml:"item"`
	} `xml:"snapshotTierStatusSet"`
}

type modifySnapshotTierResponse struct {
	XMLName          xml.Name `xml:"ModifySnapshotTierResponse"`
	RequestID        string   `xml:"requestId"`
	SnapshotID       string   `xml:"snapshotId"`
	TieringStartTime string   `xml:"tieringStartTime"`
}

func (h *Handler) handleCopySnapshot(vals url.Values, reqID string) (any, error) {
	sourceID := vals.Get("SourceSnapshotId")
	description := vals.Get("Description")
	encrypted := vals.Get("Encrypted") == ec2BooleanTrue
	kmsKeyID := vals.Get("KmsKeyId")

	snap, err := h.Backend.CopySnapshot(sourceID, description, encrypted, kmsKeyID)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, resourceTypeSnapshot)
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{snap.SnapshotID}, tags); err != nil {
			return nil, err
		}
	}

	return &copySnapshotResponse{
		RequestID:  reqID,
		SnapshotID: snap.SnapshotID,
		TagSet:     tagItemsFromMap(tags),
	}, nil
}

func (h *Handler) handleCreateSnapshots(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceSpecification.InstanceId")
	if instanceID == "" {
		return nil, fmt.Errorf("%w: InstanceSpecification.InstanceId is required", ErrInvalidParameter)
	}

	excludeBootVolume := vals.Get("InstanceSpecification.ExcludeBootVolume") == "true"
	excludeDataVolumeIDs := parseMemberList(vals, "InstanceSpecification.ExcludeDataVolumeId")
	description := vals.Get("Description")

	snaps, err := h.Backend.CreateSnapshots(instanceID, excludeBootVolume, excludeDataVolumeIDs, description)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, resourceTypeSnapshot)

	resp := &createSnapshotsResponse{RequestID: reqID}
	for _, snap := range snaps {
		if len(tags) > 0 {
			if err = h.Backend.CreateTags([]string{snap.SnapshotID}, tags); err != nil {
				return nil, err
			}
		}

		resp.SnapshotSet.Items = append(resp.SnapshotSet.Items, snapshotSetItem{
			SnapshotID:  snap.SnapshotID,
			VolumeID:    snap.VolumeID,
			Description: snap.Description,
			State:       snap.State,
			Progress:    snap.Progress,
			StartTime:   snap.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			OwnerID:     snap.OwnerID,
			TagSet:      tagItemsFromMap(tags),
			VolumeSize:  snap.VolumeSize,
			Encrypted:   snap.Encrypted,
		})
	}

	return resp, nil
}

func (h *Handler) handleGetSnapshotBlockPublicAccessState(_ url.Values, reqID string) (any, error) {
	state := h.Backend.GetSnapshotBlockPublicAccessState()

	return &snapshotBlockAccessStateResponse{
		XMLName:   xml.Name{Local: "GetSnapshotBlockPublicAccessStateResponse"},
		RequestID: reqID,
		State:     state,
	}, nil
}

func (h *Handler) handleEnableSnapshotBlockPublicAccess(
	vals url.Values,
	reqID string,
) (any, error) {
	state := vals.Get("State")
	if state == "" {
		state = "block-all-sharing"
	}
	if err := h.Backend.EnableSnapshotBlockPublicAccess(state); err != nil {
		return nil, err
	}

	return &enableSnapshotBlockPublicAccessResponse{RequestID: reqID, State: state}, nil
}

func (h *Handler) handleDisableSnapshotBlockPublicAccess(_ url.Values, reqID string) (any, error) {
	h.Backend.DisableSnapshotBlockPublicAccess()

	return &disableSnapshotBlockPublicAccessResponse{RequestID: reqID, State: "unblocked"}, nil
}

// applySnapshotTierFilters matches DescribeSnapshotTierStatusInput's real
// filters (api_op_DescribeSnapshotTierStatus.go doc comment: snapshot-id,
// volume-id, last-tiering-operation). last-tiering-operation is left
// unenforced -- this backend tracks only the current tier, not the archive/
// restore operation history the filter values describe -- rather than
// fabricating a match against untracked state.
func applySnapshotTierFilters(items []SnapshotTierItem, filters map[string][]string) []SnapshotTierItem {
	if len(filters) == 0 {
		return items
	}

	out := items[:0:0]
itemLoop:
	for _, item := range items {
		for name, values := range filters {
			switch name {
			case "snapshot-id":
				if !anyEqual(item.SnapshotID, values) {
					continue itemLoop
				}
			case filterKeyVolumeID:
				if !anyEqual(item.VolumeID, values) {
					continue itemLoop
				}
			}
		}

		out = append(out, item)
	}

	return out
}

func (h *Handler) handleDescribeSnapshotTierStatus(vals url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeSnapshotTierStatus(nil)
	items = applySnapshotTierFilters(items, parseEC2Filters(vals))

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	items, nextToken = pageSlice(items, offset, maxResults)

	resp := &describeSnapshotTierStatusResponse{RequestID: reqID, NextToken: nextToken}
	for _, item := range items {
		resp.SnapshotTierStatusSet.Items = append(
			resp.SnapshotTierStatusSet.Items,
			snapshotTierStatusItem{ //nolint:staticcheck // xml tags differ from backend type field names
				SnapshotID:  item.SnapshotID,
				VolumeID:    item.VolumeID,
				StorageTier: item.StorageTier,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleModifySnapshotTier(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	storageTier := vals.Get("StorageTier")
	if storageTier == "" {
		storageTier = "archive"
	}
	if err := h.Backend.ModifySnapshotTier(snapshotID, storageTier); err != nil {
		return nil, err
	}

	return &modifySnapshotTierResponse{
		RequestID:        reqID,
		SnapshotID:       snapshotID,
		TieringStartTime: "2006-01-02T15:04:05.000Z",
	}, nil
}

func (h *Handler) handleResetSnapshotAttribute(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	if err := h.Backend.ResetSnapshotAttribute(snapshotID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "ResetSnapshotAttributeResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

type lockSnapshotResponse struct {
	XMLName       xml.Name `xml:"LockSnapshotResponse"`
	RequestID     string   `xml:"requestId"`
	SnapshotID    string   `xml:"snapshotId"`
	LockState     string   `xml:"lockState"`
	LockCreatedOn string   `xml:"lockCreatedOn"`
	LockExpiresOn string   `xml:"lockExpiresOn,omitempty"`
	LockDuration  int      `xml:"lockDuration,omitempty"`
}

type describeLockedSnapshotsResponse struct {
	XMLName     xml.Name `xml:"DescribeLockedSnapshotsResponse"`
	RequestID   string   `xml:"requestId"`
	NextToken   string   `xml:"nextToken,omitempty"`
	SnapshotSet struct {
		Items []snapshotLockItem `xml:"item"`
	} `xml:"snapshotSet"`
}

func (h *Handler) handleLockSnapshot(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	lockMode := vals.Get("LockMode")
	if lockMode == "" {
		lockMode = "compliance"
	}
	var durationDays int
	if d := vals.Get("LockDuration"); d != "" {
		_, _ = fmt.Sscan(d, &durationDays)
	}

	lock, err := h.Backend.LockSnapshot(snapshotID, lockMode, durationDays)
	if err != nil {
		return nil, err
	}

	resp := &lockSnapshotResponse{
		RequestID:     reqID,
		SnapshotID:    lock.SnapshotID,
		LockState:     lock.LockState,
		LockDuration:  lock.LockDurationDays,
		LockCreatedOn: lock.LockCreatedOn.UTC().Format("2006-01-02T15:04:05.000Z"),
	}
	if !lock.LockExpiresOn.IsZero() {
		resp.LockExpiresOn = lock.LockExpiresOn.UTC().Format("2006-01-02T15:04:05.000Z")
	}

	return resp, nil
}

type unlockSnapshotResponse struct {
	XMLName    xml.Name `xml:"UnlockSnapshotResponse"`
	RequestID  string   `xml:"requestId"`
	SnapshotID string   `xml:"snapshotId"`
}

func (h *Handler) handleUnlockSnapshot(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	if err := h.Backend.UnlockSnapshot(snapshotID); err != nil {
		return nil, err
	}

	return &unlockSnapshotResponse{
		RequestID:  reqID,
		SnapshotID: snapshotID,
	}, nil
}

func (h *Handler) handleDescribeLockedSnapshots(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	locks := h.Backend.DescribeLockedSnapshots(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	locks, nextToken = pageSlice(locks, offset, maxResults)

	resp := &describeLockedSnapshotsResponse{RequestID: reqID, NextToken: nextToken}
	for _, l := range locks {
		item := snapshotLockItem{
			SnapshotID:       l.SnapshotID,
			LockState:        l.LockState,
			LockCreatedOn:    l.LockCreatedOn.UTC().Format("2006-01-02T15:04:05.000Z"),
			LockDurationDays: l.LockDurationDays,
		}
		if !l.LockExpiresOn.IsZero() {
			item.LockExpiresOn = l.LockExpiresOn.UTC().Format("2006-01-02T15:04:05.000Z")
		}
		resp.SnapshotSet.Items = append(resp.SnapshotSet.Items, item)
	}

	return resp, nil
}

type listSnapshotsInRecycleBinResponse struct {
	XMLName     xml.Name `xml:"ListSnapshotsInRecycleBinResponse"`
	RequestID   string   `xml:"requestId"`
	NextToken   string   `xml:"nextToken,omitempty"`
	SnapshotSet struct {
		Items []recycleBinSnapshotItem `xml:"item"`
	} `xml:"snapshotSet"`
}

// snapshotTaskDetailItem matches types.SnapshotTaskDetail, nested under
// ImportSnapshotTask/ImportSnapshotOutput's "snapshotTaskDetail" element
// (ec2@v1.319.1 deserializers.go:158042) -- status does NOT sit at the top
// level of importSnapshotTaskItem/importSnapshotResponse.
type snapshotTaskDetailItem struct {
	Status      string `xml:"status,omitempty"`
	SnapshotID  string `xml:"snapshotId,omitempty"`
	Description string `xml:"description,omitempty"`
	KmsKeyID    string `xml:"kmsKeyId,omitempty"`
	Encrypted   bool   `xml:"encrypted"`
}

// importSnapshotTaskItem matches types.ImportSnapshotTask (ec2@v1.319.1
// deserializers.go:109707).
type importSnapshotTaskItem struct {
	ImportTaskID       string                 `xml:"importTaskId"`
	Description        string                 `xml:"description,omitempty"`
	SnapshotTaskDetail snapshotTaskDetailItem `xml:"snapshotTaskDetail"`
}

// importSnapshotResponse matches ImportSnapshotOutput (ec2@v1.319.1
// deserializers.go:215941, same description/snapshotTaskDetail nesting).
type importSnapshotResponse struct {
	XMLName            xml.Name               `xml:"ImportSnapshotResponse"`
	RequestID          string                 `xml:"requestId"`
	ImportTaskID       string                 `xml:"importTaskId"`
	Description        string                 `xml:"description,omitempty"`
	SnapshotTaskDetail snapshotTaskDetailItem `xml:"snapshotTaskDetail"`
}

type describeImportSnapshotTasksResponse struct {
	XMLName               xml.Name `xml:"DescribeImportSnapshotTasksResponse"`
	RequestID             string   `xml:"requestId"`
	NextToken             string   `xml:"nextToken,omitempty"`
	ImportSnapshotTaskSet struct {
		Items []importSnapshotTaskItem `xml:"item"`
	} `xml:"importSnapshotTaskSet"`
}

type fastLaunchImageItem struct {
	LaunchTemplate        *fastLaunchLaunchTemplateItem `xml:"launchTemplate,omitempty"`
	SnapshotConfiguration *fastLaunchSnapshotConfigItem `xml:"snapshotConfiguration,omitempty"`
	ImageID               string                        `xml:"imageId"`
	State                 string                        `xml:"state"`
	ResourceType          string                        `xml:"resourceType,omitempty"`
	OwnerID               string                        `xml:"ownerId,omitempty"`
	MaxParallelLaunches   int                           `xml:"maxParallelLaunches,omitempty"`
}

func toFastLaunchImageItem(item FastLaunchImageItem, ownerID string) fastLaunchImageItem {
	out := fastLaunchImageItem{
		ImageID:             item.ImageID,
		State:               item.State,
		ResourceType:        item.ResourceType,
		OwnerID:             ownerID,
		MaxParallelLaunches: item.MaxParallelLaunches,
	}
	if item.HasLaunchTemplate {
		out.LaunchTemplate = &fastLaunchLaunchTemplateItem{
			LaunchTemplateID:   item.LaunchTemplateID,
			LaunchTemplateName: item.LaunchTemplateName,
			Version:            item.LaunchTemplateVersion,
		}
	}

	if item.HasSnapshotConfiguration {
		out.SnapshotConfiguration = &fastLaunchSnapshotConfigItem{TargetResourceCount: item.SnapshotTargetResourceCount}
	}

	return out
}

type enableDisableFastSnapshotRestoresResponse struct {
	XMLName    xml.Name
	RequestID  string `xml:"requestId"`
	Successful struct {
		Items []fastSnapshotRestoreItem `xml:"item"`
	} `xml:"successful"`
}

type describeFastSnapshotRestoresResponse struct {
	XMLName                xml.Name `xml:"DescribeFastSnapshotRestoresResponse"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
	FastSnapshotRestoreSet struct {
		Items []fastSnapshotRestoreItem `xml:"item"`
	} `xml:"fastSnapshotRestoreSet"`
}

func (h *Handler) handleListSnapshotsInRecycleBin(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	snaps := h.Backend.ListSnapshotsInRecycleBin(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	snaps, nextToken = pageSlice(snaps, offset, maxResults)

	resp := &listSnapshotsInRecycleBinResponse{RequestID: reqID, NextToken: nextToken}
	for _, snap := range snaps {
		resp.SnapshotSet.Items = append(
			resp.SnapshotSet.Items,
			recycleBinSnapshotItem{SnapshotID: snap.SnapshotID},
		)
	}

	return resp, nil
}

// handleRestoreSnapshotFromRecycleBin: RestoreSnapshotFromRecycleBinOutput is
// a near-full snapshot detail object, not a bare Return -- and the fields are
// buildable from the Snapshot this backend already holds in hand at the
// point it restores one (see RestoreSnapshotFromRecycleBin, snapshots.go).
// NOT fixed here: nothing in this backend ever populates recycleBinSnapshots
// in the first place (grep confirms no .Put call on that table anywhere --
// DeleteSnapshot deletes outright, it never moves a snapshot to the recycle
// bin), so this op can never succeed against a real snapshot today
// regardless of response shape. That's a deeper gap than "response not
// wired up": it's "the precondition state this op reads can never exist "
// -- fixing DeleteSnapshot to model recycle-bin retention is out of scope
// for a wire-shape pass. See PARITY.md.
func (h *Handler) handleRestoreSnapshotFromRecycleBin(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SnapshotId")
	if err := h.Backend.RestoreSnapshotFromRecycleBin(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreSnapshotFromRecycleBinResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRestoreSnapshotTier(vals url.Values, reqID string) (any, error) {
	id := vals.Get("SnapshotId")
	if err := h.Backend.RestoreSnapshotTier(id); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "RestoreSnapshotTierResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleImportSnapshot(vals url.Values, reqID string) (any, error) {
	description := vals.Get("Description")
	encrypted := vals.Get("Encrypted") == ec2BooleanTrue
	kmsKeyID := vals.Get("KmsKeyId")

	task, err := h.Backend.ImportSnapshot(description, encrypted, kmsKeyID)
	if err != nil {
		return nil, err
	}

	return &importSnapshotResponse{
		RequestID:    reqID,
		ImportTaskID: task.ImportTaskID,
		Description:  task.Description,
		SnapshotTaskDetail: snapshotTaskDetailItem{
			Status:      task.Status,
			SnapshotID:  task.SnapshotID,
			Description: task.Description,
			Encrypted:   task.Encrypted,
			KmsKeyID:    task.KmsKeyID,
		},
	}, nil
}

func (h *Handler) handleDescribeImportSnapshotTasks(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ImportTaskId")
	tasks := h.Backend.DescribeImportSnapshotTasks(ids)

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	tasks, nextToken = pageSlice(tasks, offset, maxResults)

	resp := &describeImportSnapshotTasksResponse{RequestID: reqID, NextToken: nextToken}
	for _, t := range tasks {
		resp.ImportSnapshotTaskSet.Items = append(
			resp.ImportSnapshotTaskSet.Items,
			importSnapshotTaskItem{
				ImportTaskID: t.ImportTaskID,
				Description:  t.Description,
				SnapshotTaskDetail: snapshotTaskDetailItem{
					Status:      t.Status,
					SnapshotID:  t.SnapshotID,
					Description: t.Description,
					Encrypted:   t.Encrypted,
					KmsKeyID:    t.KmsKeyID,
				},
			},
		)
	}

	return resp, nil
}

// fastSnapshotRestoreResultSet renders the (snapshotId, availabilityZone,
// state) triples this backend actually applied. Enable/DisableFastSnapshotRestores
// complete synchronously here, so every requested pair is reported in its real
// terminal state -- never the transient "enabling"/"disabling" real AWS uses
// for an async operation this mock doesn't have.
func fastSnapshotRestoreResultSet(snaps, azs []string, state string) []fastSnapshotRestoreItem {
	items := make([]fastSnapshotRestoreItem, 0, len(snaps)*len(azs))
	for _, snap := range snaps {
		for _, az := range azs {
			items = append(items, fastSnapshotRestoreItem{SnapshotID: snap, AvailabilityZone: az, State: state})
		}
	}

	return items
}

func (h *Handler) handleEnableFastSnapshotRestores(vals url.Values, reqID string) (any, error) {
	snaps := parseMemberList(vals, "SourceSnapshotId")
	azs := parseMemberList(vals, "AvailabilityZone")
	if err := h.Backend.EnableFastSnapshotRestores(snaps, azs); err != nil {
		return nil, err
	}

	resp := &enableDisableFastSnapshotRestoresResponse{
		XMLName:   xml.Name{Local: "EnableFastSnapshotRestoresResponse"},
		RequestID: reqID,
	}
	resp.Successful.Items = fastSnapshotRestoreResultSet(snaps, azs, stateEnabledFastLaunch)

	return resp, nil
}

func (h *Handler) handleDisableFastSnapshotRestores(vals url.Values, reqID string) (any, error) {
	snaps := parseMemberList(vals, "SourceSnapshotId")
	azs := parseMemberList(vals, "AvailabilityZone")
	if err := h.Backend.DisableFastSnapshotRestores(snaps, azs); err != nil {
		return nil, err
	}

	resp := &enableDisableFastSnapshotRestoresResponse{
		XMLName:   xml.Name{Local: "DisableFastSnapshotRestoresResponse"},
		RequestID: reqID,
	}
	resp.Successful.Items = fastSnapshotRestoreResultSet(snaps, azs, "disabled")

	return resp, nil
}

func (h *Handler) handleDescribeFastSnapshotRestores(vals url.Values, reqID string) (any, error) {
	items := h.Backend.DescribeFastSnapshotRestores()

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	items, nextToken := pageSlice(items, offset, maxResults)

	resp := &describeFastSnapshotRestoresResponse{RequestID: reqID, NextToken: nextToken}
	for _, item := range items {
		resp.FastSnapshotRestoreSet.Items = append(
			resp.FastSnapshotRestoreSet.Items,
			fastSnapshotRestoreItem(item),
		)
	}

	return resp, nil
}

func (h *Handler) handleCreateSnapshot(vals url.Values, reqID string) (any, error) {
	snap, err := h.Backend.CreateSnapshot(vals.Get("VolumeId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, resourceTypeSnapshot)
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{snap.SnapshotID}, tags); err != nil {
			return nil, err
		}
	}

	return &createSnapshotResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		SnapshotID:  snap.SnapshotID,
		VolumeID:    snap.VolumeID,
		State:       snap.State,
		Progress:    snap.Progress,
		Description: snap.Description,
		VolumeSize:  snap.VolumeSize,
		StartTime:   snap.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
		KmsKeyID:    snap.KmsKeyID,
		OwnerID:     snap.OwnerID,
		TagSet:      tagItemsFromMap(tags),
		Encrypted:   snap.Encrypted,
	}, nil
}

func (h *Handler) handleDescribeSnapshots(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	snaps := h.Backend.DescribeSnapshots(ids)

	filters := parseEC2Filters(vals)
	snaps = applySnapshotFilters(snaps, filters, h.Backend)

	maxResults := 0
	if v := vals.Get("MaxResults"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxResults); scanErr != nil || maxResults < 5 || maxResults > 1000 {
			return nil, fmt.Errorf("%w: MaxResults must be between 5 and 1000", ErrInvalidParameter)
		}
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		n := page.DecodeHMACToken(tok, ec2PaginationSalt)
		if n == 0 {
			return nil, fmt.Errorf("%w: the pagination token is not valid", ErrInvalidPaginationToken)
		}
		offset = n
	}

	var nextToken string
	if maxResults > 0 {
		if offset > len(snaps) {
			offset = len(snaps)
		}
		snaps = snaps[offset:]
		if len(snaps) > maxResults {
			nextToken = page.EncodeHMACToken(offset+maxResults, ec2PaginationSalt)
			snaps = snaps[:maxResults]
		}
	}

	items := make([]snapshotItem, 0, len(snaps))
	for _, s := range snaps {
		items = append(items, snapshotItem{
			SnapshotID:  s.SnapshotID,
			VolumeID:    s.VolumeID,
			State:       s.State,
			Progress:    s.Progress,
			Description: s.Description,
			VolumeSize:  s.VolumeSize,
			StartTime:   s.StartTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			KmsKeyID:    s.KmsKeyID,
			OwnerID:     s.OwnerID,
			TagSet:      tagItemsFromMap(h.Backend.TagsForResource(s.SnapshotID)),
			Encrypted:   s.Encrypted,
		})
	}

	return &describeSnapshotsResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		SnapshotSet: snapshotSet{Items: items},
		NextToken:   nextToken,
	}, nil
}

func (h *Handler) handleDeleteSnapshot(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeleteSnapshot(vals.Get("SnapshotId"))
}

// ---- AMI lifecycle handlers ----

type createSnapshotResponse struct {
	XMLName     xml.Name        `xml:"CreateSnapshotResponse"`
	Xmlns       string          `xml:"xmlns,attr"`
	RequestID   string          `xml:"requestId"`
	SnapshotID  string          `xml:"snapshotId"`
	VolumeID    string          `xml:"volumeId"`
	State       string          `xml:"status"`
	Progress    string          `xml:"progress"`
	Description string          `xml:"description,omitempty"`
	StartTime   string          `xml:"startTime"`
	KmsKeyID    string          `xml:"kmsKeyId,omitempty"`
	OwnerID     string          `xml:"ownerId,omitempty"`
	TagSet      []simpleTagItem `xml:"tagSet>item"`
	VolumeSize  int             `xml:"volumeSize"`
	Encrypted   bool            `xml:"encrypted"`
}

type snapshotItem struct {
	SnapshotID  string          `xml:"snapshotId"`
	VolumeID    string          `xml:"volumeId"`
	State       string          `xml:"status"`
	Progress    string          `xml:"progress"`
	Description string          `xml:"description,omitempty"`
	StartTime   string          `xml:"startTime"`
	KmsKeyID    string          `xml:"kmsKeyId,omitempty"`
	OwnerID     string          `xml:"ownerId,omitempty"`
	TagSet      []simpleTagItem `xml:"tagSet>item"`
	VolumeSize  int             `xml:"volumeSize"`
	Encrypted   bool            `xml:"encrypted"`
}

type snapshotSet struct {
	Items []snapshotItem `xml:"item"`
}

type describeSnapshotsResponse struct {
	XMLName     xml.Name    `xml:"DescribeSnapshotsResponse"`
	Xmlns       string      `xml:"xmlns,attr"`
	RequestID   string      `xml:"requestId"`
	NextToken   string      `xml:"nextToken,omitempty"`
	SnapshotSet snapshotSet `xml:"snapshotSet"`
}

// registerSnapshotsOps registers the Snapshots operation handlers.
func registerSnapshotsOps(h *Handler, ops map[string]ec2ActionFn) {
	maps.Copy(ops, map[string]ec2ActionFn{
		"CopySnapshot":                      h.handleCopySnapshot,
		"CreateSnapshots":                   h.handleCreateSnapshots,
		"GetSnapshotBlockPublicAccessState": h.handleGetSnapshotBlockPublicAccessState,
		"EnableSnapshotBlockPublicAccess":   h.handleEnableSnapshotBlockPublicAccess,
		"DisableSnapshotBlockPublicAccess":  h.handleDisableSnapshotBlockPublicAccess,
		"DescribeSnapshotTierStatus":        h.handleDescribeSnapshotTierStatus,
		"ModifySnapshotTier":                h.handleModifySnapshotTier,
		"ResetSnapshotAttribute":            h.handleResetSnapshotAttribute,
		"LockSnapshot":                      h.handleLockSnapshot,
		"UnlockSnapshot":                    h.handleUnlockSnapshot,
		"DescribeLockedSnapshots":           h.handleDescribeLockedSnapshots,
		"ListSnapshotsInRecycleBin":         h.handleListSnapshotsInRecycleBin,
		"RestoreSnapshotFromRecycleBin":     h.handleRestoreSnapshotFromRecycleBin,
		"RestoreSnapshotTier":               h.handleRestoreSnapshotTier,
		"ImportSnapshot":                    h.handleImportSnapshot,
		"DescribeImportSnapshotTasks":       h.handleDescribeImportSnapshotTasks,
		"EnableFastSnapshotRestores":        h.handleEnableFastSnapshotRestores,
		"DisableFastSnapshotRestores":       h.handleDisableFastSnapshotRestores,
		"DescribeFastSnapshotRestores":      h.handleDescribeFastSnapshotRestores,
		"CreateSnapshot":                    h.handleCreateSnapshot,
		"DescribeSnapshots":                 h.handleDescribeSnapshots,
		"DeleteSnapshot":                    h.handleDeleteSnapshot,
	})
}

// snapshotsSupportedOperations lists the operation names registered by
// registerSnapshotsOps, for GetSupportedOperations().
func snapshotsSupportedOperations() []string {
	return []string{
		"CopySnapshot",
		"CreateSnapshots",
		"GetSnapshotBlockPublicAccessState",
		"EnableSnapshotBlockPublicAccess",
		"DisableSnapshotBlockPublicAccess",
		"DescribeSnapshotTierStatus",
		"ModifySnapshotTier",
		"ResetSnapshotAttribute",
		"LockSnapshot",
		"UnlockSnapshot",
		"DescribeLockedSnapshots",
		"ListSnapshotsInRecycleBin",
		"RestoreSnapshotFromRecycleBin",
		"RestoreSnapshotTier",
		"ImportSnapshot",
		"DescribeImportSnapshotTasks",
		"EnableFastSnapshotRestores",
		"DisableFastSnapshotRestores",
		"DescribeFastSnapshotRestores",
		"CreateSnapshot",
		"DescribeSnapshots",
		"DeleteSnapshot",
	}
}

type describeSnapshotAttributeResponse struct {
	XMLName    xml.Name `xml:"DescribeSnapshotAttributeResponse"`
	Xmlns      string   `xml:"xmlns,attr"`
	RequestID  string   `xml:"requestId"`
	SnapshotID string   `xml:"snapshotId"`
	// createVolumePermission is the only attribute modelled; others return empty.
	CreateVolumePermission launchPermissionList `xml:"createVolumePermission"`
}

type modifySnapshotAttributeResponse struct {
	XMLName   xml.Name `xml:"ModifySnapshotAttributeResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

// handleDescribeVolumeAttribute returns a stub value for the requested volume attribute.
// autoEnableIO is the only attribute modelled; others return false.

func (h *Handler) handleDescribeSnapshotAttribute(vals url.Values, reqID string) (any, error) {
	snapshotID := vals.Get("SnapshotId")
	if snapshotID == "" {
		return nil, fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	attr := vals.Get("Attribute")
	if attr == "" {
		return nil, fmt.Errorf("%w: Attribute is required", ErrInvalidParameter)
	}

	resp := &describeSnapshotAttributeResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		SnapshotID: snapshotID,
	}

	if attr == "createVolumePermission" {
		resp.CreateVolumePermission = launchPermissionList{
			Items: []launchPermissionItem{{Group: "all"}},
		}
	}

	return resp, nil
}

// handleModifySnapshotAttribute is a stub that accepts any attribute modification and returns success.
func (h *Handler) handleModifySnapshotAttribute(vals url.Values, reqID string) (any, error) {
	if vals.Get("SnapshotId") == "" {
		return nil, fmt.Errorf("%w: SnapshotId is required", ErrInvalidParameter)
	}

	return &modifySnapshotAttributeResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}
