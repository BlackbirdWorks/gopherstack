package directoryservice

import (
	"context"
	"sort"
	"time"

	"github.com/google/uuid"
)

// hybridUpdateTypeAdminAccount/hybridUpdateTypeSelfManaged are the real
// types.HybridUpdateType enum values ("SelfManagedInstances" and
// "HybridAdministratorAccount").
const (
	hybridUpdateTypeAdminAccount = "HybridAdministratorAccount"
	hybridUpdateTypeSelfManaged  = "SelfManagedInstances"

	// hybridUpdateStatusUpdated is the real types.UpdateStatus value this
	// backend produces: every UpdateHybridAD call is applied synchronously
	// and always succeeds.
	hybridUpdateStatusUpdated = "Updated"
)

// CreateHybridAD creates a hybrid directory. Real CreateHybridADInput is
// {AssessmentId, SecretArn, Tags} -- AWS derives the new directory's Name and
// other descriptive fields from AssessmentId's own AssessmentConfiguration
// (DnsName etc.), which this backend cannot capture since StartADAssessment
// doesn't accept AssessmentConfiguration (a separate, already-tracked gap;
// see PARITY.md). Rather than fabricating a domain name, this backend derives
// them from the SAME assessment's snapshotted source-directory fields
// (SourceDirectoryName etc., captured at StartADAssessment time from the
// existing directory that was assessed) -- genuinely real data, not invented.
// SecretArn is real-shape "used once and not stored" (accepted, validated,
// discarded), matching AWS's own documented behavior.
func (b *InMemoryBackend) CreateHybridAD(
	ctx context.Context,
	assessmentID, secretArn string,
	tags []Tag,
) (*Directory, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateHybridAD")
	defer b.mu.Unlock()

	if assessmentID == "" || secretArn == "" {
		return nil, ErrInvalidParameter
	}

	a, ok := b.adAssessmentGet(region, assessmentID)
	if !ok {
		return nil, ErrAssessmentNotFound
	}

	// "You must have a successful directory assessment ... before you use
	// this operation." No real error code is documented for an unsuccessful
	// assessment; ErrInvalidParameter is this backend's established
	// generic-validation-failure error, an interpretive choice like several
	// other judgment calls already flagged in PARITY.md.
	if a.Status != assessmentStatusSuccess {
		return nil, ErrInvalidParameter
	}

	edition := DirectoryEdition(a.SourceDirectoryEdition)
	if edition == "" {
		edition = DirectoryEditionEnterprise
	}

	d := b.newStoredDirectory(
		region,
		a.SourceDirectoryName,
		a.SourceDirectoryShortName,
		a.SourceDirectoryDescription,
		DirectoryTypeMicrosoftAD,
		"",
		edition,
		"",
		nil,
		tags,
	)
	d.IsHybridAD = true
	b.directoryPut(d)
	b.aliasesStore(region)[d.Alias] = d.DirectoryID

	cp := b.describeDirectory(d)

	return &cp, nil
}

// UpdateHybridAD updates a hybrid directory's administrator-account secret
// and/or self-managed instance settings. Real UpdateHybridADInput requires at
// least one of HybridAdministratorAccountUpdate (adminAccountSecretArn, used
// once and not stored, same as CreateHybridAD's SecretArn) or
// SelfManagedInstancesSettings (selfManagedDNSIPs/selfManagedInstanceIDs,
// required to have a 1:1 correspondence per the real docs). Real
// UpdateHybridADOutput is {AssessmentId, DirectoryId} -- AWS triggers a fresh
// assessment to validate the update. This backend triggers a REAL assessment
// via startADAssessmentLocked (the same op StartADAssessment uses), since
// UpdateHybridAD always targets an existing directory -- the one mode this
// backend's assessment engine genuinely supports.
func (b *InMemoryBackend) UpdateHybridAD(
	ctx context.Context,
	directoryID, adminAccountSecretArn string,
	selfManagedDNSIPs, selfManagedInstanceIDs []string,
) (string, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateHybridAD")
	defer b.mu.Unlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok || !d.IsHybridAD {
		return "", ErrDirectoryNotFound
	}

	hasAdminUpdate := adminAccountSecretArn != ""
	hasInstancesUpdate := len(selfManagedDNSIPs) > 0 || len(selfManagedInstanceIDs) > 0

	if !hasAdminUpdate && !hasInstancesUpdate {
		return "", ErrInvalidParameter
	}

	if hasInstancesUpdate && len(selfManagedDNSIPs) != len(selfManagedInstanceIDs) {
		return "", ErrInvalidParameter
	}

	assessmentID, err := b.startADAssessmentLocked(region, directoryID)
	if err != nil {
		return "", err
	}

	now := time.Now().UTC()

	if hasInstancesUpdate {
		prevDNSIPs := d.HybridDNSIPs
		prevInstanceIDs := d.HybridInstanceIDs
		d.HybridDNSIPs = selfManagedDNSIPs
		d.HybridInstanceIDs = selfManagedInstanceIDs

		b.hybridADUpdatePut(&storedHybridADUpdate{
			region:              region,
			RequestID:           uuid.NewString(),
			DirectoryID:         directoryID,
			AssessmentID:        assessmentID,
			UpdateType:          hybridUpdateTypeSelfManaged,
			InitiatedBy:         "Customer",
			Status:              hybridUpdateStatusUpdated,
			StartTime:           now,
			LastUpdatedDateTime: now,
			NewDNSIPs:           selfManagedDNSIPs,
			NewInstanceIDs:      selfManagedInstanceIDs,
			PreviousDNSIPs:      prevDNSIPs,
			PreviousInstanceIDs: prevInstanceIDs,
		})
	}

	if hasAdminUpdate {
		b.hybridADUpdatePut(&storedHybridADUpdate{
			region:              region,
			RequestID:           uuid.NewString(),
			DirectoryID:         directoryID,
			AssessmentID:        assessmentID,
			UpdateType:          hybridUpdateTypeAdminAccount,
			InitiatedBy:         "Customer",
			Status:              hybridUpdateStatusUpdated,
			StartTime:           now,
			LastUpdatedDateTime: now,
		})
	}

	return assessmentID, nil
}

// DescribeHybridADUpdate returns update-activity history for a hybrid
// directory, split by real UpdateType into the two groups real
// types.HybridUpdateActivities carries (hybridAdministratorAccount/
// selfManagedInstances return values, in that order). updateType, if
// non-empty, filters to a single group (matching the real optional
// DescribeHybridADUpdateInput.UpdateType request member).
func (b *InMemoryBackend) DescribeHybridADUpdate(
	ctx context.Context,
	directoryID, updateType string,
) ([]HybridADUpdateEntry, []HybridADUpdateEntry, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeHybridADUpdate")
	defer b.mu.RUnlock()

	d, ok := b.directoryGet(region, directoryID)
	if !ok || !d.IsHybridAD {
		return nil, nil, ErrDirectoryNotFound
	}

	var adminAccount, selfManaged []HybridADUpdateEntry

	for _, u := range b.hybridADUpdatesInRegion(region) {
		if u.DirectoryID != directoryID {
			continue
		}

		if updateType != "" && u.UpdateType != updateType {
			continue
		}

		entry := HybridADUpdateEntry{
			AssessmentID:        u.AssessmentID,
			InitiatedBy:         u.InitiatedBy,
			Status:              u.Status,
			StatusReason:        u.StatusReason,
			StartTime:           u.StartTime,
			LastUpdatedDateTime: u.LastUpdatedDateTime,
			NewDNSIPs:           u.NewDNSIPs,
			NewInstanceIDs:      u.NewInstanceIDs,
			PreviousDNSIPs:      u.PreviousDNSIPs,
			PreviousInstanceIDs: u.PreviousInstanceIDs,
		}

		switch u.UpdateType {
		case hybridUpdateTypeAdminAccount:
			adminAccount = append(adminAccount, entry)
		case hybridUpdateTypeSelfManaged:
			selfManaged = append(selfManaged, entry)
		}
	}

	sortHybridADUpdateEntries(adminAccount)
	sortHybridADUpdateEntries(selfManaged)

	return adminAccount, selfManaged, nil
}

func sortHybridADUpdateEntries(entries []HybridADUpdateEntry) {
	sort.Slice(entries, func(i, j int) bool { return entries[i].StartTime.Before(entries[j].StartTime) })
}
