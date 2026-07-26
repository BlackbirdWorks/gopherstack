package guardduty

import (
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// featureAIAnalyst is the real DetectorFeature name (see
	// aws-sdk-go-v2/service/guardduty/types.DetectorFeatureAiAnalyst) that
	// real CreateInvestigation requires to be ENABLED on the detector, per
	// its doc: to use this operation, the AI_ANALYST feature must be
	// enabled on your detector.
	featureAIAnalyst = "AI_ANALYST"

	// investigationStatusRunning is the only status this backend ever
	// assigns an investigation -- see the Investigation type doc for why.
	investigationStatusRunning = "RUNNING"

	// defaultInvestigationsPageSize and maxInvestigationsPageSize mirror the
	// ListFindings convention (see findings.go): the real
	// ListInvestigationsInput doc states "The default value is 50" for
	// MaxResults without documenting an explicit maximum, so 50 is used as
	// the cap here too, consistent with every other paginated op in this
	// package.
	defaultInvestigationsPageSize = 50
	maxInvestigationsPageSize     = 50
)

// detectorHasEnabledFeature reports whether d has featureName present in its
// Features list with Status == ENABLED, mirroring the same
// Detector.Features this package already models for Create/UpdateDetector
// (see models.go's DetectorFeature).
func detectorHasEnabledFeature(d *Detector, featureName string) bool {
	for _, f := range d.Features {
		if f.Name == featureName && f.Status == statusEnabled {
			return true
		}
	}

	return false
}

// CreateInvestigation starts a new GuardDuty Extended Threat Detection
// investigation for detectorID.
//
// This backend has no threat-analysis engine (no Bedrock model, no finding
// correlation, no account-level analysis), so the investigation it creates
// never progresses past RUNNING -- see the Investigation type doc (models.go)
// for why that is the honest choice here rather than fabricating a COMPLETED
// result with invented findings/risk/summary.
//
// detectorID is validated against this backend's real detector table (a
// nonexistent detector fails with ErrDetectorNotFound, the same
// ResourceNotFoundException every other detector-scoped op in this package
// returns), and the real "AI_ANALYST feature must be enabled on your
// detector" precondition is enforced against the same Detector.Features this
// package already models for Create/UpdateDetector.
func (b *InMemoryBackend) CreateInvestigation(detectorID, triggerPrompt string) (*Investigation, error) {
	b.mu.Lock("CreateInvestigation")
	defer b.mu.Unlock()

	d, ok := b.detectors.Get(detectorID)
	if !ok {
		return nil, ErrDetectorNotFound
	}

	if !detectorHasEnabledFeature(d, featureAIAnalyst) {
		return nil, ErrValidation
	}

	if triggerPrompt == "" {
		return nil, ErrValidation
	}

	inv := &Investigation{
		InvestigationID: strings.ReplaceAll(uuid.New().String(), "-", ""),
		DetectorID:      detectorID,
		Status:          investigationStatusRunning,
		TriggerPrompt:   triggerPrompt,
		TriggeredBy:     b.accountID,
		StartTime:       time.Now().UTC(),
	}
	b.investigations.Put(inv)

	return inv, nil
}

// GetInvestigation retrieves an investigation by ID.
func (b *InMemoryBackend) GetInvestigation(detectorID, investigationID string) (*Investigation, error) {
	b.mu.RLock("GetInvestigation")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	inv, ok := b.investigations.Get(detectorKey(detectorID, investigationID))
	if !ok {
		return nil, ErrInvestigationNotFound
	}

	return inv, nil
}

// InvestigationsQuery holds the optional sort/pagination parameters for
// ListInvestigations, mirroring types.ListInvestigationsInput's
// SortCriteria, MaxResults, and NextToken fields.
type InvestigationsQuery struct {
	SortAttr   string
	SortOrder  string
	NextToken  string
	MaxResults int32
}

// ListInvestigations returns investigations for a detector, sorted per
// q.SortAttr/q.SortOrder and paginated per q.MaxResults/q.NextToken.
//
// Every investigation this backend ever creates has an identical Status
// (RUNNING) and empty EndTime/RiskLevel/Confidence -- see the Investigation
// type doc -- so START_TIME is the only sort attribute that can ever produce
// a real ordering; the others (END_TIME, STATUS, RISK_LEVEL, CONFIDENCE) are
// accepted without error (matching a real client that requests them) but
// resolve to the same StartTime-based order since there is nothing real to
// differentiate investigations by. This is not a gap: it is the honest
// consequence of every investigation being permanently RUNNING.
func (b *InMemoryBackend) ListInvestigations(
	detectorID string, q InvestigationsQuery,
) ([]*Investigation, string, error) {
	b.mu.RLock("ListInvestigations")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, "", ErrDetectorNotFound
	}

	all := append([]*Investigation(nil), b.investigationsByDetector.Get(detectorID)...)

	sort.SliceStable(all, func(i, j int) bool {
		if q.SortOrder == "DESC" {
			return all[j].StartTime.Before(all[i].StartTime)
		}

		return all[i].StartTime.Before(all[j].StartTime)
	})

	offset, err := decodeToken(q.NextToken)
	if err != nil {
		return nil, "", ErrValidation
	}

	size := resolvePageSize(int(q.MaxResults), defaultInvestigationsPageSize, maxInvestigationsPageSize)
	page, nextToken := paginate(all, offset, size)

	return page, nextToken, nil
}
