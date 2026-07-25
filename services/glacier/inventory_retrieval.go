package glacier

import (
	"fmt"
	"strconv"
	"time"
)

// parsedInventoryRetrievalParams is the validated/normalized form of an
// InitiateJob request's InventoryRetrievalParameters, ready to store on a Job.
type parsedInventoryRetrievalParams struct {
	StartDate string
	EndDate   string
	Limit     string
	Marker    string
}

// parseInventoryRetrievalParams validates the (optional) InventoryRetrievalParameters
// supplied on an InitiateJob request for a range vault-inventory retrieval.
// StartDate/EndDate must parse as ISO-8601 timestamps and Limit must be a positive
// integer string; Marker is opaque and passed through unvalidated (it is expected to
// be an ArchiveId echoed back from a previous range retrieval's inventory output,
// per AWS's "Range Inventory Retrieval" continuation pattern).
func parseInventoryRetrievalParams(p *inventoryRetrievalParamsRequest) (*parsedInventoryRetrievalParams, error) {
	out := &parsedInventoryRetrievalParams{Marker: p.Marker}

	if p.StartDate != "" {
		if _, err := parseInventoryDate(p.StartDate); err != nil {
			return nil, fmt.Errorf(
				"%w: InventoryRetrievalParameters.StartDate: %w",
				ErrValidation,
				err,
			)
		}

		out.StartDate = p.StartDate
	}

	if p.EndDate != "" {
		if _, err := parseInventoryDate(p.EndDate); err != nil {
			return nil, fmt.Errorf(
				"%w: InventoryRetrievalParameters.EndDate: %w",
				ErrValidation,
				err,
			)
		}

		out.EndDate = p.EndDate
	}

	if p.Limit != "" {
		n, err := strconv.Atoi(p.Limit)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf(
				"%w: InventoryRetrievalParameters.Limit must be a positive integer",
				ErrValidation,
			)
		}

		out.Limit = p.Limit
	}

	return out, nil
}

// inventoryDateLayouts are the ISO-8601 timestamp forms AWS accepts for
// InventoryRetrievalParameters.StartDate/EndDate, tried in order.
var inventoryDateLayouts = []string{ //nolint:gochecknoglobals // fixed lookup table, mirrors errCodeLookup-style tables
	time.RFC3339,
	"2006-01-02T15:04:05.000Z",
	"2006-01-02",
}

// parseInventoryDate parses an ISO-8601 date/timestamp as accepted by AWS for range
// inventory retrieval bounds.
func parseInventoryDate(s string) (time.Time, error) {
	for _, layout := range inventoryDateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, nil
		}
	}

	return time.Time{}, fmt.Errorf("%w: invalid date format %q", ErrValidation, s)
}

// filterArchivesForInventory applies a job's (optional) range-inventory-retrieval
// StartDate/EndDate/Marker/Limit filters to a vault's archive list. archives must
// already be sorted by ArchiveID (as ListArchives returns them) for Marker-based
// continuation to behave deterministically.
//
// Semantics (matching AWS's Range Inventory Retrieval): StartDate is an inclusive
// lower bound and EndDate an exclusive upper bound on Archive.CreationDate; Marker
// resumes strictly after a previously-seen ArchiveId; Limit caps the result count.
func filterArchivesForInventory(j *Job, archives []*Archive) []*Archive {
	start, hasStart := parsedBound(j.InventoryRetrievalStartDate)
	end, hasEnd := parsedBound(j.InventoryRetrievalEndDate)

	limit := -1
	if j.InventoryRetrievalLimit != "" {
		if n, err := strconv.Atoi(j.InventoryRetrievalLimit); err == nil {
			limit = n
		}
	}

	out := make([]*Archive, 0, len(archives))
	afterMarker := j.InventoryRetrievalMarker == ""

	for _, a := range archives {
		if !afterMarker {
			if a.ArchiveID == j.InventoryRetrievalMarker {
				afterMarker = true
			}

			continue
		}

		if !inventoryDateInRange(a.CreationDate, start, hasStart, end, hasEnd) {
			continue
		}

		out = append(out, a)

		if limit > 0 && len(out) >= limit {
			break
		}
	}

	return out
}

// parsedBound parses an optional date bound, reporting ok=false for an empty or
// unparseable value (treated as "no bound" rather than an error -- validation
// already rejected malformed bounds at InitiateJob time).
func parsedBound(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}

	t, err := parseInventoryDate(s)
	if err != nil {
		return time.Time{}, false
	}

	return t, true
}

// inventoryDateInRange reports whether an archive's CreationDate falls within
// [start, end) given the job's configured bounds. An archive whose CreationDate
// fails to parse is conservatively kept (no bound can be evaluated against it).
func inventoryDateInRange(creationDate string, start time.Time, hasStart bool, end time.Time, hasEnd bool) bool {
	if !hasStart && !hasEnd {
		return true
	}

	created, err := parseInventoryDate(creationDate)
	if err != nil {
		return true
	}

	if hasStart && created.Before(start) {
		return false
	}

	if hasEnd && !created.Before(end) {
		return false
	}

	return true
}
