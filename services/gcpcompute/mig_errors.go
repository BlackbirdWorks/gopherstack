package gcpcompute

import (
	"net/http"
	"slices"
	"sort"
	"time"

	computeapi "google.golang.org/api/compute/v1"
)

const (
	maxMIGErrors      = 100
	maxFailingMembers = 100
	errDedupeWindow   = time.Minute
	retryBase         = 10 * time.Second
	retryCap          = 5 * time.Minute
	retryMaxShift     = 5
	codeCondition     = "CONDITION_NOT_MET"
	codeNotFound      = "RESOURCE_NOT_FOUND"
	codeExists        = "RESOURCE_ALREADY_EXISTS"
	codeQuota         = "QUOTA_EXCEEDED"
	codeIPExhausted   = "IP_SPACE_EXHAUSTED"
	flexibleMaxRank   = 1000
	flexiblePlatform  = 256
	flexField         = "resource.instanceFlexibilityPolicy"
)

// migError is one entry of instanceGroupManagers.listErrors.
type migError struct {
	Time     time.Time `json:"time"`
	Code     string    `json:"code"`
	Message  string    `json:"message"`
	Action   string    `json:"action"`
	Instance string    `json:"instance"`
	Template string    `json:"template"`
}

// instanceErrorCode maps an instance creation failure to the code a managed instance error carries.
func instanceErrorCode(e *APIError) string {
	switch {
	case e.Status == http.StatusNotFound:
		return codeNotFound
	case e.Status == http.StatusConflict:
		return codeExists
	case e.Status == http.StatusTooManyRequests || e.Reason == "quotaExceeded":
		return codeQuota
	case e.Reason == "ipSpaceExhausted":
		return codeIPExhausted
	}

	return codeCondition
}

// recordMIGErrorLocked appends a creation failure to the group's bounded error log; a repeat of the previous
// failure within a minute only refreshes it.
func (b *InMemoryBackend) recordMIGErrorLocked(rec *migRec, action, zone, name, tmpl string, cause *APIError) {
	now := b.now().UTC()
	e := &migError{
		Time: now, Code: instanceErrorCode(cause), Message: cause.Message, Action: action,
		Instance: zoneLink(rec.Project, zone, "instances", name), Template: tmpl,
	}

	if n := len(rec.Errors); n > 0 {
		last := rec.Errors[n-1]
		if last.Action == action && last.Instance == e.Instance && last.Code == e.Code && last.Message == e.Message &&
			last.Template == tmpl &&
			now.Sub(last.Time) < errDedupeWindow {
			last.Time = now

			return
		}
	}

	rec.Errors = append(rec.Errors, e)
	if len(rec.Errors) > maxMIGErrors {
		rec.Errors = slices.Clone(rec.Errors[len(rec.Errors)-maxMIGErrors:])
	}
}

// tryCreateLocked creates the instance of a member and records the failure, if any, in the group's error log.
func (b *InMemoryBackend) tryCreateLocked(
	project string, rec *migRec, action, zone, name, tmpl string,
) *APIError {
	err := b.createInstanceForLocked(project, rec, zone, name, tmpl)
	if err != nil {
		b.recordMIGErrorLocked(rec, action, zone, name, tmpl, err)
	}

	return err
}

// noteAttemptLocked updates a member's retry schedule after a creation attempt.
func (b *InMemoryBackend) noteAttemptLocked(m *memberRec, err *APIError) {
	if err == nil {
		m.Attempts, m.RetryAt, m.LastError, m.LastErrorCode = 0, time.Time{}, "", ""

		return
	}

	m.Attempts++
	m.LastError, m.LastErrorCode = err.Message, instanceErrorCode(err)
	m.RetryAt = b.now().UTC().Add(min(retryBase<<min(m.Attempts-1, retryMaxShift), retryCap))
}

func (b *InMemoryBackend) listMIGErrors(project, scope, loc, name string) ([]entry, error) {
	b.mu.RLock("ListMIGErrors")
	defer b.mu.RUnlock()

	rec, err := b.getMIGLocked(project, scope, loc, name)
	if err != nil {
		return nil, err
	}

	sorted := slices.Clone(rec.Errors)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Time.Before(sorted[j].Time) })

	out := make([]entry, len(sorted))

	for i, e := range sorted {
		out[i] = entry{scope: scopeGlobal, item: &computeapi.InstanceManagedByIgmError{
			Timestamp: e.Time.Format(timeLayout),
			Error:     &computeapi.InstanceManagedByIgmErrorManagedInstanceError{Code: e.Code, Message: e.Message},
			InstanceActionDetails: &computeapi.InstanceManagedByIgmErrorInstanceActionDetails{
				Action: e.Action, Instance: e.Instance,
				Version: &computeapi.ManagedInstanceVersion{InstanceTemplate: e.Template},
			},
		}}
	}

	return out, nil
}

func (b *InMemoryBackend) validFlexibilityPolicy(
	scope string, p *computeapi.InstanceGroupManagerInstanceFlexibilityPolicy,
) *APIError {
	if p == nil || len(p.InstanceSelections) == 0 {
		return nil
	}

	if scope != scopeRegion {
		return errInvalid("Invalid value for field '%s': only valid for regional groups.", flexField)
	}

	for name, sel := range p.InstanceSelections {
		if err := validName(flexField+".instanceSelections.key", name); err != nil {
			return err
		}

		if err := validSelection(flexField+".instanceSelections."+name, &sel); err != nil {
			return err
		}
	}

	return nil
}

func validSelection(
	f string, sel *computeapi.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection,
) *APIError {
	if len(sel.MachineTypes) == 0 {
		return errField(f+".machineTypes", "", "At least one machine type is required.")
	}

	seen := map[string]bool{}

	for _, mt := range sel.MachineTypes {
		if _, ok := findMachine(mt); !ok {
			if _, _, custom := customMachine(mt); !custom {
				return errField(f+".machineTypes", mt, "Unknown machine type.")
			}
		}

		if seen[mt] {
			return errField(f+".machineTypes", mt, "Duplicate machine type.")
		}

		seen[mt] = true
	}

	if sel.Rank < 0 || sel.Rank > flexibleMaxRank {
		return errField(f+".rank", "", "Must be between 0 and 1000.")
	}

	if len(sel.MinCpuPlatform) > flexiblePlatform {
		return errField(f+".minCpuPlatform", "", "Too long.")
	}

	return nil
}

type flexCandidate struct {
	selection *computeapi.InstanceGroupManagerInstanceFlexibilityPolicyInstanceSelection
	machine   string
}

// flexCandidates lists machine types in the order a new instance tries them: lowest rank first, then by name.
func flexCandidates(p *computeapi.InstanceGroupManagerInstanceFlexibilityPolicy) []flexCandidate {
	if p == nil {
		return nil
	}

	names := make([]string, 0, len(p.InstanceSelections))
	for n := range p.InstanceSelections {
		names = append(names, n)
	}

	sort.Slice(names, func(i, j int) bool {
		a, c := p.InstanceSelections[names[i]], p.InstanceSelections[names[j]]
		if a.Rank != c.Rank {
			return a.Rank < c.Rank
		}

		return names[i] < names[j]
	})

	var out []flexCandidate

	for _, n := range names {
		sel := p.InstanceSelections[n]
		for _, mt := range sel.MachineTypes {
			out = append(out, flexCandidate{selection: &sel, machine: mt})
		}
	}

	return out
}
