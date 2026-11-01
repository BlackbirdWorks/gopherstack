package ocifilestorage

import (
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	qFileSystemLevel = "FILE_SYSTEM_LEVEL"
	qDefaultGroup    = "DEFAULT_GROUP"
	qDefaultUser     = "DEFAULT_USER"
	qIndividualGroup = "INDIVIDUAL_GROUP"
	qIndividualUser  = "INDIVIDUAL_USER"
	maxQuotaGB       = 1 << 31
)

type createQuotaIn struct {
	PrincipalID   *int    `json:"principalId"`
	IsHardQuota   *bool   `json:"isHardQuota"`
	QuotaLimitGB  *int    `json:"quotaLimitInGigabytes"`
	DisplayName   *string `json:"displayName"`
	PrincipalType string  `json:"principalType"`
}

type updateQuotaIn struct {
	DisplayName  *string `json:"displayName"`
	QuotaLimitGB *int    `json:"quotaLimitInGigabytes"`
}

// QuotaFilter selects quota rules in a list.
type QuotaFilter struct {
	PrincipalID *int
	Type        string
	Violators   bool
}

func quotaView(w *QuotaRule) map[string]any {
	return project(w, "id", "fileSystemId", "isHardQuota", "displayName", "quotaLimitInGigabytes", "timeCreated",
		"timeUpdated", "principalType", "principalId")
}

func quotaListView(w *QuotaRule) map[string]any {
	m := quotaView(w)
	m["usageInBytes"] = 0

	return m
}

func individual(t string) bool { return t == qIndividualGroup || t == qIndividualUser }

func checkQuotaLimit(v *int) *APIError {
	if v == nil {
		return errMissing("quotaLimitInGigabytes is required")
	}

	if *v < 1 || *v > maxQuotaGB {
		return errParam("quotaLimitInGigabytes must be at least 1")
	}

	return nil
}

func (b *InMemoryBackend) CreateQuotaRule(fsID string, in createQuotaIn) (map[string]any, string, error) {
	if aerr := validateQuotaIn(in); aerr != nil {
		return nil, "", aerr
	}

	b.mu.Lock("CreateQuotaRule")
	defer b.mu.Unlock()

	fs, aerr := b.fss.usable(fsID)
	if aerr != nil {
		return nil, "", aerr
	}

	for _, q := range b.quotas {
		w := q.W
		if w.FileSystemID == fsID && w.PrincipalType == in.PrincipalType &&
			deref(w.PrincipalID, -1) == deref(in.PrincipalID, -1) {
			return nil, "", errConflict("A quota rule for this principal already exists on the file system")
		}
	}

	now := b.nowUTC()
	name := "QuotaRule-" + now.Format("20060102-150405")

	if in.DisplayName != nil {
		name = *in.DisplayName
	}

	if aerr = checkName(name); aerr != nil {
		return nil, "", aerr
	}

	q := &QuotaRule{
		base: b.newBase(kindQuota, tagsIn{}, nil), FileSystemID: fs.W.ID, DisplayName: name,
		PrincipalType: in.PrincipalType, PrincipalID: in.PrincipalID, QuotaLimitGB: *in.QuotaLimitGB,
		IsHardQuota: *in.IsHardQuota, TimeUpdated: now,
	}
	q.State = stActive
	e := &entry[*QuotaRule]{W: q, Etag: newEtag(), Seq: b.nextSeq()}
	b.quotas[q.ID] = e

	return quotaView(q), e.Etag, nil
}

func validateQuotaIn(in createQuotaIn) *APIError {
	types := []string{qFileSystemLevel, qDefaultGroup, qDefaultUser, qIndividualGroup, qIndividualUser}
	if !slices.Contains(types, in.PrincipalType) {
		return errParam("principalType must be one of " + strings.Join(types, ", "))
	}

	if in.IsHardQuota == nil {
		return errMissing("isHardQuota is required")
	}

	if aerr := checkQuotaLimit(in.QuotaLimitGB); aerr != nil {
		return aerr
	}

	switch {
	case individual(in.PrincipalType) && (in.PrincipalID == nil || *in.PrincipalID < 0):
		return errMissing("principalId is required for individual quota rules")
	case !individual(in.PrincipalType) && in.PrincipalID != nil:
		return errParam("principalId applies only to INDIVIDUAL_USER and INDIVIDUAL_GROUP rules")
	}

	return nil
}

func (b *InMemoryBackend) quotaOf(fsID, id string) (*entry[*QuotaRule], *APIError) {
	if _, aerr := b.fss.usable(fsID); aerr != nil {
		return nil, aerr
	}

	e, ok := b.quotas[id]
	if !ok || e.W.FileSystemID != fsID {
		return nil, errNotFound(id)
	}

	return e, nil
}

// GetQuotaRule returns one quota rule.
func (b *InMemoryBackend) GetQuotaRule(fsID, id string) (map[string]any, string, error) {
	b.mu.RLock("GetQuotaRule")
	defer b.mu.RUnlock()

	e, aerr := b.quotaOf(fsID, id)
	if aerr != nil {
		return nil, "", aerr
	}

	return quotaView(e.W), e.Etag, nil
}

// ListQuotaRules lists the quota rules of a file system.
func (b *InMemoryBackend) ListQuotaRules(fsID string, f QuotaFilter, o ListOpts) (page.Page[map[string]any], error) {
	b.mu.RLock("ListQuotaRules")
	defer b.mu.RUnlock()

	if _, aerr := b.fss.usable(fsID); aerr != nil {
		return page.Page[map[string]any]{}, aerr
	}

	keep := func(e *entry[*QuotaRule]) bool {
		w := e.W

		return w.FileSystemID == fsID && (f.Type == "" || w.PrincipalType == f.Type) &&
			(f.PrincipalID == nil || deref(w.PrincipalID, -1) == *f.PrincipalID) && !f.Violators
	}

	return listOf(b.quotas, keep, ListOpts{Page: o.Page, Limit: o.Limit, Order: o.Order},
		func(w *QuotaRule) string { return w.DisplayName },
		func(e *entry[*QuotaRule]) map[string]any { return quotaListView(e.W) }), nil
}

// UpdateQuotaRule changes a quota rule's name and limit.
func (b *InMemoryBackend) UpdateQuotaRule(fsID, id, ifMatch string, in updateQuotaIn) (map[string]any, string, error) {
	b.mu.Lock("UpdateQuotaRule")
	defer b.mu.Unlock()

	e, aerr := b.quotaOf(fsID, id)
	if aerr != nil {
		return nil, "", aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return nil, "", aerr
	}

	if in.QuotaLimitGB != nil {
		if aerr = checkQuotaLimit(in.QuotaLimitGB); aerr != nil {
			return nil, "", aerr
		}

		e.W.QuotaLimitGB = *in.QuotaLimitGB
	}

	if in.DisplayName != nil {
		if aerr = checkName(*in.DisplayName); aerr != nil {
			return nil, "", aerr
		}

		e.W.DisplayName = *in.DisplayName
	}

	e.W.TimeUpdated = b.nowUTC()
	e.Etag = newEtag()

	return quotaView(e.W), e.Etag, nil
}

// DeleteQuotaRule removes a quota rule.
func (b *InMemoryBackend) DeleteQuotaRule(fsID, id, ifMatch string) error {
	b.mu.Lock("DeleteQuotaRule")
	defer b.mu.Unlock()

	e, aerr := b.quotaOf(fsID, id)
	if aerr != nil {
		return aerr
	}

	if aerr = checkEtag(e.Etag, ifMatch); aerr != nil {
		return aerr
	}

	delete(b.quotas, id)

	return nil
}
