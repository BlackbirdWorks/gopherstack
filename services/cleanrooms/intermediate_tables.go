package cleanrooms

import (
	"fmt"
	"maps"
	"slices"
	"sort"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

func (b *InMemoryBackend) intermediateTableARN(membershipID, id string) string {
	return arn.Build(
		"cleanrooms",
		b.region,
		b.accountID,
		fmt.Sprintf("membership/%s/intermediatetable/%s", membershipID, id),
	)
}

func toIntermediateTableSummary(t *IntermediateTable) *IntermediateTableSummary {
	return &IntermediateTableSummary{
		CollaborationArn:  t.CollaborationArn,
		MembershipArn:     t.MembershipArn,
		Arn:               t.Arn,
		CollaborationID:   t.CollaborationID,
		MembershipID:      t.MembershipID,
		ID:                t.ID,
		Name:              t.Name,
		Description:       t.Description,
		Status:            t.Status,
		AnalysisRuleTypes: t.AnalysisRuleTypes,
		RetentionInDays:   t.RetentionInDays,
		CreateTime:        t.CreateTime,
		UpdateTime:        t.UpdateTime,
	}
}

// populationQueryString extracts the SQL query text (if any) from a stored
// PopulationAnalysisConfiguration ({"sqlParameters": {"queryString": ...,
// "analysisTemplateArn": ...}}, matching
// awsRestjson1_serializeDocumentPopulationAnalysisConfiguration's "v1"-less
// "sqlParameters" union tag). An analysis-template-backed configuration
// (queryString absent, analysisTemplateArn set) has no free SQL text for
// this backend to attach to the underlying ProtectedQuery, so it returns "".
func populationQueryString(cfg map[string]any) string {
	sqlParams, _ := cfg["sqlParameters"].(map[string]any)
	if sqlParams == nil {
		return ""
	}
	q, _ := sqlParams["queryString"].(string)

	return q
}

func (b *InMemoryBackend) CreateIntermediateTable(
	membershipID, name, description, kmsKeyArn string,
	populationAnalysisConfiguration map[string]any,
	retentionInDays int32,
	tags map[string]string,
) (*IntermediateTable, error) {
	if name == "" {
		return nil, ErrValidation
	}
	b.mu.Lock("CreateIntermediateTable")
	defer b.mu.Unlock()
	mem, ok := b.memberships.Get(membershipID)
	if !ok {
		return nil, ErrNotFound
	}
	id := uuid.NewString()
	ts := b.now()
	collab, _ := b.collaborations.Get(mem.CollaborationID)
	var collabArn string
	if collab != nil {
		collabArn = collab.Arn
	}
	it := &IntermediateTable{
		PopulationAnalysisConfiguration: populationAnalysisConfiguration,
		CollaborationArn:                collabArn,
		MembershipArn:                   mem.Arn,
		Arn:                             b.intermediateTableARN(membershipID, id),
		CollaborationID:                 mem.CollaborationID,
		MembershipID:                    membershipID,
		ID:                              id,
		Name:                            name,
		Description:                     description,
		KmsKeyArn:                       kmsKeyArn,
		Status:                          itStatusCreated,
		RetentionInDays:                 retentionInDays,
		CreateTime:                      ts,
		UpdateTime:                      ts,
		Tags:                            tags,
	}
	b.intermediateTables.Put(it)
	if len(tags) > 0 {
		b.tagsByArn[it.Arn] = maps.Clone(tags)
	}

	return it, nil
}

func (b *InMemoryBackend) GetIntermediateTable(membershipID, tableID string) (*IntermediateTable, error) {
	b.mu.Lock("GetIntermediateTable")
	defer b.mu.Unlock()
	b.advanceIntermediateTablesLocked()
	it, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}

	return it, nil
}

func (b *InMemoryBackend) ListIntermediateTables(
	membershipID, maxResults, nextToken string,
) ([]*IntermediateTableSummary, string, error) {
	b.mu.Lock("ListIntermediateTables")
	defer b.mu.Unlock()
	b.advanceIntermediateTablesLocked()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return nil, "", ErrNotFound
	}
	page, next := listItems(
		b.intermediateTablesByMembership.Get(membershipID),
		nil,
		toIntermediateTableSummary,
		func(a, c *IntermediateTableSummary) bool { return a.ID < c.ID },
		maxResults, nextToken,
	)

	return page, next, nil
}

// UpdateIntermediateTable updates the description and KMS key of an
// intermediate table. The real UpdateIntermediateTableInput additionally
// accepts a "columns" list to retype existing schema columns, but this
// backend never populates IntermediateTable.Schema in the first place (see
// the IntermediateTable doc comment) -- there is no real column data to
// retype, so that input is not modeled here rather than accepted and
// silently discarded against fabricated column state. See PARITY.md.
func (b *InMemoryBackend) UpdateIntermediateTable(
	membershipID, tableID, description, kmsKeyArn string,
) (*IntermediateTable, error) {
	b.mu.Lock("UpdateIntermediateTable")
	defer b.mu.Unlock()
	it, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}
	if description != "" {
		it.Description = description
	}
	if kmsKeyArn != "" {
		it.KmsKeyArn = kmsKeyArn
	}
	it.UpdateTime = b.now()

	return it, nil
}

// DeleteIntermediateTable hard-deletes the table and cascades to its
// analysis rule and versions, matching the real API's documented behavior
// ("the service marks it as DELETED, removes its analysis rule and schema,
// and triggers storage cleanup") -- this backend has no separate DELETED
// status value to transition through (not a real IntermediateTableStatus
// enum value, see models.go), so a real hard delete (GetIntermediateTable
// then returns ResourceNotFoundException, matching every other resource's
// delete convention in this service) models the same observable end state.
func (b *InMemoryBackend) DeleteIntermediateTable(membershipID, tableID string) error {
	b.mu.Lock("DeleteIntermediateTable")
	defer b.mu.Unlock()
	key := membershipKey(membershipID, tableID)
	it, ok := b.intermediateTables.Get(key)
	if !ok {
		return ErrNotFound
	}
	delete(b.tagsByArn, it.Arn)
	b.intermediateTables.Delete(key)

	for _, rule := range slices.Clone(b.itAnalysisRulesByTable.Get(tableID)) {
		b.itAnalysisRules.Delete(itAnalysisRuleKey(rule.IntermediateTableIdentifier, rule.AnalysisRuleType))
	}
	for _, ver := range slices.Clone(b.intermediateTableVersionsByTable.Get(tableID)) {
		b.intermediateTableVersions.Delete(itVersionKey(ver.TableID, ver.VersionID))
	}

	return nil
}

func (b *InMemoryBackend) ListIntermediateTableVersions(
	membershipID, tableID, maxResults, nextToken string,
) ([]*IntermediateTableVersionSummary, string, error) {
	b.mu.Lock("ListIntermediateTableVersions")
	defer b.mu.Unlock()
	b.advanceIntermediateTablesLocked()
	if _, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID)); !ok {
		return nil, "", ErrNotFound
	}
	items := slices.Clone(b.intermediateTableVersionsByTable.Get(tableID))
	sort.Slice(items, func(i, j int) bool { return items[i].VersionID < items[j].VersionID })
	page, next := paginate(items, maxResults, nextToken)

	return page, next, nil
}

// PopulateIntermediateTable starts a real ProtectedQuery against the
// table's stored PopulationAnalysisConfiguration -- matching the real API's
// documented contract ("Use this value with GetProtectedQuery to track the
// population progress") -- and records a new POPULATE_STARTED version. It
// does not, and cannot, fabricate a row count or a Schema: this emulator has
// no SQL engine to actually execute the query, so the honest state
// transition is limited to status/version bookkeeping (see
// advanceIntermediateTablesLocked, which resolves the version and table
// status once the underlying ProtectedQuery reaches a terminal status, the
// same "advance on next read" pattern StartProtectedQuery already uses).
//
// payerAccountID is accepted (matching the real
// PopulateIntermediateTableInput.AnalysisPayerAccountId wire field) but not
// persisted: ProtectedQuery has no modeled payer-account field anywhere
// else in this backend either (queryComputePayerAccountId is a pre-existing
// documented PARITY.md gap on ProtectedQuery/ProtectedJob), so there is
// nowhere honest to attach it without inventing a new field outside that
// gap's scope.
func (b *InMemoryBackend) PopulateIntermediateTable(
	membershipID, tableID, payerAccountID string,
	computeConfiguration map[string]any,
	parameters map[string]string,
) (*PopulateIntermediateTableOutput, error) {
	_ = payerAccountID
	b.mu.Lock("PopulateIntermediateTable")
	defer b.mu.Unlock()
	b.advanceIntermediateTablesLocked()

	it, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}
	if it.Status == itStatusDisallowedByDataProvider {
		return nil, ErrConflict
	}

	q, err := b.startProtectedQueryLocked(
		membershipID, populationQueryString(it.PopulationAnalysisConfiguration), nil, computeConfiguration,
	)
	if err != nil {
		return nil, err
	}

	ts := b.now()
	var expiration float64
	if it.RetentionInDays > 0 {
		expiration = ts + float64(it.RetentionInDays)*secondsPerDay
	}
	ver := &IntermediateTableVersionSummary{
		Parameters:     parameters,
		MembershipID:   membershipID,
		AnalysisID:     q.ID,
		AnalysisType:   analysisTypeQuery,
		Status:         itVersionStatusPopulateStarted,
		TableID:        tableID,
		VersionID:      uuid.NewString(),
		KmsKeyArn:      it.KmsKeyArn,
		CreateTime:     ts,
		ExpirationTime: expiration,
	}
	b.intermediateTableVersions.Put(ver)

	it.Status = itStatusPopulateStarted
	it.StatusReason = ""
	it.UpdateTime = ts

	return &PopulateIntermediateTableOutput{
		AnalysisID:   q.ID,
		AnalysisType: analysisTypeQuery,
		VersionID:    ver.VersionID,
	}, nil
}

// advanceIntermediateTablesLocked resolves every POPULATE_STARTED
// intermediate table version whose underlying ProtectedQuery has reached a
// terminal status, mirroring advanceProtectedQueriesLocked's "advance on
// next read" pattern (see protected_queries.go) since this backend has no
// background worker to advance state asynchronously (Handler.StartWorker is
// a no-op, see PARITY.md leaks note). Called from every IntermediateTable
// read path. Callers must hold b.mu (write lock).
func (b *InMemoryBackend) advanceIntermediateTablesLocked() {
	b.advanceProtectedQueriesLocked()
	b.intermediateTableVersions.Range(func(v *IntermediateTableVersionSummary) bool {
		if v.Status != itVersionStatusPopulateStarted {
			return true
		}
		q, ok := b.protectedQueries.Get(membershipKey(v.MembershipID, v.AnalysisID))
		if !ok || !isTerminalProtectedQueryStatus(q.Status) {
			return true
		}
		b.resolveIntermediateTableVersionLocked(v, q)

		return true
	})
}

// resolveIntermediateTableVersionLocked moves v and its parent table from
// POPULATE_STARTED to a terminal status once q (the ProtectedQuery started
// by PopulateIntermediateTable) has itself resolved. Callers must hold b.mu.
func (b *InMemoryBackend) resolveIntermediateTableVersionLocked(v *IntermediateTableVersionSummary, q *ProtectedQuery) {
	if q.Status == protectedQueryStatusSuccess {
		v.Status = itVersionStatusPopulateSuccess
	} else {
		v.Status = itVersionStatusPopulateFailed
	}
	it, ok := b.intermediateTables.Get(membershipKey(v.MembershipID, v.TableID))
	if !ok || it.Status != itStatusPopulateStarted {
		return
	}
	it.Status = v.Status
	it.UpdateTime = b.now()
	if v.Status != itVersionStatusPopulateSuccess {
		it.StatusReason = "population query did not succeed"

		return
	}
	activeVersion := map[string]any{
		"analysisId":           v.AnalysisID,
		"analysisType":         v.AnalysisType,
		"versionId":            v.VersionID,
		"inheritedConstraints": map[string]any{},
	}
	if v.KmsKeyArn != "" {
		activeVersion["kmsKeyArn"] = v.KmsKeyArn
	}
	if v.ExpirationTime > 0 {
		activeVersion["expirationTime"] = v.ExpirationTime
	}
	if len(v.Parameters) > 0 {
		activeVersion["parameters"] = v.Parameters
	}
	it.IntermediateTableVersion = activeVersion
}

// DisallowIntermediateTable marks every intermediate table in membershipID
// named name DISALLOWED_BY_DATA_PROVIDER, matching the real semantics
// documented on IntermediateTableStatus (the table owner/data provider
// withdrawing consent for further population/query). includeDescendants
// (default true on the wire) requests cascading the disallow to descendant
// intermediate tables built on top of this one, but this backend never
// populates TableDependencies (see the IntermediateTable doc comment), so
// there is no dependency graph to cascade through -- the direct-name-match
// transition below is real; the cascade is a documented no-op, not a
// fabricated one (see PARITY.md).
func (b *InMemoryBackend) DisallowIntermediateTable(membershipID, name string, includeDescendants bool) error {
	b.mu.Lock("DisallowIntermediateTable")
	defer b.mu.Unlock()
	if _, ok := b.memberships.Get(membershipID); !ok {
		return ErrNotFound
	}
	reason := "disallowed by data provider"
	if includeDescendants {
		reason = "disallowed by data provider (descendants requested, not modeled)"
	}
	found := false
	ts := b.now()
	for _, it := range b.intermediateTablesByMembership.Get(membershipID) {
		if it.Name != name {
			continue
		}
		found = true
		it.Status = itStatusDisallowedByDataProvider
		it.StatusReason = reason
		it.UpdateTime = ts
	}
	if !found {
		return ErrNotFound
	}

	return nil
}

func (b *InMemoryBackend) CreateIntermediateTableAnalysisRule(
	membershipID, tableID, ruleType string,
	policy map[string]any,
) (*IntermediateTableAnalysisRule, error) {
	b.mu.Lock("CreateIntermediateTableAnalysisRule")
	defer b.mu.Unlock()
	it, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID))
	if !ok {
		return nil, ErrNotFound
	}
	if b.itAnalysisRules.Has(itAnalysisRuleKey(tableID, ruleType)) {
		return nil, ErrAlreadyExists
	}
	ts := b.now()
	rule := &IntermediateTableAnalysisRule{
		AnalysisRulePolicy:          policy,
		IntermediateTableArn:        it.Arn,
		IntermediateTableIdentifier: tableID,
		AnalysisRuleType:            ruleType,
		CreateTime:                  ts,
		UpdateTime:                  ts,
	}
	b.itAnalysisRules.Put(rule)
	if !contains(it.AnalysisRuleTypes, ruleType) {
		it.AnalysisRuleTypes = append(it.AnalysisRuleTypes, ruleType)
	}

	return rule, nil
}

func (b *InMemoryBackend) GetIntermediateTableAnalysisRule(
	_, tableID, ruleType string,
) (*IntermediateTableAnalysisRule, error) {
	b.mu.RLock("GetIntermediateTableAnalysisRule")
	defer b.mu.RUnlock()
	rule, ok := b.itAnalysisRules.Get(itAnalysisRuleKey(tableID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}

	return rule, nil
}

func (b *InMemoryBackend) UpdateIntermediateTableAnalysisRule(
	_, tableID, ruleType string,
	policy map[string]any,
) (*IntermediateTableAnalysisRule, error) {
	b.mu.Lock("UpdateIntermediateTableAnalysisRule")
	defer b.mu.Unlock()
	rule, ok := b.itAnalysisRules.Get(itAnalysisRuleKey(tableID, ruleType))
	if !ok {
		return nil, ErrNotFound
	}
	rule.AnalysisRulePolicy = policy
	rule.UpdateTime = b.now()

	return rule, nil
}

func (b *InMemoryBackend) DeleteIntermediateTableAnalysisRule(membershipID, tableID, ruleType string) error {
	b.mu.Lock("DeleteIntermediateTableAnalysisRule")
	defer b.mu.Unlock()
	if !b.itAnalysisRules.Delete(itAnalysisRuleKey(tableID, ruleType)) {
		return ErrNotFound
	}
	if it, ok := b.intermediateTables.Get(membershipKey(membershipID, tableID)); ok {
		it.AnalysisRuleTypes = removeFrom(it.AnalysisRuleTypes, ruleType)
	}

	return nil
}
