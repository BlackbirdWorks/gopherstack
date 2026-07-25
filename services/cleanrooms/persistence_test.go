package cleanrooms_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

type seedState struct {
	collab      *cleanrooms.Collaboration
	membership  *cleanrooms.Membership
	table       *cleanrooms.ConfiguredTable
	ctRule      *cleanrooms.ConfiguredTableAnalysisRule
	assoc       *cleanrooms.ConfiguredTableAssociation
	ctaRule     *cleanrooms.ConfiguredTableAssociationAnalysisRule
	template    *cleanrooms.AnalysisTemplate
	budget      *cleanrooms.PrivacyBudgetTemplate
	idMapping   *cleanrooms.IDMappingTable
	idNamespace *cleanrooms.IDNamespaceAssociation
	cama        *cleanrooms.ConfiguredAudienceModelAssociation
	changeReq   *cleanrooms.CollaborationChangeRequest
	query       *cleanrooms.ProtectedQuery
	job         *cleanrooms.ProtectedJob
}

func seedFullState(t *testing.T, b *cleanrooms.InMemoryBackend) seedState {
	t.Helper()

	collab, err := b.CreateCollaboration(
		"collab-1", "a collab", "creator", []string{"CAN_QUERY"}, nil, "ENABLED",
		map[string]string{"env": "test"},
	)
	require.NoError(t, err)

	membership, err := b.CreateMembership(
		collab.CollaborationIdentifier, "ENABLED", []string{"CAN_QUERY"}, nil, nil,
		map[string]string{"team": "core"},
	)
	require.NoError(t, err)

	table, err := b.CreateConfiguredTable(
		"table-1", "a table", map[string]any{"referenceArn": "arn:aws:glue:table"}, []string{"col1"}, "DIRECT_QUERY",
		map[string]string{"owner": "data-team"},
	)
	require.NoError(t, err)

	ctRule, err := b.CreateConfiguredTableAnalysisRule(
		table.ConfiguredTableIdentifier, "AGGREGATION", map[string]any{"joinColumns": []any{"id"}},
	)
	require.NoError(t, err)

	assoc, err := b.CreateConfiguredTableAssociation(
		membership.MembershipIdentifier, "assoc-1", "an association", table.ConfiguredTableIdentifier,
		"arn:aws:iam::111122223333:role/cleanrooms", map[string]string{"purpose": "analysis"},
	)
	require.NoError(t, err)

	ctaRule, err := b.CreateConfiguredTableAssociationAnalysisRule(
		membership.MembershipIdentifier, assoc.ConfiguredTableAssociationIdentifier, "AGGREGATION",
		map[string]any{"joinColumns": []any{"id"}},
	)
	require.NoError(t, err)

	template, err := b.CreateAnalysisTemplate(
		membership.MembershipIdentifier, "template-1", "a template", "SQL",
		map[string]any{"text": "SELECT 1"}, nil, map[string]string{"kind": "sql"},
	)
	require.NoError(t, err)

	budget, err := b.CreatePrivacyBudgetTemplate(
		membership.MembershipIdentifier, "DIFFERENTIAL_PRIVACY", "NOT_ALLOWED",
		map[string]any{"epsilon": float64(10)}, map[string]string{"budget": "test"},
	)
	require.NoError(t, err)

	idMapping, err := b.CreateIDMappingTable(
		membership.MembershipIdentifier, "id-mapping-1", "an id mapping table",
		map[string]any{"inputReferenceArn": "arn:aws:cleanrooms-ml:id-mapping-workflow"}, "arn:aws:kms:key/1",
		map[string]string{"kind": "idmapping"},
	)
	require.NoError(t, err)

	idNamespace, err := b.CreateIDNamespaceAssociation(
		membership.MembershipIdentifier, "id-namespace-1", "an id namespace association",
		map[string]any{"inputReferenceArn": "arn:aws:entityresolution:id-namespace"}, nil,
		map[string]string{"kind": "idnamespace"},
	)
	require.NoError(t, err)

	cama, err := b.CreateConfiguredAudienceModelAssociation(
		membership.MembershipIdentifier, "arn:aws:cleanrooms-ml:configured-audience-model/1", "cama-1",
		"a configured audience model association", true, map[string]string{"kind": "cama"},
	)
	require.NoError(t, err)

	changeReq, err := b.CreateCollaborationChangeRequest(
		collab.CollaborationIdentifier,
		[]map[string]any{{"specificationType": "MEMBER", "specification": map[string]any{"reason": "test"}}},
	)
	require.NoError(t, err)

	query, err := b.StartProtectedQuery(
		membership.MembershipIdentifier, "SELECT 1", map[string]any{"outputFormat": "CSV"}, nil,
	)
	require.NoError(t, err)

	job, err := b.StartProtectedJob(
		membership.MembershipIdentifier, "PYTHON", map[string]any{"pythonPath": "job.py"}, nil,
	)
	require.NoError(t, err)

	return seedState{
		collab:      collab,
		membership:  membership,
		table:       table,
		ctRule:      ctRule,
		assoc:       assoc,
		ctaRule:     ctaRule,
		template:    template,
		budget:      budget,
		idMapping:   idMapping,
		idNamespace: idNamespace,
		cama:        cama,
		changeReq:   changeReq,
		query:       query,
		job:         job,
	}
}

func assertTopLevelRestored(t *testing.T, fresh *cleanrooms.InMemoryBackend, seed seedState) {
	t.Helper()

	newCollab, err := fresh.CreateCollaboration("collab-2", "", "creator-2", nil, nil, "", nil)
	require.NoError(t, err)
	assert.Contains(t, newCollab.Arn, "111122223333")
	assert.Contains(t, newCollab.Arn, "us-west-2")

	gotCollab, err := fresh.GetCollaboration(seed.collab.CollaborationIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.collab.Name, gotCollab.Name)

	gotMembership, err := fresh.GetMembership(seed.membership.MembershipIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.collab.CollaborationIdentifier, gotMembership.CollaborationID)

	gotTable, err := fresh.GetConfiguredTable(seed.table.ConfiguredTableIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.table.Name, gotTable.Name)

	gotCTRule, err := fresh.GetConfiguredTableAnalysisRule(seed.table.ConfiguredTableIdentifier, "AGGREGATION")
	require.NoError(t, err)
	assert.Equal(t, seed.ctRule.Type, gotCTRule.Type)

	gotCTARule, err := fresh.GetConfiguredTableAssociationAnalysisRule(
		seed.membership.MembershipIdentifier, seed.assoc.ConfiguredTableAssociationIdentifier, "AGGREGATION",
	)
	require.NoError(t, err)
	assert.Equal(t, seed.ctaRule.Type, gotCTARule.Type)

	require.NoError(t, fresh.DeleteConfiguredTable(seed.table.ConfiguredTableIdentifier))
	_, err = fresh.GetConfiguredTableAnalysisRule(seed.table.ConfiguredTableIdentifier, "AGGREGATION")
	require.Error(t, err)
}

func assertMembershipNestedRestored(t *testing.T, fresh *cleanrooms.InMemoryBackend, seed seedState) {
	t.Helper()
	membershipID := seed.membership.MembershipIdentifier

	gotAssoc, err := fresh.GetConfiguredTableAssociation(membershipID, seed.assoc.ConfiguredTableAssociationIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.assoc.Name, gotAssoc.Name)
	assocItems, _, err := fresh.ListConfiguredTableAssociations(membershipID, "", "")
	require.NoError(t, err)
	assert.Len(t, assocItems, 1)

	gotTemplate, err := fresh.GetAnalysisTemplate(membershipID, seed.template.AnalysisTemplateIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.template.Name, gotTemplate.Name)
	templateItems, _, err := fresh.ListAnalysisTemplates(membershipID, "", "")
	require.NoError(t, err)
	assert.Len(t, templateItems, 1)

	gotBudget, err := fresh.GetPrivacyBudgetTemplate(membershipID, seed.budget.PrivacyBudgetTemplateIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.budget.PrivacyBudgetType, gotBudget.PrivacyBudgetType)
	budgetItems, _, err := fresh.ListPrivacyBudgetTemplates(membershipID, "", "", "")
	require.NoError(t, err)
	assert.Len(t, budgetItems, 1)

	gotIDMapping, err := fresh.GetIDMappingTable(membershipID, seed.idMapping.IDMappingTableIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.idMapping.Name, gotIDMapping.Name)
	idMappingItems, _, err := fresh.ListIDMappingTables(membershipID, "", "")
	require.NoError(t, err)
	assert.Len(t, idMappingItems, 1)

	gotIDNamespace, err := fresh.GetIDNamespaceAssociation(
		membershipID,
		seed.idNamespace.IDNamespaceAssociationIdentifier,
	)
	require.NoError(t, err)
	assert.Equal(t, seed.idNamespace.Name, gotIDNamespace.Name)
	idNamespaceItems, _, err := fresh.ListIDNamespaceAssociations(membershipID, "", "")
	require.NoError(t, err)
	assert.Len(t, idNamespaceItems, 1)

	gotCama, err := fresh.GetConfiguredAudienceModelAssociation(
		membershipID,
		seed.cama.ConfiguredAudienceModelAssociationIdentifier,
	)
	require.NoError(t, err)
	assert.Equal(t, seed.cama.Name, gotCama.Name)
	camaItems, _, err := fresh.ListConfiguredAudienceModelAssociations(membershipID, "", "")
	require.NoError(t, err)
	assert.Len(t, camaItems, 1)

	gotQuery, err := fresh.GetProtectedQuery(membershipID, seed.query.ID)
	require.NoError(t, err)
	assert.Equal(t, "SUBMITTED", seed.query.Status)
	assert.Equal(t, "SUCCESS", gotQuery.Status)
	queryItems, _, err := fresh.ListProtectedQueries(membershipID, "", "", "")
	require.NoError(t, err)
	assert.Len(t, queryItems, 1)

	gotJob, err := fresh.GetProtectedJob(membershipID, seed.job.ID)
	require.NoError(t, err)
	assert.Equal(t, seed.job.Type, gotJob.Type)
	assert.Equal(t, "SUBMITTED", seed.job.Status)
	assert.Equal(t, "SUCCESS", gotJob.Status)
	jobItems, _, err := fresh.ListProtectedJobs(membershipID, "", "", "")
	require.NoError(t, err)
	assert.Len(t, jobItems, 1)

	err = fresh.DeleteConfiguredTableAssociation(membershipID, seed.assoc.ConfiguredTableAssociationIdentifier)
	require.NoError(t, err)
	_, err = fresh.GetConfiguredTableAssociationAnalysisRule(
		membershipID,
		seed.assoc.ConfiguredTableAssociationIdentifier,
		"AGGREGATION",
	)
	require.Error(t, err)
}

func assertCollaborationNestedRestored(t *testing.T, fresh *cleanrooms.InMemoryBackend, seed seedState) {
	t.Helper()
	collaborationID := seed.collab.CollaborationIdentifier

	gotChangeReq, err := fresh.GetCollaborationChangeRequest(collaborationID, seed.changeReq.ChangeRequestIdentifier)
	require.NoError(t, err)
	assert.Equal(t, seed.changeReq.Type, gotChangeReq.Type)
	changeReqItems, _, err := fresh.ListCollaborationChangeRequests(collaborationID, "", "")
	require.NoError(t, err)
	assert.Len(t, changeReqItems, 1)

	schemaItems, _, err := fresh.ListSchemas(collaborationID, "", "", "")
	require.NoError(t, err)
	assert.Empty(t, schemaItems)
}

func assertTagsRestored(t *testing.T, fresh *cleanrooms.InMemoryBackend, seed seedState) {
	t.Helper()

	tags, err := fresh.ListTagsForResource(seed.collab.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])

	membershipTags, err := fresh.ListTagsForResource(seed.membership.Arn)
	require.NoError(t, err)
	assert.Equal(t, "core", membershipTags["team"])

	tableTags, err := fresh.ListTagsForResource(seed.table.Arn)
	require.NoError(t, err)
	assert.Equal(t, "data-team", tableTags["owner"])

	assocTags, err := fresh.ListTagsForResource(seed.assoc.Arn)
	require.NoError(t, err)
	assert.Equal(t, "analysis", assocTags["purpose"])

	templateTags, err := fresh.ListTagsForResource(seed.template.Arn)
	require.NoError(t, err)
	assert.Equal(t, "sql", templateTags["kind"])

	budgetTags, err := fresh.ListTagsForResource(seed.budget.Arn)
	require.NoError(t, err)
	assert.Equal(t, "test", budgetTags["budget"])

	idMappingTags, err := fresh.ListTagsForResource(seed.idMapping.Arn)
	require.NoError(t, err)
	assert.Equal(t, "idmapping", idMappingTags["kind"])

	idNSTags, err := fresh.ListTagsForResource(seed.idNamespace.Arn)
	require.NoError(t, err)
	assert.Equal(t, "idnamespace", idNSTags["kind"])

	camaTags, err := fresh.ListTagsForResource(seed.cama.Arn)
	require.NoError(t, err)
	assert.Equal(t, "cama", camaTags["kind"])
}

func TestInMemoryBackend_Restore(t *testing.T) {
	t.Parallel()

	type args struct {
		snapJSON []byte
	}
	type wants struct {
		err         bool
		emptyCollab bool
	}

	tests := []struct {
		name  string
		args  args
		wants wants
	}{
		{
			name:  "Invalid data",
			args:  args{snapJSON: []byte("not-valid-json")},
			wants: wants{err: true, emptyCollab: false},
		},
		{
			name:  "Version mismatch",
			args:  args{snapJSON: []byte(`{"version":999,"tables":{}}`)},
			wants: wants{err: false, emptyCollab: true},
		},
		{
			name:  "Old snapshot decodes as zero",
			args:  args{snapJSON: []byte(`{"collaborations":{}}`)},
			wants: wants{err: false, emptyCollab: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			if tt.wants.emptyCollab {
				_, err := b.CreateCollaboration("seed-collab", "", "creator", nil, nil, "", nil)
				require.NoError(t, err)
			}

			err := b.Restore(t.Context(), tt.args.snapJSON)
			if tt.wants.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.wants.emptyCollab {
					items, _ := b.ListCollaborations("", "", "")
					assert.Empty(t, items)
				}
			}
		})
	}
}

func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	type args struct{}
	type wants struct{}

	tests := []struct {
		args  args
		wants wants
		name  string
	}{
		{
			name:  "Full state snapshot restore",
			args:  args{},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			original := cleanrooms.NewInMemoryBackend("111122223333", "us-west-2")
			seed := seedFullState(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			require.NoError(t, fresh.Restore(t.Context(), snap))

			assertTagsRestored(t, fresh, seed)
			assertTopLevelRestored(t, fresh, seed)
			assertMembershipNestedRestored(t, fresh, seed)
			assertCollaborationNestedRestored(t, fresh, seed)
		})
	}
}

func TestHandler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	type args struct{}
	type wants struct{}

	tests := []struct {
		args  args
		wants wants
		name  string
	}{
		{
			name:  "Handler snapshot restore",
			args:  args{},
			wants: wants{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			originalBackend := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			h := cleanrooms.NewHandler(originalBackend)

			snap := h.Snapshot(t.Context())
			require.NotNil(t, snap)

			freshBackend := cleanrooms.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
			h2 := cleanrooms.NewHandler(freshBackend)
			require.NoError(t, h2.Restore(t.Context(), snap))
		})
	}
}
