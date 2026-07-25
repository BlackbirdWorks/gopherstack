package cleanrooms

import (
	"encoding/json"
	"net/http"
	"strings"
)

// classifyPath maps (method, path) to an operation name and primary resource.
func classifyPath(method, path string) (string, string) {
	// Trim leading slash and split
	path = strings.TrimPrefix(path, "/")
	segs := strings.Split(path, "/")
	if len(segs) == 0 {
		return opUnknown, ""
	}

	root := segs[0]

	switch root {
	case "collaborations":
		return classifyCollaborations(method, segs)
	case "configuredTables":
		return classifyConfiguredTables(method, segs)
	case "memberships":
		return classifyMemberships(method, segs)
	case "tags":
		return classifyTags(method, segs)
	}

	return opUnknown, ""
}

func classifyCollaborations(method string, segs []string) (string, string) {
	// /collaborations
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateCollaboration, ""
		case http.MethodGet:
			return opListCollaborations, ""
		}
	}
	// /collaborations/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetCollaboration, id
		case http.MethodDelete:
			return opDeleteCollaboration, id
		case http.MethodPatch:
			return opUpdateCollaboration, id
		}
	}
	// /collaborations/{id}/{sub}[/...]
	if len(segs) >= segsWithSub {
		id := segs[1]
		sub := segs[2]

		return classifyCollaboration(method, id, sub, segs)
	}

	return opUnknown, ""
}

// classifyCollaboration handles sub-resource routing for /collaborations/{id}/{sub}[/...].
func classifyCollaboration(method, id, sub string, segs []string) (string, string) {
	switch sub {
	case subAnalysisTemplates:
		return classifyCollabAnalysisTemplates(method, id, segs)
	case "batch-analysistemplates", "batch-schema", "batch-schema-analysis-rule":
		return classifyCollabBatchPost(method, id, sub)
	case "changeRequests":
		return classifyCollabChangeRequests(method, id, segs)
	case subCAMAAssociations:
		return classifyCollabCAMAAssocs(method, id, segs)
	case subIDNamespaceAssocs:
		return classifyCollabIDNamespaceAssocs(method, id, segs)
	case "member":
		return classifyCollabMember(method, id, segs)
	case "members":
		if method == http.MethodGet {
			return opListMembers, id
		}
	case subPrivacyBudgetTmpls:
		return classifyCollabPrivacyBudgetTmpls(method, id, segs)
	case "privacybudgets":
		if method == http.MethodGet {
			return opListCollaborationPrivacyBudgets, id
		}
	case subSchemas:
		return classifyCollabSchemas(method, id, segs)
	}

	return opUnknown, ""
}

func classifyCollabBatchPost(method, id, sub string) (string, string) {
	if method != http.MethodPost {
		return opUnknown, ""
	}
	switch sub {
	case "batch-analysistemplates":
		return opBatchGetCollaborationAnalysisTemplate, id
	case "batch-schema":
		return opBatchGetSchema, id
	case "batch-schema-analysis-rule":
		return opBatchGetSchemaAnalysisRule, id
	}

	return opUnknown, ""
}

func classifyCollabMember(method, id string, segs []string) (string, string) {
	// /collaborations/{id}/member/{accountId}
	if len(segs) == segsWithSubID && method == http.MethodDelete {
		return opDeleteMember, id
	}

	return opUnknown, ""
}

func classifyCollabAnalysisTemplates(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationAnalysisTemplates, id
	}
	// GetCollaborationAnalysisTemplate's path parameter is analysisTemplateArn
	// (arn:...:membership/{id}/analysistemplate/{id}), which contains real "/"
	// characters. Go's http server decodes the request's percent-encoded %2F
	// back into a literal "/" in URL.Path (see injectCollaborationParams,
	// which re-joins segs[3:]), so the ARN spans more than one path segment
	// here -- ">=", not "==", or this op is permanently unroutable.
	if len(segs) >= segsWithSubID && method == http.MethodGet {
		return opGetCollaborationAnalysisTemplate, id
	}

	return opUnknown, ""
}

func classifyCollabChangeRequests(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateCollaborationChangeRequest, id
		case http.MethodGet:
			return opListCollaborationChangeRequests, id
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetCollaborationChangeRequest, id
		case http.MethodPatch:
			return opUpdateCollaborationChangeRequest, id
		}
	}

	return opUnknown, ""
}

func classifyCollabCAMAAssocs(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationConfiguredAudienceModelAssociations, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationConfiguredAudienceModelAssociation, id
	}

	return opUnknown, ""
}

func classifyCollabIDNamespaceAssocs(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationIDNamespaceAssociations, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationIDNamespaceAssociation, id
	}

	return opUnknown, ""
}

func classifyCollabPrivacyBudgetTmpls(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListCollaborationPrivacyBudgetTemplates, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetCollaborationPrivacyBudgetTemplate, id
	}

	return opUnknown, ""
}

func classifyCollabSchemas(method, id string, segs []string) (string, string) {
	if len(segs) == segsWithSub && method == http.MethodGet {
		return opListSchemas, id
	}
	if len(segs) == segsWithSubID && method == http.MethodGet {
		return opGetSchema, id
	}
	// /collaborations/{id}/schemas/{name}/analysisRule/{type}
	if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule && method == http.MethodGet {
		return opGetSchemaAnalysisRule, id
	}

	return opUnknown, ""
}

func classifyConfiguredTables(method string, segs []string) (string, string) {
	// /configuredTables
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredTable, ""
		case http.MethodGet:
			return opListConfiguredTables, ""
		}
	}
	// /configuredTables/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetConfiguredTable, id
		case http.MethodDelete:
			return opDeleteConfiguredTable, id
		case http.MethodPatch:
			return opUpdateConfiguredTable, id
		}
	}
	// /configuredTables/{id}/analysisRule[/{type}]
	if len(segs) >= segsWithSub && segs[2] == subAnalysisRule {
		return classifyConfiguredTableAnalysisRule(method, segs)
	}

	return opUnknown, ""
}

func classifyConfiguredTableAnalysisRule(method string, segs []string) (string, string) {
	id := segs[1]
	if len(segs) == segsWithSub && method == http.MethodPost {
		return opCreateConfiguredTableAnalysisRule, id
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAnalysisRule, id
		case http.MethodDelete:
			return opDeleteConfiguredTableAnalysisRule, id
		case http.MethodPatch:
			return opUpdateConfiguredTableAnalysisRule, id
		}
	}

	return opUnknown, ""
}

func classifyMemberships(method string, segs []string) (string, string) {
	// /memberships
	if len(segs) == segsRoot {
		switch method {
		case http.MethodPost:
			return opCreateMembership, ""
		case http.MethodGet:
			return opListMemberships, ""
		}
	}
	// /memberships/{id}
	if len(segs) == segsWithID {
		id := segs[1]
		switch method {
		case http.MethodGet:
			return opGetMembership, id
		case http.MethodDelete:
			return opDeleteMembership, id
		case http.MethodPatch:
			return opUpdateMembership, id
		}
	}
	if len(segs) < segsWithSub {
		return opUnknown, ""
	}
	membershipID := segs[1]
	sub := segs[2]

	return classifyMembership(method, membershipID, sub, segs)
}

// classifyMembership handles sub-resource routing for /memberships/{id}/{sub}[/...].
func classifyMembership(method, membershipID, sub string, segs []string) (string, string) {
	switch sub {
	case subAnalysisTemplates:
		return classifyMemAnalysisTemplates(method, membershipID, segs)
	case "configuredTableAssociations":
		return classifyMemCTAssociations(method, membershipID, segs)
	case subCAMAAssociations:
		return classifyMemCAMAAssocs(method, membershipID, segs)
	case "idmappingtables":
		return classifyMemIDMappingTables(method, membershipID, segs)
	case subIDNamespaceAssocs:
		return classifyMemIDNamespaceAssocs(method, membershipID, segs)
	case subIntermediateTables:
		return classifyMemIntermediateTables(method, membershipID, segs)
	case subPrivacyBudgetTmpls:
		return classifyMemPrivacyBudgetTmpls(method, membershipID, segs)
	case subProtectedJobs:
		return classifyMemProtectedJobs(method, membershipID, segs)
	case subProtectedQueries:
		return classifyMemProtectedQueries(method, membershipID, segs)
	}

	return classifyMembershipSingleOp(method, membershipID, sub)
}

// classifyMembershipSingleOp handles the membership sub-resources that are a
// single endpoint with no nested ID segment (disallowIntermediateTable,
// previewprivacyimpact, privacybudgets), factored out of classifyMembership
// to keep its cyclomatic complexity down.
func classifyMembershipSingleOp(method, membershipID, sub string) (string, string) {
	switch sub {
	case subDisallowIT:
		if method == http.MethodPost {
			return opDisallowIntermediateTable, membershipID
		}
	case "previewprivacyimpact":
		if method == http.MethodPost {
			return opPreviewPrivacyImpact, membershipID
		}
	case "privacybudgets":
		if method == http.MethodGet {
			return opListPrivacyBudgets, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemAnalysisTemplates(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateAnalysisTemplate, membershipID
		case http.MethodGet:
			return opListAnalysisTemplates, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetAnalysisTemplate, membershipID
		case http.MethodDelete:
			return opDeleteAnalysisTemplate, membershipID
		case http.MethodPatch:
			return opUpdateAnalysisTemplate, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemCTAssociations(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredTableAssociation, membershipID
		case http.MethodGet:
			return opListConfiguredTableAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAssociation, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredTableAssociation, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredTableAssociation, membershipID
		}
	}
	if len(segs) >= segsWithSubSub && segs[4] == subAnalysisRule {
		return classifyMemCTAssocAnalysisRule(method, membershipID, segs)
	}

	return opUnknown, ""
}

func classifyMemCTAssocAnalysisRule(method, membershipID string, segs []string) (string, string) {
	// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule
	if len(segs) == segsWithSubSub && method == http.MethodPost {
		return opCreateConfiguredTableAssociationAnalysisRule, membershipID
	}
	// /memberships/{id}/configuredTableAssociations/{assocId}/analysisRule/{type}
	if len(segs) == segsWithSubSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredTableAssociationAnalysisRule, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredTableAssociationAnalysisRule, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredTableAssociationAnalysisRule, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemCAMAAssocs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateConfiguredAudienceModelAssociation, membershipID
		case http.MethodGet:
			return opListConfiguredAudienceModelAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetConfiguredAudienceModelAssociation, membershipID
		case http.MethodDelete:
			return opDeleteConfiguredAudienceModelAssociation, membershipID
		case http.MethodPatch:
			return opUpdateConfiguredAudienceModelAssociation, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemIDMappingTables(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateIDMappingTable, membershipID
		case http.MethodGet:
			return opListIDMappingTables, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetIDMappingTable, membershipID
		case http.MethodDelete:
			return opDeleteIDMappingTable, membershipID
		case http.MethodPatch:
			return opUpdateIDMappingTable, membershipID
		}
	}
	// /memberships/{id}/idmappingtables/{tableId}/populate
	if len(segs) == segsWithSubSub && segs[4] == "populate" && method == http.MethodPost {
		return opPopulateIDMappingTable, membershipID
	}

	return opUnknown, ""
}

func classifyMemIDNamespaceAssocs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateIDNamespaceAssociation, membershipID
		case http.MethodGet:
			return opListIDNamespaceAssociations, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetIDNamespaceAssociation, membershipID
		case http.MethodDelete:
			return opDeleteIDNamespaceAssociation, membershipID
		case http.MethodPatch:
			return opUpdateIDNamespaceAssociation, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemIntermediateTables(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreateIntermediateTable, membershipID
		case http.MethodGet:
			return opListIntermediateTables, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetIntermediateTable, membershipID
		case http.MethodDelete:
			return opDeleteIntermediateTable, membershipID
		case http.MethodPatch:
			return opUpdateIntermediateTable, membershipID
		}
	}
	if len(segs) >= segsWithSubSub {
		return classifyMemIntermediateTableSub(method, membershipID, segs)
	}

	return opUnknown, ""
}

// classifyMemIntermediateTableSub handles
// /memberships/{id}/intermediateTables/{tableId}/{versions|populate|analysisRule}[/...].
func classifyMemIntermediateTableSub(method, membershipID string, segs []string) (string, string) {
	switch segs[4] {
	case "versions":
		if len(segs) == segsWithSubSub && method == http.MethodGet {
			return opListIntermediateTableVersions, membershipID
		}
	case "populate":
		if len(segs) == segsWithSubSub && method == http.MethodPost {
			return opPopulateIntermediateTable, membershipID
		}
	case subAnalysisRule:
		return classifyMemITAnalysisRule(method, membershipID, segs)
	}

	return opUnknown, ""
}

func classifyMemITAnalysisRule(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSubSub && method == http.MethodPost {
		return opCreateIntermediateTableAnalysisRule, membershipID
	}
	if len(segs) == segsWithSubSubID {
		switch method {
		case http.MethodGet:
			return opGetIntermediateTableAnalysisRule, membershipID
		case http.MethodDelete:
			return opDeleteIntermediateTableAnalysisRule, membershipID
		case http.MethodPatch:
			return opUpdateIntermediateTableAnalysisRule, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemPrivacyBudgetTmpls(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opCreatePrivacyBudgetTemplate, membershipID
		case http.MethodGet:
			return opListPrivacyBudgetTemplates, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetPrivacyBudgetTemplate, membershipID
		case http.MethodDelete:
			return opDeletePrivacyBudgetTemplate, membershipID
		case http.MethodPatch:
			return opUpdatePrivacyBudgetTemplate, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemProtectedJobs(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opStartProtectedJob, membershipID
		case http.MethodGet:
			return opListProtectedJobs, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetProtectedJob, membershipID
		case http.MethodPatch:
			return opUpdateProtectedJob, membershipID
		}
	}

	return opUnknown, ""
}

func classifyMemProtectedQueries(method, membershipID string, segs []string) (string, string) {
	if len(segs) == segsWithSub {
		switch method {
		case http.MethodPost:
			return opStartProtectedQuery, membershipID
		case http.MethodGet:
			return opListProtectedQueries, membershipID
		}
	}
	if len(segs) == segsWithSubID {
		switch method {
		case http.MethodGet:
			return opGetProtectedQuery, membershipID
		case http.MethodPatch:
			return opUpdateProtectedQuery, membershipID
		}
	}

	return opUnknown, ""
}

func classifyTags(method string, segs []string) (string, string) {
	if len(segs) < segsWithID {
		return opUnknown, ""
	}
	resourceArn := strings.Join(segs[1:], "/")
	switch method {
	case http.MethodGet:
		return opListTagsForResource, resourceArn
	case http.MethodPost:
		return opTagResource, resourceArn
	case http.MethodDelete:
		return opUntagResource, resourceArn
	}

	return opUnknown, ""
}

// injectPathParams merges URL path segments into the request body JSON.
func injectPathParams(path, _ string, body []byte) []byte {
	path = strings.TrimPrefix(path, "/")
	segs := strings.Split(path, "/")

	var m map[string]json.RawMessage
	if len(body) > 0 {
		_ = json.Unmarshal(body, &m)
	}
	if m == nil {
		m = make(map[string]json.RawMessage)
	}

	setStr := func(key, val string) {
		if val != "" {
			b, _ := json.Marshal(val)
			m[key] = b
		}
	}

	switch {
	case len(segs) >= segsWithID && segs[0] == "collaborations":
		injectCollaborationParams(segs, setStr)
	case len(segs) >= segsWithID && segs[0] == "configuredTables":
		setStr("configuredTableIdentifier", segs[1])
		if len(segs) == segsWithSubID && segs[2] == subAnalysisRule {
			setStr("analysisRuleType", segs[3])
		}
	case len(segs) >= segsWithID && segs[0] == "memberships":
		injectMembershipParams(segs, setStr)
	case len(segs) >= segsWithID && segs[0] == subTags:
		arnVal := strings.Join(segs[1:], "/")
		setStr("resourceArn", arnVal)
	}

	out, _ := json.Marshal(m)

	return out
}

// injectCollaborationParams injects path parameters for /collaborations/... routes.
func injectCollaborationParams(segs []string, setStr func(string, string)) {
	setStr("collaborationIdentifier", segs[1])
	if len(segs) >= segsWithSubID {
		switch segs[2] {
		case subAnalysisTemplates:
			// analysisTemplateArn is an ARN (arn:...:membership/{id}/analysistemplate/{id})
			// and so spans every remaining segment once URL.Path has decoded its
			// embedded "/" characters back to literal slashes; see
			// classifyCollabAnalysisTemplates.
			setStr("analysisTemplateArn", strings.Join(segs[3:], "/"))
		case "changeRequests":
			setStr("changeRequestIdentifier", segs[3])
		case subCAMAAssociations:
			setStr("configuredAudienceModelAssociationIdentifier", segs[3])
		case subIDNamespaceAssocs:
			setStr("idNamespaceAssociationIdentifier", segs[3])
		case "member":
			setStr("accountId", segs[3])
		case subPrivacyBudgetTmpls:
			setStr("privacyBudgetTemplateIdentifier", segs[3])
		case subSchemas:
			setStr("name", segs[3])
			if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule {
				setStr("type", segs[5])
			}
		}
	}
}

// injectMembershipParams injects path parameters for /memberships/... routes.
func injectMembershipParams(segs []string, setStr func(string, string)) {
	setStr("membershipIdentifier", segs[1])
	if len(segs) >= segsWithSubID {
		switch segs[2] {
		case subAnalysisTemplates:
			setStr("analysisTemplateIdentifier", segs[3])
		case "configuredTableAssociations":
			setStr("configuredTableAssociationIdentifier", segs[3])
			if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule {
				setStr("analysisRuleType", segs[5])
			}
		case subCAMAAssociations:
			setStr("configuredAudienceModelAssociationIdentifier", segs[3])
		case "idmappingtables":
			setStr("idMappingTableIdentifier", segs[3])
		case subIDNamespaceAssocs:
			setStr("idNamespaceAssociationIdentifier", segs[3])
		case subIntermediateTables:
			setStr("intermediateTableIdentifier", segs[3])
			if len(segs) == segsWithSubSubID && segs[4] == subAnalysisRule {
				setStr("analysisRuleType", segs[5])
			}
		case subPrivacyBudgetTmpls:
			setStr("privacyBudgetTemplateIdentifier", segs[3])
		case subProtectedJobs:
			setStr("protectedJobIdentifier", segs[3])
		case subProtectedQueries:
			setStr("protectedQueryIdentifier", segs[3])
		}
	}
}
