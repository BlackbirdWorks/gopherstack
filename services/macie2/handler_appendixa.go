package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// --- path parsers ---

func parseJobPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /jobs
		if method == http.MethodPost {
			return opCreateClassificationJob, ""
		}
	case depthResource: // /jobs/{jobId|list}
		switch parts[1] {
		case "list": //nolint:goconst // existing issue.
			if method == http.MethodPost {
				return opListClassificationJobs, ""
			}
		default:
			switch method {
			case http.MethodGet:
				return opDescribeClassificationJob, parts[1]
			case http.MethodPatch:
				return opUpdateClassificationJob, parts[1]
			}
		}
	}

	return opUnknown, ""
}

func parseMembersPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /members
		switch method {
		case http.MethodPost:
			return opCreateMember, ""
		case http.MethodGet:
			return opListMembers, ""
		}
	case depthResource: // /members/{id|disassociate}
		switch parts[1] {
		case "disassociate": //nolint:goconst // existing issue.
			// /members/disassociate — no, that's in a 3-part path below
		default:
			switch method {
			case http.MethodGet:
				return opGetMember, parts[1]
			case http.MethodDelete:
				return opDeleteMember, parts[1]
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[1] == "disassociate" && method == http.MethodPost {
			return opDisassociateMember, parts[2]
		}
	}

	return opUnknown, ""
}

func parseInvitationsPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /invitations
		switch method {
		case http.MethodPost:
			return opCreateInvitations, ""
		case http.MethodGet:
			return opListInvitations, ""
		}
	case depthResource: // /invitations/{action}
		switch parts[1] {
		case "accept":
			if method == http.MethodPost {
				return opAcceptInvitation, ""
			}
		case "decline":
			if method == http.MethodPost {
				return opDeclineInvitations, ""
			}
		case "delete":
			if method == http.MethodPost {
				return opDeleteInvitations, ""
			}
		case "count":
			if method == http.MethodGet {
				return opGetInvitationsCount, ""
			}
		}
	}

	return opUnknown, ""
}

func parseAdministratorPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /administrator
		if method == http.MethodGet {
			return opGetAdministratorAccount, ""
		}
	case depthResource: // /administrator/disassociate
		if parts[1] == "disassociate" && method == http.MethodPost {
			return opDisassociateFromAdministratorAccount, ""
		}
	}

	return opUnknown, ""
}

func parseMasterPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /master
		if method == http.MethodGet {
			return opGetMasterAccount, ""
		}
	case depthResource: // /master/disassociate
		if parts[1] == "disassociate" && method == http.MethodPost {
			return opDisassociateFromMasterAccount, ""
		}
	}

	return opUnknown, ""
}

func parseAdminPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /admin
		switch method {
		case http.MethodGet:
			return opListOrganizationAdminAccounts, ""
		case http.MethodPost:
			return opEnableOrganizationAdminAccount, ""
		case http.MethodDelete:
			return opDisableOrganizationAdminAccount, ""
		}
	case depthResource: // /admin/configuration
		if parts[1] == "configuration" { //nolint:goconst // existing issue.
			switch method {
			case http.MethodGet:
				return opDescribeOrganizationConfiguration, ""
			case http.MethodPatch:
				return opUpdateOrganizationConfiguration, ""
			}
		}
	}

	return opUnknown, ""
}

func parseAutomatedDiscoveryPath(method string, parts []string) (string, string) {
	// /automated-discovery/configuration
	// /automated-discovery/accounts
	if len(parts) < 2 { //nolint:mnd // existing issue.
		return opUnknown, ""
	}

	switch parts[1] {
	case "configuration":
		switch method {
		case http.MethodGet:
			return opGetAutomatedDiscoveryConfiguration, ""
		case http.MethodPut:
			return opUpdateAutomatedDiscoveryConfiguration, ""
		}
	case "accounts":
		switch method {
		case http.MethodGet:
			return opListAutomatedDiscoveryAccounts, ""
		case http.MethodPatch:
			return opBatchUpdateAutomatedDiscoveryAccounts, ""
		}
	}

	return opUnknown, ""
}

func parseDatasourcesPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthResource: // /datasources/{s3|search-resources}
		switch parts[1] {
		case "s3":
			if method == http.MethodPost {
				return opDescribeBuckets, ""
			}
		case "search-resources":
			if method == http.MethodPost {
				return opSearchResources, ""
			}
		}
	case 3: //nolint:mnd // existing issue.
		if parts[1] == "s3" && parts[2] == "statistics" && //nolint:goconst // existing issue.
			method == http.MethodPost {
			return opGetBucketStatistics, ""
		}
	}

	return opUnknown, ""
}

func parseClassExportCfgPath(method string, _ []string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetClassificationExportConfiguration, ""
	case http.MethodPut:
		return opPutClassificationExportConfiguration, ""
	}

	return opUnknown, ""
}

func parseClassScopesPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /classification-scopes
		if method == http.MethodGet {
			return opListClassificationScopes, ""
		}
	case depthResource: // /classification-scopes/{id}
		switch method {
		case http.MethodGet:
			return opGetClassificationScope, parts[1]
		case http.MethodPatch:
			return opUpdateClassificationScope, parts[1]
		}
	}

	return opUnknown, ""
}

func parseFindingsPubCfgPath(method string, _ []string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetFindingsPublicationConfiguration, ""
	case http.MethodPut:
		return opPutFindingsPublicationConfiguration, ""
	}

	return opUnknown, ""
}

func parseResourceProfilesPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /resource-profiles
		switch method {
		case http.MethodGet:
			return opGetResourceProfile, ""
		case http.MethodPatch:
			return opUpdateResourceProfile, ""
		}
	case depthResource: // /resource-profiles/{artifacts|detections}
		switch parts[1] {
		case "artifacts":
			if method == http.MethodGet {
				return opListResourceProfileArtifacts, ""
			}
		case "detections":
			switch method {
			case http.MethodGet:
				return opListResourceProfileDetections, ""
			case http.MethodPatch:
				return opUpdateResourceProfileDetections, ""
			}
		}
	}

	return opUnknown, ""
}

func parseRevealCfgPath(method string, _ []string) (string, string) {
	switch method {
	case http.MethodGet:
		return opGetRevealConfiguration, ""
	case http.MethodPut:
		return opUpdateRevealConfiguration, ""
	}

	return opUnknown, ""
}

func parseUsagePath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /usage
		if method == http.MethodGet {
			return opGetUsageTotals, ""
		}
	case depthResource: // /usage/statistics
		if parts[1] == "statistics" && method == http.MethodPost {
			return opGetUsageStatistics, ""
		}
	}

	return opUnknown, ""
}

func parseManagedDataIDsPath(method string, parts []string) (string, string) {
	// /managed-data-identifiers/list
	if len(parts) == depthResource && parts[1] == "list" && method == http.MethodGet {
		return opListManagedDataIdentifiers, ""
	}

	return opUnknown, ""
}

func parseTemplatesPath(method string, parts []string) (string, string) {
	// /templates/sensitivity-inspections[/{id}]
	if len(parts) < 2 || parts[1] != "sensitivity-inspections" {
		return opUnknown, ""
	}

	switch len(parts) {
	case depthResource: // /templates/sensitivity-inspections
		if method == http.MethodGet {
			return opListSensitivityInspectionTemplates, ""
		}
	case 3: //nolint:mnd // existing issue.
		switch method {
		case http.MethodGet:
			return opGetSensitivityInspectionTemplate, parts[2]
		case http.MethodPut:
			return opUpdateSensitivityInspectionTemplate, parts[2]
		}
	}

	return opUnknown, ""
}

// --- dispatch groups ---

func (h *Handler) dispatchClassificationJobOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateClassificationJob:
		result, code, err := h.handleCreateClassificationJob(body)

		return result, code, true, err

	case opDescribeClassificationJob:
		id := extractID(path, pathJobs)
		result, code, err := h.handleDescribeClassificationJob(id)

		return result, code, true, err

	case opListClassificationJobs:
		result, code, err := h.handleListClassificationJobs(body)

		return result, code, true, err

	case opUpdateClassificationJob:
		id := extractID(path, pathJobs)
		code, err := h.handleUpdateClassificationJob(id, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchMemberOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateMember:
		code, err := h.handleCreateMember(body)

		return nil, code, true, err

	case opGetMember:
		accountID := extractID(path, pathMembers)
		result, code, err := h.handleGetMember(accountID)

		return result, code, true, err

	case opDeleteMember:
		accountID := extractID(path, pathMembers)
		code, err := h.handleDeleteMember(accountID)

		return nil, code, true, err

	case opListMembers:
		result, code, err := h.handleListMembers()

		return result, code, true, err

	case opDisassociateMember:
		accountID := extractDisassociateMemberID(path)
		code, err := h.handleDisassociateMember(accountID)

		return nil, code, true, err

	case opUpdateMemberSession:
		accountID := extractMacieMemberID(path)
		code, err := h.handleUpdateMemberSession(accountID, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchInvitationOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opCreateInvitations:
		result, code, err := h.handleCreateInvitations(body)

		return result, code, true, err

	case opAcceptInvitation:
		code, err := h.handleAcceptInvitation(body)

		return nil, code, true, err

	case opDeclineInvitations:
		result, code, err := h.handleDeclineInvitations(body)

		return result, code, true, err

	case opDeleteInvitations:
		result, code, err := h.handleDeleteInvitations(body)

		return result, code, true, err

	case opGetInvitationsCount:
		result, code, err := h.handleGetInvitationsCount()

		return result, code, true, err

	case opListInvitations:
		result, code, err := h.handleListInvitations()

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchAdminOps(
	op, path, query string, //nolint:revive,unparam // existing issue.
	body []byte,
) (any, int, bool, error) {
	switch op {
	case opGetAdministratorAccount:
		result, code, err := h.handleGetAdministratorAccount()

		return result, code, true, err

	case opDisassociateFromAdministratorAccount:
		code, err := h.handleDisassociateFromAdministratorAccount()

		return nil, code, true, err

	case opGetMasterAccount:
		result, code, err := h.handleGetMasterAccount()

		return result, code, true, err

	case opDisassociateFromMasterAccount:
		code, err := h.handleDisassociateFromMasterAccount()

		return nil, code, true, err

	case opEnableOrganizationAdminAccount:
		code, err := h.handleEnableOrganizationAdminAccount(body)

		return nil, code, true, err

	case opDisableOrganizationAdminAccount:
		accountID := extractQueryParam(query, "adminAccountId")
		code, err := h.handleDisableOrganizationAdminAccount(accountID)

		return nil, code, true, err

	case opListOrganizationAdminAccounts:
		result, code, err := h.handleListOrganizationAdminAccounts()

		return result, code, true, err

	case opDescribeOrganizationConfiguration:
		result, code, err := h.handleDescribeOrganizationConfiguration()

		return result, code, true, err

	case opUpdateOrganizationConfiguration:
		code, err := h.handleUpdateOrganizationConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchAutomatedDiscoveryOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetAutomatedDiscoveryConfiguration:
		result, code, err := h.handleGetAutomatedDiscoveryConfiguration()

		return result, code, true, err

	case opUpdateAutomatedDiscoveryConfiguration:
		code, err := h.handleUpdateAutomatedDiscoveryConfiguration(body)

		return nil, code, true, err

	case opListAutomatedDiscoveryAccounts:
		result, code, err := h.handleListAutomatedDiscoveryAccounts()

		return result, code, true, err

	case opBatchUpdateAutomatedDiscoveryAccounts:
		code, err := h.handleBatchUpdateAutomatedDiscoveryAccounts(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchBucketOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opDescribeBuckets:
		result, code, err := h.handleDescribeBuckets(body)

		return result, code, true, err

	case opGetBucketStatistics:
		result, code, err := h.handleGetBucketStatistics(body)

		return result, code, true, err

	case opBatchGetCustomDataIdentifiers:
		result, code, err := h.handleBatchGetCustomDataIdentifiers(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchConfigOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetClassificationExportConfiguration:
		result, code, err := h.handleGetClassificationExportConfiguration()

		return result, code, true, err

	case opPutClassificationExportConfiguration:
		code, err := h.handlePutClassificationExportConfiguration(body)

		return nil, code, true, err

	case opGetFindingsPublicationConfiguration:
		result, code, err := h.handleGetFindingsPublicationConfiguration()

		return result, code, true, err

	case opPutFindingsPublicationConfiguration:
		code, err := h.handlePutFindingsPublicationConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchScopeOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetClassificationScope:
		id := extractID(path, pathClassScopes)
		result, code, err := h.handleGetClassificationScope(id)

		return result, code, true, err

	case opListClassificationScopes:
		result, code, err := h.handleListClassificationScopes()

		return result, code, true, err

	case opUpdateClassificationScope:
		id := extractID(path, pathClassScopes)
		code, err := h.handleUpdateClassificationScope(id, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchResourceProfileOps(op, query string, body []byte) (any, int, bool, error) {
	resourceARN := extractQueryParam(query, "resourceArn")

	switch op {
	case opGetResourceProfile:
		result, code, err := h.handleGetResourceProfile(resourceARN)

		return result, code, true, err

	case opUpdateResourceProfile:
		code, err := h.handleUpdateResourceProfile(resourceARN, body)

		return nil, code, true, err

	case opListResourceProfileArtifacts:
		result, code, err := h.handleListResourceProfileArtifacts(resourceARN)

		return result, code, true, err

	case opListResourceProfileDetections:
		result, code, err := h.handleListResourceProfileDetections(resourceARN)

		return result, code, true, err

	case opUpdateResourceProfileDetections:
		code, err := h.handleUpdateResourceProfileDetections(resourceARN, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchRevealOps(op string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetRevealConfiguration:
		result, code, err := h.handleGetRevealConfiguration()

		return result, code, true, err

	case opUpdateRevealConfiguration:
		code, err := h.handleUpdateRevealConfiguration(body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchSensitivityTemplateOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetSensitivityInspectionTemplate:
		id := extractTemplateID(path)
		result, code, err := h.handleGetSensitivityInspectionTemplate(id)

		return result, code, true, err

	case opListSensitivityInspectionTemplates:
		result, code, err := h.handleListSensitivityInspectionTemplates()

		return result, code, true, err

	case opUpdateSensitivityInspectionTemplate:
		id := extractTemplateID(path)
		code, err := h.handleUpdateSensitivityInspectionTemplate(id, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchUsageOps(op, query string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetUsageStatistics:
		result, code, err := h.handleGetUsageStatistics(body)

		return result, code, true, err

	case opGetUsageTotals:
		timeRange := extractQueryParam(query, "timeRange")
		result, code, err := h.handleGetUsageTotals(timeRange)

		return result, code, true, err

	case opListManagedDataIdentifiers:
		result, code, err := h.handleListManagedDataIdentifiers()

		return result, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) dispatchSearchOps(op string, body []byte) (any, int, bool, error) {
	if op == opSearchResources {
		result, code, err := h.handleSearchResources(body)

		return result, code, true, err
	}

	return nil, 0, false, nil
}

// --- handler implementations ---

func (h *Handler) handleCreateClassificationJob(body []byte) (any, int, error) {
	var req struct {
		Tags               map[string]string `json:"tags"`
		S3JobDefinition    map[string]any    `json:"s3JobDefinition"`
		ScheduleFrequency  map[string]any    `json:"scheduleFrequency"`
		ClientToken        string            `json:"clientToken"`
		Description        string            `json:"description"`
		JobType            string            `json:"jobType"`
		Name               string            `json:"name"`
		SamplingPercentage int32             `json:"samplingPercentage"`
		InitialRun         bool              `json:"initialRun"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	if req.Name == "" || req.JobType == "" {
		return nil, http.StatusBadRequest, ErrValidation
	}

	id, jobArn, err := h.Backend.CreateClassificationJob(
		req.Name, req.Description, req.JobType, req.ClientToken,
		req.S3JobDefinition, req.ScheduleFrequency, req.Tags,
		req.SamplingPercentage, req.InitialRun,
	)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]string{"jobArn": jobArn, "jobId": id, "jobStatus": "RUNNING"}, http.StatusOK, nil
}

func (h *Handler) handleDescribeClassificationJob(jobID string) (any, int, error) {
	job, err := h.Backend.DescribeClassificationJob(jobID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return job, http.StatusOK, nil
}

func (h *Handler) handleListClassificationJobs(body []byte) (any, int, error) {
	var req struct {
		FilterCriteria map[string]any `json:"filterCriteria"`
		SortCriteria   map[string]any `json:"sortCriteria"`
		NextToken      string         `json:"nextToken"`
		MaxResults     int            `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	jobs, nextToken, err := h.Backend.ListClassificationJobs(req.FilterCriteria, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"items": jobs} //nolint:goconst // existing issue.
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleUpdateClassificationJob(jobID string, body []byte) (int, error) {
	var req struct {
		JobStatus string `json:"jobStatus"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateClassificationJob(jobID, req.JobStatus); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCreateMember(body []byte) (int, error) {
	var req struct {
		Account map[string]string `json:"account"`
		Tags    map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	accountID := req.Account["accountId"]
	email := req.Account["email"]

	if accountID == "" {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.CreateMember(accountID, email, req.Tags); err != nil {
		if errors.Is(err, awserr.ErrConflict) {
			return http.StatusConflict, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetMember(accountID string) (any, int, error) {
	m, err := h.Backend.GetMember(accountID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return m, http.StatusOK, nil
}

func (h *Handler) handleDeleteMember(accountID string) (int, error) {
	if err := h.Backend.DeleteMember(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListMembers() (any, int, error) {
	members, err := h.Backend.ListMembers(false)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"members": members}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateMember(accountID string) (int, error) {
	if err := h.Backend.DisassociateMember(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleUpdateMemberSession(accountID string, body []byte) (int, error) {
	var req struct {
		Status string `json:"status"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateMemberSession(accountID, req.Status); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleCreateInvitations(body []byte) (any, int, error) {
	var req struct {
		Message                  string   `json:"message"`
		AccountIDs               []string `json:"accountIds"`
		DisableEmailNotification bool     `json:"disableEmailNotification"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.CreateInvitations(req.AccountIDs, req.Message, req.DisableEmailNotification)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil //nolint:goconst // existing issue.
}

func (h *Handler) handleAcceptInvitation(body []byte) (int, error) {
	var req struct {
		AdministratorAccountID string `json:"administratorAccountId"`
		InvitationID           string `json:"invitationId"`
		MasterId               string `json:"masterId"` //nolint:revive,staticcheck // existing issue.
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	adminID := req.AdministratorAccountID
	if adminID == "" {
		adminID = req.MasterId
	}

	if err := h.Backend.AcceptInvitation(adminID, req.InvitationID); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDeclineInvitations(body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.DeclineInvitations(req.AccountIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil
}

func (h *Handler) handleDeleteInvitations(body []byte) (any, int, error) {
	var req struct {
		AccountIDs []string `json:"accountIds"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	unprocessed, err := h.Backend.DeleteInvitations(req.AccountIDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"unprocessedAccounts": unprocessed}, http.StatusOK, nil
}

func (h *Handler) handleGetInvitationsCount() (any, int, error) {
	count, err := h.Backend.GetInvitationsCount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]int64{"invitationsCount": count}, http.StatusOK, nil
}

func (h *Handler) handleListInvitations() (any, int, error) {
	invitations, err := h.Backend.ListInvitations()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"invitations": invitations}, http.StatusOK, nil
}

func (h *Handler) handleGetAdministratorAccount() (any, int, error) {
	acct, err := h.Backend.GetAdministratorAccount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"administrator": acct}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromAdministratorAccount() (int, error) {
	if err := h.Backend.DisassociateFromAdministratorAccount(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetMasterAccount() (any, int, error) {
	acct, err := h.Backend.GetMasterAccount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"master": acct}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromMasterAccount() (int, error) {
	if err := h.Backend.DisassociateFromMasterAccount(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleEnableOrganizationAdminAccount(body []byte) (int, error) {
	var req struct {
		AdminAccountID string `json:"adminAccountId"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.EnableOrganizationAdminAccount(req.AdminAccountID); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDisableOrganizationAdminAccount(accountID string) (int, error) {
	if err := h.Backend.DisableOrganizationAdminAccount(accountID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListOrganizationAdminAccounts() (any, int, error) {
	accounts, err := h.Backend.ListOrganizationAdminAccounts()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"adminAccounts": accounts}, http.StatusOK, nil
}

func (h *Handler) handleDescribeOrganizationConfiguration() (any, int, error) {
	cfg, err := h.Backend.DescribeOrganizationConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handleUpdateOrganizationConfiguration(body []byte) (int, error) {
	var req struct {
		AutoEnable bool `json:"autoEnable"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateOrganizationConfiguration(req.AutoEnable); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetAutomatedDiscoveryConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetAutomatedDiscoveryConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handleUpdateAutomatedDiscoveryConfiguration(body []byte) (int, error) {
	var req struct {
		AutoEnableOrganizationMembers string `json:"autoEnableOrganizationMembers"`
		Status                        string `json:"status"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateAutomatedDiscoveryConfiguration(
		req.AutoEnableOrganizationMembers, req.Status,
	); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListAutomatedDiscoveryAccounts() (any, int, error) {
	accounts, err := h.Backend.ListAutomatedDiscoveryAccounts()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"items": accounts}, http.StatusOK, nil
}

func (h *Handler) handleBatchUpdateAutomatedDiscoveryAccounts(body []byte) (int, error) {
	var req struct {
		Accounts []AutoDiscoveryAccountUpdate `json:"accounts"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.BatchUpdateAutomatedDiscoveryAccounts(req.Accounts); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleDescribeBuckets(body []byte) (any, int, error) {
	var req struct {
		Criteria map[string]any `json:"criteria"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	buckets, err := h.Backend.DescribeBuckets(req.Criteria)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"buckets": buckets}, http.StatusOK, nil
}

func (h *Handler) handleGetBucketStatistics(body []byte) (any, int, error) {
	var req struct {
		AccountID string `json:"accountId"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	stats, err := h.Backend.GetBucketStatistics(req.AccountID)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return stats, http.StatusOK, nil
}

func (h *Handler) handleBatchGetCustomDataIdentifiers(body []byte) (any, int, error) {
	var req struct {
		IDs []string `json:"ids"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, http.StatusBadRequest, ErrValidation
	}

	items, err := h.Backend.BatchGetCustomDataIdentifiers(req.IDs)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	found := make(map[string]bool, len(items))
	for _, item := range items {
		found[item.ID] = true
	}

	notFound := make([]string, 0)

	for _, id := range req.IDs {
		if !found[id] {
			notFound = append(notFound, id)
		}
	}

	resp := map[string]any{"customDataIdentifiers": items}
	if len(notFound) > 0 {
		resp["notFoundIdentifierIds"] = notFound
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleGetClassificationExportConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetClassificationExportConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"configuration": cfg}, http.StatusOK, nil
}

func (h *Handler) handlePutClassificationExportConfiguration(body []byte) (int, error) {
	var req struct {
		Configuration *ClassificationExportConfig `json:"configuration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.PutClassificationExportConfiguration(req.Configuration); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetClassificationScope(scopeID string) (any, int, error) {
	scope, err := h.Backend.GetClassificationScope(scopeID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return scope, http.StatusOK, nil
}

func (h *Handler) handleListClassificationScopes() (any, int, error) {
	scopes, err := h.Backend.ListClassificationScopes()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"classificationScopes": scopes}, http.StatusOK, nil
}

func (h *Handler) handleUpdateClassificationScope(scopeID string, body []byte) (int, error) {
	var req struct {
		S3 *ClassificationScopeS3 `json:"s3"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateClassificationScope(scopeID, req.S3); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetFindingsPublicationConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetFindingsPublicationConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return cfg, http.StatusOK, nil
}

func (h *Handler) handlePutFindingsPublicationConfiguration(body []byte) (int, error) {
	var cfg FindingsPublicationConfig

	if err := json.Unmarshal(body, &cfg); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.PutFindingsPublicationConfiguration(&cfg); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetResourceProfile(resourceARN string) (any, int, error) {
	profile, err := h.Backend.GetResourceProfile(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return profile, http.StatusOK, nil
}

func (h *Handler) handleUpdateResourceProfile(resourceARN string, body []byte) (int, error) {
	var req struct {
		SensitivityScoreOverride int32 `json:"sensitivityScoreOverride"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateResourceProfile(resourceARN, req.SensitivityScoreOverride); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListResourceProfileArtifacts(resourceARN string) (any, int, error) {
	artifacts, err := h.Backend.ListResourceProfileArtifacts(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"artifacts": artifacts}, http.StatusOK, nil
}

func (h *Handler) handleListResourceProfileDetections(resourceARN string) (any, int, error) {
	detections, err := h.Backend.ListResourceProfileDetections(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"detections": detections}, http.StatusOK, nil
}

func (h *Handler) handleUpdateResourceProfileDetections(resourceARN string, body []byte) (int, error) {
	var req struct {
		SuppressDataIdentifiers []map[string]any `json:"suppressDataIdentifiers"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateResourceProfileDetections(resourceARN, req.SuppressDataIdentifiers); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetRevealConfiguration() (any, int, error) {
	cfg, err := h.Backend.GetRevealConfiguration()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"configuration": cfg}, http.StatusOK, nil
}

func (h *Handler) handleUpdateRevealConfiguration(body []byte) (int, error) {
	var req struct {
		Configuration *RevealConfiguration `json:"configuration"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if req.Configuration == nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateRevealConfiguration(req.Configuration.KmsKeyID, req.Configuration.Status); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetSensitiveDataOccurrences(findingID string) (any, int, error) {
	result, err := h.Backend.GetSensitiveDataOccurrences(findingID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return result, http.StatusOK, nil
}

func (h *Handler) handleGetSensitiveDataOccurrencesAvailability(findingID string) (any, int, error) {
	status, reasons, err := h.Backend.GetSensitiveDataOccurrencesAvailability(findingID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"code": status}
	if len(reasons) > 0 {
		resp["reasons"] = reasons
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleGetSensitivityInspectionTemplate(templateID string) (any, int, error) {
	tmpl, err := h.Backend.GetSensitivityInspectionTemplate(templateID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return tmpl, http.StatusOK, nil
}

func (h *Handler) handleListSensitivityInspectionTemplates() (any, int, error) {
	templates, err := h.Backend.ListSensitivityInspectionTemplates()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"sensitivityInspectionTemplates": templates}, http.StatusOK, nil
}

func (h *Handler) handleUpdateSensitivityInspectionTemplate(templateID string, body []byte) (int, error) {
	var req struct {
		Excludes    map[string]any `json:"excludes"`
		Includes    map[string]any `json:"includes"`
		Description string         `json:"description"`
		Name        string         `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateSensitivityInspectionTemplate(
		templateID, req.Name, req.Description, req.Excludes, req.Includes,
	); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetUsageStatistics(body []byte) (any, int, error) {
	var req struct {
		SortBy     map[string]any   `json:"sortBy"`
		NextToken  string           `json:"nextToken"`
		FilterBy   []map[string]any `json:"filterBy"`
		MaxResults int              `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	records, nextToken, err := h.Backend.GetUsageStatistics(req.FilterBy, req.MaxResults, req.NextToken, "")
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"records": records}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

func (h *Handler) handleGetUsageTotals(timeRange string) (any, int, error) {
	totals, err := h.Backend.GetUsageTotals(timeRange)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"usageTotals": totals}, http.StatusOK, nil
}

func (h *Handler) handleListManagedDataIdentifiers() (any, int, error) {
	items, err := h.Backend.ListManagedDataIdentifiers()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"items": items}, http.StatusOK, nil
}

func (h *Handler) handleSearchResources(body []byte) (any, int, error) {
	var req struct {
		BucketCriteria map[string]any `json:"bucketCriteria"`
		SortCriteria   map[string]any `json:"sortCriteria"`
		NextToken      string         `json:"nextToken"`
		MaxResults     int            `json:"maxResults"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, http.StatusBadRequest, ErrValidation
		}
	}

	results, nextToken, err := h.Backend.SearchResources(req.BucketCriteria, req.MaxResults, req.NextToken)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	resp := map[string]any{"matchingResources": results}
	if nextToken != "" {
		resp["nextToken"] = nextToken
	}

	return resp, http.StatusOK, nil
}

// --- path helpers ---

func extractDisassociateMemberID(path string) string {
	// /members/disassociate/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathMembers+"/disassociate/")

	return trimmed
}

func extractMacieMemberID(path string) string {
	// /macie/members/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathMacie+"/"+pathMembers+"/")

	return trimmed
}

func extractTemplateID(path string) string {
	// /templates/sensitivity-inspections/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathTemplates+"/sensitivity-inspections/")

	return trimmed
}

func extractQueryParam(query, key string) string {
	for part := range strings.SplitSeq(query, "&") {
		if v, ok := strings.CutPrefix(part, key+"="); ok {
			return v
		}
	}

	return ""
}
