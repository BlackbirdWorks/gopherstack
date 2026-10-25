package ociidentity

import (
	"maps"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func buildOps() map[string]opFunc {
	ops := map[string]opFunc{}
	for _, m := range []map[string]opFunc{
		tenancyOps(), compartmentOps(), principalOps(), policyOps(), credentialOps(), tagOps(),
	} {
		maps.Copy(ops, m)
	}

	ops[opGetWorkRequest], ops[opListWorkRequests] = (*Handler).getWorkRequest, (*Handler).listWorkRequests

	return ops
}

func tenancyOps() map[string]opFunc {
	return map[string]opFunc{
		opGetTenancy: (*Handler).getTenancy, opListRegions: (*Handler).listRegions,
		opListRegionSubs: (*Handler).listRegionSubscriptions, opListADs: (*Handler).listAvailabilityDomains,
		opListFDs: (*Handler).listFaultDomains,
	}
}

func compartmentOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateCompartment: (*Handler).createCompartment, opGetCompartment: (*Handler).getCompartment,
		opListCompartments: (*Handler).listCompartments, opUpdateCompartment: (*Handler).updateCompartment,
		opDeleteCompartment: (*Handler).deleteCompartment, opMoveCompartment: (*Handler).moveCompartment,
		opRecoverCompartment: (*Handler).recoverCompartment,
	}
}

func principalOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateUser: (*Handler).createUser, opGetUser: (*Handler).getUser, opListUsers: (*Handler).listUsers,
		opUpdateUser: (*Handler).updateUser, opDeleteUser: (*Handler).deleteUser,
		opCreateGroup: (*Handler).createGroup, opGetGroup: (*Handler).getGroup, opListGroups: (*Handler).listGroups,
		opUpdateGroup: (*Handler).updateGroup, opDeleteGroup: (*Handler).deleteGroup,
		opAddUserToGroup: (*Handler).addUserToGroup, opGetMembership: (*Handler).getMembership,
		opListMemberships: (*Handler).listMemberships, opRemoveFromGroup: (*Handler).removeFromGroup,
		opCreateDynGroup: (*Handler).createDynamicGroup, opGetDynGroup: (*Handler).getDynamicGroup,
		opListDynGroups: (*Handler).listDynamicGroups, opUpdateDynGroup: (*Handler).updateDynamicGroup,
		opDeleteDynGroup: (*Handler).deleteDynamicGroup,
	}
}

func policyOps() map[string]opFunc {
	return map[string]opFunc{
		opCreatePolicy: (*Handler).createPolicy, opGetPolicy: (*Handler).getPolicy,
		opListPolicies: (*Handler).listPolicies, opUpdatePolicy: (*Handler).updatePolicy,
		opDeletePolicy: (*Handler).deletePolicy,
	}
}

func credentialOps() map[string]opFunc {
	return map[string]opFunc{
		opUploadAPIKey: (*Handler).uploadAPIKey, opListAPIKeys: (*Handler).listAPIKeys,
		opDeleteAPIKey:    (*Handler).deleteAPIKey,
		opCreateAuthToken: (*Handler).createAuthToken, opListAuthTokens: (*Handler).listAuthTokens,
		opUpdateAuthToken: (*Handler).updateAuthToken, opDeleteAuthToken: (*Handler).deleteAuthToken,
		opCreateSecretKey: (*Handler).createSecretKey, opListSecretKeys: (*Handler).listSecretKeys,
		opUpdateSecretKey: (*Handler).updateSecretKey, opDeleteSecretKey: (*Handler).deleteSecretKey,
		opCreateSMTP: (*Handler).createSMTP, opListSMTP: (*Handler).listSMTP,
		opUpdateSMTP: (*Handler).updateSMTP, opDeleteSMTP: (*Handler).deleteSMTP,
		opResetUIPassword: (*Handler).resetUIPassword, opGetUIPassword: (*Handler).getUIPassword,
	}
}

func tagOps() map[string]opFunc {
	return map[string]opFunc{
		opCreateTagNS: (*Handler).createTagNamespace, opGetTagNS: (*Handler).getTagNamespace,
		opListTagNS: (*Handler).listTagNamespaces, opUpdateTagNS: (*Handler).updateTagNamespace,
		opDeleteTagNS: (*Handler).deleteTagNamespace,
		opCreateTag:   (*Handler).createTag, opGetTag: (*Handler).getTag, opListTags: (*Handler).listTags,
		opUpdateTag: (*Handler).updateTag, opDeleteTag: (*Handler).deleteTag,
		opCreateTagDefault: (*Handler).createTagDefault, opGetTagDefault: (*Handler).getTagDefault,
		opListTagDefaults: (*Handler).listTagDefaults, opUpdateTagDefault: (*Handler).updateTagDefault,
		opDeleteTagDefault: (*Handler).deleteTagDefault,
	}
}

func (q *req) listOpts() ListOpts {
	n, _ := strconv.Atoi(q.query("limit"))

	return ListOpts{SortBy: q.query("sortBy"), Order: q.query("sortOrder"), Page: q.query("page"), Limit: n}
}

func reply[T any](q *req, w T, etag string, err error) error {
	if err != nil {
		return err
	}

	return q.json(http.StatusOK, w, etag)
}

func listReply[T any](q *req, pg page.Page[T], err error) error {
	if err != nil {
		return err
	}

	q.nextPage(pg.Next)

	return q.json(http.StatusOK, pg.Data, "")
}

func done(q *req, err error) error {
	if err != nil {
		return err
	}

	return q.noContent("")
}

// accepted answers 202 with the work request id header.
func accepted(q *req, workRequestID string, err error) error {
	if err != nil {
		return err
	}

	q.header("opc-work-request-id", workRequestID)
	q.w.WriteHeader(http.StatusAccepted)

	return nil
}
