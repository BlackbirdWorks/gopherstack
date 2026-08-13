package iam

import (
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

func (h *Handler) iamReportingDispatchTable() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetAccountAuthorizationDetails": func(vals url.Values, reqID string) (any, error) {
			maxItems, _ := strconv.Atoi(vals.Get("MaxItems"))
			filter := parseIndexedValues(vals, "Filter.member.")

			details, nextMarker := h.Backend.GetAccountAuthorizationDetails(
				vals.Get("Marker"), maxItems, filter,
			)

			users := make([]UserDetailXML, 0, len(details.Users))
			for _, u := range details.Users {
				users = append(users, toUserDetailXML(u))
			}

			groups := make([]GroupDetailXML, 0, len(details.Groups))
			for _, g := range details.Groups {
				groups = append(groups, toGroupDetailXML(g))
			}

			roles := make([]RoleDetailXML, 0, len(details.Roles))
			for _, r := range details.Roles {
				roles = append(roles, h.toRoleDetailXML(r))
			}

			policies := make([]ManagedPolicyDetailXML, 0, len(details.Policies))
			for i := range details.Policies {
				pol := &details.Policies[i]

				versions, vErr := h.Backend.ListPolicyVersions(pol.Arn)
				if vErr != nil {
					// Should not happen (the policy was just enumerated), but degrade
					// gracefully to a single synthesized default version rather than
					// dropping the policy from the response entirely.
					versions = []StoredPolicyVersion{{
						VersionID:        "v1",
						PolicyDocument:   pol.PolicyDocument,
						IsDefaultVersion: true,
						CreateDate:       pol.CreateDate,
					}}
				}

				policies = append(policies, toManagedPolicyDetailXML(pol, versions))
			}

			return &GetAccountAuthorizationDetailsResponse{
				Xmlns: iamXMLNS,
				GetAccountAuthorizationDetailsResult: GetAccountAuthorizationDetailsResult{
					UserDetailList:  users,
					GroupDetailList: groups,
					RoleDetailList:  roles,
					Policies:        policies,
					Marker:          nextMarker,
					IsTruncated:     nextMarker != "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"SimulatePrincipalPolicy": func(vals url.Values, reqID string) (any, error) {
			actionNames := parseIndexedValues(vals, "ActionNames.member.")
			resourceArns := parseIndexedValues(vals, "ResourceArns.member.")
			resourcePolicyList := parseIndexedValues(vals, "ResourcePolicyList.member.")

			results, err := h.Backend.SimulatePrincipalPolicy(
				vals.Get("PolicySourceArn"), vals.Get("CallerArn"), vals.Get("ResourceOwner"),
				resourcePolicyList, actionNames, resourceArns,
				parseConditionContext(vals),
			)
			if err != nil {
				return nil, err
			}

			return &SimulatePrincipalPolicyResponse{
				Xmlns: iamXMLNS,
				SimulatePrincipalPolicyResult: SimulatePrincipalPolicyResult{
					EvaluationResults: simResultsToXML(results),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GenerateCredentialReport": func(_ url.Values, reqID string) (any, error) {
			return &GenerateCredentialReportResponse{
				Xmlns: iamXMLNS,
				GenerateCredentialReportResult: GenerateCredentialReportResult{
					State: "COMPLETE",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetCredentialReport": func(_ url.Values, reqID string) (any, error) {
			csv := h.Backend.GetCredentialReport()
			encoded := base64.StdEncoding.EncodeToString([]byte(csv))

			return &GetCredentialReportResponse{
				Xmlns: iamXMLNS,
				GetCredentialReportResult: GetCredentialReportResult{
					Content:       encoded,
					ReportFormat:  "text/csv",
					GeneratedTime: time.Now().UTC().Format("2006-01-02T15:04:05Z"),
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func toInlinePolicyEntriesXML(entries []InlinePolicyEntry) []InlinePolicyEntryXML {
	result := make([]InlinePolicyEntryXML, 0, len(entries))

	for _, e := range entries {
		result = append(result, InlinePolicyEntryXML{
			PolicyName:     e.PolicyName,
			PolicyDocument: encodePolicyDocument(e.PolicyDocument),
		})
	}

	return result
}

func toAttachedPoliciesXML(policies []AttachedPolicy) []AttachedPolicyXML {
	result := make([]AttachedPolicyXML, 0, len(policies))

	for _, p := range policies {
		result = append(result, AttachedPolicyXML(p))
	}

	return result
}

func toUserDetailXML(u UserDetail) UserDetailXML {
	groupList := u.GroupNames
	if groupList == nil {
		groupList = []string{}
	}

	return UserDetailXML{
		Path:                    u.Path,
		UserName:                u.UserName,
		UserID:                  u.UserID,
		Arn:                     u.Arn,
		CreateDate:              isoTime(u.CreateDate),
		UserPolicyList:          toInlinePolicyEntriesXML(u.InlinePolicies),
		AttachedManagedPolicies: toAttachedPoliciesXML(u.AttachedPolicies),
		GroupList:               groupList,
	}
}

// formValueTrue is the string "true" as submitted via HTML form values.
const formValueTrue = "true"

// summaryStateNotSupported is GetHumanReadableSummary's SummaryState value
// for entities gopherstack does not generate LLM summaries for (see PARITY.md).
const summaryStateNotSupported = "NOT_SUPPORTED"

// iamNewOpsAccountActions returns dispatch entries for account-level new operations.
func (h *Handler) iamNewOpsAccountActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateAccountAlias": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.CreateAccountAlias(vals.Get("AccountAlias")); err != nil {
				return nil, err
			}

			return &CreateAccountAliasResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"ChangePassword": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.ChangePassword(vals.Get("OldPassword"), vals.Get("NewPassword")); err != nil {
				return nil, err
			}

			return &ChangePasswordResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamNewOpsDelegationAndOIDCActions returns dispatch entries for delegation and OIDC new operations.
func (h *Handler) iamNewOpsDelegationAndOIDCActions() map[string]iamActionFn {
	return map[string]iamActionFn{
		"CreateDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			description := vals.Get("Description")
			notificationChannel := vals.Get("NotificationChannel")
			requestorWorkflowID := vals.Get("RequestorWorkflowId")

			if description == "" {
				return nil, fmt.Errorf("%w: Description must not be empty", ErrInvalidInput)
			}

			if notificationChannel == "" {
				return nil, fmt.Errorf("%w: NotificationChannel must not be empty", ErrInvalidInput)
			}

			if requestorWorkflowID == "" {
				return nil, fmt.Errorf("%w: RequestorWorkflowId must not be empty", ErrInvalidInput)
			}

			sessionDurationRaw := vals.Get("SessionDuration")

			sessionDuration, convErr := strconv.ParseInt(sessionDurationRaw, 10, 32)
			if sessionDurationRaw == "" || convErr != nil {
				return nil, fmt.Errorf("%w: SessionDuration must be a valid integer", ErrInvalidInput)
			}

			policyTemplateArn := vals.Get("Permissions.PolicyTemplateArn")
			permissionParameters := parseDelegationPermissionParameters(vals)

			// Permissions is a required *struct* member at the SDK level (must
			// be non-nil), but every field within it is optional -- an
			// omitted-vs-empty-object distinction the query wire form cannot
			// express (there is no way to send "Permissions: {}"). The best a
			// server can do is require at least one Permissions.* key.
			if policyTemplateArn == "" && len(permissionParameters) == 0 {
				return nil, fmt.Errorf("%w: Permissions must not be empty", ErrInvalidInput)
			}

			req, err := h.Backend.CreateDelegationRequest(CreateDelegationRequestInput{
				Description:          description,
				NotificationChannel:  notificationChannel,
				RequestorWorkflowID:  requestorWorkflowID,
				SessionDuration:      int32(sessionDuration),
				OnlySendByOwner:      vals.Get("OnlySendByOwner") == formValueTrue,
				OwnerAccountID:       vals.Get("OwnerAccountId"),
				RedirectURL:          vals.Get("RedirectUrl"),
				RequestMessage:       vals.Get("RequestMessage"),
				PolicyTemplateArn:    policyTemplateArn,
				PermissionParameters: permissionParameters,
			})
			if err != nil {
				return nil, err
			}

			return &CreateDelegationRequestResponse{
				Xmlns: iamXMLNS,
				CreateDelegationRequestResult: CreateDelegationRequestResult{
					ConsoleDeepLink:     delegationRequestConsoleDeepLink(req.DelegationID),
					DelegationRequestID: req.DelegationID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AcceptDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AcceptDelegationRequest(vals.Get("DelegationRequestId")); err != nil {
				return nil, err
			}

			return &AcceptDelegationRequestResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AssociateDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AssociateDelegationRequest(
				vals.Get("DelegationRequestId"),
				vals.Get("PolicyArn"),
			); err != nil {
				return nil, err
			}

			return &AssociateDelegationRequestResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},

		"AddClientIDToOpenIDConnectProvider": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.AddClientIDToOpenIDConnectProvider(
				vals.Get("OpenIDConnectProviderArn"),
				vals.Get("ClientID"),
			); err != nil {
				return nil, err
			}

			return &AddClientIDToOpenIDConnectProviderResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// parseDelegationPermissionParameters parses
// Permissions.Parameters.member.N.{Name,Type,Values.member.M} form values.
func parseDelegationPermissionParameters(vals url.Values) []DelegationPolicyParameter {
	var params []DelegationPolicyParameter

	for i := 1; ; i++ {
		name := vals.Get(fmt.Sprintf("Permissions.Parameters.member.%d.Name", i))
		if name == "" {
			return params
		}

		params = append(params, DelegationPolicyParameter{
			Name:   name,
			Type:   vals.Get(fmt.Sprintf("Permissions.Parameters.member.%d.Type", i)),
			Values: parseIndexedValues(vals, fmt.Sprintf("Permissions.Parameters.member.%d.Values.member.", i)),
		})
	}
}

// iamAccountAliasRefinementDispatch adds ListAccountAliases and DeleteAccountAlias.
func (h *Handler) iamAccountAliasRefinementDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"ListAccountAliases": func(_ url.Values, reqID string) (any, error) {
			aliases := h.Backend.ListAccountAliases()

			return &ListAccountAliasesResponse{
				Xmlns: iamXMLNS,
				ListAccountAliasesResult: ListAccountAliasesResult{
					AccountAliases: aliases,
					IsTruncated:    false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccountAlias": func(vals url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccountAlias(vals.Get("AccountAlias")); err != nil {
				return nil, err
			}

			return &DeleteAccountAliasResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

// iamPasswordPolicyDispatch adds password policy handlers:
// GetAccountPasswordPolicy, UpdateAccountPasswordPolicy, and DeleteAccountPasswordPolicy.
func (h *Handler) iamPasswordPolicyDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetAccountPasswordPolicy": func(_ url.Values, reqID string) (any, error) {
			pp := h.Backend.GetAccountPasswordPolicy()

			return &GetAccountPasswordPolicyResponse{
				Xmlns: iamXMLNS,
				GetAccountPasswordPolicyResult: GetAccountPasswordPolicyResult{
					PasswordPolicy: PasswordPolicyXML{
						MinimumPasswordLength:      pp.MinimumPasswordLength,
						MaxPasswordAge:             pp.MaxPasswordAge,
						PasswordReusePrevention:    pp.PasswordReusePrevention,
						RequireUppercaseCharacters: pp.RequireUppercaseCharacters,
						RequireLowercaseCharacters: pp.RequireLowercaseCharacters,
						RequireNumbers:             pp.RequireNumbers,
						RequireSymbols:             pp.RequireSymbols,
						AllowUsersToChangePassword: pp.AllowUsersToChangePassword,
						HardExpiry:                 pp.HardExpiry,
						ExpirePasswords:            pp.MaxPasswordAge > 0,
					},
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateAccountPasswordPolicy": func(vals url.Values, reqID string) (any, error) {
			minLen, _ := strconv.Atoi(vals.Get("MinimumPasswordLength"))
			maxAge, _ := strconv.Atoi(vals.Get("MaxPasswordAge"))
			reusePrev, _ := strconv.Atoi(vals.Get("PasswordReusePrevention"))

			pp := PasswordPolicy{
				MinimumPasswordLength:      minLen,
				MaxPasswordAge:             maxAge,
				PasswordReusePrevention:    reusePrev,
				RequireUppercaseCharacters: vals.Get("RequireUppercaseCharacters") == formValueTrue,
				RequireLowercaseCharacters: vals.Get("RequireLowercaseCharacters") == formValueTrue,
				RequireNumbers:             vals.Get("RequireNumbers") == formValueTrue,
				RequireSymbols:             vals.Get("RequireSymbols") == formValueTrue,
				AllowUsersToChangePassword: vals.Get("AllowUsersToChangePassword") != "false",
				HardExpiry:                 vals.Get("HardExpiry") == formValueTrue,
			}
			if err := h.Backend.UpdateAccountPasswordPolicy(pp); err != nil {
				return nil, err
			}

			return &UpdateAccountPasswordPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DeleteAccountPasswordPolicy": func(_ url.Values, reqID string) (any, error) {
			if err := h.Backend.DeleteAccountPasswordPolicy(); err != nil {
				return nil, err
			}

			return &DeleteAccountPasswordPolicyResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamOrgsDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"DisableOrganizationsRootCredentialsManagement": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DisableOrganizationsRootCredentialsManagementResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DisableOrganizationsRootSessions": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "DisableOrganizationsRootSessionsResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"DisableOutboundWebIdentityFederation": func(_ url.Values, reqID string) (any, error) {
			h.Backend.DisableOutboundWebIdentityFederation()

			return &DisableOutboundWebIdentityFederationResponse{
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOrganizationsRootCredentialsManagement": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableOrganizationsRootCredentialsManagementResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOrganizationsRootSessions": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "EnableOrganizationsRootSessionsResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"EnableOutboundWebIdentityFederation": func(_ url.Values, reqID string) (any, error) {
			issuer := h.Backend.EnableOutboundWebIdentityFederation()

			return &EnableOutboundWebIdentityFederationResponse{
				Xmlns: iamXMLNS,
				Result: EnableOutboundWebIdentityFederationResult{
					IssuerIdentifier: issuer,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListOrganizationsFeatures": func(_ url.Values, reqID string) (any, error) {
			return &listOrganizationsFeaturesResponse{
				XMLName: xml.Name{Local: "ListOrganizationsFeaturesResponse"},
				Xmlns:   iamXMLNS,
				ListOrganizationsFeaturesResult: listOrganizationsFeaturesResult{
					OrganizationFeatures: []string{},
					RootID:               "",
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListPoliciesGrantingServiceAccess": func(vals url.Values, reqID string) (any, error) {
			// Validation-only: the mock has no policy/service access-analysis state
			// to populate PolicyGroups, and AWS-meaningful emulation is out of
			// scope. We perform AWS-accurate input validation of the required
			// Arn and ServiceNamespaces parameters and return an empty result.
			if vals.Get("Arn") == "" {
				return nil, fmt.Errorf("%w: Arn must not be empty", ErrValidationError)
			}

			if vals.Get("ServiceNamespaces.member.1") == "" {
				return nil, fmt.Errorf("%w: at least one ServiceNamespace is required", ErrValidationError)
			}

			return &listPoliciesGrantingServiceAccessResponse{
				XMLName: xml.Name{Local: "ListPoliciesGrantingServiceAccessResponse"},
				Xmlns:   iamXMLNS,
				ListPoliciesGrantingServiceAccessResult: listPGSAResult{
					PolicyGroups: []string{},
					IsTruncated:  false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamDelegationDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GetDelegationRequest": func(vals url.Values, reqID string) (any, error) {
			// Validation-only: the mock keeps no delegation-request state to
			// return, and AWS-meaningful emulation is out of scope. We perform
			// AWS-accurate input validation of the required DelegationRequestId
			// parameter and return an empty success response.
			if vals.Get("DelegationRequestId") == "" {
				return nil, fmt.Errorf("%w: DelegationRequestId must not be empty", ErrValidationError)
			}

			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "GetDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetHumanReadableSummary": func(vals url.Values, reqID string) (any, error) {
			entityArn := vals.Get("EntityArn")
			if entityArn == "" {
				return nil, fmt.Errorf("%w: EntityArn must not be empty", ErrInvalidInput)
			}

			delegationID, ok := delegationRequestIDFromArn(entityArn)
			if !ok || !h.Backend.DelegationRequestExists(delegationID) {
				return nil, fmt.Errorf("%w: %s", ErrDelegationRequestNotFound, entityArn)
			}

			// Real GetHumanReadableSummary uses an LLM to generate a
			// natural-language permissions summary for the entity.
			// gopherstack does not fabricate that content -- an invented
			// capability is worse than an absent one (see PARITY.md) -- so a
			// known entity honestly gets NOT_SUPPORTED rather than invented
			// prose or a fake AVAILABLE/IN_PROGRESS/FAILED state.
			return &GetHumanReadableSummaryResponse{
				Xmlns: iamXMLNS,
				GetHumanReadableSummaryResult: GetHumanReadableSummaryResult{
					Locale:       vals.Get("Locale"),
					SummaryState: summaryStateNotSupported,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetOutboundWebIdentityFederationInfo": func(_ url.Values, reqID string) (any, error) {
			issuer, enabled := h.Backend.GetOutboundWebIdentityFederationInfo()

			return &GetOutboundWebIdentityFederationInfoResponse{
				Xmlns: iamXMLNS,
				Result: GetOutboundWebIdentityFederationInfoResult{
					IssuerIdentifier:  issuer,
					JwtVendingEnabled: enabled,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"ListDelegationRequests": func(_ url.Values, reqID string) (any, error) {
			return &listDelegationRequestsResponse{
				XMLName: xml.Name{Local: "ListDelegationRequestsResponse"},
				Xmlns:   iamXMLNS,
				ListDelegationRequestsResult: listDelegationRequestsResult{
					DelegationRequests: []string{},
					IsTruncated:        false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"RejectDelegationRequest": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "RejectDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"SendDelegationToken": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "SendDelegationTokenResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"UpdateDelegationRequest": func(_ url.Values, reqID string) (any, error) {
			return &iamSimpleTagResponse{
				XMLName:          xml.Name{Local: "UpdateDelegationRequestResponse"},
				Xmlns:            iamXMLNS,
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}

func (h *Handler) iamOrgsReportDispatch() map[string]iamActionFn {
	return map[string]iamActionFn{
		"GenerateOrganizationsAccessReport": func(vals url.Values, reqID string) (any, error) {
			jobID := h.Backend.GenerateOrganizationsAccessReport(vals.Get("EntityPath"))

			return &generateOrganizationsAccessReportResponse{
				XMLName: xml.Name{Local: "GenerateOrganizationsAccessReportResponse"},
				Xmlns:   iamXMLNS,
				GenerateOrganizationsAccessReportResult: generateOrgAccessReportResult{
					JobID: jobID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GenerateServiceLastAccessedDetails": func(_ url.Values, reqID string) (any, error) {
			return &generateServiceLastAccessedDetailsResponse{
				XMLName: xml.Name{Local: "GenerateServiceLastAccessedDetailsResponse"},
				Xmlns:   iamXMLNS,
				GenerateServiceLastAccessedDetailsResult: generateSLADResult{
					JobID: "sladjob-" + reqID,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetOrganizationsAccessReport": func(vals url.Values, reqID string) (any, error) {
			jobID := vals.Get("JobId")
			status, createdAt, found := h.Backend.GetOrganizationsAccessReport(jobID)
			if !found {
				status = jobStatusCompleted
				createdAt = time.Now()
			}

			return &getOrganizationsAccessReportResponse{
				XMLName: xml.Name{Local: "GetOrganizationsAccessReportResponse"},
				Xmlns:   iamXMLNS,
				GetOrganizationsAccessReportResult: getOrgAccessReportResult{
					JobStatus:                   status,
					JobCreationDate:             isoTime(createdAt),
					AccessDetails:               []string{},
					IsTruncated:                 false,
					NumberOfServicesAccessible:  0,
					NumberOfServicesNotAccessed: 0,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
		"GetServiceLastAccessedDetailsWithEntities": func(_ url.Values, reqID string) (any, error) {
			return &getServiceLastAccessedDetailsWithEntitiesResponse{
				XMLName: xml.Name{Local: "GetServiceLastAccessedDetailsWithEntitiesResponse"},
				Xmlns:   iamXMLNS,
				GetServiceLastAccessedDetailsWithEntitiesResult: getSLADWithEntitiesResult{
					JobStatus:         jobStatusCompleted,
					JobCreationDate:   isoTime(time.Now()),
					EntityDetailsList: []string{},
					IsTruncated:       false,
				},
				ResponseMetadata: ResponseMetadata{RequestID: reqID},
			}, nil
		},
	}
}
