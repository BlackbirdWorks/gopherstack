package organizations

import (
	"errors"
	"maps"
)

type errorDetail struct {
	message string
	reason  string
}

const msgInvalidPattern = "You provided a value that does not match the required pattern."

func errorDetails() map[error]errorDetail {
	out := lookupErrorDetails()
	maps.Copy(out, constraintErrorDetails())

	return out
}

func lookupErrorDetails() map[error]errorDetail {
	return map[error]errorDetail{
		ErrOrgNotFound:      {message: "Your account is not a member of an organization."},
		ErrOrgAlreadyExists: {message: "This account is already a member of an organization."},
		ErrAccountNotFound:  {message: "You specified an account that doesn't exist."},
		ErrParentNotFound: {
			message: "We can't find a root or organizational unit (OU) with the ParentId that you specified.",
		},
		ErrOUNotFound:     {message: "You specified an organizational unit that doesn't exist."},
		ErrPolicyNotFound: {message: "You specified a policy that doesn't exist."},
		ErrPolicyTypeAlreadyEnabled: {
			message: "The specified policy type is already enabled in the specified root.",
		},
		ErrPolicyTypeNotEnabled: {
			message: "The specified policy type is not currently enabled in this root. " +
				"You cannot attach policies of the specified type to entities in a root " +
				"until you enable that type in the root.",
		},
		ErrCreateAccountStatusNotFound: {
			message: "We can't find a create account request with the CreateAccountRequestId that you specified.",
		},
		ErrDuplicatePolicyAttachment: {
			message: "The selected policy is already attached to the specified target.",
		},
		ErrPolicyNotAttached: {message: "The policy is not attached to the specified target."},
		ErrInvalidInput:      {message: msgInvalidPattern},
		ErrChildNotFound: {
			message: "We can't find an organizational unit (OU) or Amazon Web Services account " +
				"with the ChildId that you specified.",
		},
		ErrDelegatedAdminNotFound: {
			message: "You specified an account that is not a delegated administrator for this service.",
		},
		ErrDelegatedAdminAlreadyExists: {
			message: "The provided account is already a delegated administrator for your organization.",
		},
		ErrPolicyLimitExceeded: {
			message: "You've reached the limit on the number of policies of this type in your organization.",
			reason:  "POLICY_NUMBER_LIMIT_EXCEEDED",
		},
		ErrHandshakeNotFound: {
			message: "We can't find a handshake with the HandshakeId that you specified.",
		},
		ErrHandshakeConstraintViolation: {
			message: "The specified handshake is already in the requested state. " +
				"For example, you can't accept a handshake that was already accepted.",
		},
	}
}

func constraintErrorDetails() map[error]errorDetail {
	return map[error]errorDetail{
		ErrResourcePolicyNotFound: {message: "You specified a resource policy that doesn't exist."},
		ErrEffectivePolicyNotFound: {
			message: "A policy of the specified type does not exist for the specified target.",
		},
		ErrAccountAlreadyClosed: {message: "The specified account is already closed."},
		ErrOUDepthLimitExceeded: {
			message: "You've reached the maximum depth of nested organizational units.",
			reason:  "OU_DEPTH_LIMIT_EXCEEDED",
		},
		ErrDuplicateOrganizationalUnit: {
			message: "An organizational unit (OU) with the specified name already exists in the parent.",
		},
		ErrTargetNotFound: {
			message: "We can't find a root, OU, account, or policy with the TargetId that you specified.",
		},
		ErrServiceNotEnabled: {
			message: "The specified service does not have trusted access enabled for the organization.",
			reason:  "SERVICE_ACCESS_NOT_ENABLED",
		},
		ErrPolicyInUse: {
			message: "The policy is attached to one or more entities. " +
				"You must detach it from all roots, OUs, and accounts before performing this operation.",
		},
		ErrOrganizationNotEmpty: {
			message: "To delete an organization you must first remove all member accounts " +
				"(except the management account).",
		},
		ErrDuplicateHandshake: {
			message: "A handshake for the specified account is already in progress.",
		},
		ErrPolicyTypeAttached: {
			message: "You can't disable a policy type while policies of that type are attached to targets.",
			reason:  "MIN_POLICY_TYPE_ATTACHMENT_LIMIT_EXCEEDED",
		},
		ErrMalformedPolicyDocument: {
			message: "The provided policy document does not meet the requirements of the specified policy type.",
		},
		ErrPolicyContentLimitExceeded: {
			message: "The policy content exceeds the maximum size for its policy type.",
			reason:  "POLICY_CONTENT_LIMIT_EXCEEDED",
		},
		ErrTagLimitExceeded: {
			message: "You can have at most 50 tags on a resource.",
			reason:  "MAX_TAG_LIMIT_EXCEEDED",
		},
		ErrInvalidSystemTags: {
			message: "Tag keys with the aws: prefix are reserved for system tags.",
			reason:  "INVALID_SYSTEM_TAGS_PARAMETER",
		},
		ErrDuplicateTagKey: {
			message: "The same tag key was specified more than once.",
			reason:  "DUPLICATE_TAG_KEY",
		},
		ErrInvalidTagKeyLength:   {message: msgInvalidPattern},
		ErrInvalidTagValueLength: {message: msgInvalidPattern},
		ErrResponsibilityTransferNotFound: {
			message: "We can't find a responsibility transfer with the Id that you specified.",
		},
		ErrInvalidResponsibilityTransferTransition: {
			message: "The responsibility transfer is not in a state that can be terminated.",
		},
		ErrResponsibilityTransferAlreadyInStatus: {
			message: "The responsibility transfer has already ended.",
		},
		ErrOrganizationalUnitNotEmpty: {
			message: "You specified an organizational unit that contains other organizational units or accounts. " +
				"Remove them first.",
		},
		ErrMasterCannotLeaveOrganization: {
			message: "You can't remove the management account from the organization.",
		},
		ErrSourceParentNotFound: {
			message: "We can't find a source root or OU with the ParentId that you specified.",
		},
		ErrDestinationParentNotFound: {
			message: "We can't find a destination root or OU with the ParentId that you specified.",
		},
		ErrCannotRemoveDelegatedAdministratorFromOrg: {
			message: "You can't remove an account that is a delegated administrator for a service.",
			reason:  "CANNOT_REMOVE_DELEGATED_ADMINISTRATOR_FROM_ORG",
		},
		ErrAccessDeniedManagedPolicy: {
			message: "You can't modify or delete an Amazon Web Services managed policy.",
		},
		ErrCannotCloseManagementAccount: {
			message: "You can't close the management account of an organization.",
			reason:  "CANNOT_CLOSE_MANAGEMENT_ACCOUNT",
		},
	}
}

func detailFor(err error) errorDetail {
	for sentinel, d := range errorDetails() {
		if errors.Is(err, sentinel) {
			return d
		}
	}

	return errorDetail{}
}
