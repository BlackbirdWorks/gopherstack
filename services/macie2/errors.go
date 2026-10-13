package macie2

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrNotEnabled is returned when Macie is not enabled.
	ErrNotEnabled = awserr.New("Macie is not enabled for this account", awserr.ErrNotFound)
	// ErrAllowListNotFound is returned when an allow list does not exist.
	ErrAllowListNotFound = notFound("allow list")
	// ErrSessionAlreadyExists is returned when Macie is already enabled.
	ErrSessionAlreadyExists = awserr.New("Macie is already enabled for this account", awserr.ErrConflict)
	// ErrAllowListAlreadyExists is returned when an allow list already exists.
	ErrAllowListAlreadyExists = conflict("an allow list with this name already exists")
	// ErrAllowListInUse is returned when a DeleteAllowList request targets an
	// allow list that a non-terminal classification job still references,
	// and the request didn't set IgnoreJobChecks.
	ErrAllowListInUse = conflict("the allow list is used by an active classification job")
	// ErrCustomDataIDNotFound is returned when a custom data identifier does not exist.
	ErrCustomDataIDNotFound = notFound("custom data identifier")
	// ErrFindingsFilterNotFound is returned when a findings filter does not exist.
	ErrFindingsFilterNotFound = notFound("findings filter")
	// ErrFindingNotFound is returned when a finding does not exist.
	ErrFindingNotFound = notFound("finding")
	// ErrTaggedResourceNotFound is returned when a tag operation targets an unknown resource ARN.
	ErrTaggedResourceNotFound = notFound("resource")
	// ErrValidation is returned on invalid input.
	ErrValidation = awserr.New("The request contains invalid input", awserr.ErrInvalidParameter)
	// ErrRevealNotClassification is returned when GetSensitiveDataOccurrences
	// targets a finding whose category isn't CLASSIFICATION.
	ErrRevealNotClassification = awserr.New("The finding does not support revealing data", awserr.ErrInvalidParameter)
	// ErrRevealNotEnabled is returned when GetSensitiveDataOccurrences is
	// called while Macie or the reveal configuration isn't enabled.
	ErrRevealNotEnabled = awserr.New("Macie or reveal is not enabled", awserr.ErrInvalidParameter)
)

var (
	// ErrClassificationJobNotFound is returned when a classification job does not exist.
	ErrClassificationJobNotFound = notFound("classification job")
	// ErrMemberNotFound is returned when a member does not exist.
	ErrMemberNotFound = notFound("member")
	// ErrMemberAlreadyExists is returned when a member already exists.
	ErrMemberAlreadyExists = conflict("the member account already exists")
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = notFound("invitation")
	// ErrOrgAdminNotFound is returned when an org admin account does not exist.
	ErrOrgAdminNotFound = notFound("organization admin account")
	// ErrClassificationScopeNotFound is returned when a classification scope does not exist.
	ErrClassificationScopeNotFound = notFound("classification scope")
	// ErrSensitivityTemplateNotFound is returned when a sensitivity inspection template does not exist.
	ErrSensitivityTemplateNotFound = notFound("sensitivity inspection template")
	// ErrJobStatusTransition is returned when UpdateClassificationJob's requested
	// JobStatus isn't valid for the job's current status (api_op_UpdateClassificationJob.go).
	ErrJobStatusTransition = conflict("the requested status is not valid for the job's current status")
)

func notFound(what string) error {
	return awserr.New("The request failed because the specified "+what+" wasn't found", awserr.ErrNotFound)
}

func conflict(why string) error {
	return awserr.New("The request failed because "+why, awserr.ErrConflict)
}
