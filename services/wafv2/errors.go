package wafv2

import "github.com/blackbirdworks/gopherstack/pkgs/awserr"

var (
	// ErrWebACLNotFound is returned when a WebACL does not exist.
	ErrWebACLNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrWebACLAlreadyExists is returned when a WebACL with the same name already exists.
	ErrWebACLAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrIPSetNotFound is returned when an IPSet does not exist.
	ErrIPSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrIPSetAlreadyExists is returned when an IPSet with the same name already exists.
	ErrIPSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrAssociationNotFound is returned when a WebACL association does not exist.
	ErrAssociationNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetNotFound is returned when a RegexPatternSet does not exist.
	ErrRegexPatternSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRegexPatternSetAlreadyExists is returned when a RegexPatternSet with the same name already exists.
	ErrRegexPatternSetAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrRuleGroupNotFound is returned when a RuleGroup does not exist.
	ErrRuleGroupNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrRuleGroupAlreadyExists is returned when a RuleGroup with the same name already exists.
	ErrRuleGroupAlreadyExists = awserr.New("WAFDuplicateItemException", awserr.ErrConflict)
	// ErrAPIKeyNotFound is returned when an API key does not exist.
	ErrAPIKeyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrLoggingConfigNotFound is returned when a logging configuration does not exist.
	ErrLoggingConfigNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrPermissionPolicyNotFound is returned when a permission policy does not exist.
	ErrPermissionPolicyNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrOptimisticLock is returned when the LockToken does not match.
	ErrOptimisticLock = awserr.New("WAFOptimisticLockException", awserr.ErrConflict)
	// ErrAssociatedItem is returned when a resource is referenced by another resource.
	ErrAssociatedItem = awserr.New("WAFAssociatedItemException", awserr.ErrConflict)
	// ErrLimitsExceeded is returned when a resource limit is exceeded.
	ErrLimitsExceeded = awserr.New("WAFLimitsExceededException", awserr.ErrConflict)
	// ErrInvalidOperation is returned when an operation is invalid.
	ErrInvalidOperation = awserr.New("WAFInvalidOperationException", awserr.ErrInvalidParameter)
	// ErrUnavailableEntity is returned when a resource is temporarily unavailable.
	ErrUnavailableEntity = awserr.New("WAFUnavailableEntityException", awserr.ErrConflict)
	// ErrTagOperation is returned when a tag operation fails validation.
	ErrTagOperation = awserr.New("WAFTagOperationException", awserr.ErrInvalidParameter)
	// ErrConfigurationWarning is returned when there is a configuration warning.
	ErrConfigurationWarning = awserr.New("WAFConfigurationWarningException", awserr.ErrInvalidParameter)
	// ErrManagedRuleSetNotFound is returned when a managed rule set does not exist.
	ErrManagedRuleSetNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrMobileSdkReleaseNotFound is returned when a mobile SDK release is not in the catalog.
	ErrMobileSdkReleaseNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
	// ErrManagedRuleGroupNotFound is returned when a managed rule group is not in the catalog.
	ErrManagedRuleGroupNotFound = awserr.New("WAFNonexistentItemException", awserr.ErrNotFound)
)
