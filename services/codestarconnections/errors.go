package codestarconnections

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Validation limits. ConnectionName/HostName max lengths and TagKey/TagValue/
// TagList/Url limits are taken verbatim from botocore's codestar-connections/
// 2019-12-01/service-2.json shapes (ConnectionName: max 32; HostName: max 64;
// TagKey: max 128; TagValue: max 256; TagList: max 200; Url: max 512). Note
// ConnectionName and HostName have DIFFERENT max lengths in the real API --
// a previous implementation incorrectly applied the 32-char ConnectionName
// limit to host names too. Also note botocore's ConnectionName/HostName
// patterns are `[\s\S]*` / `.*` respectively -- i.e. no character-class
// restriction at all (any string up to the length limit is valid); a
// previous implementation invented a `[a-zA-Z0-9_.\-]+` restriction that
// does not exist in the real service and would incorrectly reject valid
// names containing spaces or other punctuation.
const (
	maxConnectionNameLen   = 32
	maxHostNameLen         = 64
	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	maxTagsPerResource     = 200
	maxProviderEndpointLen = 512
)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrResourceAlreadyExists is returned when a repository link or sync
	// configuration with the same identity already exists. CreateConnection
	// and CreateHost have no equivalent: neither's own error switch
	// (codestarconnections@v1.38.4 deserializers.go) has any code for a
	// name collision, so a duplicate name is not rejected there -- only
	// CreateRepositoryLink/CreateSyncConfiguration register a dedicated
	// ResourceAlreadyExistsException.
	ErrResourceAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails. The real
	// aws-sdk-go-v2/service/codestarconnections@v1.38.4 types/errors.go has
	// no ValidationException type at all (its modeled exception set is
	// AccessDeniedException/ConcurrentModificationException/
	// ConditionalCheckFailedException/ConflictException/
	// InternalServerException/InvalidInputException/LimitExceededException/
	// ResourceAlreadyExistsException/ResourceNotFoundException/
	// ResourceUnavailableException/RetryLatestCommitFailedException/
	// SyncBlockerDoesNotExistException/SyncConfigurationStillExistsException/
	// ThrottlingException/UnsupportedOperationException/
	// UnsupportedProviderTypeException/UpdateOutOfSyncException --
	// InvalidInputException is the real type for malformed/missing-required-
	// field input, confirmed against every mutating op's error list in
	// botocore's codestar-connections/2019-12-01/service-2.json (e.g.
	// CreateRepositoryLink, CreateSyncConfiguration, GetResourceSyncStatus all
	// list InvalidInputException, none list ValidationException). A previous
	// implementation invented a "ValidationException" type here that does not
	// exist in the real service.
	ErrValidation = awserr.New("InvalidInputException", awserr.ErrInvalidParameter)
	// ErrTagLimitExceeded is returned when a create/tag operation would push a
	// resource's tag count over maxTagsPerResource. Real CreateConnection's
	// complete error list is [LimitExceededException, ResourceNotFoundException,
	// ResourceUnavailableException] and CreateHost's is [LimitExceededException]
	// only (botocore codestar-connections/2019-12-01/service-2.json) -- neither
	// documents InvalidInputException at all -- and TagResource's is
	// [ResourceNotFoundException, LimitExceededException]. LimitExceededException
	// is therefore the correct real type for "too many tags" everywhere, not
	// the generic ErrValidation/InvalidInputException a previous implementation
	// used.
	ErrTagLimitExceeded = awserr.New("LimitExceededException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a host cannot be deleted because a
	// connection still references it ("Before you delete a host, all
	// connections associated to the host must be deleted."). A previous
	// implementation used ConflictException here on the theory that "no
	// dedicated typed error is documented for this case" -- but DeleteHost's
	// real, complete error list (botocore codestar-connections/2019-12-01/
	// service-2.json) is exactly [ResourceNotFoundException,
	// ResourceUnavailableException]; ConflictException is not a possible
	// error for this operation at all (it belongs to UpdateHost's error list
	// instead). ResourceUnavailableException is the correct real type.
	ErrResourceInUse = awserr.New("ResourceUnavailableException", awserr.ErrConflict)
	// ErrSyncConfigStillExists is returned when a repository link cannot be
	// deleted because a sync configuration still references it. The real
	// DeleteRepositoryLink operation documents SyncConfigurationStillExistsException
	// for exactly this case.
	ErrSyncConfigStillExists = awserr.New("SyncConfigurationStillExistsException", awserr.ErrConflict)
	// ErrSyncBlockerNotFound is returned by UpdateSyncBlocker when the blocker ID
	// does not exist (or was created in a different region). The real operation
	// documents SyncBlockerDoesNotExistException for this case; it does NOT
	// resolve unknown IDs gracefully.
	ErrSyncBlockerNotFound = awserr.New("SyncBlockerDoesNotExistException", awserr.ErrNotFound)
)

// validProviderTypes returns the set of valid provider types for connections and hosts.
func validProviderTypes() map[string]bool {
	return map[string]bool{
		"Bitbucket":              true,
		"GitHub":                 true,
		"GitHubEnterpriseServer": true,
		"GitLab":                 true,
		"GitLabSelfManaged":      true,
	}
}

// validSyncTypes returns the set of sync configuration types accepted by AWS CodeStar Connections.
func validSyncTypes() map[string]bool {
	return map[string]bool{
		"CFN_STACK_SYNC": true,
	}
}

// validPublishDeploymentStatus is the set of accepted values.
func validPublishDeploymentStatus() map[string]bool {
	return map[string]bool{
		"ENABLED":  true,
		"DISABLED": true,
	}
}

// validTriggerResourceUpdateOn is the set of accepted values.
func validTriggerResourceUpdateOn() map[string]bool {
	return map[string]bool{
		"ANY_CHANGE":  true,
		"FILE_CHANGE": true,
	}
}

// validateConnectionName validates a connection name against the real
// ConnectionName shape (botocore: max 32, min 1, pattern "[\s\S]*" -- i.e.
// non-empty and length-bounded only, no character-class restriction).
func validateConnectionName(name string) error {
	return validateResourceName(name, maxConnectionNameLen)
}

// validateHostName validates a host name against the real HostName shape
// (botocore: max 64, min 1, pattern ".*" -- i.e. non-empty and
// length-bounded only, no character-class restriction). HostName's max
// length (64) is intentionally distinct from ConnectionName's (32); reusing
// validateConnectionName for host names would incorrectly reject valid
// 33-64 character host names.
func validateHostName(name string) error {
	return validateResourceName(name, maxHostNameLen)
}

func validateResourceName(name string, maxLen int) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	if len(name) > maxLen {
		return fmt.Errorf("%w: name must not exceed %d characters", ErrValidation, maxLen)
	}

	return nil
}

// validateTags validates tag key/value lengths and total count.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags", ErrTagLimitExceeded, maxTagsPerResource)
	}

	for k, v := range tags {
		if k == "" {
			return fmt.Errorf("%w: tag key must not be empty", ErrValidation)
		}

		if len(k) > maxTagKeyLen {
			return fmt.Errorf("%w: tag key %q exceeds %d characters", ErrValidation, k, maxTagKeyLen)
		}

		if len(v) > maxTagValueLen {
			return fmt.Errorf("%w: tag value for key %q exceeds %d characters", ErrValidation, k, maxTagValueLen)
		}
	}

	return nil
}
