package codestarconnections

import (
	"fmt"
	"regexp"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Validation limits.
const (
	maxConnectionNameLen   = 32
	maxTagKeyLen           = 128
	maxTagValueLen         = 256
	maxTagsPerResource     = 200
	maxProviderEndpointLen = 512
)

// connectionNameRE matches valid connection and host names: 1-32 alphanumeric, hyphen, underscore, dot.
var connectionNameRE = regexp.MustCompile(`^[a-zA-Z0-9_.\-]+$`)

var (
	// ErrNotFound is returned when a requested resource does not exist.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a connection or host with the same name
	// already exists. The real CreateConnection/CreateHost operations do not
	// document a dedicated typed exception for this, so it maps to the generic
	// InvalidInputException (see handler.go's error switch).
	ErrAlreadyExists = awserr.New("InvalidInputException", awserr.ErrAlreadyExists)
	// ErrResourceAlreadyExists is returned when a repository link or sync
	// configuration with the same identity already exists. Unlike
	// ErrAlreadyExists above, the real CreateRepositoryLink/CreateSyncConfiguration
	// operations both register a dedicated ResourceAlreadyExistsException for
	// this case (confirmed against aws-sdk-go-v2's per-op error deserializers).
	ErrResourceAlreadyExists = awserr.New("ResourceAlreadyExistsException", awserr.ErrAlreadyExists)
	// ErrValidation is returned when input validation fails.
	ErrValidation = awserr.New("ValidationException", awserr.ErrInvalidParameter)
	// ErrResourceInUse is returned when a host cannot be deleted because a
	// connection still references it. The real DeleteHost operation does not
	// document a dedicated typed exception for this either, so it maps to the
	// generic ConflictException ("two conflicting operations... on the same
	// resource"), which at least exists in the real service's error catalog
	// (unlike a fabricated "ResourceInUseException", which does not).
	ErrResourceInUse = awserr.New("ConflictException", awserr.ErrConflict)
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

// validateConnectionName validates the connection/host name rules.
func validateConnectionName(name string) error {
	if name == "" {
		return fmt.Errorf("%w: name is required", ErrValidation)
	}

	if len(name) > maxConnectionNameLen {
		return fmt.Errorf("%w: name must not exceed %d characters", ErrValidation, maxConnectionNameLen)
	}

	if !connectionNameRE.MatchString(name) {
		return fmt.Errorf("%w: name must match [a-zA-Z0-9_.\\-]+", ErrValidation)
	}

	return nil
}

// validateTags validates tag key/value lengths and total count.
func validateTags(tags map[string]string) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf("%w: cannot have more than %d tags", ErrValidation, maxTagsPerResource)
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
