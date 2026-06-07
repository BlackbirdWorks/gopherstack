package cloudformation

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
)

// iamRoleARNPattern matches valid IAM role ARNs:
// arn:aws[-gov|-cn]*:iam::<accountID>:role[/path]/<roleName>
var iamRoleARNPattern = regexp.MustCompile(
	`^arn:aws(?:-cn|-gov|-iso|-iso-b)?:iam::\d{12}:role(/[^/]+)*/[^/\s]+$`,
)

// ValidateRoleARN checks that the provided role ARN is syntactically valid.
// Returns ErrInvalidRoleARN if the ARN does not match the expected IAM role format.
func ValidateRoleARN(roleARN string) error {
	if roleARN == "" {
		return nil
	}
	if !iamRoleARNPattern.MatchString(roleARN) {
		return fmt.Errorf("%w: %q must match arn:aws:iam::<account>:role/<name>", ErrInvalidRoleARN, roleARN)
	}

	return nil
}

// DescribeType returns detailed information about a registered CloudFormation type.
// Lookup is by typeName, arn, or versionID — at least one must be non-empty.
func (b *InMemoryBackend) DescribeType(typeName, arn, versionID string) (*TypeDetails, error) {
	b.mu.RLock("DescribeType")
	defer b.mu.RUnlock()

	var reg *RegisteredType
	switch {
	case arn != "":
		r, ok := b.typeRegistry[arn]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, arn)
		}
		reg = r
	case typeName != "":
		key := "arn:aws:cloudformation:::type/resource/" + typeName
		r, ok := b.typeRegistry[key]
		if !ok {
			return nil, fmt.Errorf("%w: %s", ErrTypeNotFound, typeName)
		}
		reg = r
	default:
		return nil, fmt.Errorf("%w: TypeName or Arn is required", ErrTypeNotFound)
	}

	// Determine which version to return.
	resolvedVersionID := reg.DefaultVersion
	if versionID != "" {
		// Verify the version exists.
		found := false
		for _, v := range b.typeVersions[reg.TypeArn] {
			if v.VersionID == versionID {
				found = true
				resolvedVersionID = versionID

				break
			}
		}
		if !found && len(b.typeVersions[reg.TypeArn]) > 0 {
			return nil, fmt.Errorf("%w: %s version %s", ErrTypeVersionNotFound, reg.TypeName, versionID)
		}
	}

	isDefaultVersion := resolvedVersionID == reg.DefaultVersion
	visibility := typeVisibilityPrivate
	if reg.IsPublished {
		visibility = typeVisibilityPublic
	}
	deprecatedStatus := "LIVE"
	if reg.Status == typeStatusDeprecated {
		deprecatedStatus = typeStatusDeprecated
	}

	return &TypeDetails{
		TypeName:         reg.TypeName,
		TypeArn:          reg.TypeArn,
		Type:             reg.Type,
		Visibility:       visibility,
		Status:           reg.Status,
		VersionID:        resolvedVersionID,
		DefaultVersionID: reg.DefaultVersion,
		IsActivated:      reg.IsActivated,
		IsDefaultVersion: isDefaultVersion,
		DeprecatedStatus: deprecatedStatus,
	}, nil
}

// SimulateDrift marks all resources of a stack as DRIFTED for testing purposes.
// This is a test-only helper not part of the StorageBackend interface.
func (b *InMemoryBackend) SimulateDrift(stackName string) error {
	b.mu.Lock("SimulateDrift")
	defer b.mu.Unlock()

	stack, ok := b.resolveStack(stackName)
	if !ok {
		return fmt.Errorf("%w: %s", ErrStackNotFound, stackName)
	}

	resMap := b.resources[stack.StackID]
	if len(resMap) == 0 {
		return nil
	}

	// Create a drift detection record that reflects drifted resources.
	detectionID := uuid.New().String()
	b.driftDetections[detectionID] = &DriftDetectionStatus{
		StackID:                   stack.StackID,
		StackDriftDetectionID:     detectionID,
		StackDriftStatus:          driftStatusDrifted,
		DetectionStatus:           "DETECTION_COMPLETE",
		DriftedStackResourceCount: len(resMap),
		Timestamp:                 time.Now(),
	}

	return nil
}

// DescribeStackResourceDrifts returns drift information for all resources in a stack.
// Uses per-resource drift statuses from DetectStackDrift when available;
// falls back to the legacy SimulateDrift DRIFTED flag for backward compatibility.
func (b *InMemoryBackend) DescribeStackResourceDrifts(nameOrID string) ([]StackResourceDrift, error) {
	b.mu.RLock("DescribeStackResourceDrifts")
	defer b.mu.RUnlock()

	stack, ok := b.resolveStack(nameOrID)
	if !ok {
		return nil, ErrStackNotFound
	}

	// Use per-resource drift statuses if populated by DetectStackDrift.
	perResourceStatuses := b.resourceDriftStatus[stack.StackID]

	// Legacy SimulateDrift fallback: if any detection has DRIFTED status and
	// no per-resource data exists, mark all as DRIFTED.
	legacyDrifted := false
	if len(perResourceStatuses) == 0 {
		for _, det := range b.driftDetections {
			if det.StackID == stack.StackID && det.StackDriftStatus == driftStatusDrifted {
				legacyDrifted = true

				break
			}
		}
	}

	resMap := b.resources[stack.StackID]
	drifts := make([]StackResourceDrift, 0, len(resMap))
	for _, res := range resMap {
		status := driftStatusInSync
		if len(perResourceStatuses) > 0 {
			if s, ok2 := perResourceStatuses[res.LogicalID]; ok2 {
				status = s
			}
		} else if legacyDrifted {
			status = driftStatusDrifted
		}
		drifts = append(drifts, StackResourceDrift{
			StackID:                  stack.StackID,
			LogicalResourceID:        res.LogicalID,
			PhysicalResourceID:       res.PhysicalID,
			ResourceType:             res.Type,
			StackResourceDriftStatus: status,
			Timestamp:                res.Timestamp,
		})
	}

	sortResourceDrifts(drifts)

	return drifts, nil
}

func sortResourceDrifts(drifts []StackResourceDrift) {
	n := len(drifts)
	for i := 1; i < n; i++ {
		for j := i; j > 0 && drifts[j].LogicalResourceID < drifts[j-1].LogicalResourceID; j-- {
			drifts[j], drifts[j-1] = drifts[j-1], drifts[j]
		}
	}
}

// IAM PassRole simulation helpers.

// requireIAMCapability checks whether the template references IAM resources and
// the caller declared the appropriate capability. It returns ErrInsufficientCapabilities
// if the template creates IAM resources but the capability is absent.
func requireIAMCapability(templateBody string, capabilities []string) error {
	if templateBody == "" {
		return nil
	}
	hasIAM := strings.Contains(templateBody, "AWS::IAM::")
	if !hasIAM {
		return nil
	}
	for _, c := range capabilities {
		if c == "CAPABILITY_IAM" || c == "CAPABILITY_NAMED_IAM" || c == "CAPABILITY_AUTO_EXPAND" {
			return nil
		}
	}

	return ErrInsufficientCapabilities
}

// validateStackOptions validates the options for CreateStack and UpdateStack.
// It checks RoleARN format and IAM capability requirements.
func validateStackOptions(templateBody string, opts StackOptions) error {
	if err := ValidateRoleARN(opts.RoleARN); err != nil {
		return err
	}

	return requireIAMCapability(templateBody, opts.Capabilities)
}
