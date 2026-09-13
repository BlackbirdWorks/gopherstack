package ram

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// policyShareNamePrefix names every CREATED_FROM_POLICY share this backend creates via
// PutPolicyBasedShare. Real AWS auto-generates these names too (they are never
// user-supplied for a policy-derived share).
const policyShareNamePrefix = "PolicyBasedShare-"

// policyPermissionNamePrefix names the managed permission PutPolicyBasedShare derives
// from a resource-based policy's actions (real AWS: "a managed permission that has the
// same IAM permissions as the original resource-based policy",
// api_op_PromoteResourceShareCreatedFromPolicy.go doc).
const policyPermissionNamePrefix = "CreatedFromPolicy-"

// PutPolicyBasedShare creates or updates the CREATED_FROM_POLICY resource share that
// represents resourceARN's cross-account resource-based-policy grant. This is glue's
// ResourceShareCreator seam target (services/glue/interfaces.go): glue's
// PutResourcePolicy calls it, outside its own lock, whenever a hybrid resource policy's
// Principal grants access to another account (services/lambda/lifecycle.go's
// capture/release/call/re-lock pattern -- ram's lock must never nest inside glue's).
// Idempotent: a share already tracking resourceARN has its principal associations and
// derived permission re-synced from the given values instead of duplicating state.
func (b *InMemoryBackend) PutPolicyBasedShare(resourceARN string, principals, actions []string) error {
	b.mu.Lock("PutPolicyBasedShare")
	defer b.mu.Unlock()

	now := time.Now()

	rs := b.findPolicyShareLocked(resourceARN)
	if rs == nil {
		shareID := uuid.NewString()
		rs = &ResourceShare{
			Name:                    policyShareNamePrefix + shareID,
			ARN:                     arn.Build("ram", b.region, b.accountID, "resource-share/"+shareID),
			OwningAccountID:         b.accountID,
			Status:                  statusActive,
			FeatureSet:              featureSetCreatedFromPolicy,
			AllowExternalPrincipals: true,
			PolicyResourceARN:       resourceARN,
			Tags:                    make(map[string]string),
			CreationTime:            now,
			LastUpdatedTime:         now,
		}
		b.resourceShares.Put(rs)

		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  rs.ARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  resourceARN,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		})
	} else {
		rs.LastUpdatedTime = now
	}

	b.syncPolicySharePrincipalsLocked(rs, principals, now)
	b.putPolicyDerivedPermissionLocked(rs, resourceARN, actions, now)

	return nil
}

// DeletePolicyBasedShare removes the CREATED_FROM_POLICY resource share tracking
// resourceARN, if one exists. Called by glue's DeleteResourcePolicy, or by
// PutResourcePolicy when an updated policy no longer grants any cross-account
// principal. No-op (not an error) when no such share exists, mirroring
// DisassociateResourceSharePermission's "nothing to do" precedent for an unassociated
// entity -- a caller resyncing state on every policy write shouldn't have to track
// whether a share was ever created.
func (b *InMemoryBackend) DeletePolicyBasedShare(resourceARN string) error {
	b.mu.Lock("DeletePolicyBasedShare")
	defer b.mu.Unlock()

	rs := b.findPolicyShareLocked(resourceARN)
	if rs == nil {
		return nil
	}

	now := time.Now()

	for _, a := range b.associations {
		if a.ResourceShareARN == rs.ARN && a.Status == associationStatusAssociated {
			a.Status = associationStatusDisassociated
			a.LastUpdatedTime = now
		}
	}

	delete(b.sharePermissions, rs.ARN)

	rs.Status = statusDeleted
	rs.LastUpdatedTime = now

	return nil
}

// findPolicyShareLocked returns the non-deleted CREATED_FROM_POLICY share tracking
// resourceARN, if any. A share already promoted to STANDARD (isCreatedFromPolicy false)
// is deliberately not matched: once promoted it "becomes ... fully manageable in RAM"
// (PromoteResourceShareCreatedFromPolicy doc) and must no longer be touched by this
// policy-sync seam -- a later PutPolicyBasedShare call for the same resource creates a
// fresh CREATED_FROM_POLICY share instead. Caller must hold the write lock.
func (b *InMemoryBackend) findPolicyShareLocked(resourceARN string) *ResourceShare {
	for _, rs := range b.resourceShares.All() {
		if rs.Status != statusDeleted && rs.PolicyResourceARN == resourceARN && isCreatedFromPolicy(rs) {
			return rs
		}
	}

	return nil
}

// syncPolicySharePrincipalsLocked associates every principal not already ASSOCIATED on
// rs (reactivating a prior DISASSOCIATED row in place, matching
// reactivateOrCreateLocked's pattern in share_associations.go) and disassociates any
// currently-ASSOCIATED principal no longer in principals. Caller must hold the write lock.
func (b *InMemoryBackend) syncPolicySharePrincipalsLocked(rs *ResourceShare, principals []string, now time.Time) {
	desired := make(map[string]struct{}, len(principals))
	for _, p := range principals {
		desired[p] = struct{}{}
	}

	existing := make(map[string]*ResourceShareAssociation)

	for _, a := range b.associations {
		if a.ResourceShareARN == rs.ARN && a.AssociationType == associationTypePrincipal {
			existing[a.AssociatedEntity] = a
		}
	}

	for _, p := range principals {
		if a, ok := existing[p]; ok {
			if a.Status != associationStatusAssociated {
				a.Status = associationStatusAssociated
				a.StatusMessage = ""
				a.LastUpdatedTime = now
			}

			continue
		}

		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  rs.ARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			External:          b.isExternalPrincipal(p),
			CreationTime:      now,
			LastUpdatedTime:   now,
		})
	}

	for entity, a := range existing {
		if _, want := desired[entity]; !want && a.Status == associationStatusAssociated {
			a.Status = associationStatusDisassociated
			a.LastUpdatedTime = now
		}
	}
}

// putPolicyDerivedPermissionLocked creates or updates rs's single CREATED_FROM_POLICY
// managed permission from actions, replacing any prior policy template. Real AWS scopes
// this permission to the resource's own type and marks it visible only to the resource
// share owner (PromoteResourceShareCreatedFromPolicy doc); this backend has no separate
// visibility model, so PermissionType=CREATED_FROM_POLICY (permissions.go's existing
// PromotePermissionCreatedFromPolicy) is the only distinguishing state. Caller must hold
// the write lock.
func (b *InMemoryBackend) putPolicyDerivedPermissionLocked(
	rs *ResourceShare, resourceARN string, actions []string, now time.Time,
) {
	shareID := strings.TrimPrefix(rs.Name, policyShareNamePrefix)
	permARN := b.permissionARN(policyPermissionNamePrefix + shareID)

	p, exists := b.permissions.Get(permARN)
	if !exists {
		p = &Permission{
			ARN:            permARN,
			Name:           policyPermissionNamePrefix + shareID,
			ResourceType:   resourceTypeFromARN(resourceARN),
			PermissionType: permissionTypeCreatedFromPolicy,
			Tags:           make(map[string]string),
			CreationTime:   now,
			DefaultVersion: 1,
			LatestVersion:  1,
			Versions:       make(map[int32]*PermissionVersion),
		}
	}

	p.LastUpdatedTime = now
	p.Versions[p.DefaultVersion] = &PermissionVersion{
		Version:         p.DefaultVersion,
		PolicyTemplate:  policyTemplateFromActions(actions),
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	b.permissions.Put(p)

	if b.sharePermissions[rs.ARN] == nil {
		b.sharePermissions[rs.ARN] = make(map[string]int32)
	}

	b.sharePermissions[rs.ARN] = map[string]int32{permARN: p.DefaultVersion}
}

// policyTemplateFromActions builds a minimal single-statement policy template string
// (RAM permission PolicyTemplate shape, see Permission.Versions) covering actions. Falls
// back to an empty-Action statement rather than erroring: json.Marshal of a []string
// cannot fail.
func policyTemplateFromActions(actions []string) string {
	body, err := json.Marshal(map[string]any{
		"Effect":   "Allow",
		"Action":   actions,
		"Resource": "*",
	})
	if err != nil {
		return `{"Effect":"Allow","Action":[],"Resource":"*"}`
	}

	return string(body)
}
