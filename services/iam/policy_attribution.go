package iam

import (
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/iampolicystream"
)

// policyAttributor is an optional EnforcementBackend capability that names the
// policy a deciding document came from.
type policyAttributor interface {
	AttributePolicyDoc(principalArn, doc string) (iampolicystream.PolicyRef, bool)
	PermissionsBoundaryArnForPrincipal(principalArn string) string
}

// AttributePolicyDoc finds which managed or inline policy of the principal (or
// of a group it belongs to) holds doc.
func (b *InMemoryBackend) AttributePolicyDoc(principalArn, doc string) (iampolicystream.PolicyRef, bool) {
	bk, kind, name, err := b.principalBackend(principalArn)
	if err != nil {
		return iampolicystream.PolicyRef{}, false
	}

	bk.mu.RLock("AttributePolicyDoc")
	defer bk.mu.RUnlock()

	if kind == entityUser {
		return bk.attributeUserDocLocked(name, doc)
	}

	return bk.attributeEntityDocLocked(entityRole, name, bk.rolePolicies[name], bk.roleInlinePolicies[name], doc)
}

func (b *InMemoryBackend) attributeUserDocLocked(user, doc string) (iampolicystream.PolicyRef, bool) {
	managed, inline := b.userPolicies[user], b.userInlinePolicies[user]
	if ref, ok := b.attributeEntityDocLocked(entityUser, user, managed, inline, doc); ok {
		return ref, true
	}

	for _, group := range slices.Sorted(maps.Keys(b.groupMembers)) {
		if !slices.Contains(b.groupMembers[group], user) {
			continue
		}

		if ref, ok := b.attributeEntityDocLocked(
			"group", group, b.groupPolicies[group], b.groupInlinePolicies[group], doc,
		); ok {
			return ref, true
		}
	}

	return iampolicystream.PolicyRef{}, false
}

func (b *InMemoryBackend) attributeEntityDocLocked(
	kind, name string, managed []string, inline map[string]string, doc string,
) (iampolicystream.PolicyRef, bool) {
	for _, arn := range managed {
		if b.policyDocByARNLocked(arn) == doc {
			return iampolicystream.PolicyRef{Type: iampolicystream.PolicyManaged, Arn: arn}, true
		}
	}

	for _, pn := range slices.Sorted(maps.Keys(inline)) {
		if inline[pn] == doc {
			return iampolicystream.PolicyRef{
				Type: iampolicystream.PolicyInline, Name: pn, Owner: kind + "/" + name,
			}, true
		}
	}

	return iampolicystream.PolicyRef{}, false
}

// PermissionsBoundaryArnForPrincipal returns the boundary policy ARN of the
// user or role behind principalArn, or "".
func (b *InMemoryBackend) PermissionsBoundaryArnForPrincipal(principalArn string) string {
	bk, kind, name, err := b.principalBackend(principalArn)
	if err != nil {
		return ""
	}

	bk.mu.RLock("PermissionsBoundaryArnForPrincipal")
	defer bk.mu.RUnlock()

	if kind == entityUser {
		if u, ok := bk.users.Get(name); ok {
			return u.PermissionsBoundary
		}

		return ""
	}

	if r, ok := bk.roles.Get(name); ok {
		return r.PermissionsBoundary
	}

	return ""
}

type decisionSource int

const (
	sourceNone decisionSource = iota
	sourceIdentity
	sourceResource
	sourceBoundary
	sourceSCP
)

// boundaryTrace is how the permissions boundary evaluated a request.
type boundaryTrace struct {
	match   EvalMatch
	present bool
}

// attribution holds the inputs needed to name the deciding policy.
type attribution struct {
	principalArn string
	resourceArn  string
	identity     []string
	resource     []string
}

// attribute fills dec.policy and dec.boundaryArn from the deciding document.
func (d requestDecision) attribute(backend EnforcementBackend, in attribution) requestDecision {
	at, _ := backend.(policyAttributor)

	if at != nil {
		d.boundaryArn = at.PermissionsBoundaryArnForPrincipal(in.principalArn)
	}

	switch d.source {
	case sourceBoundary:
		d.policy = &iampolicystream.PolicyRef{Type: iampolicystream.PolicyBoundary, Arn: d.boundaryArn}
	case sourceResource:
		d.policy = &iampolicystream.PolicyRef{Type: iampolicystream.PolicyResource, Owner: in.resourceArn}
	case sourceIdentity:
		d.policy = identityPolicyRef(at, in, d.match.PolicyIndex)
	case sourceSCP, sourceNone:
	}

	return d
}

func identityPolicyRef(at policyAttributor, in attribution, idx int) *iampolicystream.PolicyRef {
	ref := &iampolicystream.PolicyRef{Type: iampolicystream.PolicyIdentity}
	if at == nil || idx < 0 || idx >= len(in.identity) {
		return ref
	}

	if found, ok := at.AttributePolicyDoc(in.principalArn, in.identity[idx]); ok {
		return &found
	}

	return ref
}

func (d requestDecision) boundaryRef() *iampolicystream.BoundaryRef {
	if !d.boundary.present {
		return nil
	}

	return &iampolicystream.BoundaryRef{
		Arn: d.boundaryArn, Sid: d.boundary.match.Sid,
		StatementIdx: d.boundary.match.StatementIdx, Matched: d.boundary.match.Matched,
	}
}

func (d requestDecision) denyType() string {
	switch {
	case d.allow:
		return ""
	case d.explicit:
		return iampolicystream.DenyExplicit
	}

	return iampolicystream.DenyImplicit
}
