package sagemaker

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
)

// IamIdentity mirrors types.IamIdentity (sagemaker@v1.263.2).
type IamIdentity struct {
	Arn            string `json:"Arn,omitempty"`
	PrincipalID    string `json:"PrincipalId,omitempty"`
	SourceIdentity string `json:"SourceIdentity,omitempty"`
}

// UserContext mirrors types.UserContext. IamIdentity is populated for model
// package groups, model packages and projects only (types.go UserContext doc).
type UserContext struct {
	IamIdentity *IamIdentity `json:"IamIdentity,omitempty"`
}

// callerUserContext returns the UserContext of the request's principal. The
// result is always non-nil so required members (e.g. DescribeModelPackageGroup
// CreatedBy) serialize as {} when the caller is unauthenticated.
func callerUserContext(ctx context.Context) *UserContext {
	p := awsmeta.GetPrincipal(ctx)
	if p == nil || p.Arn == "" {
		return &UserContext{}
	}

	return &UserContext{IamIdentity: &IamIdentity{
		Arn:            p.Arn,
		PrincipalID:    p.UserID,
		SourceIdentity: p.SourceIdentity,
	}}
}

// optionalCallerUserContext is callerUserContext for members that are omitted, not {}, when the caller is unresolved.
func optionalCallerUserContext(ctx context.Context) *UserContext {
	if p := awsmeta.GetPrincipal(ctx); p == nil || p.Arn == "" {
		return nil
	}

	return callerUserContext(ctx)
}

func cloneUserContext(u *UserContext) *UserContext {
	if u == nil {
		return nil
	}

	cp := *u
	if u.IamIdentity != nil {
		id := *u.IamIdentity
		cp.IamIdentity = &id
	}

	return &cp
}
