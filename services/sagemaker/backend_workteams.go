package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrWorkteamNotFound is returned when a workteam does not exist.
var ErrWorkteamNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Workteam
// ---------------------------------------------------------------------------

// CognitoMemberDefinition identifies an Amazon Cognito user group.
type CognitoMemberDefinition struct {
	UserPool  string `json:"UserPool"`
	UserGroup string `json:"UserGroup"`
	ClientID  string `json:"ClientId"`
}

// OidcMemberDefinition identifies a list of OIDC IdP user groups.
type OidcMemberDefinition struct {
	Groups []string `json:"Groups,omitempty"`
}

// MemberDefinition identifies workers that make up a work team, using either
// an Amazon Cognito user group or an OIDC IdP user group.
type MemberDefinition struct {
	CognitoMemberDefinition *CognitoMemberDefinition `json:"CognitoMemberDefinition,omitempty"`
	OidcMemberDefinition    *OidcMemberDefinition    `json:"OidcMemberDefinition,omitempty"`
}

// Workteam represents a SageMaker Ground Truth workteam.
type Workteam struct {
	CreateDate        time.Time          `json:"CreateDate"`
	LastUpdatedDate   time.Time          `json:"LastUpdatedDate"`
	Tags              map[string]string  `json:"-"`
	WorkteamName      string             `json:"WorkteamName"`
	WorkteamArn       string             `json:"WorkteamArn"`
	WorkforceArn      string             `json:"WorkforceArn,omitempty"`
	Description       string             `json:"Description,omitempty"`
	SubDomain         string             `json:"SubDomain,omitempty"`
	MemberDefinitions []MemberDefinition `json:"MemberDefinitions,omitempty"`
}

func cloneWorkteam(w *Workteam) *Workteam {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.MemberDefinitions = append([]MemberDefinition(nil), w.MemberDefinitions...)

	return &cp
}

// CreateWorkteamOptions holds the parameters for creating a workteam.
type CreateWorkteamOptions struct {
	Tags              map[string]string
	Name              string
	Description       string
	WorkforceName     string
	MemberDefinitions []MemberDefinition
}

// CreateWorkteam creates a workteam.
func (b *InMemoryBackend) CreateWorkteam(ctx context.Context, opts CreateWorkteamOptions) (*Workteam, error) {
	b.mu.Lock("CreateWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: WorkteamName is required", ErrValidation)
	}

	if _, ok := b.workteamsStore(region).Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: workteam %q already exists", ErrValidation, opts.Name)
	}

	var workforceARN string

	if opts.WorkforceName != "" {
		wf, ok := b.workforcesStore(region).Get(opts.WorkforceName)
		if !ok {
			return nil, fmt.Errorf(
				"%w: workforce %q not found", ErrWorkforceNotFound, opts.WorkforceName,
			)
		}

		workforceARN = wf.WorkforceArn
	}

	workteamARN := arn.Build("sagemaker", region, b.accountID, "workteam/"+opts.Name)
	now := time.Now()

	w := &Workteam{
		WorkteamName:      opts.Name,
		WorkteamArn:       workteamARN,
		WorkforceArn:      workforceARN,
		Description:       opts.Description,
		MemberDefinitions: append([]MemberDefinition(nil), opts.MemberDefinitions...),
		SubDomain:         "https://" + generateID() + ".labeling.sagemaker.aws",
		Tags:              mergeTags(nil, opts.Tags),
		CreateDate:        now,
		LastUpdatedDate:   now,
	}
	b.workteamsStore(region).Put(w)

	return cloneWorkteam(w), nil
}

// UpdateWorkteamOptions holds the parameters for updating a workteam.
type UpdateWorkteamOptions struct {
	Name              string
	Description       string
	MemberDefinitions []MemberDefinition
}

// UpdateWorkteam updates a workteam's description and/or member definitions.
func (b *InMemoryBackend) UpdateWorkteam(ctx context.Context, opts UpdateWorkteamOptions) (*Workteam, error) {
	b.mu.Lock("UpdateWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	w, ok := b.workteamsStore(region).Get(opts.Name)
	if !ok {
		return nil, fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, opts.Name)
	}

	if opts.Description != "" {
		w.Description = opts.Description
	}

	if opts.MemberDefinitions != nil {
		w.MemberDefinitions = append([]MemberDefinition(nil), opts.MemberDefinitions...)
	}

	w.LastUpdatedDate = time.Now()

	return cloneWorkteam(w), nil
}

// DescribeWorkteam returns a workteam by name.
func (b *InMemoryBackend) DescribeWorkteam(ctx context.Context, name string) (*Workteam, error) {
	b.mu.RLock("DescribeWorkteam")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	w, ok := b.workteamsStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	return cloneWorkteam(w), nil
}

// DeleteWorkteam removes a workteam.
func (b *InMemoryBackend) DeleteWorkteam(ctx context.Context, name string) error {
	b.mu.Lock("DeleteWorkteam")
	defer b.mu.Unlock()

	region := getRegion(ctx, b.region)

	if _, ok := b.workteamsStore(region).Get(name); !ok {
		return fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	store := b.workteamsStore(region)
	store.Delete(name)

	return nil
}

// ListWorkteams returns all workteams sorted by name.
func (b *InMemoryBackend) ListWorkteams(ctx context.Context, nextToken string) ([]*Workteam, string) {
	b.mu.RLock("ListWorkteams")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	return sagemakerListKeyPaged(
		b.workteamsStoreRO(region),
		nextToken,
		cloneWorkteam,
		func(v *Workteam) string { return v.WorkteamName },
	)
}
