package sagemaker

import (
	"context"
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// ErrWorkteamNotFound is returned when a workteam does not exist.
var ErrWorkteamNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// NotificationConfiguration mirrors types.NotificationConfiguration
// (api_op_CreateWorkteam.go), configuring SNS notification of available or
// expiring work items.
type NotificationConfiguration struct {
	NotificationTopicArn string `json:"NotificationTopicArn,omitempty"`
}

// IamPolicyConstraints mirrors types.IamPolicyConstraints (types/types.go:11466-11480).
type IamPolicyConstraints struct {
	SourceIP    string `json:"SourceIp,omitempty"`
	VpcSourceIP string `json:"VpcSourceIp,omitempty"`
}

// S3Presign mirrors types.S3Presign (types/types.go:20400-20407).
type S3Presign struct {
	IamPolicyConstraints *IamPolicyConstraints `json:"IamPolicyConstraints,omitempty"`
}

// WorkerAccessConfiguration mirrors types.WorkerAccessConfiguration
// (types/types.go:24464-24470), constraining access to an Amazon S3 resource
// in the worker portal.
type WorkerAccessConfiguration struct {
	S3Presign *S3Presign `json:"S3Presign,omitempty"`
}

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
	CreateDate                time.Time                  `json:"CreateDate"`
	LastUpdatedDate           time.Time                  `json:"LastUpdatedDate"`
	Tags                      map[string]string          `json:"-"`
	NotificationConfiguration *NotificationConfiguration `json:"NotificationConfiguration,omitempty"`
	WorkerAccessConfiguration *WorkerAccessConfiguration `json:"WorkerAccessConfiguration,omitempty"`
	WorkteamName              string                     `json:"WorkteamName"`
	WorkteamArn               string                     `json:"WorkteamArn"`
	WorkforceArn              string                     `json:"WorkforceArn,omitempty"`
	Description               string                     `json:"Description,omitempty"`
	SubDomain                 string                     `json:"SubDomain,omitempty"`
	MemberDefinitions         []MemberDefinition         `json:"MemberDefinitions,omitempty"`
}

func cloneWorkteam(w *Workteam) *Workteam {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)
	cp.MemberDefinitions = append([]MemberDefinition(nil), w.MemberDefinitions...)

	if w.NotificationConfiguration != nil {
		nc := *w.NotificationConfiguration
		cp.NotificationConfiguration = &nc
	}

	if w.WorkerAccessConfiguration != nil {
		wac := *w.WorkerAccessConfiguration
		if wac.S3Presign != nil {
			s3p := *wac.S3Presign
			if s3p.IamPolicyConstraints != nil {
				ipc := *s3p.IamPolicyConstraints
				s3p.IamPolicyConstraints = &ipc
			}
			wac.S3Presign = &s3p
		}
		cp.WorkerAccessConfiguration = &wac
	}

	return &cp
}

// CreateWorkteamOptions holds the parameters for creating a workteam.
type CreateWorkteamOptions struct {
	Tags                      map[string]string
	NotificationConfiguration *NotificationConfiguration
	WorkerAccessConfiguration *WorkerAccessConfiguration
	Name                      string
	Description               string
	WorkforceName             string
	MemberDefinitions         []MemberDefinition
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
		WorkteamName:              opts.Name,
		WorkteamArn:               workteamARN,
		WorkforceArn:              workforceARN,
		Description:               opts.Description,
		MemberDefinitions:         append([]MemberDefinition(nil), opts.MemberDefinitions...),
		SubDomain:                 "https://" + generateID() + ".labeling.sagemaker.aws",
		Tags:                      mergeTags(nil, opts.Tags),
		NotificationConfiguration: opts.NotificationConfiguration,
		WorkerAccessConfiguration: opts.WorkerAccessConfiguration,
		CreateDate:                now,
		LastUpdatedDate:           now,
	}
	b.workteamsStore(region).Put(w)
	b.workteamARNIndexStore(region)[workteamARN] = opts.Name

	return cloneWorkteam(w), nil
}

// UpdateWorkteamOptions holds the parameters for updating a workteam.
type UpdateWorkteamOptions struct {
	NotificationConfiguration *NotificationConfiguration
	WorkerAccessConfiguration *WorkerAccessConfiguration
	Name                      string
	Description               string
	MemberDefinitions         []MemberDefinition
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

	if opts.NotificationConfiguration != nil {
		w.NotificationConfiguration = opts.NotificationConfiguration
	}

	if opts.WorkerAccessConfiguration != nil {
		w.WorkerAccessConfiguration = opts.WorkerAccessConfiguration
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

	w, ok := b.workteamsStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: workteam %q not found", ErrWorkteamNotFound, name)
	}

	store := b.workteamsStore(region)
	store.Delete(name)
	delete(b.workteamARNIndexStore(region), w.WorkteamArn)

	return nil
}

// ListWorkteamsFilter narrows and orders the results of ListWorkteams
// (api_op_ListWorkteams.go:30-51). The op's own doc claims SortBy defaults to
// "CreationTime", but ListWorkteamsSortByOptions (types/enums.go:5437-5442)
// has no such value — only "Name" and "CreateDate" — so CreateDate (the
// timestamp field the mismatched doc text was clearly describing) is used as
// the real default, not the nonexistent documented one.
type ListWorkteamsFilter struct {
	NameContains string
	SortBy       string
	SortOrder    string
	MaxResults   int32
}

func workteamMatchesFilter(w *Workteam, filter ListWorkteamsFilter) bool {
	return filter.NameContains == "" ||
		strings.Contains(strings.ToLower(w.WorkteamName), strings.ToLower(filter.NameContains))
}

// lessWorkteam orders a before b by sortBy (Name/default CreateDate, tie-broken by name).
func lessWorkteam(a, b *Workteam, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.WorkteamName < b.WorkteamName
	default:
		if a.CreateDate.Equal(b.CreateDate) {
			return a.WorkteamName < b.WorkteamName
		}

		return a.CreateDate.Before(b.CreateDate)
	}
}

// ListWorkteams returns workteams matching filter, sorted by filter.SortBy
// (default CreateDate) / filter.SortOrder (default Ascending, per the op's doc).
func (b *InMemoryBackend) ListWorkteams(
	ctx context.Context,
	nextToken string,
	filter ListWorkteamsFilter,
) ([]*Workteam, string) {
	b.mu.RLock("ListWorkteams")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	list := make([]*Workteam, 0, b.workteamsStoreRO(region).Len())

	for _, w := range b.workteamsStoreRO(region).All() {
		if workteamMatchesFilter(w, filter) {
			list = append(list, cloneWorkteam(w))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessWorkteam(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
