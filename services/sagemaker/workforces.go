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

// ErrWorkforceNotFound is returned when a workforce does not exist.
var ErrWorkforceNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)

// ---------------------------------------------------------------------------
// Workforce
// ---------------------------------------------------------------------------

// CognitoConfig configures an Amazon Cognito private workforce.
type CognitoConfig struct {
	UserPool string `json:"UserPool"`
	ClientID string `json:"ClientId"`
}

// OidcConfig configures a private workforce using a customer-owned OIDC IdP.
// ClientSecret is stored but never echoed back in responses, matching the
// real OidcConfigForResponse shape.
type OidcConfig struct {
	AuthenticationRequestExtraParams map[string]string `json:"AuthenticationRequestExtraParams,omitempty"`
	ClientID                         string            `json:"ClientId"`
	ClientSecret                     string            `json:"-"`
	Issuer                           string            `json:"Issuer"`
	AuthorizationEndpoint            string            `json:"AuthorizationEndpoint"`
	TokenEndpoint                    string            `json:"TokenEndpoint"`
	UserInfoEndpoint                 string            `json:"UserInfoEndpoint"`
	LogoutEndpoint                   string            `json:"LogoutEndpoint"`
	JwksURI                          string            `json:"JwksUri"`
	Scope                            string            `json:"Scope,omitempty"`
}

// SourceIPConfig is a CIDR allow list restricting worker access to a workforce.
type SourceIPConfig struct {
	Cidrs []string `json:"Cidrs"`
}

// WorkforceVpcConfig describes the VPC a workforce connects through.
type WorkforceVpcConfig struct {
	VpcID            string   `json:"VpcId"`
	VpcEndpointID    string   `json:"VpcEndpointId,omitempty"`
	SecurityGroupIDs []string `json:"SecurityGroupIds,omitempty"`
	Subnets          []string `json:"Subnets,omitempty"`
}

// Workforce represents a SageMaker workforce.
type Workforce struct {
	CreateDate         time.Time           `json:"CreateDate"`
	LastUpdatedDate    time.Time           `json:"LastUpdatedDate"`
	Tags               map[string]string   `json:"-"`
	CognitoConfig      *CognitoConfig      `json:"CognitoConfig,omitempty"`
	OidcConfig         *OidcConfig         `json:"OidcConfig,omitempty"`
	SourceIPConfig     *SourceIPConfig     `json:"SourceIpConfig,omitempty"`
	WorkforceVpcConfig *WorkforceVpcConfig `json:"WorkforceVpcConfig,omitempty"`
	WorkforceName      string              `json:"WorkforceName"`
	WorkforceArn       string              `json:"WorkforceArn"`
	Status             string              `json:"Status"`
	SubDomain          string              `json:"SubDomain,omitempty"`
	IPAddressType      string              `json:"IpAddressType,omitempty"`
}

func cloneWorkforce(w *Workforce) *Workforce {
	cp := *w
	cp.Tags = maps.Clone(w.Tags)

	if w.CognitoConfig != nil {
		c := *w.CognitoConfig
		cp.CognitoConfig = &c
	}

	if w.OidcConfig != nil {
		o := *w.OidcConfig
		o.AuthenticationRequestExtraParams = maps.Clone(w.OidcConfig.AuthenticationRequestExtraParams)
		cp.OidcConfig = &o
	}

	if w.SourceIPConfig != nil {
		s := *w.SourceIPConfig
		s.Cidrs = append([]string(nil), w.SourceIPConfig.Cidrs...)
		cp.SourceIPConfig = &s
	}

	if w.WorkforceVpcConfig != nil {
		v := *w.WorkforceVpcConfig
		v.SecurityGroupIDs = append([]string(nil), w.WorkforceVpcConfig.SecurityGroupIDs...)
		v.Subnets = append([]string(nil), w.WorkforceVpcConfig.Subnets...)
		cp.WorkforceVpcConfig = &v
	}

	return &cp
}

// CreateWorkforceOptions holds the parameters for creating a workforce.
type CreateWorkforceOptions struct {
	CognitoConfig      *CognitoConfig
	OidcConfig         *OidcConfig
	SourceIPConfig     *SourceIPConfig
	WorkforceVpcConfig *WorkforceVpcConfig
	Tags               map[string]string
	Name               string
	IPAddressType      string
}

// CreateWorkforce creates a workforce. AWS allows at most one workforce per
// account per region.
func (b *InMemoryBackend) CreateWorkforce(ctx context.Context, opts CreateWorkforceOptions) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("CreateWorkforce")
	defer b.mu.Unlock()

	if opts.Name == "" {
		return nil, fmt.Errorf("%w: WorkforceName is required", ErrValidation)
	}

	store := b.workforcesStore(region)

	if _, ok := store.Get(opts.Name); ok {
		return nil, fmt.Errorf("%w: workforce %q already exists", ErrValidation, opts.Name)
	}

	if store.Len() > 0 {
		return nil, fmt.Errorf(
			"%w: only one workforce is allowed per Amazon Web Services account per Amazon Web Services Region",
			ErrValidation,
		)
	}

	workforceARN := arn.Build("sagemaker", region, b.accountID, "workforce/"+opts.Name)
	now := time.Now()

	w := &Workforce{
		WorkforceName:      opts.Name,
		WorkforceArn:       workforceARN,
		Status:             statusActive,
		CognitoConfig:      opts.CognitoConfig,
		OidcConfig:         opts.OidcConfig,
		SourceIPConfig:     opts.SourceIPConfig,
		WorkforceVpcConfig: opts.WorkforceVpcConfig,
		Tags:               mergeTags(nil, opts.Tags),
		IPAddressType:      opts.IPAddressType,
		CreateDate:         now,
		LastUpdatedDate:    now,
	}
	if w.OidcConfig != nil {
		w.SubDomain = "https://" + generateID() + ".labeling.sagemaker.aws"
	}

	if w.WorkforceVpcConfig != nil {
		w.WorkforceVpcConfig.VpcEndpointID = "vpce-" + generateID()[:17]
	}

	store.Put(w)
	b.workforceARNIndexStore(region)[workforceARN] = opts.Name

	return cloneWorkforce(w), nil
}

// DescribeWorkforce returns a workforce by name.
func (b *InMemoryBackend) DescribeWorkforce(ctx context.Context, name string) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeWorkforce")
	defer b.mu.RUnlock()

	w, ok := b.workforcesStoreRO(region).Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	return cloneWorkforce(w), nil
}

// UpdateWorkforceOptions holds the parameters for updating a workforce.
type UpdateWorkforceOptions struct {
	OidcConfig         *OidcConfig
	SourceIPConfig     *SourceIPConfig
	WorkforceVpcConfig *WorkforceVpcConfig
	Name               string
	IPAddressType      string
}

// UpdateWorkforce updates a workforce's IdP, IP allow-list, or VPC configuration.
func (b *InMemoryBackend) UpdateWorkforce(ctx context.Context, opts UpdateWorkforceOptions) (*Workforce, error) {
	region := getRegion(ctx, b.region)

	b.mu.Lock("UpdateWorkforce")
	defer b.mu.Unlock()

	w, ok := b.workforcesStore(region).Get(opts.Name)
	if !ok {
		return nil, fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, opts.Name)
	}

	if opts.OidcConfig != nil {
		w.OidcConfig = opts.OidcConfig
	}

	if opts.SourceIPConfig != nil {
		w.SourceIPConfig = opts.SourceIPConfig
	}

	if opts.WorkforceVpcConfig != nil {
		opts.WorkforceVpcConfig.VpcEndpointID = "vpce-" + generateID()[:17]
		w.WorkforceVpcConfig = opts.WorkforceVpcConfig
	}

	if opts.IPAddressType != "" {
		w.IPAddressType = opts.IPAddressType
	}

	w.LastUpdatedDate = time.Now()

	return cloneWorkforce(w), nil
}

// DeleteWorkforce removes a workforce. Fails with ResourceInUse if the
// workforce still has one or more work teams (AWS requires DeleteWorkteam
// first, per the DeleteWorkforce API contract).
func (b *InMemoryBackend) DeleteWorkforce(ctx context.Context, name string) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DeleteWorkforce")
	defer b.mu.Unlock()

	w, ok := b.workforcesStore(region).Get(name)
	if !ok {
		return fmt.Errorf("%w: workforce %q not found", ErrWorkforceNotFound, name)
	}

	for _, wt := range b.workteamsStore(region).All() {
		if wt.WorkforceArn == w.WorkforceArn {
			return fmt.Errorf(
				"%w: workforce %q still has associated work teams", ErrWorkteamInUse, name,
			)
		}
	}

	b.workforcesStore(region).Delete(name)
	delete(b.workforceARNIndexStore(region), w.WorkforceArn)

	return nil
}

// ListWorkforcesFilter narrows and orders the results of ListWorkforces
// (api_op_ListWorkforces.go:31-48).
type ListWorkforcesFilter struct {
	NameContains string
	SortBy       string
	SortOrder    string
	MaxResults   int32
}

func workforceMatchesFilter(w *Workforce, filter ListWorkforcesFilter) bool {
	return filter.NameContains == "" ||
		strings.Contains(strings.ToLower(w.WorkforceName), strings.ToLower(filter.NameContains))
}

// lessWorkforce orders a before b by sortBy (Name/default CreateDate, tie-broken by name).
func lessWorkforce(a, b *Workforce, sortBy string) bool {
	switch sortBy {
	case keyGenericName:
		return a.WorkforceName < b.WorkforceName
	default:
		if a.CreateDate.Equal(b.CreateDate) {
			return a.WorkforceName < b.WorkforceName
		}

		return a.CreateDate.Before(b.CreateDate)
	}
}

// ListWorkforces returns workforces matching filter, sorted by filter.SortBy
// (default CreateDate) / filter.SortOrder (default Ascending). AWS supports
// at most one private workforce per account per region, so this list
// contains at most one item — but the filter/sort/pagination fields are
// still wire-decoded and honored for shape fidelity.
func (b *InMemoryBackend) ListWorkforces(
	ctx context.Context,
	nextToken string,
	filter ListWorkforcesFilter,
) ([]*Workforce, string) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("ListWorkforces")
	defer b.mu.RUnlock()

	list := make([]*Workforce, 0, b.workforcesStoreRO(region).Len())

	for _, w := range b.workforcesStoreRO(region).All() {
		if workforceMatchesFilter(w, filter) {
			list = append(list, cloneWorkforce(w))
		}
	}

	desc := strings.EqualFold(filter.SortOrder, "Descending")
	sort.Slice(list, func(i, k int) bool {
		less := lessWorkforce(list[i], list[k], filter.SortBy)
		if desc {
			return !less
		}

		return less
	})

	return paginateSlice(list, nextToken, filter.MaxResults)
}
