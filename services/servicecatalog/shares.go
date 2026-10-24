package servicecatalog

import (
	"cmp"
	"context"
	"regexp"
	"slices"
)

var accountIDRE = regexp.MustCompile(`^\d{12}$`)

func registerShareOps(m map[string]opFunc, b *InMemoryBackend) {
	m["CreatePortfolioShare"] = op(b.CreatePortfolioShare)
	m["DeletePortfolioShare"] = op(b.DeletePortfolioShare)
	m["UpdatePortfolioShare"] = op(b.UpdatePortfolioShare)
	m["DescribePortfolioShares"] = op(b.DescribePortfolioShares)
	m["DescribePortfolioShareStatus"] = op(b.DescribePortfolioShareStatus)
	m["ListPortfolioAccess"] = op(b.ListPortfolioAccess)
	m["ListOrganizationPortfolioAccess"] = op(b.ListOrganizationPortfolioAccess)
	m["AcceptPortfolioShare"] = op(b.AcceptPortfolioShare)
	m["RejectPortfolioShare"] = op(b.RejectPortfolioShare)
	m["ListAcceptedPortfolioShares"] = op(b.ListAcceptedPortfolioShares)
	m["EnableAWSOrganizationsAccess"] = op(b.EnableAWSOrganizationsAccess)
	m["DisableAWSOrganizationsAccess"] = op(b.DisableAWSOrganizationsAccess)
	m["GetAWSOrganizationsAccessStatus"] = op(b.GetAWSOrganizationsAccessStatus)
}

func shareKey(portfolioID, typ, recipient string) string {
	return portfolioID + "|" + typ + "|" + recipient
}

func (b *InMemoryBackend) sharesOfPortfolioLocked(portfolioID string) []*Share {
	var out []*Share

	for _, s := range b.shares.All() {
		if s.PortfolioID == portfolioID {
			out = append(out, s)
		}
	}

	slices.SortFunc(out, func(x, y *Share) int { return cmp.Compare(x.Key, y.Key) })

	return out
}

// ShareTarget is the AccountId / OrganizationNode pair that names a share recipient.
type ShareTarget struct {
	OrganizationNode *OrganizationNode `json:"OrganizationNode,omitempty"`
	AccountID        string            `json:"AccountId,omitempty"`
}

func (t ShareTarget) validate() error {
	switch {
	case (t.AccountID == "") == (t.OrganizationNode == nil):
		return invalid("specify exactly one of AccountId and OrganizationNode")
	case t.AccountID != "" && !accountIDRE.MatchString(t.AccountID):
		return invalid("AccountId %q is not a 12-digit account ID", t.AccountID)
	case t.OrganizationNode != nil && (t.OrganizationNode.Value == "" || !validNodeType(t.OrganizationNode.Type)):
		return invalid("OrganizationNode needs a Type of ORGANIZATION, ORGANIZATIONAL_UNIT or ACCOUNT and a Value")
	}

	return nil
}

func validNodeType(t string) bool { return t == shareOrg || t == shareOU || t == shareAccount }

// CreatePortfolioShareInput is the CreatePortfolioShare request.
type CreatePortfolioShareInput struct {
	ShareTarget
	PortfolioID     string `json:"PortfolioId"`
	ShareTagOptions bool   `json:"ShareTagOptions,omitempty"`
	SharePrincipals bool   `json:"SharePrincipals,omitempty"`
}

// ShareTokenOutput is the CreatePortfolioShare and DeletePortfolioShare response.
type ShareTokenOutput struct {
	PortfolioShareToken string `json:"PortfolioShareToken,omitempty"`
}

func (b *InMemoryBackend) orgAccessEnabledLocked(account string) bool {
	if b.org != nil {
		on, err := b.org.ServiceAccessEnabled(servicePrincipal)

		return err == nil && on
	}

	st, ok := b.accounts.Get(account)

	return ok && st.OrganizationsAccess
}

// CreatePortfolioShare shares a portfolio with an account or organization node.
func (b *InMemoryBackend) CreatePortfolioShare(
	ctx context.Context,
	in *CreatePortfolioShareInput,
) (*ShareTokenOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.Lock("CreatePortfolioShare")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(account, region, in.PortfolioID)
	if err != nil {
		return nil, err
	}

	if in.AccountID != "" {
		if in.AccountID == account {
			return nil, invalid("a portfolio cannot be shared with its owner account")
		}

		b.shares.Put(&Share{
			Key: shareKey(p.ID, shareAccount, in.AccountID), PortfolioID: p.ID, OwnerAccount: account, Region: region,
			Type: shareAccount, Recipient: in.AccountID, SharePrincipals: in.SharePrincipals,
			ShareTagOptions: in.ShareTagOptions, CreatedTime: b.now(),
		})

		return &ShareTokenOutput{}, nil
	}

	token, err := b.createOrgShareLocked(account, region, p, in)
	if err != nil {
		return nil, err
	}

	return &ShareTokenOutput{PortfolioShareToken: token}, nil
}

func (b *InMemoryBackend) createOrgShareLocked(
	account, region string,
	p *Portfolio,
	in *CreatePortfolioShareInput,
) (string, error) {
	if !b.orgAccessEnabledLocked(account) {
		return "", fail(ErrInvalidState, "AWS Organizations access is not enabled for Service Catalog")
	}

	if b.org == nil {
		return "", fail(ErrInvalidState, "no AWS Organizations is available to resolve %s", in.OrganizationNode.Type)
	}

	node := in.OrganizationNode

	members, err := b.org.AccountsUnder(node.Type, node.Value)
	if err != nil {
		return "", invalid("organization node %s %s cannot be resolved: %v", node.Type, node.Value, err)
	}

	token := newID("share-")
	nodeType := node.Type

	if nodeType == shareAccount {
		nodeType = shareMember
	}

	members = slices.DeleteFunc(slices.Clone(members), func(a string) bool { return a == account })
	slices.Sort(members)

	b.shares.Put(&Share{
		Key: shareKey(p.ID, nodeType, node.Value), PortfolioID: p.ID, OwnerAccount: account, Region: region,
		Type: nodeType, Recipient: node.Value, NodeType: node.Type, Token: token, SuccessfulShares: members,
		SharePrincipals: in.SharePrincipals, ShareTagOptions: in.ShareTagOptions, CreatedTime: b.now(), Accepted: true,
	})

	for _, a := range members {
		b.shares.Put(&Share{
			Key: shareKey(p.ID, shareMember, a), PortfolioID: p.ID, OwnerAccount: account, Region: region,
			Type: shareMember, Recipient: a, Token: token, SharePrincipals: in.SharePrincipals,
			ShareTagOptions: in.ShareTagOptions, CreatedTime: b.now(), Accepted: true,
		})
	}

	return token, nil
}

func (b *InMemoryBackend) findShareLocked(p *Portfolio, t ShareTarget) (*Share, error) {
	if t.AccountID != "" {
		if s, ok := b.shares.Get(shareKey(p.ID, shareAccount, t.AccountID)); ok {
			return s, nil
		}

		return nil, notFound("share", t.AccountID)
	}

	typ := t.OrganizationNode.Type
	if typ == shareAccount {
		typ = shareMember
	}

	if s, ok := b.shares.Get(shareKey(p.ID, typ, t.OrganizationNode.Value)); ok {
		return s, nil
	}

	return nil, notFound("share", t.OrganizationNode.Value)
}

// DeletePortfolioShareInput is the DeletePortfolioShare request.
type DeletePortfolioShareInput struct {
	ShareTarget
	PortfolioID string `json:"PortfolioId"`
}

// DeletePortfolioShare stops sharing a portfolio.
func (b *InMemoryBackend) DeletePortfolioShare(
	ctx context.Context,
	in *DeletePortfolioShareInput,
) (*ShareTokenOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	b.mu.Lock("DeletePortfolioShare")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	s, err := b.findShareLocked(p, in.ShareTarget)
	if err != nil {
		return nil, err
	}

	if s.Token != "" && s.Type != shareMember {
		for _, m := range b.sharesOfPortfolioLocked(p.ID) {
			if m.Token == s.Token && m.Type == shareMember {
				b.dropMemberAccessLocked(p.ID, m.Recipient)
				b.shares.Delete(m.Key)
			}
		}
	} else if s.Type == shareAccount {
		b.dropMemberAccessLocked(p.ID, s.Recipient)
	}

	b.shares.Delete(s.Key)

	return &ShareTokenOutput{PortfolioShareToken: s.Token}, nil
}

// dropMemberAccessLocked removes a recipient's principal associations when its access goes away.
func (b *InMemoryBackend) dropMemberAccessLocked(portfolioID, account string) {
	for _, pr := range b.principalsOfPortfolioLocked(portfolioID) {
		if pr.AccountID == account {
			b.principals.Delete(pr.Key)
		}
	}
}

// UpdatePortfolioShareInput is the UpdatePortfolioShare request.
type UpdatePortfolioShareInput struct {
	ShareTarget
	ShareTagOptions *bool  `json:"ShareTagOptions,omitempty"`
	SharePrincipals *bool  `json:"SharePrincipals,omitempty"`
	PortfolioID     string `json:"PortfolioId"`
}

// UpdatePortfolioShareOutput is the UpdatePortfolioShare response.
type UpdatePortfolioShareOutput struct {
	PortfolioShareToken string `json:"PortfolioShareToken,omitempty"`
	Status              string `json:"Status"`
}

// UpdatePortfolioShare changes what a share includes.
func (b *InMemoryBackend) UpdatePortfolioShare(
	ctx context.Context,
	in *UpdatePortfolioShareInput,
) (*UpdatePortfolioShareOutput, error) {
	if err := in.validate(); err != nil {
		return nil, err
	}

	b.mu.Lock("UpdatePortfolioShare")
	defer b.mu.Unlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	s, err := b.findShareLocked(p, in.ShareTarget)
	if err != nil {
		return nil, err
	}

	apply := func(t *Share) {
		if in.ShareTagOptions != nil {
			t.ShareTagOptions = *in.ShareTagOptions
		}

		if in.SharePrincipals != nil {
			t.SharePrincipals = *in.SharePrincipals
		}
	}

	apply(s)

	if s.Token != "" {
		for _, m := range b.sharesOfPortfolioLocked(p.ID) {
			if m.Token == s.Token {
				apply(m)
			}
		}
	}

	return &UpdatePortfolioShareOutput{PortfolioShareToken: s.Token, Status: shareCompleted}, nil
}

// DescribePortfolioSharesInput is the DescribePortfolioShares request.
type DescribePortfolioSharesInput struct {
	PortfolioID string `json:"PortfolioId"`
	Type        string `json:"Type"`
	PageToken   string `json:"PageToken,omitempty"`
	PageSize    int32  `json:"PageSize,omitempty"`
}

// PortfolioShareDetail is the PortfolioShareDetail shape.
type PortfolioShareDetail struct {
	PrincipalID     string `json:"PrincipalId"`
	Type            string `json:"Type"`
	Accepted        bool   `json:"Accepted"`
	SharePrincipals bool   `json:"SharePrincipals"`
	ShareTagOptions bool   `json:"ShareTagOptions"`
}

// DescribePortfolioSharesOutput is the DescribePortfolioShares response.
type DescribePortfolioSharesOutput struct {
	NextPageToken         string                 `json:"NextPageToken,omitempty"`
	PortfolioShareDetails []PortfolioShareDetail `json:"PortfolioShareDetails"`
}

// DescribePortfolioShares lists who a portfolio is shared with, by recipient type.
func (b *InMemoryBackend) DescribePortfolioShares(
	ctx context.Context,
	in *DescribePortfolioSharesInput,
) (*DescribePortfolioSharesOutput, error) {
	switch in.Type {
	case shareAccount, shareOrg, shareOU, shareMember:
	default:
		return nil, invalid("Type must be ACCOUNT, ORGANIZATION, ORGANIZATIONAL_UNIT or ORGANIZATION_MEMBER_ACCOUNT")
	}

	b.mu.RLock("DescribePortfolioShares")
	defer b.mu.RUnlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	var all []PortfolioShareDetail

	for _, s := range b.sharesOfPortfolioLocked(p.ID) {
		if s.Type == in.Type {
			all = append(all, PortfolioShareDetail{
				PrincipalID: s.Recipient, Type: s.Type, Accepted: s.Accepted,
				SharePrincipals: s.SharePrincipals, ShareTagOptions: s.ShareTagOptions,
			})
		}
	}

	pg, err := pageOf(all, Paging{PageToken: in.PageToken, PageSize: in.PageSize})
	if err != nil {
		return nil, err
	}

	return &DescribePortfolioSharesOutput{PortfolioShareDetails: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// DescribePortfolioShareStatusInput is the DescribePortfolioShareStatus request.
type DescribePortfolioShareStatusInput struct {
	PortfolioShareToken string `json:"PortfolioShareToken"`
}

// ShareError is the ShareError shape.
type ShareError struct {
	Error    string   `json:"Error,omitempty"`
	Message  string   `json:"Message,omitempty"`
	Accounts []string `json:"Accounts,omitempty"`
}

// ShareDetails is the ShareDetails shape.
type ShareDetails struct {
	ShareErrors      []ShareError `json:"ShareErrors,omitempty"`
	SuccessfulShares []string     `json:"SuccessfulShares,omitempty"`
}

// DescribePortfolioShareStatusOutput is the DescribePortfolioShareStatus response.
type DescribePortfolioShareStatusOutput struct {
	ShareDetails          *ShareDetails `json:"ShareDetails"`
	OrganizationNodeValue string        `json:"OrganizationNodeValue,omitempty"`
	PortfolioID           string        `json:"PortfolioId"`
	PortfolioShareToken   string        `json:"PortfolioShareToken"`
	Status                string        `json:"Status"`
}

// DescribePortfolioShareStatus reports the outcome of an organization share.
func (b *InMemoryBackend) DescribePortfolioShareStatus(
	ctx context.Context,
	in *DescribePortfolioShareStatusInput,
) (*DescribePortfolioShareStatusOutput, error) {
	account := b.callerAccount(ctx)

	b.mu.RLock("DescribePortfolioShareStatus")
	defer b.mu.RUnlock()

	for _, s := range b.shares.All() {
		if s.Token == in.PortfolioShareToken && s.Type != shareMember && s.OwnerAccount == account {
			return &DescribePortfolioShareStatusOutput{
				PortfolioID: s.PortfolioID, PortfolioShareToken: s.Token, OrganizationNodeValue: s.Recipient,
				Status: shareCompleted, ShareDetails: &ShareDetails{SuccessfulShares: s.SuccessfulShares},
			}, nil
		}
	}

	return nil, notFound("portfolio share", in.PortfolioShareToken)
}

// ListPortfolioAccessInput is the ListPortfolioAccess request.
type ListPortfolioAccessInput struct {
	PortfolioID          string `json:"PortfolioId"`
	OrganizationParentID string `json:"OrganizationParentId,omitempty"`
	PageToken            string `json:"PageToken,omitempty"`
	PageSize             int32  `json:"PageSize,omitempty"`
}

// ListPortfolioAccessOutput is the ListPortfolioAccess response.
type ListPortfolioAccessOutput struct {
	NextPageToken string   `json:"NextPageToken,omitempty"`
	AccountIDs    []string `json:"AccountIds"`
}

// ListPortfolioAccess lists the accounts a portfolio is shared with.
func (b *InMemoryBackend) ListPortfolioAccess(
	ctx context.Context,
	in *ListPortfolioAccessInput,
) (*ListPortfolioAccessOutput, error) {
	b.mu.RLock("ListPortfolioAccess")
	defer b.mu.RUnlock()

	p, err := b.ownedPortfolioLocked(b.callerAccount(ctx), b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	var under map[string]bool

	if in.OrganizationParentID != "" {
		if b.org == nil {
			return nil, fail(ErrInvalidState, "no AWS Organizations is available")
		}

		ids, oerr := b.org.AccountsUnder(shareOU, in.OrganizationParentID)
		if oerr != nil {
			return nil, invalid("organization parent %s cannot be resolved: %v", in.OrganizationParentID, oerr)
		}

		under = map[string]bool{}
		for _, a := range ids {
			under[a] = true
		}
	}

	var accounts []string

	for _, s := range b.sharesOfPortfolioLocked(p.ID) {
		if (s.Type == shareAccount || s.Type == shareMember) && (under == nil || under[s.Recipient]) {
			accounts = append(accounts, s.Recipient)
		}
	}

	slices.Sort(accounts)

	pg, err := pageOf(slices.Compact(accounts), Paging{PageToken: in.PageToken, PageSize: in.PageSize})
	if err != nil {
		return nil, err
	}

	return &ListPortfolioAccessOutput{AccountIDs: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// ListOrganizationPortfolioAccessInput is the ListOrganizationPortfolioAccess request.
type ListOrganizationPortfolioAccessInput struct {
	PortfolioID          string `json:"PortfolioId"`
	OrganizationNodeType string `json:"OrganizationNodeType"`
	PageToken            string `json:"PageToken,omitempty"`
	PageSize             int32  `json:"PageSize,omitempty"`
}

// ListOrganizationPortfolioAccessOutput is the ListOrganizationPortfolioAccess response.
type ListOrganizationPortfolioAccessOutput struct {
	NextPageToken     string             `json:"NextPageToken,omitempty"`
	OrganizationNodes []OrganizationNode `json:"OrganizationNodes"`
}

// ListOrganizationPortfolioAccess lists the organization nodes a portfolio is shared with.
func (b *InMemoryBackend) ListOrganizationPortfolioAccess(
	ctx context.Context,
	in *ListOrganizationPortfolioAccessInput,
) (*ListOrganizationPortfolioAccessOutput, error) {
	if !validNodeType(in.OrganizationNodeType) {
		return nil, invalid("OrganizationNodeType must be ORGANIZATION, ORGANIZATIONAL_UNIT or ACCOUNT")
	}

	b.mu.RLock("ListOrganizationPortfolioAccess")
	defer b.mu.RUnlock()

	account := b.callerAccount(ctx)

	if !b.orgAccessEnabledLocked(account) {
		return nil, fail(ErrInvalidState, "AWS Organizations access is not enabled for Service Catalog")
	}

	p, err := b.ownedPortfolioLocked(account, b.callerRegion(ctx), in.PortfolioID)
	if err != nil {
		return nil, err
	}

	want := in.OrganizationNodeType
	if want == shareAccount {
		want = shareMember
	}

	var nodes []OrganizationNode

	for _, s := range b.sharesOfPortfolioLocked(p.ID) {
		if s.Type == want && (s.Token != "" || s.Type != shareMember) {
			nodes = append(nodes, OrganizationNode{Type: in.OrganizationNodeType, Value: s.Recipient})
		}
	}

	pg, err := pageOf(nodes, Paging{PageToken: in.PageToken, PageSize: in.PageSize})
	if err != nil {
		return nil, err
	}

	return &ListOrganizationPortfolioAccessOutput{OrganizationNodes: orEmpty(pg.Data), NextPageToken: pg.Next}, nil
}

// AcceptPortfolioShareInput is the AcceptPortfolioShare and RejectPortfolioShare request.
type AcceptPortfolioShareInput struct {
	PortfolioID        string `json:"PortfolioId"`
	PortfolioShareType string `json:"PortfolioShareType,omitempty"`
}

func (b *InMemoryBackend) shareToLocked(portfolioID, account, shareType string) (*Share, error) {
	typ := shareAccount
	if shareType == shareViaOrgs {
		typ = shareMember
	}

	s, ok := b.shares.Get(shareKey(portfolioID, typ, account))
	if !ok {
		return nil, notFound("portfolio share", portfolioID)
	}

	return s, nil
}

// AcceptPortfolioShare accepts a share offered to the caller's account.
func (b *InMemoryBackend) AcceptPortfolioShare(
	ctx context.Context,
	in *AcceptPortfolioShareInput,
) (*EmptyOutput, error) {
	b.mu.Lock("AcceptPortfolioShare")
	defer b.mu.Unlock()

	s, err := b.shareToLocked(in.PortfolioID, b.callerAccount(ctx), in.PortfolioShareType)
	if err != nil {
		return nil, err
	}

	if p, ok := b.portfolios.Get(in.PortfolioID); !ok || p.Region != b.callerRegion(ctx) {
		return nil, notFound("portfolio", in.PortfolioID)
	}

	s.Accepted = true

	return &EmptyOutput{}, nil
}

// RejectPortfolioShare declines a share offered to the caller's account.
func (b *InMemoryBackend) RejectPortfolioShare(
	ctx context.Context,
	in *AcceptPortfolioShareInput,
) (*EmptyOutput, error) {
	account := b.callerAccount(ctx)

	b.mu.Lock("RejectPortfolioShare")
	defer b.mu.Unlock()

	s, err := b.shareToLocked(in.PortfolioID, account, in.PortfolioShareType)
	if err != nil {
		return nil, err
	}

	s.Accepted = false
	b.dropMemberAccessLocked(in.PortfolioID, account)

	return &EmptyOutput{}, nil
}

// ListAcceptedPortfolioSharesInput is the ListAcceptedPortfolioShares request.
type ListAcceptedPortfolioSharesInput struct {
	PortfolioShareType string `json:"PortfolioShareType,omitempty"`
	Paging
}

// ListAcceptedPortfolioShares lists the portfolios shared with and accepted by the caller's account.
func (b *InMemoryBackend) ListAcceptedPortfolioShares(
	ctx context.Context,
	in *ListAcceptedPortfolioSharesInput,
) (*ListPortfoliosOutput, error) {
	account, region := b.callerAccount(ctx), b.callerRegion(ctx)

	b.mu.RLock("ListAcceptedPortfolioShares")
	defer b.mu.RUnlock()

	var out []*Portfolio

	for _, p := range b.portfolios.All() {
		if p.Region != region {
			continue
		}

		for _, s := range b.sharesOfPortfolioLocked(p.ID) {
			if s.Recipient == account && s.Accepted && sharedVia(s.Type, in.PortfolioShareType) {
				out = append(out, p)

				break
			}
		}
	}

	slices.SortFunc(out, func(x, y *Portfolio) int { return cmp.Compare(x.ID, y.ID) })

	pg, err := pageOf(out, in.Paging)
	if err != nil {
		return nil, err
	}

	return &ListPortfoliosOutput{PortfolioDetails: b.portfolioDetails(pg.Data), NextPageToken: pg.Next}, nil
}

func sharedVia(shareType, filter string) bool {
	switch filter {
	case "":
		return shareType == shareAccount || shareType == shareMember
	case shareImported:
		return shareType == shareAccount
	case shareViaOrgs:
		return shareType == shareMember
	default:
		return false
	}
}

// OrgAccessStatus is the AccessStatus response.
type OrgAccessStatus struct {
	AccessStatus string `json:"AccessStatus"`
}

// EnableAWSOrganizationsAccess turns on organization-wide portfolio sharing.
func (b *InMemoryBackend) EnableAWSOrganizationsAccess(ctx context.Context, _ *EmptyOutput) (*EmptyOutput, error) {
	return &EmptyOutput{}, b.setOrgAccess(ctx, true)
}

// DisableAWSOrganizationsAccess turns off organization-wide portfolio sharing.
func (b *InMemoryBackend) DisableAWSOrganizationsAccess(ctx context.Context, _ *EmptyOutput) (*EmptyOutput, error) {
	return &EmptyOutput{}, b.setOrgAccess(ctx, false)
}

func (b *InMemoryBackend) setOrgAccess(ctx context.Context, enable bool) error {
	account := b.callerAccount(ctx)

	b.mu.Lock("setOrgAccess")
	defer b.mu.Unlock()

	if b.org != nil {
		var err error
		if enable {
			err = b.org.EnableServiceAccess(servicePrincipal)
		} else {
			err = b.org.DisableServiceAccess(servicePrincipal)
		}

		if err != nil {
			return fail(ErrInvalidState, "AWS Organizations is not available: %v", err)
		}

		return nil
	}

	b.accounts.Put(&AccountState{AccountID: account, OrganizationsAccess: enable})

	return nil
}

// GetAWSOrganizationsAccessStatus reports whether organization-wide sharing is enabled.
func (b *InMemoryBackend) GetAWSOrganizationsAccessStatus(
	ctx context.Context,
	_ *EmptyOutput,
) (*OrgAccessStatus, error) {
	b.mu.RLock("GetAWSOrganizationsAccessStatus")
	defer b.mu.RUnlock()

	if b.orgAccessEnabledLocked(b.callerAccount(ctx)) {
		return &OrgAccessStatus{AccessStatus: "ENABLED"}, nil
	}

	return &OrgAccessStatus{AccessStatus: "DISABLED"}, nil
}

func (b *InMemoryBackend) acceptedSharesLocked(portfolioID, account string) []*Share {
	var out []*Share

	for _, s := range b.sharesOfPortfolioLocked(portfolioID) {
		if s.Accepted && s.Recipient == account && (s.Type == shareAccount || s.Type == shareMember) {
			out = append(out, s)
		}
	}

	return out
}

func (b *InMemoryBackend) sharesFlagLocked(p *Portfolio, account string, flag func(*Share) bool) bool {
	return p.AccountID != account && slices.ContainsFunc(b.acceptedSharesLocked(p.ID, account), flag)
}

// sharesTagOptionsLocked reports whether the portfolio's TagOptions are visible to account.
func (b *InMemoryBackend) sharesTagOptionsLocked(account string, p *Portfolio) bool {
	return p.AccountID == account || b.sharesFlagLocked(p, account, func(s *Share) bool { return s.ShareTagOptions })
}

// principalsForAccountLocked are the principal associations account sees on p: its own, plus the owner's
// IAM_PATTERN associations when an accepted share carries SharePrincipals.
func (b *InMemoryBackend) principalsForAccountLocked(p *Portfolio, account string) []*Principal {
	viaShare := b.sharesFlagLocked(p, account, func(s *Share) bool { return s.SharePrincipals })

	var out []*Principal

	for _, pr := range b.principalsOfPortfolioLocked(p.ID) {
		if pr.AccountID == account ||
			(viaShare && pr.AccountID == p.AccountID && pr.PrincipalType == principalIAMPattern) {
			out = append(out, pr)
		}
	}

	return out
}

// sharedTagOptionsLocked maps the owner TagOptions an account receives through accepted ShareTagOptions shares.
func (b *InMemoryBackend) sharedTagOptionsLocked(account, region string) map[string]*TagOption {
	out := map[string]*TagOption{}

	for _, p := range b.portfolios.All() {
		if p.Region != region || !b.sharesFlagLocked(p, account, func(s *Share) bool { return s.ShareTagOptions }) {
			continue
		}

		assocs := b.associationsOfPortfolioLocked(p.ID)
		ids := make([]string, 0, len(assocs)+1)
		ids = append(ids, p.ID)

		for _, a := range assocs {
			ids = append(ids, a.ProductID)
		}

		for _, id := range ids {
			for _, d := range b.tagOptionsForLocked(id) {
				if t, ok := b.tagOptions.Get(d.ID); ok {
					out[t.ID] = t
				}
			}
		}
	}

	return out
}
