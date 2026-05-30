package ram

import (
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	// statusActive is the active status for a resource share.
	statusActive = "ACTIVE"
	// statusDeleted is the deleted status for a resource share.
	statusDeleted = "DELETED"
	// associationStatusAssociated is the associated status for an association.
	associationStatusAssociated = "ASSOCIATED"
	// associationStatusDisassociated is the disassociated status for an association.
	associationStatusDisassociated = "DISASSOCIATED"
	// associationTypePrincipal is the principal association type.
	associationTypePrincipal = "PRINCIPAL"
	// associationTypeResource is the resource association type.
	associationTypeResource = "RESOURCE"
	// invitationStatusPending is the pending status for an invitation.
	invitationStatusPending = "PENDING"
	// invitationStatusAccepted is the accepted status for an invitation.
	invitationStatusAccepted = "ACCEPTED"
	// invitationStatusRejected is the rejected status for an invitation.
	invitationStatusRejected = "REJECTED"
	// invitationStatusExpired is the expired status for an invitation.
	invitationStatusExpired = "EXPIRED"
	// permissionTypeCustomer is the customer managed permission type.
	permissionTypeCustomer = "CUSTOMER_MANAGED"
	// permissionTypeAWSManaged is the AWS-managed permission type.
	permissionTypeAWSManaged = "AWS_MANAGED"
	// resourceOwnerSelf is the owner filter for resources owned by the calling account.
	resourceOwnerSelf = "SELF"
	// resourceOwnerOtherAccounts is the owner filter for resources shared by other accounts.
	resourceOwnerOtherAccounts = "OTHER-ACCOUNTS"
	// resourceRegionScopeRegional indicates a resource is region-scoped.
	resourceRegionScopeRegional = "REGIONAL"
	// resourceRegionScopeGlobal indicates a resource is globally scoped.
	resourceRegionScopeGlobal = "GLOBAL"
	// accountIDLen is the number of digits in an AWS account ID.
	accountIDLen = 12
	// arnPartCountPrincipal is the min number of colon-separated parts to extract an account from an ARN.
	arnPartCountPrincipal = 6
	// arnAccountIdx is the index of the account field in a colon-split ARN.
	arnAccountIdx = 4

	// Resource type strings shared between backend and built-in permission seeds.
	resourceTypeEC2Subnet         = "ec2:Subnet"
	resourceTypeEC2VPC            = "ec2:VPC"
	resourceTypeEC2TransitGateway = "ec2:TransitGateway"
	resourceTypeEC2PrefixList     = "ec2:PrefixList"
	resourceTypeS3Bucket          = "s3:Bucket"
	resourceTypeRoute53Resolver   = "route53resolver:ResolverRule"
	resourceTypeLicenseManager    = "license-manager:LicenseConfiguration"
)

var (
	// ErrValidation is returned when a request contains an invalid or missing parameter.
	ErrValidation = awserr.New("MalformedQueryStringException", awserr.ErrInvalidParameter)
	// ErrNotFound is returned when a resource share does not exist.
	ErrNotFound = awserr.New("UnknownResourceException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a resource share already exists.
	ErrAlreadyExists = awserr.New("ResourceShareAlreadyExistsException", awserr.ErrConflict)
	// ErrPermissionNotFound is returned when a permission does not exist.
	ErrPermissionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrInvitationNotFound is returned when an invitation does not exist.
	ErrInvitationNotFound = awserr.New(
		"ResourceShareInvitationArnNotFoundException",
		awserr.ErrNotFound,
	)
	// ErrInvitationAlreadyAccepted is returned when accepting an already-accepted invitation.
	ErrInvitationAlreadyAccepted = awserr.New(
		"ResourceShareInvitationAlreadyAcceptedException",
		awserr.ErrConflict,
	)
	// ErrInvitationAlreadyRejected is returned when accepting or rejecting an already-rejected invitation.
	ErrInvitationAlreadyRejected = awserr.New(
		"ResourceShareInvitationAlreadyRejectedException",
		awserr.ErrConflict,
	)
	// ErrInvitationExpired is returned when acting on an expired invitation.
	ErrInvitationExpired = awserr.New(
		"ResourceShareInvitationExpiredException",
		awserr.ErrConflict,
	)
	// ErrPermissionVersionNotFound is returned when a permission version does not exist.
	ErrPermissionVersionNotFound = awserr.New("InvalidParameterException", awserr.ErrNotFound)
	// ErrOperationNotPermitted is returned when an operation is not permitted on an AWS-managed resource.
	ErrOperationNotPermitted = awserr.New("OperationNotPermittedException", awserr.ErrConflict)
	// ErrPermissionInUse is returned when deleting a permission that is associated with active shares.
	ErrPermissionInUse = awserr.New("PermissionInUseException", awserr.ErrConflict)
)

// ResourceShare represents an AWS RAM resource share.
type ResourceShare struct {
	LastUpdatedTime         time.Time         `json:"lastUpdatedTime"`
	CreationTime            time.Time         `json:"creationTime"`
	Tags                    map[string]string `json:"tags,omitempty"`
	Name                    string            `json:"name"`
	ARN                     string            `json:"arn"`
	OwningAccountID         string            `json:"owningAccountId"`
	Status                  string            `json:"status"`
	StatusMessage           string            `json:"statusMessage,omitempty"`
	AllowExternalPrincipals bool              `json:"allowExternalPrincipals"`
}

// ResourceShareAssociation represents a principal or resource associated with a resource share.
type ResourceShareAssociation struct {
	LastUpdatedTime   time.Time `json:"lastUpdatedTime"`
	CreationTime      time.Time `json:"creationTime"`
	ResourceShareARN  string    `json:"resourceShareArn"`
	ResourceShareName string    `json:"resourceShareName"`
	AssociatedEntity  string    `json:"associatedEntity"`
	AssociationType   string    `json:"associationType"`
	Status            string    `json:"status"`
	StatusMessage     string    `json:"statusMessage,omitempty"`
	External          bool      `json:"external"`
}

// PermissionVersion holds a single versioned policy document for a managed permission.
type PermissionVersion struct {
	CreationTime    time.Time `json:"creationTime"`
	LastUpdatedTime time.Time `json:"lastUpdatedTime"`
	PolicyTemplate  string    `json:"policyTemplate"`
	Version         int32     `json:"version"`
}

// Permission represents a managed RAM permission (AWS-managed or customer-managed).
type Permission struct {
	CreationTime          time.Time                    `json:"creationTime"`
	LastUpdatedTime       time.Time                    `json:"lastUpdatedTime"`
	Tags                  map[string]string            `json:"tags,omitempty"`
	Versions              map[int32]*PermissionVersion `json:"versions"`
	ARN                   string                       `json:"arn"`
	Name                  string                       `json:"name"`
	ResourceType          string                       `json:"resourceType"`
	PermissionType        string                       `json:"permissionType"`
	ResourceRegionScope   string                       `json:"resourceRegionScope"`
	LatestVersion         int32                        `json:"latestVersion"`
	DefaultVersion        int32                        `json:"defaultVersion"`
	IsResourceTypeDefault bool                         `json:"isResourceTypeDefault"`
	Deleted               bool                         `json:"deleted"`
}

// clonePermission returns a deep copy of p.
func clonePermission(p *Permission) *Permission {
	cp := *p
	cp.Tags = maps.Clone(p.Tags)
	cp.Versions = make(map[int32]*PermissionVersion, len(p.Versions))

	for v, pv := range p.Versions {
		pvCopy := *pv
		cp.Versions[v] = &pvCopy
	}

	return &cp
}

// builtInPermDef defines a built-in AWS-managed RAM permission.
type builtInPermDef struct {
	name                string
	resourceType        string
	resourceRegionScope string
	policy              string
}

//nolint:gochecknoglobals,lll // read-only table initialized once; JSON policy strings must remain intact
var awsBuiltInPermissions = []builtInPermDef{
	{
		name:                "AWSRAMDefaultPermissionEC2Subnet",
		resourceType:        resourceTypeEC2Subnet,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["ec2:CreateTags","ec2:Describe*","ec2:*NetworkInterface*","ec2:*Route*","ec2:*SubnetCidrBlock*"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
	{
		name:                "AWSRAMDefaultPermissionEC2VPC",
		resourceType:        resourceTypeEC2VPC,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["ec2:CreateTags","ec2:Describe*","ec2:*Vpc*","ec2:*SecurityGroup*","ec2:*Dhcp*"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
	{
		name:                "AWSRAMDefaultPermissionEC2TransitGateway",
		resourceType:        resourceTypeEC2TransitGateway,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["ec2:CreateTags","ec2:Describe*","ec2:*TransitGateway*"],"Resource":"*"}`,
	},
	{
		name:                "AWSRAMDefaultPermissionEC2PrefixList",
		resourceType:        resourceTypeEC2PrefixList,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["ec2:CreateTags","ec2:Describe*","ec2:*ManagedPrefixList*","ec2:GetManagedPrefixListEntries"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
	{
		name:                "AWSRAMDefaultPermissionS3Bucket",
		resourceType:        resourceTypeS3Bucket,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["s3:ListBucket","s3:GetObject","s3:GetBucketLocation","s3:GetBucketAcl"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
	{
		name:                "AWSRAMDefaultPermissionRoute53ResolverResolverRule",
		resourceType:        resourceTypeRoute53Resolver,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["route53resolver:ListResolverRules","route53resolver:GetResolverRule","route53resolver:*ResolverRuleAssociation*"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
	{
		name:                "AWSRAMDefaultPermissionLicenseManagerLicenseConfiguration",
		resourceType:        resourceTypeLicenseManager,
		resourceRegionScope: resourceRegionScopeRegional,
		policy:              `{"Effect":"Allow","Action":["license-manager:ListAssociationsForLicenseConfiguration","license-manager:GetLicenseConfiguration","license-manager:ListLicenseConfigurations","license-manager:CheckInLicense","license-manager:CheckoutLicense"],"Resource":"*"}`, //nolint:lll // JSON policy must be on one line
	},
}

// awsManagedPermARN builds the ARN for an AWS-managed permission.
// AWS-managed permissions use an empty account and region component.
func awsManagedPermARN(name string) string {
	return "arn:aws:ram::aws:permission/" + name
}

// ResourceShareInvitation represents an invitation to access a resource share.
type ResourceShareInvitation struct {
	CreationTime      time.Time `json:"creationTime"`
	LastUpdatedTime   time.Time `json:"lastUpdatedTime"`
	InvitationARN     string    `json:"invitationArn"`
	ResourceShareARN  string    `json:"resourceShareArn"`
	ResourceShareName string    `json:"resourceShareName"`
	SenderAccountID   string    `json:"senderAccountId"`
	ReceiverAccountID string    `json:"receiverAccountId"`
	Status            string    `json:"status"`
}

// cloneInvitation returns a deep copy of an invitation.
func cloneInvitation(inv *ResourceShareInvitation) *ResourceShareInvitation {
	cp := *inv

	return &cp
}

// cloneResourceShare returns a deep copy of rs with the Tags map cloned.
func cloneResourceShare(rs *ResourceShare) *ResourceShare {
	cp := *rs
	cp.Tags = maps.Clone(rs.Tags)

	return &cp
}

// cloneAssociation returns a deep copy of an association.
func cloneAssociation(a *ResourceShareAssociation) *ResourceShareAssociation {
	cp := *a

	return &cp
}

// InMemoryBackend is an in-memory store for AWS RAM resources.
type InMemoryBackend struct {
	resourceShares   map[string]*ResourceShare
	permissions      map[string]*Permission
	sharePermissions map[string]map[string]int32 // shareARN -> permissionARN -> version
	invitations      map[string]*ResourceShareInvitation
	mu               *lockmetrics.RWMutex
	accountID        string
	region           string
	associations     []*ResourceShareAssociation
}

// NewInMemoryBackend creates a new in-memory RAM backend seeded with AWS-managed permissions.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	b := &InMemoryBackend{
		resourceShares:   make(map[string]*ResourceShare),
		associations:     make([]*ResourceShareAssociation, 0),
		permissions:      make(map[string]*Permission),
		sharePermissions: make(map[string]map[string]int32),
		invitations:      make(map[string]*ResourceShareInvitation),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("ram"),
	}
	b.seedBuiltInPermissions()

	return b
}

// seedBuiltInPermissions populates the backend with AWS-managed default permissions.
// Called at construction and after Reset so built-in permissions are always available.
func (b *InMemoryBackend) seedBuiltInPermissions() {
	now := time.Now()

	for _, def := range awsBuiltInPermissions {
		permARN := awsManagedPermARN(def.name)
		pv := &PermissionVersion{
			Version:         1,
			PolicyTemplate:  def.policy,
			CreationTime:    now,
			LastUpdatedTime: now,
		}
		p := &Permission{
			ARN:                   permARN,
			Name:                  def.name,
			ResourceType:          def.resourceType,
			PermissionType:        permissionTypeAWSManaged,
			ResourceRegionScope:   def.resourceRegionScope,
			Tags:                  make(map[string]string),
			CreationTime:          now,
			LastUpdatedTime:       now,
			DefaultVersion:        1,
			LatestVersion:         1,
			IsResourceTypeDefault: true,
			Versions:              map[int32]*PermissionVersion{1: pv},
		}
		b.permissions[permARN] = p
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string    { return b.region }
func (b *InMemoryBackend) AccountID() string { return b.accountID }

// CreateResourceShare creates a new resource share.
func (b *InMemoryBackend) CreateResourceShare(
	name string,
	allowExternalPrincipals bool,
	tags map[string]string,
	principals, resourceARNs []string,
) (*ResourceShare, error) {
	b.mu.Lock("CreateResourceShare")
	defer b.mu.Unlock()

	// Check for name collision.
	for _, rs := range b.resourceShares {
		if rs.Name == name && rs.Status != statusDeleted {
			return nil, fmt.Errorf("%w: resource share %s already exists", ErrAlreadyExists, name)
		}
	}

	now := time.Now()
	// Use a stable UUID-based resource share ID so the ARN remains valid
	// even if the share is later renamed via UpdateResourceShare.
	shareID := uuid.NewString()
	shareARN := arn.Build("ram", b.region, b.accountID, "resource-share/"+shareID)

	rs := &ResourceShare{
		Name:                    name,
		ARN:                     shareARN,
		OwningAccountID:         b.accountID,
		Status:                  statusActive,
		AllowExternalPrincipals: allowExternalPrincipals,
		CreationTime:            now,
		LastUpdatedTime:         now,
		Tags:                    mergeTags(nil, tags),
	}
	b.resourceShares[shareARN] = rs

	// Add principal associations, enforcing AllowExternalPrincipals.
	for _, p := range principals {
		external := b.isExternalPrincipal(p)
		if external && !allowExternalPrincipals {
			return nil, fmt.Errorf(
				"%w: external principals not allowed for this resource share",
				ErrValidation,
			)
		}

		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			External:          external,
			CreationTime:      now,
			LastUpdatedTime:   now,
		})

		if external {
			receiverID := principalReceiverAccountID(p)
			b.createInvitationLocked(shareARN, name, receiverID)
		}
	}

	// Add resource associations.
	for _, r := range resourceARNs {
		b.associations = append(b.associations, &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: name,
			AssociatedEntity:  r,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			CreationTime:      now,
			LastUpdatedTime:   now,
		})
	}

	return cloneResourceShare(rs), nil
}

// GetResourceShare returns a resource share by ARN.
func (b *InMemoryBackend) GetResourceShare(shareARN string) (*ResourceShare, error) {
	b.mu.RLock("GetResourceShare")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	return cloneResourceShare(rs), nil
}

// ListResourceShares returns resource shares matching the given owner and optional status filter.
// resourceOwner must be "SELF" or "OTHER-ACCOUNTS".
//   - "SELF": shares owned by this account (not deleted, optionally filtered by status).
//   - "OTHER-ACCOUNTS": shares owned by another account where this account is a PRINCIPAL.
//
// Pass status="" to return all matching shares, or e.g. "ACTIVE" to filter by status.
func (b *InMemoryBackend) ListResourceShares(resourceOwner, status string) []*ResourceShare {
	b.mu.RLock("ListResourceShares")
	defer b.mu.RUnlock()

	var list []*ResourceShare

	switch resourceOwner {
	case resourceOwnerSelf, "":
		list = b.listOwnedShares(status)
	case resourceOwnerOtherAccounts:
		list = b.listSharedWithMe(status)
	}

	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })

	return list
}

// listOwnedShares returns non-deleted shares owned by this account, filtered by status.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) listOwnedShares(status string) []*ResourceShare {
	list := make([]*ResourceShare, 0, len(b.resourceShares))

	for _, rs := range b.resourceShares {
		if rs.Status == statusDeleted || rs.OwningAccountID != b.accountID {
			continue
		}

		if status != "" && rs.Status != status {
			continue
		}

		list = append(list, cloneResourceShare(rs))
	}

	return list
}

// listSharedWithMe returns non-deleted shares owned by other accounts where this
// account appears as an active PRINCIPAL association. Caller must hold at least a read lock.
func (b *InMemoryBackend) listSharedWithMe(status string) []*ResourceShare {
	// Build a set of share ARNs where this account is an active PRINCIPAL.
	principalShares := make(map[string]struct{})

	for _, a := range b.associations {
		if a.AssociationType == associationTypePrincipal &&
			a.Status == associationStatusAssociated &&
			a.AssociatedEntity == b.accountID {
			principalShares[a.ResourceShareARN] = struct{}{}
		}
	}

	list := make([]*ResourceShare, 0)

	for _, rs := range b.resourceShares {
		if rs.Status == statusDeleted || rs.OwningAccountID == b.accountID {
			continue
		}

		if _, ok := principalShares[rs.ARN]; !ok {
			continue
		}

		if status != "" && rs.Status != status {
			continue
		}

		list = append(list, cloneResourceShare(rs))
	}

	return list
}

// UpdateResourceShare updates an existing resource share.
// If name is changed, all matching associations are updated to reflect the new name.
func (b *InMemoryBackend) UpdateResourceShare(
	shareARN, name string,
	allowExternalPrincipals *bool,
) (*ResourceShare, error) {
	b.mu.Lock("UpdateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	if name != "" {
		rs.Name = name
		// Keep association ResourceShareName in sync.
		for _, a := range b.associations {
			if a.ResourceShareARN == shareARN {
				a.ResourceShareName = name
			}
		}
	}

	if allowExternalPrincipals != nil {
		rs.AllowExternalPrincipals = *allowExternalPrincipals
	}

	rs.LastUpdatedTime = time.Now()

	return cloneResourceShare(rs), nil
}

// DeleteResourceShare soft-deletes a resource share by marking it DELETED.
// This mirrors AWS behaviour where a share remains visible with DELETED status briefly.
func (b *InMemoryBackend) DeleteResourceShare(shareARN string) error {
	b.mu.Lock("DeleteResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	now := time.Now()
	rs.Status = statusDeleted
	rs.LastUpdatedTime = now

	// Mark all associations for this share as disassociated.
	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			a.Status = associationStatusDisassociated
			a.LastUpdatedTime = now
		}
	}

	return nil
}

// isExternalPrincipal returns true if the principal does not look like an AWS account ID
// owned by the backend account. AWS account IDs are 12-digit strings.
func (b *InMemoryBackend) isExternalPrincipal(principal string) bool {
	// An account-ID principal is exactly accountIDLen digits. Anything else is external.
	if len(principal) == accountIDLen {
		for _, c := range principal {
			if c < '0' || c > '9' {
				return true
			}
		}

		return principal != b.accountID
	}

	return true
}

// AssociateResourceShare associates principals or resource ARNs with a resource share.
// Entities that are already associated are silently skipped (idempotent), matching AWS behavior.
// Returns deep copies of the new associations so callers cannot mutate backend state.
func (b *InMemoryBackend) AssociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("AssociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	// Build a set of already-associated entities for O(1) deduplication.
	// We don't know how many belong to this share without scanning first,
	// so we start with no hint and let the map grow naturally.
	existing := make(map[string]struct{})
	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			existing[a.AssociatedEntity] = struct{}{}
		}
	}

	now := time.Now()
	added := make([]*ResourceShareAssociation, 0, len(principals)+len(resourceARNs))

	for _, p := range principals {
		if _, dup := existing[p]; dup {
			continue
		}

		external := b.isExternalPrincipal(p)
		if external && !rs.AllowExternalPrincipals {
			return nil, fmt.Errorf(
				"%w: external principals not allowed for resource share %s",
				ErrValidation,
				shareARN,
			)
		}

		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  p,
			AssociationType:   associationTypePrincipal,
			Status:            associationStatusAssociated,
			External:          external,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))

		if external {
			receiverID := principalReceiverAccountID(p)
			b.createInvitationLocked(shareARN, rs.Name, receiverID)
		}
	}

	for _, r := range resourceARNs {
		if _, dup := existing[r]; dup {
			continue
		}

		assoc := &ResourceShareAssociation{
			ResourceShareARN:  shareARN,
			ResourceShareName: rs.Name,
			AssociatedEntity:  r,
			AssociationType:   associationTypeResource,
			Status:            associationStatusAssociated,
			External:          false,
			CreationTime:      now,
			LastUpdatedTime:   now,
		}
		b.associations = append(b.associations, assoc)
		added = append(added, cloneAssociation(assoc))
	}

	return added, nil
}

// DisassociateResourceShare removes principals or resource ARNs from a resource share.
func (b *InMemoryBackend) DisassociateResourceShare(
	shareARN string,
	principals, resourceARNs []string,
) ([]*ResourceShareAssociation, error) {
	b.mu.Lock("DisassociateResourceShare")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	toRemove := make(map[string]struct{}, len(principals)+len(resourceARNs))

	for _, p := range principals {
		toRemove[p] = struct{}{}
	}

	for _, r := range resourceARNs {
		toRemove[r] = struct{}{}
	}

	var updated []*ResourceShareAssociation

	kept := b.associations[:0]

	for _, a := range b.associations {
		if a.ResourceShareARN == shareARN {
			if _, found := toRemove[a.AssociatedEntity]; found {
				cp := cloneAssociation(a)
				cp.Status = associationStatusDisassociated
				cp.LastUpdatedTime = time.Now()
				updated = append(updated, cp)

				continue
			}
		}

		kept = append(kept, a)
	}

	// Nil the truncated tail slots so the GC can collect the removed associations.
	for i := len(kept); i < len(b.associations); i++ {
		b.associations[i] = nil
	}

	b.associations = kept

	_ = rs // share lookup above ensures we return NotFound for deleted shares

	return updated, nil
}

// GetResourceShareAssociations returns associations for the given resource share ARNs and type.
func (b *InMemoryBackend) GetResourceShareAssociations(
	associationType string,
	shareARNs []string,
) []*ResourceShareAssociation {
	b.mu.RLock("GetResourceShareAssociations")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(shareARNs))

	for _, a := range shareARNs {
		arnSet[a] = struct{}{}
	}

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if associationType != "" && a.AssociationType != associationType {
			continue
		}

		if len(arnSet) > 0 {
			if _, ok := arnSet[a.ResourceShareARN]; !ok {
				continue
			}
		}

		result = append(result, cloneAssociation(a))
	}

	return result
}

// TagResource adds or updates tags on a resource share identified by ARN.
func (b *InMemoryBackend) TagResource(shareARN string, kv map[string]string) error {
	b.mu.Lock("TagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	rs.Tags = mergeTags(rs.Tags, kv)

	return nil
}

// UntagResource removes specified tag keys from a resource share.
func (b *InMemoryBackend) UntagResource(shareARN string, keys []string) error {
	b.mu.Lock("UntagResource")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	for _, k := range keys {
		delete(rs.Tags, k)
	}

	return nil
}

// ListTagsForResource returns tags for a resource share identified by ARN.
func (b *InMemoryBackend) ListTagsForResource(shareARN string) (map[string]string, error) {
	b.mu.RLock("ListTagsForResource")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	result := make(map[string]string, len(rs.Tags))
	maps.Copy(result, rs.Tags)

	return result, nil
}

// mergeTags merges new tags into existing ones, returning a new map.
func mergeTags(existing, incoming map[string]string) map[string]string {
	result := make(map[string]string, len(existing)+len(incoming))
	maps.Copy(result, existing)
	maps.Copy(result, incoming)

	return result
}

// Reset clears all in-memory state from the backend and re-seeds built-in permissions.
// It is used by the POST /_gopherstack/reset endpoint for CI pipelines and rapid local development.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	b.resourceShares = make(map[string]*ResourceShare)
	b.associations = make([]*ResourceShareAssociation, 0)
	b.permissions = make(map[string]*Permission)
	b.sharePermissions = make(map[string]map[string]int32)
	b.invitations = make(map[string]*ResourceShareInvitation)
	b.seedBuiltInPermissions()
}

// AddResourceShareInternal inserts a resource share directly, bypassing validation.
// Useful for seeding test state.
func (b *InMemoryBackend) AddResourceShareInternal(rs *ResourceShare) {
	b.mu.Lock("AddResourceShareInternal")
	defer b.mu.Unlock()

	if rs.Tags == nil {
		rs.Tags = make(map[string]string)
	}

	b.resourceShares[rs.ARN] = rs
}

// AddPermissionInternal inserts a permission directly, bypassing validation.
// Useful for seeding test state.
func (b *InMemoryBackend) AddPermissionInternal(p *Permission) {
	b.mu.Lock("AddPermissionInternal")
	defer b.mu.Unlock()

	if p.Tags == nil {
		p.Tags = make(map[string]string)
	}

	if p.Versions == nil {
		p.Versions = make(map[int32]*PermissionVersion)
	}

	b.permissions[p.ARN] = p
}

// permissionARN builds an ARN for a customer-managed RAM permission.
func (b *InMemoryBackend) permissionARN(name string) string {
	return arn.Build("ram", b.region, b.accountID, "permission/"+name)
}

// invitationARN builds an ARN for a resource share invitation.
func (b *InMemoryBackend) invitationARN(id string) string {
	return arn.Build("ram", b.region, b.accountID, "resource-share-invitation/"+id)
}

// CreatePermission creates a new customer-managed RAM permission.
func (b *InMemoryBackend) CreatePermission(
	name, resourceType, policyTemplate string,
	tags map[string]string,
) (*Permission, error) {
	b.mu.Lock("CreatePermission")
	defer b.mu.Unlock()

	permARN := b.permissionARN(name)

	if p, ok := b.permissions[permARN]; ok && !p.Deleted {
		return nil, fmt.Errorf("%w: permission %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now()
	version := int32(1)
	pv := &PermissionVersion{
		Version:         version,
		PolicyTemplate:  policyTemplate,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	p := &Permission{
		ARN:                 permARN,
		Name:                name,
		ResourceType:        resourceType,
		PermissionType:      permissionTypeCustomer,
		ResourceRegionScope: resourceRegionScopeRegional,
		Tags:                mergeTags(nil, tags),
		CreationTime:        now,
		LastUpdatedTime:     now,
		DefaultVersion:      version,
		LatestVersion:       version,
		Versions:            map[int32]*PermissionVersion{version: pv},
	}
	b.permissions[permARN] = p

	return clonePermission(p), nil
}

// CreatePermissionVersion creates a new version of an existing customer-managed RAM permission.
func (b *InMemoryBackend) CreatePermissionVersion(
	permissionARN, policyTemplate string,
) (*Permission, error) {
	b.mu.Lock("CreatePermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	now := time.Now()
	newVersion := p.LatestVersion + 1
	p.Versions[newVersion] = &PermissionVersion{
		Version:         newVersion,
		PolicyTemplate:  policyTemplate,
		CreationTime:    now,
		LastUpdatedTime: now,
	}
	p.LatestVersion = newVersion
	p.LastUpdatedTime = now

	return clonePermission(p), nil
}

// DeletePermission soft-deletes a customer-managed RAM permission and removes it from all shares.
// AWS-managed permissions cannot be deleted.
func (b *InMemoryBackend) DeletePermission(permissionARN string) error {
	b.mu.Lock("DeletePermission")
	defer b.mu.Unlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if p.PermissionType == permissionTypeAWSManaged {
		return fmt.Errorf(
			"%w: cannot delete AWS-managed permission %s",
			ErrOperationNotPermitted,
			permissionARN,
		)
	}

	// Refuse deletion if permission is associated with any active resource share.
	for _, perms := range b.sharePermissions {
		if _, inUse := perms[permissionARN]; inUse {
			return fmt.Errorf(
				"%w: permission %s is associated with one or more resource shares",
				ErrPermissionInUse,
				permissionARN,
			)
		}
	}

	p.Deleted = true

	return nil
}

// DeletePermissionVersion deletes a specific version of a customer-managed RAM permission.
// The default version cannot be deleted. If the latest version is deleted, LatestVersion
// is updated to the next-highest remaining version.
func (b *InMemoryBackend) DeletePermissionVersion(
	permissionARN string,
	permissionVersion int32,
) error {
	b.mu.Lock("DeletePermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if _, exists := p.Versions[permissionVersion]; !exists {
		return fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			permissionVersion,
			permissionARN,
		)
	}

	if p.DefaultVersion == permissionVersion {
		return fmt.Errorf("%w: cannot delete the default version of a permission", ErrValidation)
	}

	delete(p.Versions, permissionVersion)
	p.LastUpdatedTime = time.Now()

	// If we deleted the latest version, recalculate it.
	if p.LatestVersion == permissionVersion && len(p.Versions) > 0 {
		versions := make([]int32, 0, len(p.Versions))

		for v := range p.Versions {
			versions = append(versions, v)
		}

		slices.Sort(versions)
		p.LatestVersion = versions[len(versions)-1]
	}

	// Cascade: remove share-permission associations pointing at the deleted version.
	for shareARN, perms := range b.sharePermissions {
		if ver, exists := perms[permissionARN]; exists && ver == permissionVersion {
			delete(perms, permissionARN)

			if len(perms) == 0 {
				delete(b.sharePermissions, shareARN)
			}
		}
	}

	return nil
}

// GetPermission returns the details of a RAM permission, optionally at a specific version.
func (b *InMemoryBackend) GetPermission(
	permissionARN string,
	permissionVersion *int32,
) (*Permission, *PermissionVersion, error) {
	b.mu.RLock("GetPermission")
	defer b.mu.RUnlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return nil, nil, fmt.Errorf(
			"%w: permission %s not found",
			ErrPermissionNotFound,
			permissionARN,
		)
	}

	version := p.DefaultVersion
	if permissionVersion != nil {
		version = *permissionVersion
	}

	pv, exists := p.Versions[version]
	if !exists {
		return nil, nil, fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	pvCopy := *pv

	return clonePermission(p), &pvCopy, nil
}

// AssociateResourceSharePermission associates a managed permission with a resource share.
func (b *InMemoryBackend) AssociateResourceSharePermission(
	shareARN, permissionARN string,
	replace bool,
	permissionVersion *int32,
) error {
	b.mu.Lock("AssociateResourceSharePermission")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	version := p.DefaultVersion
	if permissionVersion != nil {
		version = *permissionVersion
	}

	if _, exists := p.Versions[version]; !exists {
		return fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	if b.sharePermissions[shareARN] == nil {
		b.sharePermissions[shareARN] = make(map[string]int32)
	}

	if replace {
		b.sharePermissions[shareARN] = map[string]int32{permissionARN: version}
	} else {
		b.sharePermissions[shareARN][permissionARN] = version
	}

	return nil
}

// DisassociateResourceSharePermission removes a managed permission from a resource share.
func (b *InMemoryBackend) DisassociateResourceSharePermission(
	shareARN, permissionARN string,
) error {
	b.mu.Lock("DisassociateResourceSharePermission")
	defer b.mu.Unlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	perms := b.sharePermissions[shareARN]
	if perms == nil {
		return nil
	}

	delete(perms, permissionARN)

	return nil
}

// ListResourceSharePermissions returns the permissions associated with a resource share,
// sorted by ARN for deterministic output.
func (b *InMemoryBackend) ListResourceSharePermissions(shareARN string) []*Permission {
	b.mu.RLock("ListResourceSharePermissions")
	defer b.mu.RUnlock()

	permARNs := b.sharePermissions[shareARN]
	result := make([]*Permission, 0, len(permARNs))

	for pARN := range permARNs {
		p, ok := b.permissions[pARN]
		if !ok || p.Deleted {
			continue
		}

		result = append(result, clonePermission(p))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ARN < result[j].ARN })

	return result
}

// AddInvitationInternal adds a pre-built invitation for testing or seeding.
func (b *InMemoryBackend) AddInvitationInternal(inv *ResourceShareInvitation) {
	b.mu.Lock("AddInvitationInternal")
	defer b.mu.Unlock()

	b.invitations[inv.InvitationARN] = inv
}

// AcceptResourceShareInvitation accepts a pending resource share invitation.
func (b *InMemoryBackend) AcceptResourceShareInvitation(
	invitationARN string,
) (*ResourceShareInvitation, error) {
	b.mu.Lock("AcceptResourceShareInvitation")
	defer b.mu.Unlock()

	inv, ok := b.invitations[invitationARN]
	if !ok {
		return nil, fmt.Errorf("%w: invitation %s not found", ErrInvitationNotFound, invitationARN)
	}

	switch inv.Status {
	case invitationStatusAccepted:
		return nil, fmt.Errorf("%w: invitation %s already accepted", ErrInvitationAlreadyAccepted, invitationARN)
	case invitationStatusRejected:
		return nil, fmt.Errorf("%w: invitation %s already rejected", ErrInvitationAlreadyRejected, invitationARN)
	case invitationStatusExpired:
		return nil, fmt.Errorf("%w: invitation %s has expired", ErrInvitationExpired, invitationARN)
	}

	inv.Status = invitationStatusAccepted
	inv.LastUpdatedTime = time.Now()

	return cloneInvitation(inv), nil
}

// createInvitationLocked creates a pending invitation without acquiring a lock.
// Caller must hold the write lock.
func (b *InMemoryBackend) createInvitationLocked(shareARN, shareNm, receiverAcctID string) {
	invID := uuid.NewString()
	invARN := b.invitationARN(invID)
	now := time.Now()

	b.invitations[invARN] = &ResourceShareInvitation{
		InvitationARN:     invARN,
		ResourceShareARN:  shareARN,
		ResourceShareName: shareNm,
		SenderAccountID:   b.accountID,
		ReceiverAccountID: receiverAcctID,
		Status:            invitationStatusPending,
		CreationTime:      now,
		LastUpdatedTime:   now,
	}
}

// principalReceiverAccountID extracts the effective AWS account ID from a principal string.
// For 12-digit account IDs the string itself is returned; for ARNs the account field is extracted.
func principalReceiverAccountID(principal string) string {
	if len(principal) == accountIDLen {
		return principal
	}

	// ARN format: arn:partition:service:region:account-id:...
	parts := strings.SplitN(principal, ":", arnPartCountPrincipal)
	if len(parts) >= arnAccountIdx+1 && parts[0] == "arn" {
		return parts[arnAccountIdx]
	}

	return principal
}

// CreateInvitation creates a pending invitation for a resource share.
// This is a helper for the mock that mirrors what AWS does when AssociateResourceShare is called with principals.
func (b *InMemoryBackend) CreateInvitation(
	shareARN, shareNm, senderAcctID, receiverAcctID string,
) *ResourceShareInvitation {
	b.mu.Lock("CreateInvitation")
	defer b.mu.Unlock()

	invID := uuid.NewString()
	invARN := b.invitationARN(invID)
	now := time.Now()

	inv := &ResourceShareInvitation{
		InvitationARN:     invARN,
		ResourceShareARN:  shareARN,
		ResourceShareName: shareNm,
		SenderAccountID:   senderAcctID,
		ReceiverAccountID: receiverAcctID,
		Status:            invitationStatusPending,
		CreationTime:      now,
		LastUpdatedTime:   now,
	}
	b.invitations[invARN] = inv

	return cloneInvitation(inv)
}

// GetResourceShareInvitations returns invitations filtered by ARN or resource share ARN,
// sorted by creation time (oldest first) for deterministic output.
func (b *InMemoryBackend) GetResourceShareInvitations(
	invitationARNs, shareARNs []string,
) []*ResourceShareInvitation {
	b.mu.RLock("GetResourceShareInvitations")
	defer b.mu.RUnlock()

	arnSet := make(map[string]struct{}, len(invitationARNs))

	for _, a := range invitationARNs {
		arnSet[a] = struct{}{}
	}

	shareSet := make(map[string]struct{}, len(shareARNs))

	for _, s := range shareARNs {
		shareSet[s] = struct{}{}
	}

	result := make([]*ResourceShareInvitation, 0, len(b.invitations))

	for _, inv := range b.invitations {
		if len(arnSet) > 0 {
			if _, ok := arnSet[inv.InvitationARN]; !ok {
				continue
			}
		}

		if len(shareSet) > 0 {
			if _, ok := shareSet[inv.ResourceShareARN]; !ok {
				continue
			}
		}

		result = append(result, cloneInvitation(inv))
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreationTime.Before(result[j].CreationTime)
	})

	return result
}

// GetResourcePolicies returns resource-based policy documents for shared resources.
// In the mock, we return empty strings since we do not track actual resource policies.
func (b *InMemoryBackend) GetResourcePolicies(resourceARNs []string) []string {
	b.mu.RLock("GetResourcePolicies")
	defer b.mu.RUnlock()

	result := make([]string, 0, len(resourceARNs))

	for range resourceARNs {
		result = append(result, "{}")
	}

	return result
}

// ListPermissions returns all non-deleted customer-managed permissions,
// optionally filtered by resource type, sorted by ARN.
func (b *InMemoryBackend) ListPermissions(resourceType string) []*Permission {
	b.mu.RLock("ListPermissions")
	defer b.mu.RUnlock()

	result := make([]*Permission, 0, len(b.permissions))

	for _, p := range b.permissions {
		if p.Deleted {
			continue
		}

		if resourceType != "" && p.ResourceType != resourceType {
			continue
		}

		result = append(result, clonePermission(p))
	}

	sort.Slice(result, func(i, j int) bool { return result[i].ARN < result[j].ARN })

	return result
}

// ListPermissionVersions returns all versions of a permission, sorted ascending by version number.
func (b *InMemoryBackend) ListPermissionVersions(
	permissionARN string,
) ([]*PermissionVersion, error) {
	b.mu.RLock("ListPermissionVersions")
	defer b.mu.RUnlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	result := make([]*PermissionVersion, 0, len(p.Versions))

	for _, pv := range p.Versions {
		pvCopy := *pv
		result = append(result, &pvCopy)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].Version < result[j].Version })

	return result, nil
}

// ListPermissionAssociations returns all share-permission associations filtered optionally
// by permissionARN, sorted by share ARN + permission ARN.
func (b *InMemoryBackend) ListPermissionAssociations(
	permissionARN string,
) []SharePermissionAssociation {
	b.mu.RLock("ListPermissionAssociations")
	defer b.mu.RUnlock()

	result := make([]SharePermissionAssociation, 0, len(b.sharePermissions))

	for shareARN, perms := range b.sharePermissions {
		for pARN, ver := range perms {
			if permissionARN != "" && pARN != permissionARN {
				continue
			}

			result = append(result, SharePermissionAssociation{
				ShareARN:      shareARN,
				PermissionARN: pARN,
				Version:       ver,
			})
		}
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].ShareARN != result[j].ShareARN {
			return result[i].ShareARN < result[j].ShareARN
		}

		return result[i].PermissionARN < result[j].PermissionARN
	})

	return result
}

// SharePermissionAssociation represents a share-permission link for ListPermissionAssociations.
type SharePermissionAssociation struct {
	ShareARN      string
	PermissionARN string
	Version       int32
}

// ListResources returns resources (resource-type associations) for shares, optionally filtered
// by resource owner, share ARN, or resource type. Sorted by ARN.
func (b *InMemoryBackend) ListResources(
	_ /* resourceOwner */, shareARN, _ /* resourceType */ string,
) []*ResourceShareAssociation {
	b.mu.RLock("ListResources")
	defer b.mu.RUnlock()

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if a.AssociationType != associationTypeResource {
			continue
		}

		if a.Status == associationStatusDisassociated {
			continue
		}

		if shareARN != "" && a.ResourceShareARN != shareARN {
			continue
		}

		result = append(result, cloneAssociation(a))
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].AssociatedEntity < result[j].AssociatedEntity },
	)

	return result
}

// ListPrincipals returns principal associations for shares, optionally filtered
// by resource owner or share ARN. Sorted by associated entity.
func (b *InMemoryBackend) ListPrincipals(
	_ /* resourceOwner */, shareARN string,
) []*ResourceShareAssociation {
	b.mu.RLock("ListPrincipals")
	defer b.mu.RUnlock()

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if a.AssociationType != associationTypePrincipal {
			continue
		}

		if a.Status == associationStatusDisassociated {
			continue
		}

		if shareARN != "" && a.ResourceShareARN != shareARN {
			continue
		}

		result = append(result, cloneAssociation(a))
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].AssociatedEntity < result[j].AssociatedEntity },
	)

	return result
}

// ListPendingInvitationResources returns the resource associations for the resource share
// associated with the given invitation, filtered to resources that are in an active state.
func (b *InMemoryBackend) ListPendingInvitationResources(
	invitationARN string,
) ([]*ResourceShareAssociation, error) {
	b.mu.RLock("ListPendingInvitationResources")
	defer b.mu.RUnlock()

	inv, ok := b.invitations[invitationARN]
	if !ok {
		return nil, fmt.Errorf("%w: invitation %s not found", ErrInvitationNotFound, invitationARN)
	}

	result := make([]*ResourceShareAssociation, 0, len(b.associations))

	for _, a := range b.associations {
		if a.ResourceShareARN != inv.ResourceShareARN {
			continue
		}

		if a.AssociationType != associationTypeResource {
			continue
		}

		result = append(result, cloneAssociation(a))
	}

	sort.Slice(
		result,
		func(i, j int) bool { return result[i].AssociatedEntity < result[j].AssociatedEntity },
	)

	return result, nil
}

// SetDefaultPermissionVersion updates the default version of a customer-managed permission.
func (b *InMemoryBackend) SetDefaultPermissionVersion(
	permissionARN string,
	version int32,
) (*Permission, error) {
	b.mu.Lock("SetDefaultPermissionVersion")
	defer b.mu.Unlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	if _, exists := p.Versions[version]; !exists {
		return nil, fmt.Errorf(
			"%w: version %d of permission %s not found",
			ErrPermissionVersionNotFound,
			version,
			permissionARN,
		)
	}

	p.DefaultVersion = version
	p.LastUpdatedTime = time.Now()

	return clonePermission(p), nil
}

// RejectResourceShareInvitation rejects a pending resource share invitation.
func (b *InMemoryBackend) RejectResourceShareInvitation(
	invitationARN string,
) (*ResourceShareInvitation, error) {
	b.mu.Lock("RejectResourceShareInvitation")
	defer b.mu.Unlock()

	inv, ok := b.invitations[invitationARN]
	if !ok {
		return nil, fmt.Errorf("%w: invitation %s not found", ErrInvitationNotFound, invitationARN)
	}

	switch inv.Status {
	case invitationStatusAccepted:
		return nil, fmt.Errorf("%w: invitation %s already accepted", ErrInvitationAlreadyAccepted, invitationARN)
	case invitationStatusRejected:
		return nil, fmt.Errorf("%w: invitation %s already rejected", ErrInvitationAlreadyRejected, invitationARN)
	case invitationStatusExpired:
		return nil, fmt.Errorf("%w: invitation %s has expired", ErrInvitationExpired, invitationARN)
	}

	inv.Status = invitationStatusRejected
	inv.LastUpdatedTime = time.Now()

	return cloneInvitation(inv), nil
}

// PromotePermissionCreatedFromPolicy promotes a CREATED_FROM_POLICY permission
// to a CUSTOMER_MANAGED permission with the given name.
// In this mock, it simply returns the existing permission (since all permissions are customer-managed).
func (b *InMemoryBackend) PromotePermissionCreatedFromPolicy(
	permissionARN string, _ /* name */ string,
) (*Permission, error) {
	b.mu.RLock("PromotePermissionCreatedFromPolicy")
	defer b.mu.RUnlock()

	p, ok := b.permissions[permissionARN]
	if !ok || p.Deleted {
		return nil, fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, permissionARN)
	}

	return clonePermission(p), nil
}

// PromoteResourceShareCreatedFromPolicy promotes a resource share to standard feature set.
// In this mock, it simply returns the existing share unchanged.
func (b *InMemoryBackend) PromoteResourceShareCreatedFromPolicy(
	shareARN string,
) (*ResourceShare, error) {
	b.mu.RLock("PromoteResourceShareCreatedFromPolicy")
	defer b.mu.RUnlock()

	rs, ok := b.resourceShares[shareARN]
	if !ok || rs.Status == statusDeleted {
		return nil, fmt.Errorf("%w: resource share %s not found", ErrNotFound, shareARN)
	}

	return cloneResourceShare(rs), nil
}

// ReplacePermissionAssociations replaces all associations using fromPermissionARN
// with toPermissionARN across all resource shares.
// Returns a work ID for ListReplacePermissionAssociationsWork.
func (b *InMemoryBackend) ReplacePermissionAssociations(
	fromPermissionARN, toPermissionARN string,
) (string, error) {
	b.mu.Lock("ReplacePermissionAssociations")
	defer b.mu.Unlock()

	_, fromOK := b.permissions[fromPermissionARN]
	if !fromOK {
		return "", fmt.Errorf(
			"%w: permission %s not found",
			ErrPermissionNotFound,
			fromPermissionARN,
		)
	}

	toP, toOK := b.permissions[toPermissionARN]
	if !toOK || toP.Deleted {
		return "", fmt.Errorf("%w: permission %s not found", ErrPermissionNotFound, toPermissionARN)
	}

	for shareARN, perms := range b.sharePermissions {
		if _, has := perms[fromPermissionARN]; has {
			ver := toP.DefaultVersion
			delete(perms, fromPermissionARN)
			perms[toPermissionARN] = ver
			b.sharePermissions[shareARN] = perms
		}
	}

	return "replace-work-" + fromPermissionARN, nil
}
