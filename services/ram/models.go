package ram

import (
	"maps"
	"time"
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

// ResourceSharePermissionDetail pairs a managed permission with the specific
// version that is associated with a particular resource share. AWS tracks the
// associated version per (share, permission) pair -- AssociateResourceSharePermission
// can pin a non-default version -- so this must be reported per share rather
// than assumed to be the permission's current default version.
type ResourceSharePermissionDetail struct {
	Permission *Permission
	Version    int32
}

// SharePermissionAssociation represents a share-permission link for ListPermissionAssociations.
type SharePermissionAssociation struct {
	ShareARN      string
	PermissionARN string
	Version       int32
}

// ReplacePermissionAssociationsWork tracks the background task created by a
// ReplacePermissionAssociations call, retrievable via ListReplacePermissionAssociationsWork.
// This mock performs the underlying association swap synchronously, so a work
// item's Status is always terminal (COMPLETED) by the time it is stored --
// there is no separate async completion step to model.
type ReplacePermissionAssociationsWork struct {
	CreationTime          time.Time
	LastUpdatedTime       time.Time
	ID                    string
	FromPermissionARN     string
	ToPermissionARN       string
	Status                string
	StatusMessage         string
	FromPermissionVersion int32
	ToPermissionVersion   int32
}

// cloneReplaceWork returns a deep copy of w.
func cloneReplaceWork(w *ReplacePermissionAssociationsWork) *ReplacePermissionAssociationsWork {
	cp := *w

	return &cp
}
