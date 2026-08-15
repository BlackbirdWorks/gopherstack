package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildConnectionAliasesOps returns the map of connection alias operations.
func (h *Handler) buildConnectionAliasesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateConnectionAlias":       service.WrapOp(h.handleCreateConnectionAlias),
		"DescribeConnectionAliases":   service.WrapOp(h.handleDescribeConnectionAliases),
		"DeleteConnectionAlias":       service.WrapOp(h.handleDeleteConnectionAlias),
		"AssociateConnectionAlias":    service.WrapOp(h.handleAssociateConnectionAlias),
		"DisassociateConnectionAlias": service.WrapOp(h.handleDisassociateConnectionAlias),
		"DescribeConnectionAliasPermissions": service.WrapOp(
			h.handleDescribeConnectionAliasPermissions,
		),
		"UpdateConnectionAliasPermission": service.WrapOp(
			h.handleUpdateConnectionAliasPermission,
		),
	}
}

type createConnectionAliasInput struct {
	ConnectionString string    `json:"ConnectionString"`
	Tags             []tagItem `json:"Tags"`
}

type createConnectionAliasOutput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateConnectionAlias(
	_ context.Context, req *createConnectionAliasInput,
) (*createConnectionAliasOutput, error) {
	id, err := h.Backend.CreateConnectionAlias(req.ConnectionString, tagsToMap(req.Tags))
	if err != nil {
		return nil, err
	}

	return &createConnectionAliasOutput{AliasId: id}, nil
}

type describeConnectionAliasesInput struct {
	ResourceId string   `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string   `json:"NextToken"`
	AliasIds   []string `json:"AliasIds"` //nolint:revive // existing issue.
	Limit      int32    `json:"Limit"`
}

// connAliasResp mirrors the real ConnectionAlias shape (field-diffed against
// deserializers.go's awsAwsjson11_deserializeDocumentConnectionAlias): there
// is no top-level "ConnectionIdentifier" -- that field only exists on the
// nested Associations[] entries (ConnectionAliasAssociation), and on
// AssociateConnectionAliasOutput, a distinct op response.
type connAliasResp struct {
	AliasId          string               `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	ConnectionString string               `json:"ConnectionString"`
	State            string               `json:"State"`
	OwnerAccountId   string               `json:"OwnerAccountId"` //nolint:revive,staticcheck // existing issue.
	Associations     []connAliasAssocResp `json:"Associations,omitempty"`
}

type connAliasAssocResp struct {
	AssociationStatus    string `json:"AssociationStatus"`
	AssociatedAccountId  string `json:"AssociatedAccountId,omitempty"` //nolint:revive,staticcheck // existing issue.
	ResourceId           string `json:"ResourceId,omitempty"`          //nolint:revive,staticcheck // existing issue.
	ConnectionIdentifier string `json:"ConnectionIdentifier,omitempty"`
}

type describeConnectionAliasesOutput struct {
	NextToken         string          `json:"NextToken,omitempty"`
	ConnectionAliases []connAliasResp `json:"ConnectionAliases"`
}

func (h *Handler) handleDescribeConnectionAliases(
	_ context.Context, req *describeConnectionAliasesInput,
) (*describeConnectionAliasesOutput, error) {
	aliases, nextToken, err := h.Backend.DescribeConnectionAliases(
		req.AliasIds, req.ResourceId, req.Limit, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connAliasResp, 0, len(aliases))
	for _, a := range aliases {
		item := connAliasResp{
			AliasId:          a.AliasID,
			ConnectionString: a.ConnectionString,
			State:            a.State,
			OwnerAccountId:   a.OwnerAccountID,
		}
		if a.AssociatedResource != "" {
			item.Associations = []connAliasAssocResp{{
				AssociationStatus:    "ASSOCIATED_WITH_OWNER_ACCOUNT",
				ResourceId:           a.AssociatedResource,
				ConnectionIdentifier: a.ConnectionIdentifier,
			}}
		}
		items = append(items, item)
	}

	return &describeConnectionAliasesOutput{ConnectionAliases: items, NextToken: nextToken}, nil
}

type deleteConnectionAliasInput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteConnectionAlias(
	_ context.Context, req *deleteConnectionAliasInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteConnectionAlias(req.AliasId)
}

type associateConnectionAliasInput struct {
	AliasId    string `json:"AliasId"`    //nolint:revive,staticcheck // existing issue.
	ResourceId string `json:"ResourceId"` //nolint:revive,staticcheck // existing issue.
}

type associateConnectionAliasOutput struct {
	ConnectionIdentifier string `json:"ConnectionIdentifier"`
}

func (h *Handler) handleAssociateConnectionAlias(
	_ context.Context, req *associateConnectionAliasInput,
) (*associateConnectionAliasOutput, error) {
	ci, err := h.Backend.AssociateConnectionAlias(req.AliasId, req.ResourceId)
	if err != nil {
		return nil, err
	}

	return &associateConnectionAliasOutput{ConnectionIdentifier: ci}, nil
}

type disassociateConnectionAliasInput struct {
	AliasId string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDisassociateConnectionAlias(
	_ context.Context, req *disassociateConnectionAliasInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DisassociateConnectionAlias(req.AliasId)
}

type describeConnectionAliasPermissionsInput struct {
	AliasId    string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type connAliasPermResp struct {
	SharedAccountId  string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	AllowAssociation bool   `json:"AllowAssociation"`
}

type describeConnectionAliasPermissionsOutput struct {
	AliasId                    string              `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	NextToken                  string              `json:"NextToken,omitempty"`
	ConnectionAliasPermissions []connAliasPermResp `json:"ConnectionAliasPermissions"`
}

func (h *Handler) handleDescribeConnectionAliasPermissions(
	_ context.Context, req *describeConnectionAliasPermissionsInput,
) (*describeConnectionAliasPermissionsOutput, error) {
	aliasID, perms, nextToken, err := h.Backend.DescribeConnectionAliasPermissions(
		req.AliasId, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]connAliasPermResp, 0, len(perms))
	for _, p := range perms {
		items = append(items, connAliasPermResp{
			SharedAccountId:  p.AccountID,
			AllowAssociation: p.AllowAssociation,
		})
	}

	return &describeConnectionAliasPermissionsOutput{
		AliasId:                    aliasID,
		ConnectionAliasPermissions: items,
		NextToken:                  nextToken,
	}, nil
}

type updateConnectionAliasPermissionInput struct {
	AliasId                   string `json:"AliasId"` //nolint:revive,staticcheck // existing issue.
	ConnectionAliasPermission struct {
		SharedAccountId  string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
		AllowAssociation bool   `json:"AllowAssociation"`
	} `json:"ConnectionAliasPermission"`
}

func (h *Handler) handleUpdateConnectionAliasPermission(
	_ context.Context, req *updateConnectionAliasPermissionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateConnectionAliasPermission(
		req.AliasId,
		req.ConnectionAliasPermission.SharedAccountId,
		req.ConnectionAliasPermission.AllowAssociation,
	)
}
