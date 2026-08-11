package workspaces

import (
	"context"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildBundlesOps returns the map of workspace bundle operations.
func (h *Handler) buildBundlesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"DescribeWorkspaceBundles": service.WrapOp(h.handleDescribeWorkspaceBundles),
		"CreateWorkspaceBundle":    service.WrapOp(h.handleCreateWorkspaceBundle),
		"DeleteWorkspaceBundle":    service.WrapOp(h.handleDeleteWorkspaceBundle),
		"UpdateWorkspaceBundle":    service.WrapOp(h.handleUpdateWorkspaceBundle),
		"DescribeBundleAssociations": service.WrapOp(
			h.handleDescribeBundleAssociations,
		),
	}
}

// --- DescribeWorkspaceBundles ---

type describeBundlesInput struct {
	Owner     string   `json:"Owner"`
	NextToken string   `json:"NextToken"`
	BundleIDs []string `json:"BundleIds"`
}

type describeBundlesOutput struct {
	NextToken string       `json:"NextToken,omitempty"`
	Bundles   []bundleResp `json:"Bundles"`
}

type bundleComputeTypeResp struct {
	Name string `json:"Name,omitempty"`
}

// bundleStorageResp.Capacity is a string on the wire (workspaces@v1.73.1
// types.go: UserStorage.Capacity, RootStorage.Capacity are both *string),
// not a number -- a real client's response deserialization fails outright
// otherwise.
type bundleStorageResp struct {
	Capacity string `json:"Capacity,omitempty"`
}

type bundleResp struct {
	BundleID    string                `json:"BundleId"`
	Name        string                `json:"Name"`
	Owner       string                `json:"Owner"`
	Description string                `json:"Description"`
	ImageID     string                `json:"ImageId,omitempty"`
	ComputeType bundleComputeTypeResp `json:"ComputeType"`
	UserStorage bundleStorageResp     `json:"UserStorage"`
	RootStorage bundleStorageResp     `json:"RootStorage"`
}

func (h *Handler) handleDescribeWorkspaceBundles(
	ctx context.Context,
	req *describeBundlesInput,
) (*describeBundlesOutput, error) {
	bundles, nextToken, err := h.Backend.DescribeWorkspaceBundles(
		ctx,
		req.BundleIDs,
		req.Owner,
		req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]bundleResp, 0, len(bundles))
	for _, bun := range bundles {
		items = append(items, bundleResp{
			BundleID:    bun.BundleID,
			Name:        bun.Name,
			Owner:       bun.Owner,
			Description: bun.Description,
			ImageID:     bun.ImageID,
			ComputeType: bundleComputeTypeResp{Name: bun.ComputeType.Name},
			UserStorage: bundleStorageResp{Capacity: strconv.Itoa(int(bun.UserStorage.Capacity))},
			RootStorage: bundleStorageResp{Capacity: strconv.Itoa(int(bun.RootStorage.Capacity))},
		})
	}

	return &describeBundlesOutput{Bundles: items, NextToken: nextToken}, nil
}

type createWorkspaceBundleInput struct {
	BundleName        string `json:"BundleName"`
	BundleDescription string `json:"BundleDescription"`
	ImageId           string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	ComputeType       struct {
		Name string `json:"Name"`
	} `json:"ComputeType"`
	UserStorage struct {
		Capacity string `json:"Capacity"`
	} `json:"UserStorage"`
	RootStorage struct {
		Capacity string `json:"Capacity"`
	} `json:"RootStorage"`
	Tags []tagItem `json:"Tags"`
}

type workspaceBundleResp struct {
	BundleId    string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Owner       string `json:"Owner"`
	Description string `json:"Description"`
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	ComputeType struct {
		Name string `json:"Name"`
	} `json:"ComputeType"`
	UserStorage struct {
		Capacity string `json:"Capacity"`
	} `json:"UserStorage"`
	RootStorage struct {
		Capacity string `json:"Capacity"`
	} `json:"RootStorage"`
}

type createWorkspaceBundleOutput struct {
	WorkspaceBundle workspaceBundleResp `json:"WorkspaceBundle"`
}

func (h *Handler) handleCreateWorkspaceBundle(
	_ context.Context, req *createWorkspaceBundleInput,
) (*createWorkspaceBundleOutput, error) {
	bun, err := h.Backend.CreateWorkspaceBundle(
		req.BundleName,
		req.BundleDescription,
		req.ImageId,
		req.ComputeType.Name,
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	resp := workspaceBundleResp{
		BundleId:    bun.BundleID,
		Name:        bun.Name,
		Description: bun.Description,
		ImageId:     bun.ImageID,
	}
	resp.ComputeType.Name = bun.ComputeType

	return &createWorkspaceBundleOutput{WorkspaceBundle: resp}, nil
}

type deleteWorkspaceBundleInput struct {
	BundleId string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteWorkspaceBundle(
	_ context.Context, req *deleteWorkspaceBundleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteWorkspaceBundle(req.BundleId)
}

type updateWorkspaceBundleInput struct {
	BundleId string `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	ImageId  string `json:"ImageId"`  //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleUpdateWorkspaceBundle(
	_ context.Context, req *updateWorkspaceBundleInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateWorkspaceBundle(req.BundleId, req.ImageId)
}

type describeBundleAssociationsInput struct {
	BundleId                string   `json:"BundleId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

// bundleResourceAssociationResp mirrors the real BundleResourceAssociation
// shape; Created/LastUpdatedTime are wire-format epoch-seconds numbers. The
// pointer field is grouped separately below so gofmt's column alignment
// doesn't widen every other field to match its longer type name.
type bundleResourceAssociationResp struct {
	StateReason *associationStateReasonResp `json:"StateReason,omitempty"`

	AssociatedResourceId   string  `json:"AssociatedResourceId,omitempty"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceType string  `json:"AssociatedResourceType,omitempty"`
	BundleId               string  `json:"BundleId,omitempty"` //nolint:revive,staticcheck // existing issue.
	State                  string  `json:"State,omitempty"`
	Created                float64 `json:"Created,omitempty"`
	LastUpdatedTime        float64 `json:"LastUpdatedTime,omitempty"`
}

type describeBundleAssociationsOutput struct {
	Associations []bundleResourceAssociationResp `json:"Associations"`
}

func bundleAssociationToResp(a BundleResourceAssociation) bundleResourceAssociationResp {
	resp := bundleResourceAssociationResp{
		AssociatedResourceId:   a.AssociatedResourceID,
		AssociatedResourceType: a.AssociatedResourceType,
		BundleId:               a.BundleID,
		State:                  a.State,
		Created:                awstime.Epoch(a.Created),
		LastUpdatedTime:        awstime.Epoch(a.LastUpdatedTime),
	}

	if a.StateReasonErrorCode != "" || a.StateReasonErrorMessage != "" {
		resp.StateReason = &associationStateReasonResp{
			ErrorCode:    a.StateReasonErrorCode,
			ErrorMessage: a.StateReasonErrorMessage,
		}
	}

	return resp
}

func (h *Handler) handleDescribeBundleAssociations(
	_ context.Context, req *describeBundleAssociationsInput,
) (*describeBundleAssociationsOutput, error) {
	associations, err := h.Backend.DescribeBundleAssociations(
		req.BundleId, req.AssociatedResourceTypes,
	)
	if err != nil {
		return nil, err
	}

	items := make([]bundleResourceAssociationResp, 0, len(associations))
	for _, a := range associations {
		items = append(items, bundleAssociationToResp(a))
	}

	return &describeBundleAssociationsOutput{Associations: items}, nil
}
