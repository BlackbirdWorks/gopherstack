package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildImagesOps returns the map of workspace image operations.
func (h *Handler) buildImagesOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CopyWorkspaceImage":          service.WrapOp(h.handleCopyWorkspaceImage),
		"CreateWorkspaceImage":        service.WrapOp(h.handleCreateWorkspaceImage),
		"DeleteWorkspaceImage":        service.WrapOp(h.handleDeleteWorkspaceImage),
		"ImportWorkspaceImage":        service.WrapOp(h.handleImportWorkspaceImage),
		"ImportCustomWorkspaceImage":  service.WrapOp(h.handleImportCustomWorkspaceImage),
		"CreateUpdatedWorkspaceImage": service.WrapOp(h.handleCreateUpdatedWorkspaceImage),
		"DescribeWorkspaceImages":     service.WrapOp(h.handleDescribeWorkspaceImages),
		"DescribeWorkspaceImagePermissions": service.WrapOp(
			h.handleDescribeWorkspaceImagePermissions,
		),
		"UpdateWorkspaceImagePermission": service.WrapOp(
			h.handleUpdateWorkspaceImagePermission,
		),
		"DescribeCustomWorkspaceImageImport": service.WrapOp(
			h.handleDescribeCustomWorkspaceImageImport,
		),
		"DescribeImageAssociations": service.WrapOp(h.handleDescribeImageAssociations),
	}
}

type copyWorkspaceImageInput struct {
	Name          string    `json:"Name"`
	SourceImageId string    `json:"SourceImageId"` //nolint:revive,staticcheck // existing issue.
	SourceRegion  string    `json:"SourceRegion"`
	Description   string    `json:"Description"`
	Tags          []tagItem `json:"Tags"`
}

type copyWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCopyWorkspaceImage(
	_ context.Context, req *copyWorkspaceImageInput,
) (*copyWorkspaceImageOutput, error) {
	id, err := h.Backend.CopyWorkspaceImage(
		req.Name, req.SourceImageId, req.SourceRegion, req.Description, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &copyWorkspaceImageOutput{ImageId: id}, nil
}

type createWorkspaceImageInput struct {
	Name        string    `json:"Name"`
	Description string    `json:"Description"`
	WorkspaceId string    `json:"WorkspaceId"` //nolint:revive,staticcheck // existing issue.
	Tags        []tagItem `json:"Tags"`
}

type workspaceImageResp struct {
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Description string `json:"Description"`
	State       string `json:"State"`
	Created     string `json:"Created,omitempty"`
}

type createWorkspaceImageOutput struct {
	ImageId     string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string `json:"Name"`
	Description string `json:"Description"`
	State       string `json:"State"`
	Created     string `json:"Created,omitempty"`
}

func (h *Handler) handleCreateWorkspaceImage(
	_ context.Context, req *createWorkspaceImageInput,
) (*createWorkspaceImageOutput, error) {
	img, err := h.Backend.CreateWorkspaceImage(
		req.Name,
		req.Description,
		req.WorkspaceId,
		tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createWorkspaceImageOutput{
		ImageId:     img.ImageID,
		Name:        img.Name,
		Description: img.Description,
		State:       img.State,
		Created:     img.Created.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type deleteWorkspaceImageInput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleDeleteWorkspaceImage(
	_ context.Context, req *deleteWorkspaceImageInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteWorkspaceImage(req.ImageId)
}

type importWorkspaceImageInput struct {
	Ec2ImageId       string    `json:"Ec2ImageId"` //nolint:revive,staticcheck // existing issue.
	ImageName        string    `json:"ImageName"`
	ImageDescription string    `json:"ImageDescription"`
	Tags             []tagItem `json:"Tags"`
}

type importWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleImportWorkspaceImage(
	_ context.Context, req *importWorkspaceImageInput,
) (*importWorkspaceImageOutput, error) {
	id, err := h.Backend.ImportWorkspaceImage(
		req.Ec2ImageId, req.ImageName, req.ImageDescription, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &importWorkspaceImageOutput{ImageId: id}, nil
}

type importCustomWorkspaceImageInput struct {
	ImageName        string `json:"ImageName"`
	ImageDescription string `json:"ImageDescription"`
}

type importCustomWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State   string `json:"State"`
}

func (h *Handler) handleImportCustomWorkspaceImage(
	_ context.Context, req *importCustomWorkspaceImageInput,
) (*importCustomWorkspaceImageOutput, error) {
	img, err := h.Backend.ImportCustomWorkspaceImage(req.ImageName, req.ImageDescription)
	if err != nil {
		return nil, err
	}

	return &importCustomWorkspaceImageOutput{ImageId: img.ImageID, State: img.State}, nil
}

type createUpdatedWorkspaceImageInput struct {
	SourceImageId string    `json:"SourceImageId"` //nolint:revive,staticcheck // existing issue.
	Name          string    `json:"Name"`
	Description   string    `json:"Description"`
	Tags          []tagItem `json:"Tags"`
}

type createUpdatedWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleCreateUpdatedWorkspaceImage(
	_ context.Context, req *createUpdatedWorkspaceImageInput,
) (*createUpdatedWorkspaceImageOutput, error) {
	id, err := h.Backend.CreateUpdatedWorkspaceImage(
		req.SourceImageId, req.Name, req.Description, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &createUpdatedWorkspaceImageOutput{ImageId: id}, nil
}

type describeWorkspaceImagesInput struct {
	ImageType  string   `json:"ImageType"`
	NextToken  string   `json:"NextToken"`
	ImageIds   []string `json:"ImageIds"` //nolint:revive // existing issue.
	MaxResults int32    `json:"MaxResults"`
}

type describeWorkspaceImagesOutput struct {
	NextToken string               `json:"NextToken,omitempty"`
	Images    []workspaceImageResp `json:"Images"`
}

func (h *Handler) handleDescribeWorkspaceImages(
	_ context.Context, req *describeWorkspaceImagesInput,
) (*describeWorkspaceImagesOutput, error) {
	images, nextToken, err := h.Backend.DescribeWorkspaceImages(
		req.ImageIds, req.ImageType, req.MaxResults, req.NextToken,
	)
	if err != nil {
		return nil, err
	}

	items := make([]workspaceImageResp, 0, len(images))
	for _, img := range images {
		items = append(items, workspaceImageResp{
			ImageId:     img.ImageID,
			Name:        img.Name,
			Description: img.Description,
			State:       img.State,
			Created:     img.Created.Format("2006-01-02T15:04:05Z"),
		})
	}

	return &describeWorkspaceImagesOutput{Images: items, NextToken: nextToken}, nil
}

type describeWorkspaceImagePermissionsInput struct {
	ImageId    string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	NextToken  string `json:"NextToken"`
	MaxResults int32  `json:"MaxResults"`
}

type imgPermResp struct {
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	ImagePermission struct {
		AllowCopyImage bool `json:"AllowCopyImage"`
	} `json:"ImagePermission"`
}

type describeWorkspaceImagePermissionsOutput struct {
	ImageId          string        `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	NextToken        string        `json:"NextToken,omitempty"`
	ImagePermissions []imgPermResp `json:"ImagePermissions"`
}

func (h *Handler) handleDescribeWorkspaceImagePermissions(
	_ context.Context, req *describeWorkspaceImagePermissionsInput,
) (*describeWorkspaceImagePermissionsOutput, error) {
	imageID, perms, err := h.Backend.DescribeWorkspaceImagePermissions(req.ImageId)
	if err != nil {
		return nil, err
	}

	items := make([]imgPermResp, 0, len(perms))
	for accountID, allowCopy := range perms {
		r := imgPermResp{SharedAccountId: accountID}
		r.ImagePermission.AllowCopyImage = allowCopy
		items = append(items, r)
	}

	return &describeWorkspaceImagePermissionsOutput{
		ImageId:          imageID,
		ImagePermissions: items,
	}, nil
}

type updateWorkspaceImagePermissionInput struct {
	ImageId         string `json:"ImageId"`         //nolint:revive,staticcheck // existing issue.
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
	AllowCopyImage  bool   `json:"AllowCopyImage"`
}

func (h *Handler) handleUpdateWorkspaceImagePermission(
	_ context.Context, req *updateWorkspaceImagePermissionInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UpdateWorkspaceImagePermission(
		req.ImageId, req.SharedAccountId, req.AllowCopyImage,
	)
}

type describeCustomWorkspaceImageImportInput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

type describeCustomWorkspaceImageImportOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State   string `json:"State"`
	Created string `json:"Created,omitempty"`
}

func (h *Handler) handleDescribeCustomWorkspaceImageImport(
	_ context.Context, req *describeCustomWorkspaceImageImportInput,
) (*describeCustomWorkspaceImageImportOutput, error) {
	img, err := h.Backend.DescribeCustomWorkspaceImageImport(req.ImageId)
	if err != nil {
		return nil, err
	}

	return &describeCustomWorkspaceImageImportOutput{
		ImageId: img.ImageID,
		State:   img.State,
		Created: img.Created.Format("2006-01-02T15:04:05Z"),
	}, nil
}

type describeImageAssociationsInput struct {
	ImageId                 string   `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

type describeImageAssociationsOutput struct {
	Associations []any `json:"Associations"`
}

func (h *Handler) handleDescribeImageAssociations(
	_ context.Context, _ *describeImageAssociationsInput,
) (*describeImageAssociationsOutput, error) {
	return &describeImageAssociationsOutput{Associations: []any{}}, nil
}
