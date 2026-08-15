package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
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

// workspaceImageResp's Created field is a wire-format epoch-seconds number
// (awsjson1.1 unixTimestamp), not an ISO8601 string -- field-diffed against
// the real WorkspaceImage.Created (*time.Time); see awstime.Epoch.
type workspaceImageResp struct {
	ImageId     string  `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string  `json:"Name"`
	Description string  `json:"Description"`
	State       string  `json:"State"`
	Created     float64 `json:"Created,omitempty"`
}

type createWorkspaceImageOutput struct {
	ImageId     string  `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	Name        string  `json:"Name"`
	Description string  `json:"Description"`
	State       string  `json:"State"`
	Created     float64 `json:"Created,omitempty"`
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
		Created:     awstime.Epoch(img.Created),
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
	IngestionProcess string    `json:"IngestionProcess"`
	Tags             []tagItem `json:"Tags"`
}

type importWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) handleImportWorkspaceImage(
	_ context.Context, req *importWorkspaceImageInput,
) (*importWorkspaceImageOutput, error) {
	id, err := h.Backend.ImportWorkspaceImage(
		req.Ec2ImageId, req.ImageName, req.ImageDescription, req.IngestionProcess, tagsToMap(req.Tags),
	)
	if err != nil {
		return nil, err
	}

	return &importWorkspaceImageOutput{ImageId: id}, nil
}

// imageSourceJSON mirrors the ImageSourceIdentifier tagged union's wire
// shape (workspaces@v1.73.1 serializers.go's
// awsAwsjson11_serializeDocumentImageSourceIdentifier): exactly one key is
// ever present, so all three fields are omitempty on both directions.
type imageSourceJSON struct {
	Ec2ImageId           string `json:"Ec2ImageId,omitempty"`      //nolint:revive,staticcheck // AWS wire name.
	Ec2ImportTaskId      string `json:"Ec2ImportTaskId,omitempty"` //nolint:revive,staticcheck // AWS wire name.
	ImageBuildVersionArn string `json:"ImageBuildVersionArn,omitempty"`
}

func (s *imageSourceJSON) toModel() *ImageSource {
	if s == nil {
		return nil
	}

	return &ImageSource{
		Ec2ImageID:           s.Ec2ImageId,
		Ec2ImportTaskID:      s.Ec2ImportTaskId,
		ImageBuildVersionArn: s.ImageBuildVersionArn,
	}
}

func imageSourceToJSON(s *ImageSource) *imageSourceJSON {
	if s == nil {
		return nil
	}

	return &imageSourceJSON{
		Ec2ImageId:           s.Ec2ImageID,
		Ec2ImportTaskId:      s.Ec2ImportTaskID,
		ImageBuildVersionArn: s.ImageBuildVersionArn,
	}
}

type importCustomWorkspaceImageInput struct {
	ImageSource                    *imageSourceJSON `json:"ImageSource"`
	ImageName                      string           `json:"ImageName"`
	ImageDescription               string           `json:"ImageDescription"`
	ComputeType                    string           `json:"ComputeType"`
	InfrastructureConfigurationArn string           `json:"InfrastructureConfigurationArn"`
	OsVersion                      string           `json:"OsVersion"`
	Platform                       string           `json:"Platform"`
	Protocol                       string           `json:"Protocol"`
}

type importCustomWorkspaceImageOutput struct {
	ImageId string `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State   string `json:"State"`
}

func (h *Handler) handleImportCustomWorkspaceImage(
	_ context.Context, req *importCustomWorkspaceImageInput,
) (*importCustomWorkspaceImageOutput, error) {
	spec := customWorkspaceImageImportSpec{
		ImageSource:                    req.ImageSource.toModel(),
		ComputeType:                    req.ComputeType,
		InfrastructureConfigurationArn: req.InfrastructureConfigurationArn,
		OsVersion:                      req.OsVersion,
		Platform:                       req.Platform,
		Protocol:                       req.Protocol,
	}

	img, err := h.Backend.ImportCustomWorkspaceImage(req.ImageName, req.ImageDescription, spec)
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
			Created:     awstime.Epoch(img.Created),
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
	ImageSource                    *imageSourceJSON `json:"ImageSource,omitempty"`
	ImageId                        string           `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	State                          string           `json:"State"`
	InfrastructureConfigurationArn string           `json:"InfrastructureConfigurationArn,omitempty"`
	Created                        float64          `json:"Created,omitempty"`
}

func (h *Handler) handleDescribeCustomWorkspaceImageImport(
	_ context.Context, req *describeCustomWorkspaceImageImportInput,
) (*describeCustomWorkspaceImageImportOutput, error) {
	img, err := h.Backend.DescribeCustomWorkspaceImageImport(req.ImageId)
	if err != nil {
		return nil, err
	}

	return &describeCustomWorkspaceImageImportOutput{
		ImageId:                        img.ImageID,
		State:                          img.State,
		Created:                        awstime.Epoch(img.Created),
		ImageSource:                    imageSourceToJSON(img.ImageSource),
		InfrastructureConfigurationArn: img.InfrastructureConfigurationArn,
	}, nil
}

type describeImageAssociationsInput struct {
	ImageId                 string   `json:"ImageId"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceTypes []string `json:"AssociatedResourceTypes"`
}

// associationStateReasonResp mirrors the real AssociationStateReason shape,
// shared (structurally) by image and bundle associations.
type associationStateReasonResp struct {
	ErrorCode    string `json:"ErrorCode,omitempty"`
	ErrorMessage string `json:"ErrorMessage,omitempty"`
}

// imageResourceAssociationResp mirrors the real ImageResourceAssociation
// shape; Created/LastUpdatedTime are wire-format epoch-seconds numbers. The
// pointer field is grouped separately below so gofmt's column alignment
// doesn't widen every other field to match its longer type name.
type imageResourceAssociationResp struct {
	StateReason *associationStateReasonResp `json:"StateReason,omitempty"`

	AssociatedResourceId   string  `json:"AssociatedResourceId,omitempty"` //nolint:revive,staticcheck // existing issue.
	AssociatedResourceType string  `json:"AssociatedResourceType,omitempty"`
	ImageId                string  `json:"ImageId,omitempty"` //nolint:revive,staticcheck // existing issue.
	State                  string  `json:"State,omitempty"`
	Created                float64 `json:"Created,omitempty"`
	LastUpdatedTime        float64 `json:"LastUpdatedTime,omitempty"`
}

type describeImageAssociationsOutput struct {
	Associations []imageResourceAssociationResp `json:"Associations"`
}

func imageAssociationToResp(a ImageResourceAssociation) imageResourceAssociationResp {
	resp := imageResourceAssociationResp{
		AssociatedResourceId:   a.AssociatedResourceID,
		AssociatedResourceType: a.AssociatedResourceType,
		ImageId:                a.ImageID,
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

func (h *Handler) handleDescribeImageAssociations(
	_ context.Context, req *describeImageAssociationsInput,
) (*describeImageAssociationsOutput, error) {
	associations, err := h.Backend.DescribeImageAssociations(
		req.ImageId, req.AssociatedResourceTypes,
	)
	if err != nil {
		return nil, err
	}

	items := make([]imageResourceAssociationResp, 0, len(associations))
	for _, a := range associations {
		items = append(items, imageAssociationToResp(a))
	}

	return &describeImageAssociationsOutput{Associations: items}, nil
}
