package appstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- Image handlers ---

type copyImageInput struct {
	SourceImageName             string `json:"SourceImageName"`
	DestinationImageName        string `json:"DestinationImageName"`
	DestinationRegion           string `json:"DestinationRegion"`
	DestinationImageDescription string `json:"DestinationImageDescription"`
}

func (h *Handler) opCopyImage(_ context.Context, body []byte) (any, error) {
	var req copyImageInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	img, err := h.Backend.CopyImage(
		req.SourceImageName, req.DestinationImageName,
		req.DestinationRegion, req.DestinationImageDescription,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"DestinationImageName": img.Name}, nil
}

type createImportedImageInput struct {
	DryRun               *bool             `json:"DryRun"`
	Tags                 map[string]string `json:"Tags"`
	Name                 string            `json:"Name"`
	Description          string            `json:"Description"`
	DisplayName          string            `json:"DisplayName"`
	AgentSoftwareVersion string            `json:"AgentSoftwareVersion"`
}

func (h *Handler) opCreateImportedImage(_ context.Context, body []byte) (any, error) {
	var req createImportedImageInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	switch req.AgentSoftwareVersion {
	case "", "CURRENT_LATEST", "ALWAYS_LATEST":
	default:
		return nil, fmt.Errorf("%w: invalid AgentSoftwareVersion %q",
			awserr.ErrInvalidParameter, req.AgentSoftwareVersion)
	}

	if req.DryRun != nil && *req.DryRun {
		if imgs, _ := h.Backend.DescribeImages([]string{req.Name}, ""); len(imgs) > 0 {
			return nil, ErrAlreadyExists
		}

		return map[string]any{}, nil
	}

	img, err := h.Backend.CreateImportedImage(req.Name, req.Description, req.DisplayName, req.Tags)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Image": imageToResponse(img)}, nil
}

type createUpdatedImageInput struct {
	DryRun              *bool             `json:"DryRun"`
	NewImageTags        map[string]string `json:"NewImageTags"`
	ExistingImageName   string            `json:"ExistingImageName"`
	NewImageName        string            `json:"NewImageName"`
	NewImageDescription string            `json:"NewImageDescription"`
	NewImageDisplayName string            `json:"NewImageDisplayName"`
}

func (h *Handler) opCreateUpdatedImage(_ context.Context, body []byte) (any, error) {
	var req createUpdatedImageInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if req.DryRun != nil && *req.DryRun {
		if _, err := h.Backend.DescribeImages([]string{req.ExistingImageName}, ""); err != nil {
			return nil, err
		}

		return map[string]any{"canUpdateImage": true}, nil
	}

	img, err := h.Backend.CreateUpdatedImage(
		req.ExistingImageName, req.NewImageName, req.NewImageDescription, req.NewImageDisplayName, req.NewImageTags,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"image": imageToResponse(img)}, nil
}

type deleteImageInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteImage(_ context.Context, body []byte) (any, error) {
	var req deleteImageInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	img, err := h.Backend.DeleteImage(req.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{"Image": imageToResponse(img)}, nil
}

type describeImagesInput struct {
	pageReq
	Type  string   `json:"Type"`
	Names []string `json:"Names"`
	Arns  []string `json:"Arns"`
}

func (h *Handler) opDescribeImages(_ context.Context, body []byte) (any, error) {
	var req describeImagesInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	names := req.Names
	if len(names) == 0 {
		names = req.Arns
	}

	imgs, err := h.Backend.DescribeImages(names, req.Type)
	if err != nil {
		return nil, err
	}

	imgs, next, err := pageOf(imgs, func(v *Image) string { return v.Name }, req.pageReq, maxDescribePageSize)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(imgs))
	for _, img := range imgs {
		resp = append(resp, imageToResponse(img))
	}

	return withNext(map[string]any{"Images": resp}, next), nil
}

type imagePermissionsInput struct {
	ImagePermissions *struct {
		AllowFleet        bool `json:"AllowFleet"`
		AllowImageBuilder bool `json:"AllowImageBuilder"`
	} `json:"ImagePermissions"`
	Name            string `json:"Name"`
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) opUpdateImagePermissions(_ context.Context, body []byte) (any, error) {
	var req imagePermissionsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	allowFleet, allowIB := false, false
	if req.ImagePermissions != nil {
		allowFleet = req.ImagePermissions.AllowFleet
		allowIB = req.ImagePermissions.AllowImageBuilder
	}

	if err := h.Backend.UpdateImagePermissions(req.Name, req.SharedAccountId, allowFleet, allowIB); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type deleteImagePermissionsInput struct {
	Name            string `json:"Name"`
	SharedAccountId string `json:"SharedAccountId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) opDeleteImagePermissions(_ context.Context, body []byte) (any, error) {
	var req deleteImagePermissionsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteImagePermissions(req.Name, req.SharedAccountId); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeImagePermissionsInput struct {
	pageReq
	Name                string   `json:"Name"`
	SharedAwsAccountIds []string `json:"SharedAwsAccountIds"` //nolint:revive // matches real SDK field name (Aws not AWS)
}

func (h *Handler) opDescribeImagePermissions(_ context.Context, body []byte) (any, error) {
	var req describeImagePermissionsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	perms, err := h.Backend.DescribeImagePermissions(req.Name, req.SharedAwsAccountIds)
	if err != nil {
		return nil, err
	}

	perms, next, err := pageOf(
		perms,
		func(v *SharedImagePermissions) string { return v.SharedAccountID },
		req.pageReq,
		maxDescribePageSize,
	)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(perms))
	for _, p := range perms {
		resp = append(resp, map[string]any{
			"sharedAccountId": p.SharedAccountID,
			"imagePermissions": map[string]any{
				"allowFleet":        p.ImagePermissions.AllowFleet,
				"allowImageBuilder": p.ImagePermissions.AllowImageBuilder,
			},
		})
	}

	return withNext(map[string]any{
		"Name":                       req.Name, //nolint:goconst // existing issue.
		"SharedImagePermissionsList": resp,
	}, next), nil
}

// --- ImageBuilder handlers ---

// createImageBuilderInput has no Platform member: the real
// CreateImageBuilderInput (appstream@v1.64.5 api_op_CreateImageBuilder.go)
// declares none -- Platform is CreateFleet/CreateAppBlockBuilder-only; an
// image builder's platform is implied by its source image.
type createImageBuilderInput struct {
	Tags                        map[string]string    `json:"Tags"`
	VpcConfig                   *vpcConfigJSON       `json:"VpcConfig"`
	DomainJoinInfo              *domainJoinInfoJSON  `json:"DomainJoinInfo"`
	RootVolumeConfig            *volumeConfigJSON    `json:"RootVolumeConfig"`
	EnableDefaultInternetAccess *bool                `json:"EnableDefaultInternetAccess"`
	DisableIMDSV1               *bool                `json:"DisableIMDSV1"`
	Name                        string               `json:"Name"`
	Description                 string               `json:"Description"`
	InstanceType                string               `json:"InstanceType"`
	IamRoleArn                  string               `json:"IamRoleArn"`
	AppstreamAgentVersion       string               `json:"AppstreamAgentVersion"`
	AccessEndpoints             []accessEndpointJSON `json:"AccessEndpoints"`
	DisplayName                 string               `json:"DisplayName"`
	ImageName                   string               `json:"ImageName"`
	ImageArn                    string               `json:"ImageArn"`
	SoftwaresToInstall          []string             `json:"SoftwaresToInstall"`
	SoftwaresToUninstall        []string             `json:"SoftwaresToUninstall"`
}

func (h *Handler) opCreateImageBuilder(_ context.Context, body []byte) (any, error) {
	var req createImageBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	opts := CreateImageBuilderOptions{
		Tags:                        req.Tags,
		EnableDefaultInternetAccess: req.EnableDefaultInternetAccess,
		DisableIMDSV1:               req.DisableIMDSV1,
		RootVolumeConfig:            req.RootVolumeConfig.toModel(),
		VpcConfig:                   req.VpcConfig.toModel(),
		DomainJoinInfo:              req.DomainJoinInfo.toModel(),
		AccessEndpoints:             toAccessEndpoints(req.AccessEndpoints),
		IamRoleArn:                  req.IamRoleArn,
		AppstreamAgentVersion:       req.AppstreamAgentVersion,
		DisplayName:                 req.DisplayName,
		ImageName:                   req.ImageName,
		ImageArn:                    req.ImageArn,
		SoftwaresToInstall:          req.SoftwaresToInstall,
		SoftwaresToUninstall:        req.SoftwaresToUninstall,
	}

	ib, err := h.Backend.CreateImageBuilder(req.Name, req.Description, "", req.InstanceType, opts)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ImageBuilder": imageBuilderToResponse(ib)}, nil //nolint:goconst // existing issue.
}

type deleteImageBuilderInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteImageBuilder(_ context.Context, body []byte) (any, error) {
	var req deleteImageBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ib, err := h.Backend.DeleteImageBuilder(req.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ImageBuilder": imageBuilderToResponse(ib)}, nil
}

type describeImageBuildersInput struct {
	pageReq
	Names []string `json:"Names"`
}

func (h *Handler) opDescribeImageBuilders(_ context.Context, body []byte) (any, error) {
	var req describeImageBuildersInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	ibs, err := h.Backend.DescribeImageBuilders(req.Names)
	if err != nil {
		return nil, err
	}

	ibs, next, err := pageOf(ibs, func(v *ImageBuilder) string { return v.Name }, req.pageReq, maxDescribePageSize)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(ibs))
	for _, ib := range ibs {
		resp = append(resp, imageBuilderToResponse(ib))
	}

	return withNext(map[string]any{"ImageBuilders": resp}, next), nil
}

type startImageBuilderInput struct {
	Name                  string `json:"Name"`
	AppstreamAgentVersion string `json:"AppstreamAgentVersion"`
}

// opStartImageBuilder starts an image builder. Real AWS's StartImageBuilderOutput
// carries only the ImageBuilder shape -- no StreamingURL; a prior version of
// this handler invented one, which real SDK clients would never receive.
func (h *Handler) opStartImageBuilder(_ context.Context, body []byte) (any, error) {
	var req startImageBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StartImageBuilder(req.Name, req.AppstreamAgentVersion); err != nil {
		return nil, err
	}

	ibs, err := h.Backend.DescribeImageBuilders([]string{req.Name})
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"ImageBuilder": imageBuilderToResponse(ibs[0]),
	}, nil
}

type stopImageBuilderInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opStopImageBuilder(_ context.Context, body []byte) (any, error) {
	var req stopImageBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ib, err := h.Backend.StopImageBuilder(req.Name)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ImageBuilder": imageBuilderToResponse(ib)}, nil
}

type createImageBuilderStreamingURLInput struct {
	Name     string `json:"Name"`
	Validity int64  `json:"Validity"`
}

func (h *Handler) opCreateImageBuilderStreamingURL(_ context.Context, body []byte) (any, error) {
	var req createImageBuilderStreamingURLInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	url, expires, err := h.Backend.CreateImageBuilderStreamingURL(req.Name, req.Validity)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyStreamingURL: url,
		keyExpires:      awstime.Epoch(expires),
	}, nil
}

// --- Software association handlers ---

type associateSoftwareInput struct {
	ImageBuilderName string   `json:"ImageBuilderName"`
	Software         []string `json:"SoftwareNames"`
}

func (h *Handler) opAssociateSoftwareToImageBuilder(_ context.Context, body []byte) (any, error) {
	var req associateSoftwareInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.AssociateSoftwareToImageBuilder(req.ImageBuilderName, req.Software); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) opDisassociateSoftwareFromImageBuilder(_ context.Context, body []byte) (any, error) {
	var req associateSoftwareInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateSoftwareFromImageBuilder(req.ImageBuilderName, req.Software); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeSoftwareAssociationsInput struct {
	AssociatedResource string `json:"AssociatedResource"`
	pageReq
}

func (h *Handler) opDescribeSoftwareAssociations(_ context.Context, body []byte) (any, error) {
	var req describeSoftwareAssociationsInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	assocs, err := h.Backend.DescribeSoftwareAssociations(req.AssociatedResource)
	if err != nil {
		return nil, err
	}

	assocs, next, err := pageOf(
		assocs,
		func(v SoftwareAssociation) string { return v.Software },
		req.pageReq,
		maxDescribePageSize,
	)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(assocs))
	for _, a := range assocs {
		resp = append(resp, map[string]any{
			"SoftwareName": a.Software,
			keyStatus:      "INSTALLED",
		})
	}

	return withNext(map[string]any{
		"AssociatedResource":   req.AssociatedResource,
		"SoftwareAssociations": resp,
	}, next), nil
}

type startSoftwareDeploymentInput struct {
	ImageBuilderName string `json:"ImageBuilderName"`
}

func (h *Handler) opStartSoftwareDeploymentToImageBuilder(_ context.Context, body []byte) (any, error) {
	var req startSoftwareDeploymentInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StartSoftwareDeploymentToImageBuilder(req.ImageBuilderName); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

// --- ExportImageTask handlers ---
//
// CreateExportImageTask exports a WorkSpaces Applications image to an EC2
// AMI (not to S3 -- a prior version of this handler invented an S3Destination
// request shape and an ExportImageTaskId-only response that don't exist in
// the real CreateExportImageTaskInput/Output).

type createExportImageTaskInput struct {
	TagSpecifications map[string]string `json:"TagSpecifications"`
	ImageName         string            `json:"ImageName"`
	AmiName           string            `json:"AmiName"`
	AmiDescription    string            `json:"AmiDescription"`
	IamRoleArn        string            `json:"IamRoleArn"`
}

func (h *Handler) opCreateExportImageTask(_ context.Context, body []byte) (any, error) {
	var req createExportImageTaskInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	task, err := h.Backend.CreateExportImageTask(
		req.ImageName, req.AmiName, req.AmiDescription, req.IamRoleArn, req.TagSpecifications,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ExportImageTask": exportImageTaskToResponse(task)}, nil
}

type getExportImageTaskInput struct {
	TaskId string `json:"TaskId"` //nolint:revive,staticcheck // existing issue.
}

func (h *Handler) opGetExportImageTask(_ context.Context, body []byte) (any, error) {
	var req getExportImageTaskInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	task, err := h.Backend.GetExportImageTask(req.TaskId)
	if err != nil {
		return nil, err
	}

	return map[string]any{"ExportImageTask": exportImageTaskToResponse(task)}, nil
}

// listExportImageTasksInput evaluates only the State filter; other names are undocumented.
type listExportImageTasksInput struct {
	NextToken  string        `json:"NextToken"`
	Filters    []filterInput `json:"Filters"`
	MaxResults int32         `json:"MaxResults"`
}

type filterInput struct {
	Name   string   `json:"Name"`
	Values []string `json:"Values"`
}

const filterNameState = "State"

func stateFilterValues(filters []filterInput) []string {
	var out []string

	for _, f := range filters {
		if f.Name == filterNameState {
			out = append(out, f.Values...)
		}
	}

	return out
}

func (h *Handler) opListExportImageTasks(_ context.Context, body []byte) (any, error) {
	var req listExportImageTasksInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	tasks, nextToken, err := h.Backend.ListExportImageTasks(
		req.MaxResults,
		req.NextToken,
		stateFilterValues(req.Filters),
	)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(tasks))
	for _, t := range tasks {
		resp = append(resp, exportImageTaskToResponse(t))
	}

	out := map[string]any{"ExportImageTasks": resp}
	if nextToken != "" {
		out["NextToken"] = nextToken
	}

	return out, nil
}

// --- Response helpers ---

func imageToResponse(img *Image) map[string]any {
	resp := map[string]any{
		"Name":         img.Name,
		"Arn":          img.Arn,         //nolint:goconst // existing issue.
		"Description":  img.Description, //nolint:goconst // existing issue.
		"Platform":     img.Platform,    //nolint:goconst // existing issue.
		"Visibility":   img.Visibility,
		"State":        img.State, //nolint:goconst // existing issue.
		"BaseImageArn": img.BaseImageArn,
		"CreatedTime":  awstime.Epoch(img.CreatedTime), //nolint:goconst // existing issue.
		keyTags:        img.Tags,
	}

	if img.DisplayName != "" {
		resp["DisplayName"] = img.DisplayName
	}

	return resp
}

func imageBuilderToResponse(ib *ImageBuilder) map[string]any {
	resp := map[string]any{
		"Name":         ib.Name,
		"Arn":          ib.Arn,
		"Description":  ib.Description,
		"Platform":     ib.Platform,
		"InstanceType": ib.InstanceType, //nolint:goconst // existing issue.
		"State":        ib.State,
		"CreatedTime":  awstime.Epoch(ib.CreatedTime),
		keyTags:        ib.Tags,
	}

	if ib.ImageArn != "" {
		resp["ImageArn"] = ib.ImageArn
	}

	if ib.DisplayName != "" {
		resp["DisplayName"] = ib.DisplayName
	}

	if ib.EnableDefaultInternetAccess != nil {
		resp["EnableDefaultInternetAccess"] = *ib.EnableDefaultInternetAccess
	}

	if ib.DisableIMDSV1 != nil {
		resp["DisableIMDSV1"] = *ib.DisableIMDSV1
	}

	if len(ib.VpcConfig.SecurityGroupIDs) > 0 || len(ib.VpcConfig.SubnetIDs) > 0 {
		resp["VpcConfig"] = vpcConfigToJSON(ib.VpcConfig)
	}

	if ib.DomainJoinInfo.DirectoryName != "" || ib.DomainJoinInfo.OrganizationalUnitDistinguishedName != "" {
		resp["DomainJoinInfo"] = domainJoinInfoToJSON(ib.DomainJoinInfo)
	}

	if len(ib.AccessEndpoints) > 0 {
		resp["AccessEndpoints"] = accessEndpointsToJSON(ib.AccessEndpoints)
	}

	if ib.RootVolumeConfig != nil {
		resp["RootVolumeConfig"] = map[string]any{"VolumeSizeInGb": ib.RootVolumeConfig.VolumeSizeInGb}
	}

	if ib.IamRoleArn != "" {
		resp["IamRoleArn"] = ib.IamRoleArn
	}

	if ib.AppstreamAgentVersion != "" {
		resp["AppstreamAgentVersion"] = ib.AppstreamAgentVersion
	}

	return resp
}

// exportImageTaskToResponse builds the real ExportImageTask wire shape:
// TaskId/ImageArn/AmiName/CreatedDate are always present; AmiDescription,
// AmiId, and TagSpecifications are optional and only included when set.
func exportImageTaskToResponse(t *ExportImageTask) map[string]any {
	resp := map[string]any{
		"TaskId":      t.TaskID,
		"ImageArn":    t.ImageArn,
		"AmiName":     t.AmiName,
		"CreatedDate": awstime.Epoch(t.CreatedDate),
		"State":       t.State,
	}

	if t.AmiDescription != "" {
		resp["AmiDescription"] = t.AmiDescription
	}

	if t.AmiID != "" {
		resp["AmiId"] = t.AmiID
	}

	if len(t.TagSpecifications) > 0 {
		resp["TagSpecifications"] = t.TagSpecifications
	}

	return resp
}
