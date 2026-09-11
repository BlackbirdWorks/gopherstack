package appstream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// --- AppBlock handlers ---

// scriptDetailsJSON mirrors appstream@v1.64.5 types.ScriptDetails's wire shape.
type scriptDetailsJSON struct {
	ScriptS3Location     *s3LocationJSON `json:"ScriptS3Location"`
	ExecutablePath       string          `json:"ExecutablePath"`
	ExecutableParameters string          `json:"ExecutableParameters"`
	TimeoutInSeconds     int             `json:"TimeoutInSeconds"`
}

func (j *scriptDetailsJSON) toModel() *ScriptDetails {
	if j == nil {
		return nil
	}

	return &ScriptDetails{
		ScriptS3Location:     j.ScriptS3Location.toModel(),
		ExecutablePath:       j.ExecutablePath,
		ExecutableParameters: j.ExecutableParameters,
		TimeoutInSeconds:     j.TimeoutInSeconds,
	}
}

func scriptDetailsToJSON(sd *ScriptDetails) map[string]any {
	return map[string]any{
		"ScriptS3Location":     s3LocationToJSON(sd.ScriptS3Location),
		"ExecutablePath":       sd.ExecutablePath,
		"ExecutableParameters": sd.ExecutableParameters,
		"TimeoutInSeconds":     sd.TimeoutInSeconds,
	}
}

type createAppBlockInput struct {
	Tags                   map[string]string  `json:"Tags"`
	SourceS3Location       *s3LocationJSON    `json:"SourceS3Location"`
	SetupScriptDetails     *scriptDetailsJSON `json:"SetupScriptDetails"`
	PostSetupScriptDetails *scriptDetailsJSON `json:"PostSetupScriptDetails"`
	Name                   string             `json:"Name"`
	Description            string             `json:"Description"`
	DisplayName            string             `json:"DisplayName"`
	PackagingType          string             `json:"PackagingType"`
}

func (h *Handler) opCreateAppBlock(_ context.Context, body []byte) (any, error) {
	var req createAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	ab, err := h.Backend.CreateAppBlock(req.Name, req.Description, CreateAppBlockOptions{
		Tags:                   req.Tags,
		SourceS3Location:       req.SourceS3Location.toModel(),
		DisplayName:            req.DisplayName,
		PackagingType:          req.PackagingType,
		SetupScriptDetails:     req.SetupScriptDetails.toModel(),
		PostSetupScriptDetails: req.PostSetupScriptDetails.toModel(),
	})
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlock": appBlockToResponse(ab)}, nil
}

type deleteAppBlockInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteAppBlock(_ context.Context, body []byte) (any, error) {
	var req deleteAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteAppBlock(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlocksInput struct {
	Arns []string `json:"Arns"`
}

func (h *Handler) opDescribeAppBlocks(_ context.Context, body []byte) (any, error) {
	var req describeAppBlocksInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	abs, err := h.Backend.DescribeAppBlocks(req.Arns)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(abs))
	for _, ab := range abs {
		resp = append(resp, appBlockToResponse(ab))
	}

	return map[string]any{"AppBlocks": resp}, nil
}

// --- AppBlockBuilder handlers ---

type appBlockBuilderVpcConfigInput struct {
	SecurityGroupIDs []string `json:"SecurityGroupIds"`
	SubnetIDs        []string `json:"SubnetIds"`
}

type createAppBlockBuilderInput struct {
	Tags         map[string]string              `json:"Tags"`
	VpcConfig    *appBlockBuilderVpcConfigInput `json:"VpcConfig"`
	Name         string                         `json:"Name"`
	Description  string                         `json:"Description"`
	Platform     string                         `json:"Platform"`
	InstanceType string                         `json:"InstanceType"`
}

func (h *Handler) opCreateAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req createAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if req.VpcConfig == nil {
		return nil, fmt.Errorf("%w: VpcConfig is required", awserr.ErrInvalidParameter)
	}

	vpcConfig := VpcConfig{SecurityGroupIDs: req.VpcConfig.SecurityGroupIDs, SubnetIDs: req.VpcConfig.SubnetIDs}

	bb, err := h.Backend.CreateAppBlockBuilder(
		req.Name, req.Description, req.Platform, req.InstanceType, vpcConfig, req.Tags,
	)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bb)}, nil //nolint:goconst // existing issue.
}

type deleteAppBlockBuilderInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opDeleteAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req deleteAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DeleteAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlockBuildersInput struct {
	Names []string `json:"Names"`
}

func (h *Handler) opDescribeAppBlockBuilders(_ context.Context, body []byte) (any, error) {
	var req describeAppBlockBuildersInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders(req.Names)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(bbs))
	for _, bb := range bbs {
		resp = append(resp, appBlockBuilderToResponse(bb))
	}

	return map[string]any{"AppBlockBuilders": resp}, nil
}

type appBlockBuilderNameInput struct {
	Name string `json:"Name"`
}

func (h *Handler) opStartAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StartAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders([]string{req.Name})
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bbs[0])}, nil
}

func (h *Handler) opStopAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderNameInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.StopAppBlockBuilder(req.Name); err != nil {
		return nil, err
	}

	bbs, err := h.Backend.DescribeAppBlockBuilders([]string{req.Name})
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bbs[0])}, nil
}

type updateAppBlockBuilderInput struct {
	VpcConfig    *appBlockBuilderVpcConfigInput `json:"VpcConfig"`
	Name         string                         `json:"Name"`
	Description  string                         `json:"Description"`
	InstanceType string                         `json:"InstanceType"`
}

func (h *Handler) opUpdateAppBlockBuilder(_ context.Context, body []byte) (any, error) {
	var req updateAppBlockBuilderInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	var vpcConfig *VpcConfig
	if req.VpcConfig != nil {
		vpcConfig = &VpcConfig{SecurityGroupIDs: req.VpcConfig.SecurityGroupIDs, SubnetIDs: req.VpcConfig.SubnetIDs}
	}

	bb, err := h.Backend.UpdateAppBlockBuilder(req.Name, req.Description, req.InstanceType, vpcConfig)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilder": appBlockBuilderToResponse(bb)}, nil
}

type createAppBlockBuilderStreamingURLInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
	Validity            int64  `json:"Validity"`
}

func (h *Handler) opCreateAppBlockBuilderStreamingURL(_ context.Context, body []byte) (any, error) {
	var req createAppBlockBuilderStreamingURLInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	url, expires, err := h.Backend.CreateAppBlockBuilderStreamingURL(req.AppBlockBuilderName, req.Validity)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		keyStreamingURL: url,
		keyExpires:      awstime.Epoch(expires),
	}, nil
}

// --- AppBlockBuilder-AppBlock association handlers ---

type appBlockBuilderAppBlockInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
	AppBlockArn         string `json:"AppBlockArn"`
}

func (h *Handler) opAssociateAppBlockBuilderAppBlock(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	assoc, err := h.Backend.AssociateAppBlockBuilderAppBlock(req.AppBlockBuilderName, req.AppBlockArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{"AppBlockBuilderAppBlockAssociation": map[string]any{
		"AppBlockBuilderName": assoc.AppBlockBuilderName,
		keyAppBlockArn:        assoc.AppBlockArn,
	}}, nil
}

func (h *Handler) opDisassociateAppBlockBuilderAppBlock(_ context.Context, body []byte) (any, error) {
	var req appBlockBuilderAppBlockInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
	}

	if err := h.Backend.DisassociateAppBlockBuilderAppBlock(req.AppBlockBuilderName, req.AppBlockArn); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

type describeAppBlockBuilderAppBlockAssociationsInput struct {
	AppBlockBuilderName string `json:"AppBlockBuilderName"`
	AppBlockArn         string `json:"AppBlockArn"`
}

func (h *Handler) opDescribeAppBlockBuilderAppBlockAssociations(_ context.Context, body []byte) (any, error) {
	var req describeAppBlockBuilderAppBlockAssociationsInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return nil, awserr.New(errInvalidParameter, awserr.ErrInvalidParameter)
		}
	}

	assocs, err := h.Backend.DescribeAppBlockBuilderAppBlockAssociations(req.AppBlockBuilderName, req.AppBlockArn)
	if err != nil {
		return nil, err
	}

	resp := make([]any, 0, len(assocs))
	for _, a := range assocs {
		resp = append(resp, map[string]any{
			"AppBlockBuilderName": a.AppBlockBuilderName,
			keyAppBlockArn:        a.AppBlockArn,
			"State":               a.State, //nolint:goconst // existing issue.
		})
	}

	return map[string]any{"AppBlockBuilderAppBlockAssociations": resp}, nil
}

// --- Response helpers ---

func appBlockToResponse(ab *AppBlock) map[string]any {
	resp := map[string]any{
		"Name":             ab.Name,        //nolint:goconst // existing issue.
		"Arn":              ab.Arn,         //nolint:goconst // existing issue.
		"Description":      ab.Description, //nolint:goconst // existing issue.
		"State":            ab.State,
		"CreatedTime":      awstime.Epoch(ab.CreatedTime), //nolint:goconst // existing issue.
		"SourceS3Location": s3LocationToJSON(ab.SourceS3Location),
	}

	if ab.DisplayName != "" {
		resp["DisplayName"] = ab.DisplayName
	}

	if ab.PackagingType != "" {
		resp["PackagingType"] = ab.PackagingType
	}

	if ab.SetupScriptDetails != nil {
		resp["SetupScriptDetails"] = scriptDetailsToJSON(ab.SetupScriptDetails)
	}

	if ab.PostSetupScriptDetails != nil {
		resp["PostSetupScriptDetails"] = scriptDetailsToJSON(ab.PostSetupScriptDetails)
	}

	return resp
}

func appBlockBuilderToResponse(bb *AppBlockBuilder) map[string]any {
	return map[string]any{
		"Name":         bb.Name,
		"Arn":          bb.Arn,
		"Description":  bb.Description,
		"Platform":     bb.Platform,     //nolint:goconst // existing issue.
		"InstanceType": bb.InstanceType, //nolint:goconst // existing issue.
		"State":        bb.State,
		"CreatedTime":  awstime.Epoch(bb.CreatedTime),
		keyTags:        bb.Tags,
		"VpcConfig": map[string]any{
			"SecurityGroupIds": bb.VpcConfig.SecurityGroupIDs,
			"SubnetIds":        bb.VpcConfig.SubnetIDs,
		},
	}
}
