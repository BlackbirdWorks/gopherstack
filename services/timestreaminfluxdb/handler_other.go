package timestreaminfluxdb

import (
	"context"
	"encoding/json"
)

func registerGroupOps(ops map[string]opFunc) {
	ops["CreateDbParameterGroup"] = bind((*Handler).createGroup)
	ops["GetDbParameterGroup"] = bind((*Handler).getGroup)
	ops["ListDbParameterGroups"] = bind((*Handler).listGroups)
}

type createGroupReq struct {
	Tags        map[string]string `json:"tags"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Parameters  json.RawMessage   `json:"parameters"`
}

func (h *Handler) createGroup(ctx context.Context, in *createGroupReq) (*wParameterGroup, error) {
	g, err := h.Backend.CreateDBParameterGroup(ctx, GroupInput{
		Tags: in.Tags, Name: in.Name, Description: in.Description, Parameters: in.Parameters,
	})
	if err != nil {
		return nil, err
	}

	return &g, nil
}

func (h *Handler) getGroup(ctx context.Context, in *identifierReq) (*wParameterGroup, error) {
	g, err := h.Backend.GetDBParameterGroup(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &g, nil
}

func (h *Handler) listGroups(ctx context.Context, in *listIn) (*listOut[wParameterGroupSummary], error) {
	pg, err := h.Backend.ListDBParameterGroups(ctx, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listOut[wParameterGroupSummary]{Items: pg.Data, NextToken: pg.Next}, nil
}

func registerBackupOps(ops map[string]opFunc) {
	ops["CreateDbBackup"] = bind((*Handler).createBackup)
	ops["GetDbBackup"] = bind((*Handler).getBackup)
	ops["ListDbBackups"] = bind((*Handler).listBackups)
	ops["DeleteDbBackup"] = bind((*Handler).deleteBackup)
	ops["RestoreFromDbBackup"] = bind((*Handler).restoreBackup)
}

type createBackupReq struct {
	RetentionDays *int32            `json:"retentionDays"`
	Tags          map[string]string `json:"tags"`
	DBResourceID  string            `json:"dbResourceId"`
	Name          string            `json:"name"`
}

func (h *Handler) createBackup(ctx context.Context, in *createBackupReq) (*wBackup, error) {
	w, err := h.Backend.CreateDBBackup(ctx, BackupInput{
		RetentionDays: in.RetentionDays, Tags: in.Tags, ResourceID: in.DBResourceID, Name: in.Name,
	})
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (h *Handler) getBackup(ctx context.Context, in *identifierReq) (*wBackup, error) {
	w, err := h.Backend.GetDBBackup(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

type listBackupsReq struct {
	DBResourceID string `json:"dbResourceId"`
	listIn
}

func (h *Handler) listBackups(ctx context.Context, in *listBackupsReq) (*listOut[wBackupSummary], error) {
	pg, err := h.Backend.ListDBBackups(ctx, in.DBResourceID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listOut[wBackupSummary]{Items: pg.Data, NextToken: pg.Next}, nil
}

func (h *Handler) deleteBackup(ctx context.Context, in *identifierReq) (*wBackup, error) {
	w, err := h.Backend.DeleteDBBackup(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

type restoreReq struct {
	LogDeliveryConfiguration *logDeliveryIn    `json:"logDeliveryConfiguration"`
	MaintenanceSchedule      *maintenanceIn    `json:"maintenanceSchedule"`
	DBBackupConfigurations   *[]backupConfigIn `json:"dbBackupConfigurations"`
	RestoreToTime            *string           `json:"restoreToTime"`
	Port                     *int32            `json:"port"`
	PubliclyAccessible       *bool             `json:"publiclyAccessible"`
	Tags                     map[string]string `json:"tags"`
	DBBackupID               string            `json:"dbBackupId"`
	Name                     string            `json:"name"`
	DeploymentType           string            `json:"deploymentType"`
	KMSKeyID                 string            `json:"kmsKeyId"`
	NetworkType              string            `json:"networkType"`
	RestoreMode              string            `json:"restoreMode"`
	VpcSubnetIDs             []string          `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs      []string          `json:"vpcSecurityGroupIds"`
}

type restoreResp struct {
	DeploymentType       string `json:"deploymentType"`
	EngineType           string `json:"engineType"`
	ResourceType         string `json:"resourceType"`
	RestoreStatus        string `json:"restoreStatus"`
	RestoredDBResourceID string `json:"restoredDbResourceId"`
}

func (h *Handler) restoreBackup(ctx context.Context, in *restoreReq) (*restoreResp, error) {
	ri := RestoreInput{
		RestoreToTime: parseDateTime(in.RestoreToTime), Port: in.Port, Publicly: in.PubliclyAccessible,
		LogDelivery: logDelivery(in.LogDeliveryConfiguration), Maintenance: maintenance(in.MaintenanceSchedule),
		Tags: in.Tags, BackupID: in.DBBackupID, Name: in.Name, DeploymentType: in.DeploymentType, KMSKeyID: in.KMSKeyID,
		NetworkType: in.NetworkType, RestoreMode: in.RestoreMode, SubnetIDs: in.VpcSubnetIDs,
		SecurityGroupIDs: in.VpcSecurityGroupIDs,
	}

	if in.DBBackupConfigurations != nil {
		ri.HasBackupConfigs = true
		ri.BackupConfigs = backupConfigs(*in.DBBackupConfigurations)
	}

	r, err := h.Backend.RestoreFromDBBackup(ctx, ri)
	if err != nil {
		return nil, err
	}

	return &restoreResp{
		DeploymentType: r.DeploymentType, EngineType: r.EngineType, ResourceType: r.ResourceType,
		RestoreStatus: statusRestoring, RestoredDBResourceID: r.ResourceID,
	}, nil
}

func registerTagOps(ops map[string]opFunc) {
	ops["TagResource"] = bind((*Handler).tagResource)
	ops["UntagResource"] = bind((*Handler).untagResource)
	ops["ListTagsForResource"] = bind((*Handler).listTags)
}

type tagReq struct {
	Tags        map[string]string `json:"tags"`
	ResourceArn string            `json:"resourceArn"`
	TagKeys     []string          `json:"tagKeys"`
}

func (h *Handler) tagResource(ctx context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.TagResource(ctx, in.ResourceArn, in.Tags)
}

func (h *Handler) untagResource(ctx context.Context, in *tagReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys)
}

type listTagsResp struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) listTags(ctx context.Context, in *tagReq) (*listTagsResp, error) {
	t, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{Tags: t}, nil
}
