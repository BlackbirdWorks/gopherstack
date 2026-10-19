package timestreaminfluxdb

import "context"

func registerInstanceOps(ops map[string]opFunc) {
	ops["CreateDbInstance"] = bind((*Handler).createDBInstance)
	ops["GetDbInstance"] = bind((*Handler).getDBInstance)
	ops["ListDbInstances"] = bind((*Handler).listDBInstances)
	ops["UpdateDbInstance"] = bind((*Handler).updateDBInstance)
	ops["DeleteDbInstance"] = bind((*Handler).deleteDBInstance)
	ops["RebootDbInstance"] = bind((*Handler).rebootDBInstance)
	ops["ListDbInstancesForCluster"] = bind((*Handler).listDBInstancesForCluster)
}

type createInstanceReq struct {
	Tags                     map[string]string `json:"tags"`
	LogDeliveryConfiguration *logDeliveryIn    `json:"logDeliveryConfiguration"`
	MaintenanceSchedule      *maintenanceIn    `json:"maintenanceSchedule"`
	DBInstanceType           string            `json:"dbInstanceType"`
	DeploymentType           string            `json:"deploymentType"`
	Name                     string            `json:"name"`
	Username                 string            `json:"username"`
	Password                 string            `json:"password"`
	Organization             string            `json:"organization"`
	Bucket                   string            `json:"bucket"`
	DBParameterGroupID       string            `json:"dbParameterGroupIdentifier"`
	DBStorageType            string            `json:"dbStorageType"`
	KMSKeyID                 string            `json:"kmsKeyId"`
	NetworkType              string            `json:"networkType"`
	VpcSubnetIDs             []string          `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs      []string          `json:"vpcSecurityGroupIds"`
	DBBackupConfigurations   []backupConfigIn  `json:"dbBackupConfigurations"`
	Port                     int32             `json:"port"`
	AllocatedStorage         int32             `json:"allocatedStorage"`
	PubliclyAccessible       bool              `json:"publiclyAccessible"`
}

func (h *Handler) createDBInstance(ctx context.Context, in *createInstanceReq) (*wInstance, error) {
	w, err := h.Backend.CreateDBInstance(ctx, InstanceInput{
		Tags:               in.Tags,
		LogDelivery:        logDelivery(in.LogDeliveryConfiguration),
		Maintenance:        maintenance(in.MaintenanceSchedule),
		Name:               in.Name,
		Username:           in.Username,
		Password:           in.Password,
		Organization:       in.Organization,
		Bucket:             in.Bucket,
		DBInstanceType:     in.DBInstanceType,
		DBStorageType:      in.DBStorageType,
		DeploymentType:     in.DeploymentType,
		NetworkType:        in.NetworkType,
		KMSKeyID:           in.KMSKeyID,
		ParameterGroupID:   in.DBParameterGroupID,
		SubnetIDs:          in.VpcSubnetIDs,
		SecurityGroupIDs:   in.VpcSecurityGroupIDs,
		BackupConfigs:      backupConfigs(in.DBBackupConfigurations),
		AllocatedStorage:   in.AllocatedStorage,
		Port:               in.Port,
		PubliclyAccessible: in.PubliclyAccessible,
	})
	if err != nil {
		return nil, err
	}

	return &w, nil
}

type identifierReq struct {
	Identifier string `json:"identifier"`
}

func (h *Handler) getDBInstance(ctx context.Context, in *identifierReq) (*wInstance, error) {
	w, err := h.Backend.GetDBInstance(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (h *Handler) listDBInstances(ctx context.Context, in *listIn) (*listOut[wInstanceSummary], error) {
	pg, err := h.Backend.ListDBInstances(ctx, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listOut[wInstanceSummary]{Items: pg.Data, NextToken: pg.Next}, nil
}

type listForClusterReq struct {
	DBClusterID string `json:"dbClusterId"`
	listIn
}

func (h *Handler) listDBInstancesForCluster(
	ctx context.Context,
	in *listForClusterReq,
) (*listOut[wInstanceSummary], error) {
	pg, err := h.Backend.ListDBInstancesForCluster(ctx, in.DBClusterID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listOut[wInstanceSummary]{Items: pg.Data, NextToken: pg.Next}, nil
}

type updateInstanceReq struct {
	LogDeliveryConfiguration *logDeliveryIn    `json:"logDeliveryConfiguration"`
	MaintenanceSchedule      *maintenanceIn    `json:"maintenanceSchedule"`
	AllocatedStorage         *int32            `json:"allocatedStorage"`
	Port                     *int32            `json:"port"`
	DBBackupConfigurations   *[]backupConfigIn `json:"dbBackupConfigurations"`
	Identifier               string            `json:"identifier"`
	DBInstanceType           string            `json:"dbInstanceType"`
	DBStorageType            string            `json:"dbStorageType"`
	DeploymentType           string            `json:"deploymentType"`
	DBParameterGroupID       string            `json:"dbParameterGroupIdentifier"`
}

func (h *Handler) updateDBInstance(ctx context.Context, in *updateInstanceReq) (*wInstance, error) {
	u := InstanceUpdate{
		LogDelivery: logDelivery(in.LogDeliveryConfiguration), Maintenance: maintenance(in.MaintenanceSchedule),
		AllocatedStorage: in.AllocatedStorage, Port: in.Port, DBInstanceType: in.DBInstanceType,
		DBStorageType: in.DBStorageType, DeploymentType: in.DeploymentType, ParameterGroupID: in.DBParameterGroupID,
	}

	if in.DBBackupConfigurations != nil {
		u.HasBackupConfigs = true
		u.BackupConfigs = backupConfigs(*in.DBBackupConfigurations)
	}

	w, err := h.Backend.UpdateDBInstance(ctx, in.Identifier, u)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

type deleteInstanceReq struct {
	Identifier             string `json:"identifier"`
	RetainAutomatedBackups bool   `json:"retainAutomatedBackups"`
}

func (h *Handler) deleteDBInstance(ctx context.Context, in *deleteInstanceReq) (*wInstance, error) {
	w, err := h.Backend.DeleteDBInstance(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (h *Handler) rebootDBInstance(ctx context.Context, in *identifierReq) (*wInstance, error) {
	w, err := h.Backend.RebootDBInstance(ctx, in.Identifier)
	if err != nil {
		return nil, err
	}

	return &w, nil
}
