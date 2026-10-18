package timestreaminfluxdb

import "context"

func registerClusterOps(ops map[string]opFunc) {
	ops["CreateDbCluster"] = bind((*Handler).createDBCluster)
	ops["GetDbCluster"] = bind((*Handler).getDBCluster)
	ops["ListDbClusters"] = bind((*Handler).listDBClusters)
	ops["UpdateDbCluster"] = bind((*Handler).updateDBCluster)
	ops["DeleteDbCluster"] = bind((*Handler).deleteDBCluster)
	ops["RebootDbCluster"] = bind((*Handler).rebootDBCluster)
}

type clusterStatusResp struct {
	DBClusterID     string `json:"dbClusterId,omitempty"`
	DBClusterStatus string `json:"dbClusterStatus"`
}

type createClusterReq struct {
	Tags                     map[string]string `json:"tags"`
	LogDeliveryConfiguration *logDeliveryIn    `json:"logDeliveryConfiguration"`
	MaintenanceSchedule      *maintenanceIn    `json:"maintenanceSchedule"`
	AllocatedStorage         *int32            `json:"allocatedStorage"`
	Port                     *int32            `json:"port"`
	Name                     string            `json:"name"`
	Username                 string            `json:"username"`
	Password                 string            `json:"password"`
	Organization             string            `json:"organization"`
	Bucket                   string            `json:"bucket"`
	DBInstanceType           string            `json:"dbInstanceType"`
	DBStorageType            string            `json:"dbStorageType"`
	DeploymentType           string            `json:"deploymentType"`
	NetworkType              string            `json:"networkType"`
	FailoverMode             string            `json:"failoverMode"`
	KMSKeyID                 string            `json:"kmsKeyId"`
	DBParameterGroupID       string            `json:"dbParameterGroupIdentifier"`
	VpcSubnetIDs             []string          `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs      []string          `json:"vpcSecurityGroupIds"`
	DBBackupConfigurations   []backupConfigIn  `json:"dbBackupConfigurations"`
	PubliclyAccessible       bool              `json:"publiclyAccessible"`
}

func (h *Handler) createDBCluster(ctx context.Context, in *createClusterReq) (*clusterStatusResp, error) {
	id, st, err := h.Backend.CreateDBCluster(ctx, ClusterInput{
		Tags:             in.Tags,
		LogDelivery:      logDelivery(in.LogDeliveryConfiguration),
		Maintenance:      maintenance(in.MaintenanceSchedule),
		AllocatedStorage: in.AllocatedStorage,
		Port:             in.Port,
		Name:             in.Name,
		Username:         in.Username,
		Password:         in.Password,
		Organization:     in.Organization,
		Bucket:           in.Bucket,
		DBInstanceType:   in.DBInstanceType,
		DBStorageType:    in.DBStorageType,
		DeploymentType:   in.DeploymentType,
		NetworkType:      in.NetworkType,
		FailoverMode:     in.FailoverMode,
		KMSKeyID:         in.KMSKeyID,
		ParameterGroupID: in.DBParameterGroupID,
		SubnetIDs:        in.VpcSubnetIDs,
		SecurityGroupIDs: in.VpcSecurityGroupIDs,
		BackupConfigs:    backupConfigs(in.DBBackupConfigurations),
		Publicly:         in.PubliclyAccessible,
	})
	if err != nil {
		return nil, err
	}

	return &clusterStatusResp{DBClusterID: id, DBClusterStatus: st}, nil
}

type clusterIDReq struct {
	DBClusterID string `json:"dbClusterId"`
}

func (h *Handler) getDBCluster(ctx context.Context, in *clusterIDReq) (*wCluster, error) {
	w, err := h.Backend.GetDBCluster(ctx, in.DBClusterID)
	if err != nil {
		return nil, err
	}

	return &w, nil
}

func (h *Handler) listDBClusters(ctx context.Context, in *listIn) (*listOut[wClusterSummary], error) {
	pg, err := h.Backend.ListDBClusters(ctx, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listOut[wClusterSummary]{Items: pg.Data, NextToken: pg.Next}, nil
}

type updateClusterReq struct {
	LogDeliveryConfiguration *logDeliveryIn    `json:"logDeliveryConfiguration"`
	MaintenanceSchedule      *maintenanceIn    `json:"maintenanceSchedule"`
	Port                     *int32            `json:"port"`
	DBBackupConfigurations   *[]backupConfigIn `json:"dbBackupConfigurations"`
	DBClusterID              string            `json:"dbClusterId"`
	DBInstanceType           string            `json:"dbInstanceType"`
	DBParameterGroupID       string            `json:"dbParameterGroupIdentifier"`
	FailoverMode             string            `json:"failoverMode"`
}

func (h *Handler) updateDBCluster(ctx context.Context, in *updateClusterReq) (*clusterStatusResp, error) {
	u := ClusterUpdate{
		LogDelivery: logDelivery(in.LogDeliveryConfiguration), Maintenance: maintenance(in.MaintenanceSchedule),
		Port: in.Port, DBInstanceType: in.DBInstanceType, ParameterGroupID: in.DBParameterGroupID,
		FailoverMode: in.FailoverMode,
	}

	if in.DBBackupConfigurations != nil {
		u.HasBackupConfigs = true
		u.BackupConfigs = backupConfigs(*in.DBBackupConfigurations)
	}

	st, err := h.Backend.UpdateDBCluster(ctx, in.DBClusterID, u)
	if err != nil {
		return nil, err
	}

	return &clusterStatusResp{DBClusterStatus: st}, nil
}

type deleteClusterReq struct {
	DBClusterID            string `json:"dbClusterId"`
	RetainAutomatedBackups bool   `json:"retainAutomatedBackups"`
}

func (h *Handler) deleteDBCluster(ctx context.Context, in *deleteClusterReq) (*clusterStatusResp, error) {
	st, err := h.Backend.DeleteDBCluster(ctx, in.DBClusterID)
	if err != nil {
		return nil, err
	}

	return &clusterStatusResp{DBClusterStatus: st}, nil
}

type rebootClusterReq struct {
	DBClusterID string   `json:"dbClusterId"`
	InstanceIDs []string `json:"instanceIds"`
}

func (h *Handler) rebootDBCluster(ctx context.Context, in *rebootClusterReq) (*clusterStatusResp, error) {
	st, err := h.Backend.RebootDBCluster(ctx, in.DBClusterID, in.InstanceIDs)
	if err != nil {
		return nil, err
	}

	return &clusterStatusResp{DBClusterStatus: st}, nil
}
