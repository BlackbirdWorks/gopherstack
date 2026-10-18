package cloudhsmv2

import "context"

type emptyResp struct{}

func registerClusterOps(ops map[string]opFunc) {
	ops["CreateCluster"] = bind((*Handler).createCluster)
	ops["DescribeClusters"] = bind((*Handler).describeClusters)
	ops["DeleteCluster"] = bind((*Handler).deleteCluster)
	ops["ModifyCluster"] = bind((*Handler).modifyCluster)
	ops["InitializeCluster"] = bind((*Handler).initializeCluster)
	ops["CreateHsm"] = bind((*Handler).createHsm)
	ops["DeleteHsm"] = bind((*Handler).deleteHsm)
}

func registerBackupOps(ops map[string]opFunc) {
	ops["DescribeBackups"] = bind((*Handler).describeBackups)
	ops["DeleteBackup"] = bind((*Handler).deleteBackup)
	ops["RestoreBackup"] = bind((*Handler).restoreBackup)
	ops["ModifyBackupAttributes"] = bind((*Handler).modifyBackupAttributes)
	ops["CopyBackupToRegion"] = bind((*Handler).copyBackupToRegion)
	ops["PutResourcePolicy"] = bind((*Handler).putResourcePolicy)
	ops["GetResourcePolicy"] = bind((*Handler).getResourcePolicy)
	ops["DeleteResourcePolicy"] = bind((*Handler).deleteResourcePolicy)
}

func registerTagOps(ops map[string]opFunc) {
	ops["TagResource"] = bind((*Handler).tagResource)
	ops["UntagResource"] = bind((*Handler).untagResource)
	ops["ListTags"] = bind((*Handler).listTags)
}

type createClusterReq struct {
	BackupRetentionPolicy *wireRetention `json:"BackupRetentionPolicy"`
	HsmType               string         `json:"HsmType"`
	Mode                  string         `json:"Mode"`
	NetworkType           string         `json:"NetworkType"`
	SourceBackupID        string         `json:"SourceBackupId"`
	SubnetIDs             []string       `json:"SubnetIds"`
	TagList               []wireTag      `json:"TagList"`
}

type clusterResp struct {
	Cluster wireCluster `json:"Cluster"`
}

func (h *Handler) createCluster(ctx context.Context, in *createClusterReq) (*clusterResp, error) {
	c, err := h.Backend.CreateCluster(ctx, CreateClusterInput{
		Retention: in.BackupRetentionPolicy, HsmType: in.HsmType, Mode: in.Mode, NetworkType: in.NetworkType,
		SourceBackupID: in.SourceBackupID, SubnetIDs: in.SubnetIDs, Tags: in.TagList,
	})
	if err != nil {
		return nil, err
	}

	return &clusterResp{Cluster: c}, nil
}

type describeClustersReq struct {
	Filters    map[string][]string `json:"Filters"`
	MaxResults *int32              `json:"MaxResults"`
	NextToken  string              `json:"NextToken"`
}

type describeClustersResp struct {
	NextToken string        `json:"NextToken,omitempty"`
	Clusters  []wireCluster `json:"Clusters"`
}

func (h *Handler) describeClusters(ctx context.Context, in *describeClustersReq) (*describeClustersResp, error) {
	pg, err := h.Backend.DescribeClusters(ctx, in.Filters, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &describeClustersResp{Clusters: pg.Data, NextToken: pg.Next}, nil
}

type clusterIDReq struct {
	ClusterID string `json:"ClusterId"`
}

func (h *Handler) deleteCluster(ctx context.Context, in *clusterIDReq) (*clusterResp, error) {
	c, err := h.Backend.DeleteCluster(ctx, in.ClusterID)
	if err != nil {
		return nil, err
	}

	return &clusterResp{Cluster: c}, nil
}

type modifyClusterReq struct {
	BackupRetentionPolicy *wireRetention `json:"BackupRetentionPolicy"`
	ClusterID             string         `json:"ClusterId"`
	HsmType               string         `json:"HsmType"`
}

func (h *Handler) modifyCluster(ctx context.Context, in *modifyClusterReq) (*clusterResp, error) {
	c, err := h.Backend.ModifyCluster(ctx, ModifyClusterInput{
		ClusterID: in.ClusterID, Retention: in.BackupRetentionPolicy, HsmType: in.HsmType,
	})
	if err != nil {
		return nil, err
	}

	return &clusterResp{Cluster: c}, nil
}

type initializeClusterReq struct {
	ClusterID   string `json:"ClusterId"`
	SignedCert  string `json:"SignedCert"`
	TrustAnchor string `json:"TrustAnchor"`
}

type initializeClusterResp struct {
	State        string `json:"State"`
	StateMessage string `json:"StateMessage,omitempty"`
}

func (h *Handler) initializeCluster(ctx context.Context, in *initializeClusterReq) (*initializeClusterResp, error) {
	state, msg, err := h.Backend.InitializeCluster(ctx, in.ClusterID, in.SignedCert, in.TrustAnchor)
	if err != nil {
		return nil, err
	}

	return &initializeClusterResp{State: state, StateMessage: msg}, nil
}

type createHsmReq struct {
	AvailabilityZone string `json:"AvailabilityZone"`
	ClusterID        string `json:"ClusterId"`
	IPAddress        string `json:"IpAddress"`
}

type createHsmResp struct {
	Hsm wireHsm `json:"Hsm"`
}

func (h *Handler) createHsm(ctx context.Context, in *createHsmReq) (*createHsmResp, error) {
	hsm, err := h.Backend.CreateHsm(ctx, in.ClusterID, in.AvailabilityZone, in.IPAddress)
	if err != nil {
		return nil, err
	}

	return &createHsmResp{Hsm: hsm}, nil
}

type deleteHsmReq struct {
	ClusterID string `json:"ClusterId"`
	EniID     string `json:"EniId"`
	EniIP     string `json:"EniIp"`
	HsmID     string `json:"HsmId"`
}

type deleteHsmResp struct {
	HsmID string `json:"HsmId"`
}

func (h *Handler) deleteHsm(ctx context.Context, in *deleteHsmReq) (*deleteHsmResp, error) {
	id, err := h.Backend.DeleteHsm(ctx, DeleteHsmInput{
		ClusterID: in.ClusterID, HsmID: in.HsmID, EniID: in.EniID, EniIP: in.EniIP,
	})
	if err != nil {
		return nil, err
	}

	return &deleteHsmResp{HsmID: id}, nil
}

type describeBackupsReq struct {
	Filters       map[string][]string `json:"Filters"`
	MaxResults    *int32              `json:"MaxResults"`
	NextToken     string              `json:"NextToken"`
	Shared        bool                `json:"Shared"`
	SortAscending bool                `json:"SortAscending"`
}

type describeBackupsResp struct {
	NextToken string       `json:"NextToken,omitempty"`
	Backups   []wireBackup `json:"Backups"`
}

func (h *Handler) describeBackups(ctx context.Context, in *describeBackupsReq) (*describeBackupsResp, error) {
	pg, err := h.Backend.DescribeBackups(ctx, DescribeBackupsInput{
		Filters: in.Filters, MaxResults: in.MaxResults, Shared: in.Shared, SortAscending: in.SortAscending,
		NextToken: in.NextToken,
	})
	if err != nil {
		return nil, err
	}

	return &describeBackupsResp{Backups: pg.Data, NextToken: pg.Next}, nil
}

type backupIDReq struct {
	BackupID string `json:"BackupId"`
}

type backupResp struct {
	Backup wireBackup `json:"Backup"`
}

func (h *Handler) deleteBackup(ctx context.Context, in *backupIDReq) (*backupResp, error) {
	bk, err := h.Backend.DeleteBackup(ctx, in.BackupID)
	if err != nil {
		return nil, err
	}

	return &backupResp{Backup: bk}, nil
}

func (h *Handler) restoreBackup(ctx context.Context, in *backupIDReq) (*backupResp, error) {
	bk, err := h.Backend.RestoreBackup(ctx, in.BackupID)
	if err != nil {
		return nil, err
	}

	return &backupResp{Backup: bk}, nil
}

type modifyBackupAttributesReq struct {
	NeverExpires *bool  `json:"NeverExpires"`
	BackupID     string `json:"BackupId"`
}

func (h *Handler) modifyBackupAttributes(ctx context.Context, in *modifyBackupAttributesReq) (*backupResp, error) {
	if in.NeverExpires == nil {
		return nil, fail(ErrInvalidRequest, "NeverExpires is required")
	}

	bk, err := h.Backend.ModifyBackupAttributes(ctx, in.BackupID, *in.NeverExpires)
	if err != nil {
		return nil, err
	}

	return &backupResp{Backup: bk}, nil
}

type copyBackupReq struct {
	BackupID          string    `json:"BackupId"`
	DestinationRegion string    `json:"DestinationRegion"`
	TagList           []wireTag `json:"TagList"`
}

type copyBackupResp struct {
	DestinationBackup wireDestinationBackup `json:"DestinationBackup"`
}

func (h *Handler) copyBackupToRegion(ctx context.Context, in *copyBackupReq) (*copyBackupResp, error) {
	d, err := h.Backend.CopyBackupToRegion(ctx, in.BackupID, in.DestinationRegion, in.TagList)
	if err != nil {
		return nil, err
	}

	return &copyBackupResp{DestinationBackup: d}, nil
}

type policyReq struct {
	Policy      string `json:"Policy"`
	ResourceARN string `json:"ResourceArn"`
}

type policyResp struct {
	Policy      string `json:"Policy,omitempty"`
	ResourceARN string `json:"ResourceArn,omitempty"`
}

func (h *Handler) putResourcePolicy(ctx context.Context, in *policyReq) (*policyResp, error) {
	p, err := h.Backend.PutResourcePolicy(ctx, in.ResourceARN, in.Policy)
	if err != nil {
		return nil, err
	}

	return &policyResp{Policy: p, ResourceARN: in.ResourceARN}, nil
}

func (h *Handler) getResourcePolicy(ctx context.Context, in *policyReq) (*policyResp, error) {
	p, err := h.Backend.GetResourcePolicy(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &policyResp{Policy: p}, nil
}

func (h *Handler) deleteResourcePolicy(ctx context.Context, in *policyReq) (*policyResp, error) {
	p, err := h.Backend.DeleteResourcePolicy(ctx, in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return &policyResp{Policy: p, ResourceARN: in.ResourceARN}, nil
}

type tagResourceReq struct {
	ResourceID string    `json:"ResourceId"`
	TagList    []wireTag `json:"TagList"`
	TagKeyList []string  `json:"TagKeyList"`
}

func (h *Handler) tagResource(ctx context.Context, in *tagResourceReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.TagResource(ctx, in.ResourceID, in.TagList)
}

func (h *Handler) untagResource(ctx context.Context, in *tagResourceReq) (*emptyResp, error) {
	return &emptyResp{}, h.Backend.UntagResource(ctx, in.ResourceID, in.TagKeyList)
}

type listTagsReq struct {
	MaxResults *int32 `json:"MaxResults"`
	NextToken  string `json:"NextToken"`
	ResourceID string `json:"ResourceId"`
}

type listTagsResp struct {
	NextToken string    `json:"NextToken,omitempty"`
	TagList   []wireTag `json:"TagList"`
}

func (h *Handler) listTags(ctx context.Context, in *listTagsReq) (*listTagsResp, error) {
	pg, err := h.Backend.ListTags(ctx, in.ResourceID, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}

	return &listTagsResp{TagList: pg.Data, NextToken: pg.Next}, nil
}
