package app

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	backupbackend "github.com/blackbirdworks/gopherstack/services/backup"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	efsbackend "github.com/blackbirdworks/gopherstack/services/efs"
	rdsbackend "github.com/blackbirdworks/gopherstack/services/rds"
	tswbackend "github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

// wireBackupResources connects Backup to the services it reads from and restores into.
func wireBackupResources(byName map[string]service.Registerable) {
	wireBackupS3(byName["Backup"], byName["S3"])
	wireBackupDynamoDB(byName["Backup"], byName["DynamoDB"])
	wireBackupEC2(byName["Backup"], byName["EC2"])
	wireBackupEFS(byName["Backup"], byName["EFS"])
	wireBackupRDS(byName["Backup"], byName["RDS"])
	wireBackupTables(byName["Backup"], byName["TimestreamWrite"])
}

// wireBackupDynamoDB lets Backup snapshot DynamoDB tables at backup time and recreate them on restore.
func wireBackupDynamoDB(backupReg, ddbReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	ddbH, ddbOk := ddbReg.(*ddbbackend.DynamoDBHandler)
	if !ddbOk {
		return
	}

	if ddbBk, bkOk := ddbH.Backend.(*ddbbackend.InMemoryDB); bkOk {
		backupH.Backend.SetDynamoDBBackend(ddbBk)
	}
}

// wireBackupEC2 lets Backup recreate EBS volumes on restore, in Backup's own region.
func wireBackupEC2(backupReg, ec2Reg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	ec2H, ec2Ok := ec2Reg.(*ec2backend.Handler)
	if !ec2Ok {
		return
	}

	for _, bk := range ec2H.RegionBackends() {
		ec2Bk, bkOk := bk.(*ec2backend.InMemoryBackend)
		if bkOk && ec2Bk.Region == backupH.Backend.Region() {
			backupH.Backend.SetVolumeService(
				ebsVolumeAdapter{bk: ec2Bk}, backupH.Backend.Region(), backupH.Backend.AccountID(),
			)
			backupH.Backend.SetInstanceService(
				instanceAdapter{bk: ec2Bk}, backupH.Backend.Region(), backupH.Backend.AccountID(),
			)

			return
		}
	}
}

type ebsVolumeAdapter struct{ bk *ec2backend.InMemoryBackend }

func (a ebsVolumeAdapter) DescribeVolume(id string) (backupbackend.VolumeInfo, bool) {
	vols := a.bk.DescribeVolumes([]string{id})
	if len(vols) == 0 {
		return backupbackend.VolumeInfo{}, false
	}

	v := vols[0]

	return backupbackend.VolumeInfo{AZ: v.AZ, VolumeType: v.VolumeType, Size: v.Size}, true
}

func (a ebsVolumeAdapter) CreateVolume(info backupbackend.VolumeInfo) (string, error) {
	vol, err := a.bk.CreateVolume(info.AZ, info.VolumeType, info.Size, "")
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return vol.ID, nil
}

type instanceAdapter struct{ bk *ec2backend.InMemoryBackend }

func (a instanceAdapter) DescribeInstance(id string) (backupbackend.InstanceInfo, bool) {
	instances := a.bk.DescribeInstances([]string{id}, "")
	if len(instances) == 0 {
		return backupbackend.InstanceInfo{}, false
	}

	i := instances[0]

	return backupbackend.InstanceInfo{ImageID: i.ImageID, InstanceType: i.InstanceType, SubnetID: i.SubnetID}, true
}

func (a instanceAdapter) LaunchInstance(info backupbackend.InstanceInfo) (string, error) {
	launched, err := a.bk.RunInstances(info.ImageID, info.InstanceType, info.SubnetID, 1)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return launched[0].ID, nil
}

// wireBackupEFS lets Backup recreate EFS file systems on restore.
func wireBackupEFS(backupReg, efsReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	if efsH, efsOk := efsReg.(*efsbackend.Handler); efsOk {
		backupH.Backend.SetFileSystemService(
			fileSystemAdapter{bk: efsH.Backend}, backupH.Backend.Region(), backupH.Backend.AccountID(),
		)
	}
}

type fileSystemAdapter struct{ bk *efsbackend.InMemoryBackend }

func (a fileSystemAdapter) DescribeFileSystem(ctx context.Context, id string) (backupbackend.FileSystemInfo, bool) {
	list, _, err := a.bk.DescribeFileSystems(ctx, id, "", "", 0)
	if err != nil || len(list) == 0 {
		return backupbackend.FileSystemInfo{}, false
	}

	fs := list[0]

	return backupbackend.FileSystemInfo{
		PerformanceMode: fs.PerformanceMode, ThroughputMode: fs.ThroughputMode, KmsKeyID: fs.KmsKeyID,
		Name: fs.Name, ProvisionedThroughputMib: fs.ProvisionedThroughputMib, Encrypted: fs.Encrypted,
	}, true
}

func (a fileSystemAdapter) CreateFileSystem(ctx context.Context, info backupbackend.FileSystemInfo) (string, error) {
	req := efsbackend.CreateFileSystemRequest{
		CreationToken:            awsid.New("backup-restore-"),
		PerformanceMode:          info.PerformanceMode,
		ThroughputMode:           info.ThroughputMode,
		KmsKeyID:                 info.KmsKeyID,
		ProvisionedThroughputMib: info.ProvisionedThroughputMib,
		Encrypted:                info.Encrypted,
	}
	if info.Name != "" {
		req.Tags = map[string]string{"Name": info.Name}
	}

	fs, err := a.bk.CreateFileSystem(ctx, req)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return fs.FileSystemID, nil
}

// wireBackupRDS lets Backup recreate RDS DB instances on restore.
func wireBackupRDS(backupReg, rdsReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	if rdsH, rdsOk := rdsReg.(*rdsbackend.Handler); rdsOk {
		backupH.Backend.SetDBInstanceService(dbInstanceAdapter{bk: rdsH.Backend})
	}
}

type dbInstanceAdapter struct{ bk *rdsbackend.InMemoryBackend }

func (a dbInstanceAdapter) DescribeDBInstance(id string) (backupbackend.DBInstanceInfo, bool) {
	list, err := a.bk.DescribeDBInstances(id)
	if err != nil || len(list) == 0 {
		return backupbackend.DBInstanceInfo{}, false
	}

	i := list[0]

	return backupbackend.DBInstanceInfo{
		Identifier: i.DBInstanceIdentifier, Engine: i.Engine, EngineVersion: i.EngineVersion,
		InstanceClass: i.DBInstanceClass, DBName: i.DBName, MasterUsername: i.MasterUsername,
		AllocatedStorage: i.AllocatedStorage,
	}, true
}

func (a dbInstanceAdapter) CreateDBInstance(info backupbackend.DBInstanceInfo) (string, error) {
	inst, err := a.bk.CreateDBInstance(
		info.Identifier, info.Engine, info.InstanceClass, info.DBName, info.MasterUsername, "",
		info.AllocatedStorage, rdsbackend.DBInstanceOptions{EngineVersion: info.EngineVersion},
	)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return inst.DBInstanceArn, nil
}

// wireBackupStacks lets Backup recreate CloudFormation stacks on restore; CloudFormation registers after the
// other cross-service wiring, so this runs from registerCloudFormationAndDashboard.
func wireBackupStacks(backupReg, cfnReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	if cfnH, cfnOk := cfnReg.(*cfnbackend.Handler); cfnOk {
		if bk, bkOk := cfnH.Backend.(*cfnbackend.InMemoryBackend); bkOk {
			backupH.Backend.SetStackService(stackAdapter{bk: bk})
		}
	}
}

// wireBackupTables lets Backup recreate Timestream tables on restore.
func wireBackupTables(backupReg, tswReg service.Registerable) {
	backupH, ok := backupReg.(*backupbackend.Handler)
	if !ok {
		return
	}

	if tswH, tswOk := tswReg.(*tswbackend.Handler); tswOk {
		backupH.Backend.SetTableService(tableAdapter{bk: tswH.Backend})
	}
}

type stackAdapter struct{ bk *cfnbackend.InMemoryBackend }

func (a stackAdapter) DescribeStack(id string) (backupbackend.StackInfo, bool) {
	st, err := a.bk.DescribeStack(id)
	if err != nil {
		return backupbackend.StackInfo{}, false
	}

	info := backupbackend.StackInfo{Name: st.StackName, Template: st.TemplateBody}

	for _, p := range st.Parameters {
		if p.NoEcho {
			continue
		}

		if info.Parameters == nil {
			info.Parameters = map[string]string{}
		}

		info.Parameters[p.ParameterKey] = p.ParameterValue
	}

	return info, true
}

func (a stackAdapter) CreateStack(ctx context.Context, info backupbackend.StackInfo) (string, error) {
	params := make([]cfnbackend.Parameter, 0, len(info.Parameters))
	for k, v := range info.Parameters {
		params = append(params, cfnbackend.Parameter{ParameterKey: k, ParameterValue: v})
	}

	st, err := a.bk.CreateStack(ctx, info.Name, info.Template, params, cfnbackend.StackOptions{})
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return st.StackID, nil
}

type tableAdapter struct{ bk *tswbackend.InMemoryBackend }

func (a tableAdapter) DescribeTable(database, table string) (backupbackend.TableInfo, bool) {
	t, err := a.bk.DescribeTable(database, table)
	if err != nil {
		return backupbackend.TableInfo{}, false
	}

	cfg, err := json.Marshal(tswbackend.CreateTableInput{
		RetentionProperties: t.RetentionProperties, MagneticStoreWriteProperties: t.MagneticStoreWriteProperties,
		Schema: t.Schema,
	}, jsonx.V1())
	if err != nil {
		return backupbackend.TableInfo{}, false
	}

	return backupbackend.TableInfo{Database: database, Table: table, Config: cfg}, true
}

func (a tableAdapter) CreateTable(info backupbackend.TableInfo) (string, error) {
	var in tswbackend.CreateTableInput
	if len(info.Config) > 0 {
		if err := json.Unmarshal(info.Config, &in, jsonx.V1()); err != nil {
			return "", fmt.Errorf("decode table configuration: %w", err)
		}
	}

	t, err := a.bk.CreateTable(info.Database, info.Table, nil, &in)
	if err != nil {
		return "", err //nolint:wrapcheck // surfaced as the restore job's StatusMessage
	}

	return t.ARN, nil
}
