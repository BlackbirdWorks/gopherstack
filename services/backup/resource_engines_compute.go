package backup

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	resourceTypeEC2 = "EC2"
	resourceTypeEFS = "EFS"
	resourceTypeRDS = "RDS"

	resourceTypeCloudFormation = "CloudFormation"
	resourceTypeTimestream     = "Timestream"
)

// StackInfo is the CloudFormation stack a backup captures; NoEcho parameters are not captured.
type StackInfo struct {
	Parameters map[string]string `json:"parameters,omitempty"`
	Name       string            `json:"name"`
	Template   string            `json:"template"`
}

// StackService is the CloudFormation surface used to snapshot and restore stacks.
type StackService interface {
	DescribeStack(id string) (StackInfo, bool)
	// CreateStack returns the new stack's ID.
	CreateStack(ctx context.Context, info StackInfo) (string, error)
}

// TableInfo is the Timestream table a backup captures; Config is the table's retention and write settings.
type TableInfo struct {
	Database string         `json:"database"`
	Table    string         `json:"table"`
	Config   jsontext.Value `json:"config,omitempty"`
}

// TableService is the Timestream surface used to snapshot and restore tables.
type TableService interface {
	DescribeTable(database, table string) (TableInfo, bool)
	// CreateTable returns the new table's ARN.
	CreateTable(info TableInfo) (string, error)
}

// InstanceInfo is the EC2 instance shape a backup captures.
type InstanceInfo struct {
	ImageID      string `json:"imageId"`
	InstanceType string `json:"instanceType"`
	SubnetID     string `json:"subnetId"`
}

// InstanceService is the EC2 surface used to snapshot and restore instances.
type InstanceService interface {
	DescribeInstance(id string) (InstanceInfo, bool)
	LaunchInstance(info InstanceInfo) (string, error)
}

// FileSystemInfo is the EFS file system configuration a backup captures.
type FileSystemInfo struct {
	PerformanceMode          string  `json:"performanceMode"`
	ThroughputMode           string  `json:"throughputMode"`
	KmsKeyID                 string  `json:"kmsKeyId,omitempty"`
	Name                     string  `json:"name,omitempty"`
	ProvisionedThroughputMib float64 `json:"provisionedThroughputMib,omitempty"`
	Encrypted                bool    `json:"encrypted"`
}

// FileSystemService is the EFS surface used to snapshot and restore file systems.
type FileSystemService interface {
	DescribeFileSystem(ctx context.Context, id string) (FileSystemInfo, bool)
	// CreateFileSystem returns the new file system's ID.
	CreateFileSystem(ctx context.Context, info FileSystemInfo) (string, error)
}

// DBInstanceInfo is the RDS DB instance configuration a backup captures.
type DBInstanceInfo struct {
	Identifier       string `json:"identifier"`
	Engine           string `json:"engine"`
	EngineVersion    string `json:"engineVersion,omitempty"`
	InstanceClass    string `json:"instanceClass"`
	DBName           string `json:"dbName,omitempty"`
	MasterUsername   string `json:"masterUsername,omitempty"`
	AllocatedStorage int    `json:"allocatedStorage"`
}

// DBInstanceService is the RDS surface used to snapshot and restore DB instances.
type DBInstanceService interface {
	DescribeDBInstance(id string) (DBInstanceInfo, bool)
	// CreateDBInstance returns the new instance's ARN.
	CreateDBInstance(info DBInstanceInfo) (string, error)
}

// SetInstanceService wires EC2 so instances are snapshotted at backup time and relaunched on restore.
func (b *InMemoryBackend) SetInstanceService(svc InstanceService, region, accountID string) {
	b.mu.Lock("SetInstanceService")
	defer b.mu.Unlock()

	b.engines[resourceTypeEC2] = &ec2Engine{svc: svc, region: region, accountID: accountID}
}

// SetFileSystemService wires EFS so file systems are recreated on restore.
func (b *InMemoryBackend) SetFileSystemService(svc FileSystemService, region, accountID string) {
	b.mu.Lock("SetFileSystemService")
	defer b.mu.Unlock()

	b.engines[resourceTypeEFS] = &efsEngine{svc: svc, region: region, accountID: accountID}
}

// SetDBInstanceService wires RDS so DB instances are recreated on restore.
func (b *InMemoryBackend) SetDBInstanceService(svc DBInstanceService) {
	b.mu.Lock("SetDBInstanceService")
	defer b.mu.Unlock()

	b.engines[resourceTypeRDS] = &rdsEngine{svc: svc}
}

type ec2Engine struct {
	svc       InstanceService
	region    string
	accountID string
}

func (e *ec2Engine) capture(_ context.Context, resourceArn string) (jsontext.Value, error) {
	info, ok := e.svc.DescribeInstance(resourceNameFromARN(resourceArn))
	if !ok {
		return nil, nil
	}

	return json.Marshal(info, jsonx.V1())
}

func (e *ec2Engine) restore(_ context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var info InstanceInfo
	if err := json.Unmarshal(data, &info, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode EC2 snapshot: %w", err)
	}

	if v := md["instanceType"]; v != "" {
		info.InstanceType = v
	}

	if v := md["subnetId"]; v != "" {
		info.SubnetID = v
	}

	id, err := e.svc.LaunchInstance(info)
	if err != nil {
		return "", fmt.Errorf("launch instance: %w", err)
	}

	return arn.Build("ec2", e.region, e.accountID, "instance/"+id), nil
}

type efsEngine struct {
	svc       FileSystemService
	region    string
	accountID string
}

func (e *efsEngine) capture(ctx context.Context, resourceArn string) (jsontext.Value, error) {
	info, ok := e.svc.DescribeFileSystem(ctx, resourceNameFromARN(resourceArn))
	if !ok {
		return nil, nil
	}

	return json.Marshal(info, jsonx.V1())
}

func (e *efsEngine) restore(ctx context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var info FileSystemInfo
	if err := json.Unmarshal(data, &info, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode EFS snapshot: %w", err)
	}

	if v := md["performanceMode"]; v != "" {
		info.PerformanceMode = v
	}

	if v := md["kmsKeyId"]; v != "" {
		info.KmsKeyID, info.Encrypted = v, true
	}

	id, err := e.svc.CreateFileSystem(ctx, info)
	if err != nil {
		return "", fmt.Errorf("create file system: %w", err)
	}

	return arn.Build("elasticfilesystem", e.region, e.accountID, "file-system/"+id), nil
}

type rdsEngine struct{ svc DBInstanceService }

func (e *rdsEngine) capture(_ context.Context, resourceArn string) (jsontext.Value, error) {
	info, ok := e.svc.DescribeDBInstance(resourceNameFromARN(resourceArn))
	if !ok {
		return nil, nil
	}

	return json.Marshal(info, jsonx.V1())
}

func (e *rdsEngine) restore(_ context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var info DBInstanceInfo
	if err := json.Unmarshal(data, &info, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode RDS snapshot: %w", err)
	}

	if v := md["DBInstanceIdentifier"]; v != "" {
		info.Identifier = v
	}

	if v := md["DBInstanceClass"]; v != "" {
		info.InstanceClass = v
	}

	arnOut, err := e.svc.CreateDBInstance(info)
	if err != nil {
		return "", fmt.Errorf("create DB instance %s: %w", info.Identifier, err)
	}

	return arnOut, nil
}

// SetStackService wires CloudFormation so stacks are recreated from their template on restore.
func (b *InMemoryBackend) SetStackService(svc StackService) {
	b.mu.Lock("SetStackService")
	defer b.mu.Unlock()

	b.engines[resourceTypeCloudFormation] = &stackEngine{svc: svc, region: b.region, accountID: b.accountID}
}

// SetTableService wires Timestream so tables are recreated on restore.
func (b *InMemoryBackend) SetTableService(svc TableService) {
	b.mu.Lock("SetTableService")
	defer b.mu.Unlock()

	b.engines[resourceTypeTimestream] = &tableEngine{svc: svc}
}

type stackEngine struct {
	svc       StackService
	region    string
	accountID string
}

func (e *stackEngine) capture(_ context.Context, resourceArn string) (jsontext.Value, error) {
	info, ok := e.svc.DescribeStack(resourceArn)
	if !ok {
		return nil, nil
	}

	return json.Marshal(info, jsonx.V1())
}

func (e *stackEngine) restore(ctx context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var info StackInfo
	if err := json.Unmarshal(data, &info, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode CloudFormation snapshot: %w", err)
	}

	info.Name += "-restored"
	if v := md["stackName"]; v != "" {
		info.Name = v
	}

	id, err := e.svc.CreateStack(ctx, info)
	if err != nil {
		return "", fmt.Errorf("create stack %s: %w", info.Name, err)
	}

	return id, nil
}

type tableEngine struct{ svc TableService }

func (e *tableEngine) capture(_ context.Context, resourceArn string) (jsontext.Value, error) {
	db, tbl, ok := timestreamTableFromARN(resourceArn)
	if !ok {
		return nil, nil
	}

	info, found := e.svc.DescribeTable(db, tbl)
	if !found {
		return nil, nil
	}

	return json.Marshal(info, jsonx.V1())
}

func (e *tableEngine) restore(_ context.Context, data jsontext.Value, md map[string]string) (string, error) {
	var info TableInfo
	if err := json.Unmarshal(data, &info, jsonx.V1()); err != nil {
		return "", fmt.Errorf("decode Timestream snapshot: %w", err)
	}

	info.Table += "-restored"
	if v := md["tableName"]; v != "" {
		info.Table = v
	}

	tableArn, err := e.svc.CreateTable(info)
	if err != nil {
		return "", fmt.Errorf("create table %s: %w", info.Table, err)
	}

	return tableArn, nil
}

// timestreamTableFromARN splits arn:aws:timestream:r:a:database/{db}/table/{tbl}.
func timestreamTableFromARN(resourceArn string) (string, string, bool) {
	parts := strings.SplitN(resourceArn, ":", arnResourceParts)
	if len(parts) < arnResourceParts {
		return "", "", false
	}

	seg := strings.Split(parts[5], "/")
	if len(seg) != timestreamARNSegments || seg[0] != "database" || seg[2] != "table" {
		return "", "", false
	}

	return seg[1], seg[3], true
}

const timestreamARNSegments = 4
