package timestreaminfluxdb

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"
)

type wS3 struct {
	BucketName string `json:"bucketName"`
	Enabled    bool   `json:"enabled"`
}

type wLogDelivery struct {
	S3Configuration wS3 `json:"s3Configuration"`
}

type wMaintenance struct {
	PreferredMaintenanceWindow string `json:"preferredMaintenanceWindow"`
	Timezone                   string `json:"timezone"`
}

type wBackupConfig struct {
	NextAutomatedBackupTime *string `json:"nextAutomatedBackupTime,omitempty"`
	CustomSchedule          string  `json:"customSchedule,omitempty"`
	Type                    string  `json:"type"`
	RetentionDays           int32   `json:"retentionDays"`
	Enabled                 bool    `json:"enabled"`
}

type wClusterConfig struct {
	DedicatedCompactor   *bool  `json:"dedicatedCompactor,omitempty"`
	IngestQueryInstances *int32 `json:"ingestQueryInstances,omitempty"`
	QueryOnlyInstances   *int32 `json:"queryOnlyInstances,omitempty"`
}

type wInstance struct {
	InfluxAuthSecretARN       *string         `json:"influxAuthParametersSecretArn,omitempty"`
	LastMaintenanceTime       *string         `json:"lastMaintenanceTime,omitempty"`
	NextMaintenanceTime       *string         `json:"nextMaintenanceTime,omitempty"`
	LogDeliveryConfiguration  *wLogDelivery   `json:"logDeliveryConfiguration,omitempty"`
	MaintenanceSchedule       *wMaintenance   `json:"maintenanceSchedule,omitempty"`
	Arn                       string          `json:"arn"`
	ID                        string          `json:"id"`
	Name                      string          `json:"name"`
	AvailabilityZone          string          `json:"availabilityZone,omitempty"`
	SecondaryAvailabilityZone string          `json:"secondaryAvailabilityZone,omitempty"`
	DBClusterID               string          `json:"dbClusterId,omitempty"`
	DBInstanceType            string          `json:"dbInstanceType"`
	DBParameterGroupID        string          `json:"dbParameterGroupIdentifier,omitempty"`
	DBStorageType             string          `json:"dbStorageType"`
	DeploymentType            string          `json:"deploymentType"`
	Endpoint                  string          `json:"endpoint,omitempty"`
	InstanceMode              string          `json:"instanceMode,omitempty"`
	KMSKeyID                  string          `json:"kmsKeyId,omitempty"`
	NetworkType               string          `json:"networkType"`
	Status                    string          `json:"status"`
	VpcSubnetIDs              []string        `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs       []string        `json:"vpcSecurityGroupIds"`
	DBBackupConfigurations    []wBackupConfig `json:"dbBackupConfigurations,omitempty"`
	InstanceModes             []string        `json:"instanceModes,omitempty"`
	AllocatedStorage          int32           `json:"allocatedStorage"`
	Port                      int32           `json:"port"`
	PubliclyAccessible        bool            `json:"publiclyAccessible"`
}

type wInstanceSummary struct {
	Arn              string   `json:"arn"`
	ID               string   `json:"id"`
	Name             string   `json:"name"`
	DBInstanceType   string   `json:"dbInstanceType"`
	DBStorageType    string   `json:"dbStorageType"`
	DeploymentType   string   `json:"deploymentType"`
	Endpoint         string   `json:"endpoint,omitempty"`
	InstanceMode     string   `json:"instanceMode,omitempty"`
	NetworkType      string   `json:"networkType"`
	Status           string   `json:"status"`
	InstanceModes    []string `json:"instanceModes,omitempty"`
	AllocatedStorage int32    `json:"allocatedStorage"`
	Port             int32    `json:"port"`
}

type wCluster struct {
	InfluxAuthSecretARN                 *string         `json:"influxAuthParametersSecretArn,omitempty"`
	LastMaintenanceTime                 *string         `json:"lastMaintenanceTime,omitempty"`
	NextMaintenanceTime                 *string         `json:"nextMaintenanceTime,omitempty"`
	ClusterConfiguration                *wClusterConfig `json:"clusterConfiguration,omitempty"`
	LogDeliveryConfiguration            *wLogDelivery   `json:"logDeliveryConfiguration,omitempty"`
	MaintenanceSchedule                 *wMaintenance   `json:"maintenanceSchedule,omitempty"`
	Arn                                 string          `json:"arn"`
	ID                                  string          `json:"id"`
	Name                                string          `json:"name"`
	DBInstanceType                      string          `json:"dbInstanceType"`
	DBParameterGroupIdentifier          string          `json:"dbParameterGroupIdentifier,omitempty"`
	DBStorageType                       string          `json:"dbStorageType"`
	DeploymentType                      string          `json:"deploymentType"`
	EffectiveDBParameterGroupIdentifier string          `json:"effectiveDbParameterGroupIdentifier,omitempty"`
	Endpoint                            string          `json:"endpoint,omitempty"`
	EngineType                          string          `json:"engineType"`
	FailoverMode                        string          `json:"failoverMode"`
	KMSKeyID                            string          `json:"kmsKeyId,omitempty"`
	NetworkType                         string          `json:"networkType"`
	ReaderEndpoint                      string          `json:"readerEndpoint,omitempty"`
	Status                              string          `json:"status"`
	VpcSubnetIDs                        []string        `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs                 []string        `json:"vpcSecurityGroupIds"`
	DBBackupConfigurations              []wBackupConfig `json:"dbBackupConfigurations,omitempty"`
	AllocatedStorage                    int32           `json:"allocatedStorage"`
	Port                                int32           `json:"port"`
	PubliclyAccessible                  bool            `json:"publiclyAccessible"`
}

type wClusterSummary struct {
	Arn              string `json:"arn"`
	ID               string `json:"id"`
	Name             string `json:"name"`
	DBInstanceType   string `json:"dbInstanceType"`
	DBStorageType    string `json:"dbStorageType"`
	DeploymentType   string `json:"deploymentType"`
	Endpoint         string `json:"endpoint,omitempty"`
	EngineType       string `json:"engineType"`
	NetworkType      string `json:"networkType"`
	ReaderEndpoint   string `json:"readerEndpoint,omitempty"`
	Status           string `json:"status"`
	AllocatedStorage int32  `json:"allocatedStorage"`
	Port             int32  `json:"port"`
}

type wParameterGroup struct {
	Arn         string          `json:"arn"`
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type wParameterGroupSummary struct {
	Arn         string `json:"arn"`
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

type wBackup struct {
	ClusterConfiguration     *wClusterConfig `json:"clusterConfiguration,omitempty"`
	LogDeliveryConfiguration *wLogDelivery   `json:"logDeliveryConfiguration,omitempty"`
	MaintenanceSchedule      *wMaintenance   `json:"maintenanceSchedule,omitempty"`
	ExpiresAfter             string          `json:"expiresAfter,omitempty"`
	KMSKeyID                 string          `json:"kmsKeyId,omitempty"`
	Name                     string          `json:"name"`
	DBInstanceType           string          `json:"dbInstanceType"`
	DBParameterGroupID       string          `json:"dbParameterGroupId,omitempty"`
	DBResourceID             string          `json:"dbResourceId"`
	DBStorageType            string          `json:"dbStorageType"`
	DeploymentType           string          `json:"deploymentType"`
	EngineType               string          `json:"engineType"`
	Arn                      string          `json:"arn"`
	FailoverMode             string          `json:"failoverMode,omitempty"`
	ID                       string          `json:"id"`
	NetworkType              string          `json:"networkType"`
	Status                   string          `json:"status"`
	Type                     string          `json:"type"`
	CreatedAt                string          `json:"createdAt"`
	VpcSubnetIDs             []string        `json:"vpcSubnetIds"`
	VpcSecurityGroupIDs      []string        `json:"vpcSecurityGroupIds"`
	AllocatedStorage         int32           `json:"allocatedStorage"`
	Port                     int32           `json:"port"`
	PubliclyAccessible       bool            `json:"publiclyAccessible"`
}

type wBackupSummary struct {
	Arn            string `json:"arn"`
	ID             string `json:"id"`
	Name           string `json:"name"`
	DBResourceID   string `json:"dbResourceId"`
	DeploymentType string `json:"deploymentType"`
	EngineType     string `json:"engineType"`
	ExpiresAfter   string `json:"expiresAfter,omitempty"`
	KMSKeyID       string `json:"kmsKeyId,omitempty"`
	Status         string `json:"status"`
	Type           string `json:"type"`
	CreatedAt      string `json:"createdAt"`
}

// dateTime renders a smithy date-time, the timestamp format this SDK parses (deserializers.go ParseDateTime).
func dateTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func strPtr(s string) *string {
	if s == "" {
		return nil
	}

	return &s
}

func timePtr(t time.Time) *string {
	if t.IsZero() {
		return nil
	}

	v := dateTime(t)

	return &v
}

func wireLogDelivery(l *LogDelivery) *wLogDelivery {
	if l == nil {
		return nil
	}

	return &wLogDelivery{S3Configuration: wS3{BucketName: l.Bucket, Enabled: l.Enabled}}
}

func wireMaintenance(m *Maintenance) *wMaintenance {
	if m == nil {
		return nil
	}

	return &wMaintenance{PreferredMaintenanceWindow: m.Window, Timezone: m.Timezone}
}

func wireClusterConfig(c *ClusterConfig) *wClusterConfig {
	if c == nil {
		return nil
	}

	return &wClusterConfig{
		DedicatedCompactor: c.DedicatedCompactor, IngestQueryInstances: c.IngestQueryInstances,
		QueryOnlyInstances: c.QueryOnlyInstances,
	}
}

func (b *InMemoryBackend) wireBackupConfigs(cfgs []BackupConfig) []wBackupConfig {
	if len(cfgs) == 0 {
		return nil
	}

	now := b.now()
	out := make([]wBackupConfig, 0, len(cfgs))

	for _, c := range cfgs {
		w := wBackupConfig{
			CustomSchedule: c.CustomSchedule, Type: c.Type, RetentionDays: c.RetentionDays, Enabled: c.Enabled,
		}
		if c.Enabled {
			w.NextAutomatedBackupTime = timePtr(nextBackup(now, c.Type))
		}

		out = append(out, w)
	}

	return out
}

// nextBackup is the next scheduled run for the fixed-cadence backup types; others have no computed time.
func nextBackup(now time.Time, kind string) time.Time {
	now = now.UTC()

	switch kind {
	case "HOURLY":
		return now.Truncate(time.Hour).Add(time.Hour)
	case "DAILY":
		return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
	case "WEEKLY":
		return time.Date(now.Year(), now.Month(), now.Day()+7-int(now.Weekday()), 0, 0, 0, 0, time.UTC)
	case "MONTHLY":
		return time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	}

	return time.Time{}
}

var weekdays = map[string]time.Weekday{ //nolint:gochecknoglobals // immutable lookup
	"Sun": time.Sunday, "Mon": time.Monday, "Tue": time.Tuesday, "Wed": time.Wednesday,
	"Thu": time.Thursday, "Fri": time.Friday, "Sat": time.Saturday,
}

// nextMaintenance returns the start of the next occurrence of the window, in the window's timezone.
func nextMaintenance(now time.Time, m *Maintenance) time.Time {
	if m == nil || m.Window == "" {
		return time.Time{}
	}

	start, _, _ := strings.Cut(m.Window, "-")
	parts := strings.Split(start, ":")

	if len(parts) != 3 { //nolint:mnd // ddd:HH:MM
		return time.Time{}
	}

	loc, err := time.LoadLocation(m.Timezone)
	if err != nil {
		loc = time.UTC
	}

	t := now.In(loc)
	hour, minute := atoi(parts[1]), atoi(parts[2])

	for d := range 8 {
		c := time.Date(t.Year(), t.Month(), t.Day()+d, hour, minute, 0, 0, loc)
		if c.Weekday() == weekdays[parts[0]] && c.After(now) {
			return c.UTC()
		}
	}

	return time.Time{}
}

const decimalBase = 10

func atoi(s string) int {
	n := 0
	for _, r := range s {
		n = n*decimalBase + int(r-'0')
	}

	return n
}

func (b *InMemoryBackend) wireInstance(sc scope, i *DBInstance) wInstance {
	st := b.state(i.Pending)
	p := i.Placement

	w := wInstance{
		Arn:                       i.ARN,
		ID:                        i.ID,
		Name:                      i.Name,
		AvailabilityZone:          i.AZ,
		SecondaryAvailabilityZone: i.StandbyAZ,
		DBClusterID:               i.ClusterID,
		DBInstanceType:            p.DBInstanceType,
		DBParameterGroupID:        p.ParameterGroupID,
		DBStorageType:             p.DBStorageType,
		DeploymentType:            p.DeploymentType,
		InstanceMode:              i.Mode,
		KMSKeyID:                  p.KMSKeyID,
		NetworkType:               p.NetworkType,
		Status:                    st,
		VpcSubnetIDs:              slices.Clone(p.SubnetIDs),
		VpcSecurityGroupIDs:       slices.Clone(p.SecurityGroupIDs),
		AllocatedStorage:          p.AllocatedStorage,
		Port:                      p.Port,
		PubliclyAccessible:        p.PubliclyAccessible,
		LogDeliveryConfiguration:  wireLogDelivery(p.LogDelivery),
		MaintenanceSchedule: wireMaintenance(
			p.Maintenance,
		),
		DBBackupConfigurations: b.wireBackupConfigs(p.BackupConfigs),
		NextMaintenanceTime:    timePtr(nextMaintenance(b.now(), p.Maintenance)),
		LastMaintenanceTime:    timePtr(i.LastMaintenance),
		InfluxAuthSecretARN:    strPtr(i.SecretARN),
	}

	if i.Mode != "" {
		w.InstanceModes = []string{i.Mode}
	}

	if st != statusCreating && st != statusRestoring && st != statusFailed {
		w.Endpoint = sc.endpoint(i.ID)
		if i.EngineHost != "" {
			w.Endpoint, w.Port = i.EngineHost, i.EnginePort
		}
	}

	return w
}

func (b *InMemoryBackend) summarizeInstance(sc scope, i *DBInstance) wInstanceSummary {
	w := b.wireInstance(sc, i)

	return wInstanceSummary{
		Arn:              w.Arn,
		ID:               w.ID,
		Name:             w.Name,
		DBInstanceType:   w.DBInstanceType,
		DBStorageType:    w.DBStorageType,
		DeploymentType:   w.DeploymentType,
		Endpoint:         w.Endpoint,
		InstanceMode:     w.InstanceMode,
		NetworkType:      w.NetworkType,
		Status:           w.Status,
		InstanceModes:    w.InstanceModes,
		AllocatedStorage: w.AllocatedStorage,
		Port:             w.Port,
	}
}

func (b *InMemoryBackend) wireCluster(sc scope, c *DBCluster) wCluster {
	st := b.clusterStateLocked(c)
	p := c.Placement

	w := wCluster{
		Arn:                                 c.ARN,
		ID:                                  c.ID,
		Name:                                c.Name,
		DBInstanceType:                      p.DBInstanceType,
		DBParameterGroupIdentifier:          p.ParameterGroupID,
		EffectiveDBParameterGroupIdentifier: p.ParameterGroupID,
		DBStorageType:                       p.DBStorageType,
		DeploymentType:                      p.DeploymentType,
		EngineType:                          c.EngineType,
		FailoverMode:                        c.FailoverMode,
		KMSKeyID:                            p.KMSKeyID,
		NetworkType:                         p.NetworkType,
		Status:                              st,
		VpcSubnetIDs:                        slices.Clone(p.SubnetIDs),
		VpcSecurityGroupIDs:                 slices.Clone(p.SecurityGroupIDs),
		AllocatedStorage:                    p.AllocatedStorage,
		Port:                                p.Port,
		PubliclyAccessible:                  p.PubliclyAccessible,
		ClusterConfiguration:                wireClusterConfig(c.Config),
		LogDeliveryConfiguration:            wireLogDelivery(p.LogDelivery),
		MaintenanceSchedule:                 wireMaintenance(p.Maintenance),
		DBBackupConfigurations:              b.wireBackupConfigs(p.BackupConfigs),
		NextMaintenanceTime:                 timePtr(nextMaintenance(b.now(), p.Maintenance)),
		LastMaintenanceTime:                 timePtr(c.LastMaintenance),
		InfluxAuthSecretARN:                 strPtr(c.SecretARN),
	}

	if st != statusCreating && st != statusRestoring {
		w.Endpoint = sc.endpoint(c.ID)
		w.ReaderEndpoint = sc.endpoint(c.ID + "-ro")
	}

	return w
}

func (b *InMemoryBackend) summarizeCluster(sc scope, c *DBCluster) wClusterSummary {
	w := b.wireCluster(sc, c)

	return wClusterSummary{
		Arn: w.Arn, ID: w.ID, Name: w.Name, DBInstanceType: w.DBInstanceType, DBStorageType: w.DBStorageType,
		DeploymentType: w.DeploymentType, Endpoint: w.Endpoint, EngineType: w.EngineType, NetworkType: w.NetworkType,
		ReaderEndpoint: w.ReaderEndpoint, Status: w.Status, AllocatedStorage: w.AllocatedStorage, Port: w.Port,
	}
}

func wireGroup(g *DBParameterGroup) wParameterGroup {
	return wParameterGroup{
		Arn: g.ARN, ID: g.ID, Name: g.Name, Description: g.Description, Parameters: g.Parameters,
	}
}

func (b *InMemoryBackend) wireBackup(bk *DBBackup) wBackup {
	p := bk.Placement

	w := wBackup{
		Arn:                 bk.ARN,
		ID:                  bk.ID,
		Name:                bk.Name,
		DBInstanceType:      p.DBInstanceType,
		DBParameterGroupID:  p.ParameterGroupID,
		DBResourceID:        bk.ResourceID,
		DBStorageType:       p.DBStorageType,
		DeploymentType:      p.DeploymentType,
		EngineType:          bk.EngineType,
		FailoverMode:        bk.FailoverMode,
		KMSKeyID:            p.KMSKeyID,
		NetworkType:         p.NetworkType,
		Status:              b.backupState(bk),
		Type:                bk.Type,
		VpcSecurityGroupIDs: slices.Clone(p.SecurityGroupIDs),
		VpcSubnetIDs:        slices.Clone(p.SubnetIDs),
		CreatedAt:           dateTime(bk.CreatedAt),
		AllocatedStorage:    p.AllocatedStorage,
		Port:                p.Port,
		PubliclyAccessible:  p.PubliclyAccessible,
		ClusterConfiguration: wireClusterConfig(
			bk.ClusterConfig,
		),
		LogDeliveryConfiguration: wireLogDelivery(p.LogDelivery),
		MaintenanceSchedule:      wireMaintenance(p.Maintenance),
	}

	if bk.RetentionDays > 0 {
		w.ExpiresAfter = bk.CreatedAt.AddDate(0, 0, int(bk.RetentionDays)).UTC().Format(time.RFC3339)
	}

	return w
}

func (b *InMemoryBackend) backupState(bk *DBBackup) string {
	switch st := b.state(bk.Pending); st {
	case statusAvailable:
		return backupCompleted
	case statusCreating:
		return backupInProgress
	default:
		return st
	}
}

func (b *InMemoryBackend) summarizeBackup(bk *DBBackup) wBackupSummary {
	w := b.wireBackup(bk)

	return wBackupSummary{
		Arn: w.Arn, ID: w.ID, Name: w.Name, DBResourceID: w.DBResourceID, DeploymentType: w.DeploymentType,
		EngineType: w.EngineType, ExpiresAfter: w.ExpiresAfter, KMSKeyID: w.KMSKeyID, Status: w.Status, Type: w.Type,
		CreatedAt: w.CreatedAt,
	}
}

func cloneTags(t map[string]string) map[string]string {
	if len(t) == 0 {
		return nil
	}

	return maps.Clone(t)
}
