package ocidatabase

import (
	"time"
)

type apexWire struct {
	ApexVersion string `json:"apexVersion"`
	OrdsVersion string `json:"ordsVersion"`
}

// AdbWire is the Autonomous Database wire shape.
type AdbWire struct {
	TimeCreated              time.Time                 `json:"timeCreated"`
	FreeformTags             map[string]string         `json:"freeformTags"`
	DefinedTags              map[string]map[string]any `json:"definedTags"`
	ConnectionStrings        *connStringsWire          `json:"connectionStrings,omitempty"`
	ConnectionURLs           *connURLWire              `json:"connectionUrls,omitempty"`
	Apex                     *apexWire                 `json:"apexDetails,omitempty"`
	CPUCoreCount             *int                      `json:"cpuCoreCount,omitempty"`
	OcpuCount                *float32                  `json:"ocpuCount,omitempty"`
	DBName                   string                    `json:"dbName"`
	PrivateEndpointLabel     string                    `json:"privateEndpointLabel,omitempty"`
	LifecycleState           string                    `json:"lifecycleState"`
	DisplayName              string                    `json:"displayName"`
	DBWorkload               string                    `json:"dbWorkload"`
	ComputeModel             string                    `json:"computeModel"`
	LicenseModel             string                    `json:"licenseModel"`
	DBVersion                string                    `json:"dbVersion"`
	CharacterSet             string                    `json:"characterSet"`
	NcharacterSet            string                    `json:"ncharacterSet"`
	InfrastructureType       string                    `json:"infrastructureType"`
	OpenMode                 string                    `json:"openMode"`
	PermissionLevel          string                    `json:"permissionLevel"`
	SubnetID                 string                    `json:"subnetId,omitempty"`
	PrivateEndpoint          string                    `json:"privateEndpoint,omitempty"`
	LifecycleDetails         string                    `json:"lifecycleDetails,omitempty"`
	PrivateEndpointIP        string                    `json:"privateEndpointIp,omitempty"`
	ID                       string                    `json:"id"`
	CompartmentID            string                    `json:"compartmentId"`
	WhitelistedIps           []string                  `json:"whitelistedIps,omitempty"`
	AccessTypes              []string                  `json:"accessTypes,omitempty"`
	NsgIDs                   []string                  `json:"nsgIds,omitempty"`
	BackupRetentionDays      int                       `json:"backupRetentionPeriodInDays"`
	DataStorageSizeInTBs     int                       `json:"dataStorageSizeInTBs"`
	DataStorageSizeInGBs     int                       `json:"dataStorageSizeInGBs"`
	ComputeCount             float32                   `json:"computeCount"`
	IsFreeTier               bool                      `json:"isFreeTier"`
	IsDedicated              bool                      `json:"isDedicated"`
	IsAutoScalingEnabled     bool                      `json:"isAutoScalingEnabled"`
	IsAutoScalingForStorage  bool                      `json:"isAutoScalingForStorageEnabled"`
	IsMtlsConnectionRequired bool                      `json:"isMtlsConnectionRequired"`
	IsAccessControlEnabled   bool                      `json:"isAccessControlEnabled"`
}

func orEmpty[K comparable, V any](m map[K]V) map[K]V {
	if m == nil {
		return map[K]V{}
	}

	return m
}

// view renders db. Callers hold the lock.
func (b *InMemoryBackend) view(db *ADB) AdbWire {
	w := AdbWire{
		ID: db.ID, CompartmentID: db.CompartmentID, LifecycleState: db.State, LifecycleDetails: db.Details,
		DBName: db.DBName, DisplayName: db.DisplayName, DBWorkload: db.Workload, ComputeModel: db.ComputeModel,
		ComputeCount: db.ComputeCount, LicenseModel: db.LicenseModel, DBVersion: db.DBVersion,
		CharacterSet: db.CharacterSet, NcharacterSet: db.NCharacterSet, InfrastructureType: "CLOUD",
		OpenMode: "READ_WRITE", PermissionLevel: "UNRESTRICTED", TimeCreated: db.Created,
		DataStorageSizeInTBs: storageTBs(db.StorageGBs), DataStorageSizeInGBs: db.StorageGBs,
		BackupRetentionDays: db.BackupDays, IsFreeTier: db.IsFreeTier, IsAutoScalingEnabled: db.AutoScaling,
		IsAutoScalingForStorage: db.StorageScales, IsMtlsConnectionRequired: db.Mtls,
		IsAccessControlEnabled: db.AccessControl, WhitelistedIps: db.Whitelisted, NsgIDs: db.NsgIDs,
		FreeformTags: orEmpty(db.FreeformTags), DefinedTags: orEmpty(db.DefinedTags),
		ConnectionURLs: connectionURLs(db), AccessTypes: []string{"PUBLIC"},
	}

	if db.ComputeModel == modelOCPU {
		cores, ocpus := int(db.ComputeCount), db.ComputeCount
		w.CPUCoreCount, w.OcpuCount = &cores, &ocpus
	}

	if db.Workload == workloadAPEX {
		w.Apex = &apexWire{ApexVersion: "24.2.0", OrdsVersion: "24.4.0"}
	}

	if db.SubnetID != "" {
		w.SubnetID, w.PrivateEndpointLabel, w.PrivateEndpointIP = db.SubnetID, db.EndpointLabel, db.EndpointIP
		w.PrivateEndpoint = privateHost(db)
		w.AccessTypes = []string{"PRIVATE"}
	}

	if ep, ok := b.endpointOf(db); ok {
		w.ConnectionStrings = ep.connectionStrings(db)
	}

	return w
}

func privateHost(db *ADB) string {
	label := db.EndpointLabel
	if label == "" {
		label = db.UniqueID
	}

	return label + ".adb." + regionName + ".oraclecloud.com"
}

// endpointOf locates where clients reach db; false while an engine database has no listeners yet.
func (b *InMemoryBackend) endpointOf(db *ADB) (endpoint, bool) {
	if db.Engine && b.eng != nil {
		lv, ok := b.eng.live[db.ID]
		if !ok {
			return endpoint{}, false
		}

		return endpoint{
			host: engineHost, service: engineService, mtlsPort: lv.mtls.port, tlsPort: lv.tls.port, engine: true,
		}, true
	}

	host := regionHost()
	if db.SubnetID != "" {
		host = privateHost(db)
	}

	return endpoint{host: host, mtlsPort: stubMtlsPort, tlsPort: stubTLSPort}, true
}

func regionHost() string { return "adb." + regionName + ".oraclecloud.com" }
