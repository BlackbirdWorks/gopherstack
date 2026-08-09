package iotwireless

// Real AWS models LoRaWAN/Sidewalk configuration with a different shape per
// call site rather than one merged type: types.LoRaWANDevice is shared by
// CreateWirelessDevice/GetWirelessDevice (api_op_CreateWirelessDevice.go:56,
// api_op_GetWirelessDevice.go:58), types.LoRaWANUpdateDevice is narrower and
// only for UpdateWirelessDevice (api_op_UpdateWirelessDevice.go:42), and
// types.LoRaWANListDevice is narrower still, for list entries
// (types.WirelessDeviceStatistics.LoRaWAN, types.go:2432). Sidewalk has the
// same three-way split: types.SidewalkCreateWirelessDevice (create request),
// types.SidewalkDevice (get/stored, a superset), types.SidewalkListDevice
// (list entry). Field names and JSON casing below are taken verbatim from
// aws-sdk-go-v2/service/iotwireless@v1.59.4/types/types.go and confirmed
// against serializers.go/deserializers.go (no @jsonName trait: wire key ==
// Go field name).

// LoRaWANDevice mirrors types.LoRaWANDevice -- the CreateWirelessDevice
// request / GetWirelessDevice response shape (types.go:723).
type LoRaWANDevice struct {
	AbpV1_0X         *AbpV1_0X  `json:"AbpV1_0_x,omitempty"`
	AbpV1_1          *AbpV1_1   `json:"AbpV1_1,omitempty"`
	FPorts           *FPorts    `json:"FPorts,omitempty"`
	OtaaV1_0X        *OtaaV1_0X `json:"OtaaV1_0_x,omitempty"`
	OtaaV1_1         *OtaaV1_1  `json:"OtaaV1_1,omitempty"`
	DevEui           *string    `json:"DevEui,omitempty"`
	DeviceProfileID  *string    `json:"DeviceProfileId,omitempty"`
	ServiceProfileID *string    `json:"ServiceProfileId,omitempty"`
}

// AbpV1_0X mirrors types.AbpV1_0X (types.go:11).
type AbpV1_0X struct {
	SessionKeys *SessionKeysAbpV1_0X `json:"SessionKeys,omitempty"`
	DevAddr     *string              `json:"DevAddr,omitempty"`
	FCntStart   *int32               `json:"FCntStart,omitempty"`
}

// AbpV1_1 mirrors types.AbpV1_1 (types.go:26).
type AbpV1_1 struct {
	SessionKeys *SessionKeysAbpV1_1 `json:"SessionKeys,omitempty"`
	DevAddr     *string             `json:"DevAddr,omitempty"`
	FCntStart   *int32              `json:"FCntStart,omitempty"`
}

// SessionKeysAbpV1_0X mirrors types.SessionKeysAbpV1_0X (types.go:1658).
type SessionKeysAbpV1_0X struct {
	AppSKey *string `json:"AppSKey,omitempty"`
	NwkSKey *string `json:"NwkSKey,omitempty"`
}

// SessionKeysAbpV1_1 mirrors types.SessionKeysAbpV1_1 (types.go:1670).
type SessionKeysAbpV1_1 struct {
	AppSKey     *string `json:"AppSKey,omitempty"`
	FNwkSIntKey *string `json:"FNwkSIntKey,omitempty"`
	NwkSEncKey  *string `json:"NwkSEncKey,omitempty"`
	SNwkSIntKey *string `json:"SNwkSIntKey,omitempty"`
}

// OtaaV1_0X mirrors types.OtaaV1_0X (types.go:1453).
type OtaaV1_0X struct {
	AppEui    *string `json:"AppEui,omitempty"`
	AppKey    *string `json:"AppKey,omitempty"`
	GenAppKey *string `json:"GenAppKey,omitempty"`
	JoinEui   *string `json:"JoinEui,omitempty"`
}

// OtaaV1_1 mirrors types.OtaaV1_1 (types.go:1473).
type OtaaV1_1 struct {
	AppKey  *string `json:"AppKey,omitempty"`
	JoinEui *string `json:"JoinEui,omitempty"`
	NwkKey  *string `json:"NwkKey,omitempty"`
}

// FPorts mirrors types.FPorts (types.go:411).
type FPorts struct {
	Positioning  *Positioning        `json:"Positioning,omitempty"`
	ClockSync    *int32              `json:"ClockSync,omitempty"`
	Fuota        *int32              `json:"Fuota,omitempty"`
	Multicast    *int32              `json:"Multicast,omitempty"`
	Applications []ApplicationConfig `json:"Applications,omitempty"`
}

// ApplicationConfig mirrors types.ApplicationConfig (types.go:67).
type ApplicationConfig struct {
	DestinationName *string `json:"DestinationName,omitempty"`
	FPort           *int32  `json:"FPort,omitempty"`
	Type            string  `json:"Type,omitempty"`
}

// Positioning mirrors types.Positioning (types.go:1551): FPort assignments
// for the GNSS/stream/ClockSync positioning functions.
type Positioning struct {
	ClockSync *int32 `json:"ClockSync,omitempty"`
	Gnss      *int32 `json:"Gnss,omitempty"`
	Stream    *int32 `json:"Stream,omitempty"`
}

// LoRaWANUpdateDevice mirrors types.LoRaWANUpdateDevice -- the narrower
// UpdateWirelessDevice request shape (types.go:1211): only DeviceProfileID/
// ServiceProfileID/Abp/FPorts are updatable, DevEui and Otaa are not.
type LoRaWANUpdateDevice struct {
	AbpV1_0X         *UpdateAbpV1_0X `json:"AbpV1_0_x,omitempty"`
	AbpV1_1          *UpdateAbpV1_1  `json:"AbpV1_1,omitempty"`
	FPorts           *UpdateFPorts   `json:"FPorts,omitempty"`
	DeviceProfileID  *string         `json:"DeviceProfileId,omitempty"`
	ServiceProfileID *string         `json:"ServiceProfileId,omitempty"`
}

// UpdateAbpV1_0X mirrors types.UpdateAbpV1_0X (types.go:2151).
type UpdateAbpV1_0X struct {
	FCntStart *int32 `json:"FCntStart,omitempty"`
}

// UpdateAbpV1_1 mirrors types.UpdateAbpV1_1 (types.go:2160).
type UpdateAbpV1_1 struct {
	FCntStart *int32 `json:"FCntStart,omitempty"`
}

// UpdateFPorts mirrors types.UpdateFPorts (types.go:2169).
type UpdateFPorts struct {
	Positioning  *Positioning        `json:"Positioning,omitempty"`
	Applications []ApplicationConfig `json:"Applications,omitempty"`
}

// LoRaWANListDevice mirrors types.LoRaWANListDevice -- the ListWirelessDevices
// entry shape (types.go:1034): DevEui only.
type LoRaWANListDevice struct {
	DevEui *string `json:"DevEui,omitempty"`
}

// LoRaWANGateway mirrors types.LoRaWANGateway, shared by
// CreateWirelessGateway/GetWirelessGateway (types.go:865).
type LoRaWANGateway struct {
	Beaconing      *Beaconing `json:"Beaconing,omitempty"`
	GatewayEui     *string    `json:"GatewayEui,omitempty"`
	MaxEirp        *float32   `json:"MaxEirp,omitempty"`
	RfRegion       *string    `json:"RfRegion,omitempty"`
	JoinEuiFilters [][]string `json:"JoinEuiFilters,omitempty"`
	NetIDFilters   []string   `json:"NetIdFilters,omitempty"`
	SubBands       []int32    `json:"SubBands,omitempty"`
}

// Beaconing mirrors types.Beaconing (types.go:84).
type Beaconing struct {
	DataRate    *int32  `json:"DataRate,omitempty"`
	Frequencies []int32 `json:"Frequencies,omitempty"`
}

// SidewalkDevice mirrors types.SidewalkDevice -- the GetWirelessDevice
// response shape (types.go:1735), a superset of SidewalkCreateWirelessDevice
// carrying AWS-assigned identity fields this backend never fabricates.
type SidewalkDevice struct {
	Positioning             *SidewalkPositioning `json:"Positioning,omitempty"`
	AmazonID                *string              `json:"AmazonId,omitempty"`
	CertificateID           *string              `json:"CertificateId,omitempty"`
	DeviceProfileID         *string              `json:"DeviceProfileId,omitempty"`
	SidewalkID              *string              `json:"SidewalkId,omitempty"`
	SidewalkManufacturingSn *string              `json:"SidewalkManufacturingSn,omitempty"`
	Status                  string               `json:"Status,omitempty"`
	DeviceCertificates      []CertificateList    `json:"DeviceCertificates,omitempty"`
	PrivateKeys             []CertificateList    `json:"PrivateKeys,omitempty"`
}

// CertificateList mirrors types.CertificateList (types.go:197).
type CertificateList struct {
	Value      *string `json:"Value,omitempty"`
	SigningAlg string  `json:"SigningAlg,omitempty"`
}

// SidewalkCreateWirelessDevice mirrors types.SidewalkCreateWirelessDevice --
// the CreateWirelessDevice request shape (types.go:1720).
type SidewalkCreateWirelessDevice struct {
	Positioning             *SidewalkPositioning `json:"Positioning,omitempty"`
	DeviceProfileID         *string              `json:"DeviceProfileId,omitempty"`
	SidewalkManufacturingSn *string              `json:"SidewalkManufacturingSn,omitempty"`
}

// SidewalkUpdateWirelessDevice mirrors types.SidewalkUpdateWirelessDevice --
// the UpdateWirelessDevice request shape (types.go:1948): only Positioning
// is updatable.
type SidewalkUpdateWirelessDevice struct {
	Positioning *SidewalkPositioning `json:"Positioning,omitempty"`
}

// SidewalkListDevice mirrors types.SidewalkListDevice -- the
// ListWirelessDevices entry shape (types.go:1828).
type SidewalkListDevice struct {
	Positioning             *SidewalkPositioning `json:"Positioning,omitempty"`
	AmazonID                *string              `json:"AmazonId,omitempty"`
	DeviceProfileID         *string              `json:"DeviceProfileId,omitempty"`
	SidewalkID              *string              `json:"SidewalkId,omitempty"`
	SidewalkManufacturingSn *string              `json:"SidewalkManufacturingSn,omitempty"`
	Status                  string               `json:"Status,omitempty"`
	DeviceCertificates      []CertificateList    `json:"DeviceCertificates,omitempty"`
}

// SidewalkPositioning mirrors types.SidewalkPositioning (types.go:1865).
type SidewalkPositioning struct {
	DestinationName *string `json:"DestinationName,omitempty"`
}

// TraceContent mirrors types.TraceContent (types.go:2130), used by
// NetworkAnalyzerConfig's Create/Get/UpdateNetworkAnalyzerConfiguration.
type TraceContent struct {
	LogLevel                string `json:"LogLevel,omitempty"`
	MulticastFrameInfo      string `json:"MulticastFrameInfo,omitempty"`
	WirelessDeviceFrameInfo string `json:"WirelessDeviceFrameInfo,omitempty"`
}

// UpdateWirelessGatewayTaskCreate mirrors types.UpdateWirelessGatewayTaskCreate
// (types.go:2182) -- the "Update" sub-struct of
// CreateWirelessGatewayTaskDefinitionInput.Update / GetWirelessGatewayTaskDefinitionOutput.Update.
type UpdateWirelessGatewayTaskCreate struct {
	LoRaWAN          *LoRaWANUpdateGatewayTaskCreate `json:"LoRaWAN,omitempty"`
	UpdateDataRole   *string                         `json:"UpdateDataRole,omitempty"`
	UpdateDataSource *string                         `json:"UpdateDataSource,omitempty"`
}

// LoRaWANUpdateGatewayTaskCreate mirrors types.LoRaWANUpdateGatewayTaskCreate
// (types.go:1232).
type LoRaWANUpdateGatewayTaskCreate struct {
	CurrentVersion  *LoRaWANGatewayVersion `json:"CurrentVersion,omitempty"`
	UpdateVersion   *LoRaWANGatewayVersion `json:"UpdateVersion,omitempty"`
	UpdateSignature *string                `json:"UpdateSignature,omitempty"`
	SigKeyCrc       *int64                 `json:"SigKeyCrc,omitempty"`
}

// LoRaWANGatewayVersion mirrors types.LoRaWANGatewayVersion (types.go:918).
type LoRaWANGatewayVersion struct {
	Model          *string `json:"Model,omitempty"`
	PackageVersion *string `json:"PackageVersion,omitempty"`
	Station        *string `json:"Station,omitempty"`
}

// LoRaWANUpdateGatewayTaskEntry mirrors types.LoRaWANUpdateGatewayTaskEntry
// (types.go:1250) -- narrower than LoRaWANUpdateGatewayTaskCreate: real
// AWS's ListWirelessGatewayTaskDefinitions entry (types.UpdateWirelessGatewayTaskEntry.LoRaWAN,
// types.go:2206) carries only CurrentVersion/UpdateVersion, not
// SigKeyCrc/UpdateSignature.
type LoRaWANUpdateGatewayTaskEntry struct {
	CurrentVersion *LoRaWANGatewayVersion `json:"CurrentVersion,omitempty"`
	UpdateVersion  *LoRaWANGatewayVersion `json:"UpdateVersion,omitempty"`
}

// loRaWANUpdateGatewayTaskEntryFrom narrows a stored
// LoRaWANUpdateGatewayTaskCreate down to the list-entry shape.
func loRaWANUpdateGatewayTaskEntryFrom(u *UpdateWirelessGatewayTaskCreate) *LoRaWANUpdateGatewayTaskEntry {
	if u == nil || u.LoRaWAN == nil {
		return nil
	}

	return &LoRaWANUpdateGatewayTaskEntry{
		CurrentVersion: u.LoRaWAN.CurrentVersion,
		UpdateVersion:  u.LoRaWAN.UpdateVersion,
	}
}

// copyLoRaWANDevice returns a shallow copy of l, or nil for nil input --
// same isolation level copyAnyMap gave the prior map[string]any field (see
// store.go): enough to stop a caller mutating the backend's stored pointer.
func copyLoRaWANDevice(l *LoRaWANDevice) *LoRaWANDevice {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// copySidewalkDevice returns a shallow copy of s, or nil for nil input.
func copySidewalkDevice(s *SidewalkDevice) *SidewalkDevice {
	if s == nil {
		return nil
	}

	cp := *s

	return &cp
}

// copyLoRaWANGateway returns a shallow copy of l, or nil for nil input.
func copyLoRaWANGateway(l *LoRaWANGateway) *LoRaWANGateway {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// copyTraceContent returns a copy of t, or nil for nil input.
func copyTraceContent(t *TraceContent) *TraceContent {
	if t == nil {
		return nil
	}

	cp := *t

	return &cp
}

// copyUpdateWirelessGatewayTaskCreate returns a shallow copy of u, or nil for
// nil input.
func copyUpdateWirelessGatewayTaskCreate(u *UpdateWirelessGatewayTaskCreate) *UpdateWirelessGatewayTaskCreate {
	if u == nil {
		return nil
	}

	cp := *u

	return &cp
}

// sidewalkDeviceFromCreate builds the stored SidewalkDevice representation
// from a CreateWirelessDevice request's narrower SidewalkCreateWirelessDevice
// shape. The AWS-assigned identity fields SidewalkDevice adds (AmazonID,
// CertificateID, DeviceCertificates, PrivateKeys, SidewalkID, Status) are
// left unset rather than fabricated.
func sidewalkDeviceFromCreate(in *SidewalkCreateWirelessDevice) *SidewalkDevice {
	if in == nil {
		return nil
	}

	return &SidewalkDevice{
		DeviceProfileID:         in.DeviceProfileID,
		Positioning:             in.Positioning,
		SidewalkManufacturingSn: in.SidewalkManufacturingSn,
	}
}

// loRaWANListDeviceFrom narrows a stored LoRaWANDevice down to the
// LoRaWANListDevice shape ListWirelessDevices returns (DevEui only).
func loRaWANListDeviceFrom(l *LoRaWANDevice) *LoRaWANListDevice {
	if l == nil {
		return nil
	}

	return &LoRaWANListDevice{DevEui: l.DevEui}
}

// sidewalkListDeviceFrom narrows a stored SidewalkDevice down to the
// SidewalkListDevice shape ListWirelessDevices returns.
func sidewalkListDeviceFrom(s *SidewalkDevice) *SidewalkListDevice {
	if s == nil {
		return nil
	}

	return &SidewalkListDevice{
		AmazonID:                s.AmazonID,
		DeviceCertificates:      s.DeviceCertificates,
		DeviceProfileID:         s.DeviceProfileID,
		Positioning:             s.Positioning,
		SidewalkID:              s.SidewalkID,
		SidewalkManufacturingSn: s.SidewalkManufacturingSn,
		Status:                  s.Status,
	}
}

// mergeLoRaWANDeviceUpdate applies the sub-fields present on update onto a
// copy of existing, matching real AWS's UpdateWirelessDevice semantics: only
// the narrower LoRaWANUpdateDevice fields (DeviceProfileID/ServiceProfileID/
// Abp/FPorts) can change; DevEui and Otaa session config survive untouched.
func mergeLoRaWANDeviceUpdate(existing *LoRaWANDevice, update *LoRaWANUpdateDevice) *LoRaWANDevice {
	if update == nil {
		return existing
	}

	result := copyLoRaWANDevice(existing)
	if result == nil {
		result = &LoRaWANDevice{}
	}

	if update.DeviceProfileID != nil {
		result.DeviceProfileID = update.DeviceProfileID
	}

	if update.ServiceProfileID != nil {
		result.ServiceProfileID = update.ServiceProfileID
	}

	if update.AbpV1_0X != nil && update.AbpV1_0X.FCntStart != nil {
		if result.AbpV1_0X == nil {
			result.AbpV1_0X = &AbpV1_0X{}
		}

		result.AbpV1_0X.FCntStart = update.AbpV1_0X.FCntStart
	}

	if update.AbpV1_1 != nil && update.AbpV1_1.FCntStart != nil {
		if result.AbpV1_1 == nil {
			result.AbpV1_1 = &AbpV1_1{}
		}

		result.AbpV1_1.FCntStart = update.AbpV1_1.FCntStart
	}

	if update.FPorts != nil {
		if result.FPorts == nil {
			result.FPorts = &FPorts{}
		}

		if update.FPorts.Applications != nil {
			result.FPorts.Applications = update.FPorts.Applications
		}

		if update.FPorts.Positioning != nil {
			result.FPorts.Positioning = update.FPorts.Positioning
		}
	}

	return result
}

// mergeSidewalkDeviceUpdate applies update onto a copy of existing, matching
// real AWS's UpdateWirelessDevice semantics: SidewalkUpdateWirelessDevice
// only carries Positioning.
func mergeSidewalkDeviceUpdate(existing *SidewalkDevice, update *SidewalkUpdateWirelessDevice) *SidewalkDevice {
	if update == nil {
		return existing
	}

	result := copySidewalkDevice(existing)
	if result == nil {
		result = &SidewalkDevice{}
	}

	if update.Positioning != nil {
		result.Positioning = update.Positioning
	}

	return result
}

// LoRaWANServiceProfile mirrors types.LoRaWANServiceProfile -- the
// CreateServiceProfile request shape (types.go:1161).
type LoRaWANServiceProfile struct {
	DrMax           *int32 `json:"DrMax,omitempty"`
	DrMin           *int32 `json:"DrMin,omitempty"`
	NbTransMax      *int32 `json:"NbTransMax,omitempty"`
	NbTransMin      *int32 `json:"NbTransMin,omitempty"`
	TxPowerIndexMax *int32 `json:"TxPowerIndexMax,omitempty"`
	TxPowerIndexMin *int32 `json:"TxPowerIndexMin,omitempty"`
	AddGwMetadata   bool   `json:"AddGwMetadata,omitempty"`
	PrAllowed       bool   `json:"PrAllowed,omitempty"`
	RaAllowed       bool   `json:"RaAllowed,omitempty"`
}

// LoRaWANGetServiceProfileInfo mirrors types.LoRaWANGetServiceProfileInfo --
// the GetServiceProfile response shape (types.go:933), wider than
// LoRaWANServiceProfile: ChannelMask, DevStatusReqFreq, DlBucketSize,
// DlRate, DlRatePolicy, HrAllowed, MinGwDiversity, NwkGeoLoc, TargetPer,
// UlBucketSize, UlRate and UlRatePolicy are AWS-computed and only ever
// appear on Get, never on Create.
type LoRaWANGetServiceProfileInfo struct {
	ChannelMask            *string `json:"ChannelMask,omitempty"`
	DevStatusReqFreq       *int32  `json:"DevStatusReqFreq,omitempty"`
	DlBucketSize           *int32  `json:"DlBucketSize,omitempty"`
	DlRate                 *int32  `json:"DlRate,omitempty"`
	DlRatePolicy           *string `json:"DlRatePolicy,omitempty"`
	MinGwDiversity         *int32  `json:"MinGwDiversity,omitempty"`
	NbTransMax             *int32  `json:"NbTransMax,omitempty"`
	NbTransMin             *int32  `json:"NbTransMin,omitempty"`
	TxPowerIndexMax        *int32  `json:"TxPowerIndexMax,omitempty"`
	TxPowerIndexMin        *int32  `json:"TxPowerIndexMin,omitempty"`
	UlBucketSize           *int32  `json:"UlBucketSize,omitempty"`
	UlRate                 *int32  `json:"UlRate,omitempty"`
	UlRatePolicy           *string `json:"UlRatePolicy,omitempty"`
	DrMax                  int32   `json:"DrMax,omitempty"`
	DrMin                  int32   `json:"DrMin,omitempty"`
	TargetPer              int32   `json:"TargetPer,omitempty"`
	AddGwMetadata          bool    `json:"AddGwMetadata,omitempty"`
	HrAllowed              bool    `json:"HrAllowed,omitempty"`
	NwkGeoLoc              bool    `json:"NwkGeoLoc,omitempty"`
	PrAllowed              bool    `json:"PrAllowed,omitempty"`
	RaAllowed              bool    `json:"RaAllowed,omitempty"`
	ReportDevStatusBattery bool    `json:"ReportDevStatusBattery,omitempty"`
	ReportDevStatusMargin  bool    `json:"ReportDevStatusMargin,omitempty"`
}

// copyLoRaWANServiceProfile returns a shallow copy of l, or nil for nil input.
func copyLoRaWANServiceProfile(l *LoRaWANServiceProfile) *LoRaWANServiceProfile {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// loRaWANGetServiceProfileInfoFrom converts the stored create-shape
// LoRaWANServiceProfile into the wider GetServiceProfile response shape.
// Get-only fields (ChannelMask, DevStatusReqFreq, ...) are AWS-computed and
// left unset rather than fabricated.
func loRaWANGetServiceProfileInfoFrom(l *LoRaWANServiceProfile) *LoRaWANGetServiceProfileInfo {
	if l == nil {
		return nil
	}

	info := &LoRaWANGetServiceProfileInfo{
		AddGwMetadata:   l.AddGwMetadata,
		PrAllowed:       l.PrAllowed,
		RaAllowed:       l.RaAllowed,
		NbTransMax:      l.NbTransMax,
		NbTransMin:      l.NbTransMin,
		TxPowerIndexMax: l.TxPowerIndexMax,
		TxPowerIndexMin: l.TxPowerIndexMin,
	}

	if l.DrMax != nil {
		info.DrMax = *l.DrMax
	}

	if l.DrMin != nil {
		info.DrMin = *l.DrMin
	}

	return info
}

// LoRaWANDeviceProfile mirrors types.LoRaWANDeviceProfile -- shared
// verbatim by CreateDeviceProfile's request and GetDeviceProfile's response
// (types.go:780), unlike ServiceProfile/FuotaTask/MulticastGroup which each
// have a narrower create shape and a wider get shape.
type LoRaWANDeviceProfile struct {
	PingSlotDr             *int32  `json:"PingSlotDr,omitempty"`
	PingSlotPeriod         *int32  `json:"PingSlotPeriod,omitempty"`
	RegParamsRevision      *string `json:"RegParamsRevision,omitempty"`
	PingSlotFreq           *int32  `json:"PingSlotFreq,omitempty"`
	ClassBTimeout          *int32  `json:"ClassBTimeout,omitempty"`
	ClassCTimeout          *int32  `json:"ClassCTimeout,omitempty"`
	MaxDutyCycle           *int32  `json:"MaxDutyCycle,omitempty"`
	MaxEirp                *int32  `json:"MaxEirp,omitempty"`
	MacVersion             *string `json:"MacVersion,omitempty"`
	SupportsJoin           *bool   `json:"SupportsJoin,omitempty"`
	RfRegion               *string `json:"RfRegion,omitempty"`
	RxDataRate2            *int32  `json:"RxDataRate2,omitempty"`
	RxDelay1               *int32  `json:"RxDelay1,omitempty"`
	RxDrOffset1            *int32  `json:"RxDrOffset1,omitempty"`
	RxFreq2                *int32  `json:"RxFreq2,omitempty"`
	FactoryPresetFreqsList []int32 `json:"FactoryPresetFreqsList,omitempty"`
	Supports32BitFCnt      bool    `json:"Supports32BitFCnt,omitempty"`
	SupportsClassB         bool    `json:"SupportsClassB,omitempty"`
	SupportsClassC         bool    `json:"SupportsClassC,omitempty"`
}

// copyLoRaWANDeviceProfile returns a shallow copy of l, or nil for nil input.
func copyLoRaWANDeviceProfile(l *LoRaWANDeviceProfile) *LoRaWANDeviceProfile {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// SidewalkCreateDeviceProfile mirrors types.SidewalkCreateDeviceProfile --
// the CreateDeviceProfile request shape (types.go:1715). It carries no
// fields at all: unlike SidewalkCreateWirelessDevice, a Sidewalk device
// profile has no client-configurable settings, so this struct's only
// purpose is to distinguish "Sidewalk key present" from "absent" on decode.
type SidewalkCreateDeviceProfile struct{}

// SidewalkGetDeviceProfile mirrors types.SidewalkGetDeviceProfile -- the
// GetDeviceProfile response shape (types.go:1796). Its fields are
// AWS-assigned at provisioning time; this backend never fabricates them, so
// they stay unset even when the profile is a Sidewalk profile.
type SidewalkGetDeviceProfile struct {
	ApplicationServerPublicKey *string                  `json:"ApplicationServerPublicKey,omitempty"`
	QualificationStatus        *bool                    `json:"QualificationStatus,omitempty"`
	DakCertificateMetadata     []DakCertificateMetadata `json:"DakCertificateMetadata,omitempty"`
}

// DakCertificateMetadata mirrors types.DakCertificateMetadata (types.go:238).
type DakCertificateMetadata struct {
	CertificateID       *string `json:"CertificateId,omitempty"`
	ApID                *string `json:"ApId,omitempty"`
	DeviceTypeID        *string `json:"DeviceTypeId,omitempty"`
	MaxAllowedSignature *int32  `json:"MaxAllowedSignature,omitempty"`
	FactorySupport      *bool   `json:"FactorySupport,omitempty"`
}

// copySidewalkGetDeviceProfile returns a shallow copy of s, or nil for nil input.
func copySidewalkGetDeviceProfile(s *SidewalkGetDeviceProfile) *SidewalkGetDeviceProfile {
	if s == nil {
		return nil
	}

	cp := *s

	return &cp
}

// sidewalkGetDeviceProfileFromCreate converts a CreateDeviceProfile
// request's SidewalkCreateDeviceProfile marker into the stored
// SidewalkGetDeviceProfile representation. present distinguishes an empty
// `"Sidewalk":{}` request object (a Sidewalk profile with no settings) from
// no Sidewalk key at all (a LoRaWAN-only profile), a distinction a nil
// *SidewalkCreateDeviceProfile alone cannot carry once decoded.
func sidewalkGetDeviceProfileFromCreate(present bool) *SidewalkGetDeviceProfile {
	if !present {
		return nil
	}

	return &SidewalkGetDeviceProfile{}
}

// LoRaWANFuotaTask mirrors types.LoRaWANFuotaTask -- shared by
// CreateFuotaTask's and UpdateFuotaTask's request (types.go:844).
type LoRaWANFuotaTask struct {
	RfRegion string `json:"RfRegion,omitempty"`
}

// LoRaWANFuotaTaskGetInfo mirrors types.LoRaWANFuotaTaskGetInfo -- the
// GetFuotaTask response shape (types.go:853). StartTime is an ISO8601
// string on the wire (smithytime.FormatDateTime/ParseDateTime,
// deserializers.go:19509) set only from StartFuotaTaskInput.LoRaWAN; this
// backend does not capture that separate input, so StartTime is always
// left unset rather than fabricated.
type LoRaWANFuotaTaskGetInfo struct {
	RfRegion  *string `json:"RfRegion,omitempty"`
	StartTime *string `json:"StartTime,omitempty"`
}

// copyLoRaWANFuotaTask returns a shallow copy of l, or nil for nil input.
func copyLoRaWANFuotaTask(l *LoRaWANFuotaTask) *LoRaWANFuotaTask {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// loRaWANFuotaTaskGetInfoFrom converts the stored create/update-shape
// LoRaWANFuotaTask into the GetFuotaTask response shape.
func loRaWANFuotaTaskGetInfoFrom(l *LoRaWANFuotaTask) *LoRaWANFuotaTaskGetInfo {
	if l == nil {
		return nil
	}

	info := &LoRaWANFuotaTaskGetInfo{}
	if l.RfRegion != "" {
		info.RfRegion = &l.RfRegion
	}

	return info
}

// DefaultSessionParametersMulticast mirrors
// types.DefaultSessionParametersMulticast (types.go:263).
type DefaultSessionParametersMulticast struct {
	DlDr   *int32 `json:"DlDr,omitempty"`
	DlFreq *int32 `json:"DlFreq,omitempty"`
}

// ParticipatingGatewaysMulticast mirrors types.ParticipatingGatewaysMulticast
// (types.go:1515).
type ParticipatingGatewaysMulticast struct {
	TransmissionInterval *int32   `json:"TransmissionInterval,omitempty"`
	GatewayList          []string `json:"GatewayList,omitempty"`
}

// LoRaWANMulticast mirrors types.LoRaWANMulticast -- shared by
// CreateMulticastGroup's and UpdateMulticastGroup's request (types.go:1043).
type LoRaWANMulticast struct {
	DefaultSessionParameters *DefaultSessionParametersMulticast `json:"DefaultSessionParameters,omitempty"`
	ParticipatingGateways    *ParticipatingGatewaysMulticast    `json:"ParticipatingGateways,omitempty"`
	DlClass                  string                             `json:"DlClass,omitempty"`
	RfRegion                 string                             `json:"RfRegion,omitempty"`
}

// LoRaWANMulticastGet mirrors types.LoRaWANMulticastGet -- the
// GetMulticastGroup response shape (types.go:1064): same fields as
// LoRaWANMulticast plus NumberOfDevicesInGroup/NumberOfDevicesRequested.
type LoRaWANMulticastGet struct {
	DefaultSessionParameters *DefaultSessionParametersMulticast `json:"DefaultSessionParameters,omitempty"`
	ParticipatingGateways    *ParticipatingGatewaysMulticast    `json:"ParticipatingGateways,omitempty"`
	NumberOfDevicesInGroup   *int32                             `json:"NumberOfDevicesInGroup,omitempty"`
	NumberOfDevicesRequested *int32                             `json:"NumberOfDevicesRequested,omitempty"`
	DlClass                  string                             `json:"DlClass,omitempty"`
	RfRegion                 string                             `json:"RfRegion,omitempty"`
}

// copyLoRaWANMulticast returns a shallow copy of l, or nil for nil input.
func copyLoRaWANMulticast(l *LoRaWANMulticast) *LoRaWANMulticast {
	if l == nil {
		return nil
	}

	cp := *l

	return &cp
}

// loRaWANMulticastGetFrom converts the stored create/update-shape
// LoRaWANMulticast plus the real device-association count into the
// GetMulticastGroup response shape. NumberOfDevicesRequested is left
// unset: this backend has no separate "requested" count distinct from the
// actual association set built by StartBulkAssociateWirelessDeviceWithMulticastGroup.
func loRaWANMulticastGetFrom(l *LoRaWANMulticast, devicesInGroup int32) *LoRaWANMulticastGet {
	if l == nil {
		return nil
	}

	return &LoRaWANMulticastGet{
		DefaultSessionParameters: l.DefaultSessionParameters,
		ParticipatingGateways:    l.ParticipatingGateways,
		DlClass:                  l.DlClass,
		RfRegion:                 l.RfRegion,
		NumberOfDevicesInGroup:   &devicesInGroup,
	}
}
