package lightsail

import "context"

// referenceDataOps returns the dispatch table for family A (9 ops).
func (h *Handler) referenceDataOps() map[string]opFunc {
	return map[string]opFunc{
		"GetBlueprints":                   h.handleGetBlueprints,
		"GetBundles":                      h.handleGetBundles,
		"GetRelationalDatabaseBlueprints": h.handleGetRelationalDatabaseBlueprints,
		"GetRelationalDatabaseBundles":    h.handleGetRelationalDatabaseBundles,
		"GetBucketBundles":                h.handleGetBucketBundles,
		"GetDistributionBundles":          h.handleGetDistributionBundles,
		"GetContainerServicePowers":       h.handleGetContainerServicePowers,
		"GetRegions":                      h.handleGetRegions,
		"GetContainerAPIMetadata":         h.handleGetContainerAPIMetadata,
	}
}

type blueprintWire struct {
	AppCategory string `json:"appCategory,omitempty"`
	BlueprintID string `json:"blueprintId,omitempty"`
	Description string `json:"description,omitempty"`
	Group       string `json:"group,omitempty"`
	Name        string `json:"name,omitempty"`
	Platform    string `json:"platform,omitempty"`
	Type        string `json:"type,omitempty"`
	Version     string `json:"version,omitempty"`
	VersionCode string `json:"versionCode,omitempty"`
	MinPower    int32  `json:"minPower,omitempty"`
	IsActive    bool   `json:"isActive,omitempty"`
}

type getBlueprintsRequest struct {
	AppCategory     string `json:"appCategory,omitempty"`
	PageToken       string `json:"pageToken,omitempty"`
	IncludeInactive bool   `json:"includeInactive,omitempty"`
}

type blueprintsListResponse struct {
	NextPageToken string          `json:"nextPageToken,omitempty"`
	Blueprints    []blueprintWire `json:"blueprints,omitempty"`
}

func (h *Handler) handleGetBlueprints(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getBlueprintsRequest](body)
	if err != nil {
		return nil, err
	}

	bps := h.Backend.GetBlueprints(req.AppCategory, req.IncludeInactive)
	out := make([]blueprintWire, len(bps))

	for i, bp := range bps {
		out[i] = blueprintWire{
			AppCategory: bp.AppCategory, BlueprintID: bp.BlueprintID, Description: bp.Description,
			Group: bp.Group, IsActive: bp.IsActive, MinPower: bp.MinPower, Name: bp.Name,
			Platform: bp.Platform, Type: bp.Type, Version: bp.Version, VersionCode: bp.VersionCode,
		}
	}

	return marshalResponse(blueprintsListResponse{Blueprints: out})
}

type bundleWire struct {
	Name                   string   `json:"name,omitempty"`
	InstanceType           string   `json:"instanceType,omitempty"`
	BundleID               string   `json:"bundleId,omitempty"`
	SupportedPlatforms     []string `json:"supportedPlatforms,omitempty"`
	SupportedAppCategories []string `json:"supportedAppCategories,omitempty"`
	Power                  int32    `json:"power,omitempty"`
	Price                  float32  `json:"price,omitempty"`
	PublicIpv4AddressCount int32    `json:"publicIpv4AddressCount,omitempty"`
	RAMSizeInGb            float32  `json:"ramSizeInGb,omitempty"`
	DiskSizeInGb           int32    `json:"diskSizeInGb,omitempty"`
	CPUCount               int32    `json:"cpuCount,omitempty"`
	TransferPerMonthInGb   int32    `json:"transferPerMonthInGb,omitempty"`
	IsActive               bool     `json:"isActive,omitempty"`
}

type bundlesListResponse struct {
	NextPageToken string       `json:"nextPageToken,omitempty"`
	Bundles       []bundleWire `json:"bundles,omitempty"`
}

func (h *Handler) handleGetBundles(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getBlueprintsRequest](body)
	if err != nil {
		return nil, err
	}

	bds := h.Backend.GetBundles(req.AppCategory, req.IncludeInactive)
	out := make([]bundleWire, len(bds))

	for i, bd := range bds {
		out[i] = bundleWire{
			BundleID: bd.BundleID, CPUCount: bd.CPUCount, DiskSizeInGb: bd.DiskSizeInGb,
			InstanceType: bd.InstanceType, IsActive: bd.IsActive, Name: bd.Name, Power: bd.Power,
			Price: bd.Price, PublicIpv4AddressCount: bd.PublicIPv4AddressCount, RAMSizeInGb: bd.RAMSizeInGb,
			SupportedAppCategories: bd.SupportedAppCategories, SupportedPlatforms: bd.SupportedPlatforms,
			TransferPerMonthInGb: bd.TransferPerMonthInGb,
		}
	}

	return marshalResponse(bundlesListResponse{Bundles: out})
}

type rdsBlueprintWire struct {
	BlueprintID              string `json:"blueprintId,omitempty"`
	Engine                   string `json:"engine,omitempty"`
	EngineDescription        string `json:"engineDescription,omitempty"`
	EngineVersion            string `json:"engineVersion,omitempty"`
	EngineVersionDescription string `json:"engineVersionDescription,omitempty"`
	IsEngineDefault          bool   `json:"isEngineDefault,omitempty"`
}

type rdsBlueprintsListResponse struct {
	NextPageToken string             `json:"nextPageToken,omitempty"`
	Blueprints    []rdsBlueprintWire `json:"blueprints,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseBlueprints(_ context.Context, _ []byte) ([]byte, error) {
	bps := h.Backend.GetRelationalDatabaseBlueprints()
	out := make([]rdsBlueprintWire, len(bps))

	for i, bp := range bps {
		out[i] = rdsBlueprintWire(bp)
	}

	return marshalResponse(rdsBlueprintsListResponse{Blueprints: out})
}

type rdsBundleWire struct {
	BundleID             string  `json:"bundleId,omitempty"`
	Name                 string  `json:"name,omitempty"`
	CPUCount             int32   `json:"cpuCount,omitempty"`
	DiskSizeInGb         int32   `json:"diskSizeInGb,omitempty"`
	Price                float32 `json:"price,omitempty"`
	RAMSizeInGb          float32 `json:"ramSizeInGb,omitempty"`
	TransferPerMonthInGb int32   `json:"transferPerMonthInGb,omitempty"`
	IsActive             bool    `json:"isActive,omitempty"`
	IsEncrypted          bool    `json:"isEncrypted,omitempty"`
}

type rdsBundlesListResponse struct {
	NextPageToken string          `json:"nextPageToken,omitempty"`
	Bundles       []rdsBundleWire `json:"bundles,omitempty"`
}

type includeInactiveRequest struct {
	PageToken       string `json:"pageToken,omitempty"`
	IncludeInactive bool   `json:"includeInactive,omitempty"`
}

func (h *Handler) handleGetRelationalDatabaseBundles(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[includeInactiveRequest](body)
	if err != nil {
		return nil, err
	}

	bds := h.Backend.GetRelationalDatabaseBundles(req.IncludeInactive)
	out := make([]rdsBundleWire, len(bds))

	for i, bd := range bds {
		out[i] = rdsBundleWire{
			BundleID: bd.BundleID, CPUCount: bd.CPUCount, DiskSizeInGb: bd.DiskSizeInGb,
			IsActive: bd.IsActive, IsEncrypted: bd.IsEncrypted, Name: bd.Name,
			Price: bd.Price, RAMSizeInGb: bd.RAMSizeInGb, TransferPerMonthInGb: bd.TransferPerMonthInGb,
		}
	}

	return marshalResponse(rdsBundlesListResponse{Bundles: out})
}

type bucketBundleWire struct {
	BundleID             string  `json:"bundleId,omitempty"`
	Name                 string  `json:"name,omitempty"`
	Price                float32 `json:"price,omitempty"`
	StoragePerMonthInGb  int32   `json:"storagePerMonthInGb,omitempty"`
	TransferPerMonthInGb int32   `json:"transferPerMonthInGb,omitempty"`
	IsActive             bool    `json:"isActive,omitempty"`
}

type bucketBundlesListResponse struct {
	Bundles []bucketBundleWire `json:"bundles,omitempty"`
}

func (h *Handler) handleGetBucketBundles(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[includeInactiveRequest](body)
	if err != nil {
		return nil, err
	}

	bds := h.Backend.GetBucketBundles(req.IncludeInactive)
	out := make([]bucketBundleWire, len(bds))

	for i, bd := range bds {
		out[i] = bucketBundleWire(bd)
	}

	return marshalResponse(bucketBundlesListResponse{Bundles: out})
}

type distributionBundleWire struct {
	BundleID             string  `json:"bundleId,omitempty"`
	Name                 string  `json:"name,omitempty"`
	Price                float32 `json:"price,omitempty"`
	TransferPerMonthInGb int32   `json:"transferPerMonthInGb,omitempty"`
	IsActive             bool    `json:"isActive,omitempty"`
}

type distributionBundlesListResponse struct {
	Bundles []distributionBundleWire `json:"bundles,omitempty"`
}

func (h *Handler) handleGetDistributionBundles(_ context.Context, _ []byte) ([]byte, error) {
	bds := h.Backend.GetDistributionBundles()
	out := make([]distributionBundleWire, len(bds))

	for i, bd := range bds {
		out[i] = distributionBundleWire(bd)
	}

	return marshalResponse(distributionBundlesListResponse{Bundles: out})
}

type containerServicePowerWire struct {
	Name        string  `json:"name,omitempty"`
	PowerID     string  `json:"powerId,omitempty"`
	CPUCount    float32 `json:"cpuCount,omitempty"`
	Price       float32 `json:"price,omitempty"`
	RAMSizeInGb float32 `json:"ramSizeInGb,omitempty"`
	IsActive    bool    `json:"isActive,omitempty"`
}

type containerServicePowersListResponse struct {
	Powers []containerServicePowerWire `json:"powers,omitempty"`
}

func (h *Handler) handleGetContainerServicePowers(_ context.Context, _ []byte) ([]byte, error) {
	powers := h.Backend.GetContainerServicePowers()
	out := make([]containerServicePowerWire, len(powers))

	for i, p := range powers {
		out[i] = containerServicePowerWire{
			CPUCount: p.CPUCount, IsActive: p.IsActive, Name: p.Name,
			Price: p.Price, PowerID: p.PowerID, RAMSizeInGb: p.RAMSizeInGb,
		}
	}

	return marshalResponse(containerServicePowersListResponse{Powers: out})
}

type availabilityZoneWire struct {
	State    string `json:"state,omitempty"`
	ZoneName string `json:"zoneName,omitempty"`
}

type regionWire struct {
	AvailabilityZones                   []availabilityZoneWire `json:"availabilityZones,omitempty"`
	ContinentCode                       string                 `json:"continentCode,omitempty"`
	Description                         string                 `json:"description,omitempty"`
	DisplayName                         string                 `json:"displayName,omitempty"`
	Name                                string                 `json:"name,omitempty"`
	RelationalDatabaseAvailabilityZones []availabilityZoneWire `json:"relationalDatabaseAvailabilityZones,omitempty"`
}

type getRegionsRequest struct {
	IncludeAvailabilityZones                   bool `json:"includeAvailabilityZones,omitempty"`
	IncludeRelationalDatabaseAvailabilityZones bool `json:"includeRelationalDatabaseAvailabilityZones,omitempty"`
}

type regionsListResponse struct {
	Regions []regionWire `json:"regions,omitempty"`
}

func (h *Handler) handleGetRegions(_ context.Context, body []byte) ([]byte, error) {
	req, err := decodeBody[getRegionsRequest](body)
	if err != nil {
		return nil, err
	}

	regions := h.Backend.GetRegions()
	out := make([]regionWire, len(regions))

	for i, r := range regions {
		w := regionWire{
			ContinentCode: r.ContinentCode,
			Description:   r.Description,
			DisplayName:   r.DisplayName,
			Name:          r.Name,
		}

		if req.IncludeAvailabilityZones {
			for _, z := range r.AvailabilityZones {
				w.AvailabilityZones = append(w.AvailabilityZones, availabilityZoneWire{State: "available", ZoneName: z})
			}
		}

		if req.IncludeRelationalDatabaseAvailabilityZones {
			for _, z := range r.AvailabilityZones {
				w.RelationalDatabaseAvailabilityZones = append(
					w.RelationalDatabaseAvailabilityZones, availabilityZoneWire{State: "available", ZoneName: z},
				)
			}
		}

		out[i] = w
	}

	return marshalResponse(regionsListResponse{Regions: out})
}

type containerAPIMetadataResponse struct {
	Metadata []map[string]string `json:"metadata,omitempty"`
}

// handleGetContainerAPIMetadata handles the sole op in this 161-op surface
// with ZERO input fields AND no InvalidInputException (PARITY.md family A).
func (h *Handler) handleGetContainerAPIMetadata(_ context.Context, _ []byte) ([]byte, error) {
	return marshalResponse(containerAPIMetadataResponse{
		Metadata: []map[string]string{{"version": "1"}},
	})
}
