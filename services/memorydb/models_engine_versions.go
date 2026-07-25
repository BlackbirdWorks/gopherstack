package memorydb

// EngineVersion describes a supported MemoryDB engine version. Description is
// internal-only documentation for the seed table (defaultEngineVersions) --
// it is NOT part of the wire response (see engineVersionObject's doc comment).
type EngineVersion struct {
	Engine               string `json:"engine"`
	EngineVersion        string `json:"engineVersion"`
	EnginePatchVersion   string `json:"enginePatchVersion"`
	ParameterGroupFamily string `json:"parameterGroupFamily"`
	Description          string `json:"description"`
}

type describeEngineVersionsRequest struct {
	MaxResults           *int32 `json:"MaxResults,omitempty"`
	ParameterGroupFamily string `json:"ParameterGroupFamily,omitempty"`
	Engine               string `json:"Engine,omitempty"`
	NextToken            string `json:"NextToken,omitempty"`
	DefaultOnly          bool   `json:"DefaultOnly,omitempty"`
}

// engineVersionObject is field-diffed against the real SDK's
// types.EngineVersionInfo (deserializers.go's
// awsAwsjson11_deserializeDocumentEngineVersionInfo: exactly Engine,
// EnginePatchVersion, EngineVersion, ParameterGroupFamily). A prior pass
// added a fabricated "Description" field; removed from the wire shape (the
// internal EngineVersion model keeps it as seed-table documentation).
type engineVersionObject struct {
	Engine               string `json:"Engine,omitempty"`
	EngineVersion        string `json:"EngineVersion,omitempty"`
	EnginePatchVersion   string `json:"EnginePatchVersion,omitempty"`
	ParameterGroupFamily string `json:"ParameterGroupFamily,omitempty"`
}

type describeEngineVersionsResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	EngineVersions []engineVersionObject `json:"EngineVersions"`
}

// -- Event request/response types --------------------------------------------
