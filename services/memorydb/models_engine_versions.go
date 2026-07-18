package memorydb

// EngineVersion describes a supported MemoryDB engine version.
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

type engineVersionObject struct {
	Engine               string `json:"Engine,omitempty"`
	EngineVersion        string `json:"EngineVersion,omitempty"`
	EnginePatchVersion   string `json:"EnginePatchVersion,omitempty"`
	ParameterGroupFamily string `json:"ParameterGroupFamily,omitempty"`
	Description          string `json:"Description,omitempty"`
}

type describeEngineVersionsResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	EngineVersions []engineVersionObject `json:"EngineVersions"`
}

// -- Event request/response types --------------------------------------------
