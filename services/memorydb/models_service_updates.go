package memorydb

import (
	"time"
)

// ServiceUpdate represents an in-memory MemoryDB service update.
//
// NOTE: the real SDK's ServiceUpdate type also has "ClusterName" and
// "NodesUpdated" fields (confirmed via deserializers.go's
// awsAwsjson11_deserializeDocumentServiceUpdate), reflecting that a real
// ServiceUpdate entry is scoped to a specific cluster it applies to. This
// backend models service updates as global (not tied to specific clusters),
// so those two fields aren't modeled -- see PARITY.md gaps (adding fabricated
// placeholder values would itself violate the no-stub rule).
type ServiceUpdate struct {
	ReleaseDate         time.Time `json:"releaseDate"`
	AutoUpdateStartDate time.Time `json:"autoUpdateStartDate"`
	ServiceUpdateName   string    `json:"serviceUpdateName"`
	Description         string    `json:"description"`
	Status              string    `json:"status"`
	Type                string    `json:"type"`
	Engine              string    `json:"engine"`
}

type describeServiceUpdatesRequest struct {
	MaxResults        *int32   `json:"MaxResults,omitempty"`
	ServiceUpdateName string   `json:"ServiceUpdateName,omitempty"`
	NextToken         string   `json:"NextToken,omitempty"`
	ClusterNames      []string `json:"ClusterNames,omitempty"`
	Status            []string `json:"Status,omitempty"`
}

// serviceUpdateObject.ReleaseDate/AutoUpdateStartDate are epoch seconds
// (float64), matching the real ServiceUpdate TStamp shapes. Engine added
// (real field, confirmed via deserializers.go's
// awsAwsjson11_deserializeDocumentServiceUpdate); ClusterName/NodesUpdated
// intentionally not modeled, see ServiceUpdate's doc comment.
type serviceUpdateObject struct {
	ServiceUpdateName   string  `json:"ServiceUpdateName,omitempty"`
	Description         string  `json:"Description,omitempty"`
	Status              string  `json:"Status,omitempty"`
	Type                string  `json:"Type,omitempty"`
	Engine              string  `json:"Engine,omitempty"`
	ReleaseDate         float64 `json:"ReleaseDate,omitempty"`
	AutoUpdateStartDate float64 `json:"AutoUpdateStartDate,omitempty"`
}

type describeServiceUpdatesResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	ServiceUpdates []serviceUpdateObject `json:"ServiceUpdates"`
}

// -- ReservedNode request/response types -------------------------------------
