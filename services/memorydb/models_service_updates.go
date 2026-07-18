package memorydb

import (
	"time"
)

// ServiceUpdate represents an in-memory MemoryDB service update.
type ServiceUpdate struct {
	ReleaseDate         time.Time `json:"releaseDate"`
	AutoUpdateStartDate time.Time `json:"autoUpdateStartDate"`
	ServiceUpdateName   string    `json:"serviceUpdateName"`
	Description         string    `json:"description"`
	Status              string    `json:"status"`
	Type                string    `json:"type"`
}

type describeServiceUpdatesRequest struct {
	MaxResults        *int32   `json:"MaxResults,omitempty"`
	ServiceUpdateName string   `json:"ServiceUpdateName,omitempty"`
	NextToken         string   `json:"NextToken,omitempty"`
	ClusterNames      []string `json:"ClusterNames,omitempty"`
	Status            []string `json:"Status,omitempty"`
}

// serviceUpdateObject.ReleaseDate/AutoUpdateStartDate are epoch seconds
// (float64), matching the real ServiceUpdate TStamp shapes.
type serviceUpdateObject struct {
	ServiceUpdateName   string  `json:"ServiceUpdateName,omitempty"`
	Description         string  `json:"Description,omitempty"`
	Status              string  `json:"Status,omitempty"`
	Type                string  `json:"Type,omitempty"`
	ReleaseDate         float64 `json:"ReleaseDate,omitempty"`
	AutoUpdateStartDate float64 `json:"AutoUpdateStartDate,omitempty"`
}

type describeServiceUpdatesResponse struct {
	NextToken      string                `json:"NextToken,omitempty"`
	ServiceUpdates []serviceUpdateObject `json:"ServiceUpdates"`
}

// -- ReservedNode request/response types -------------------------------------
