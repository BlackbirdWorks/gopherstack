package memorydb

import (
	"time"
)

// ServiceUpdate represents a MemoryDB service update, scoped to one cluster
// it applies to -- matching the real SDK's ServiceUpdate.ClusterName field
// (confirmed via deserializers.go's awsAwsjson11_deserializeDocumentServiceUpdate).
// b.serviceUpdates stores update *definitions* (ClusterName ""); DescribeServiceUpdates
// fans a definition out into one ServiceUpdate per matching cluster, filling in
// ClusterName. NodesUpdated is left "" -- this backend has no per-node update
// tracking, and the real field is honestly absent rather than fabricated.
type ServiceUpdate struct {
	ReleaseDate         time.Time `json:"releaseDate"`
	AutoUpdateStartDate time.Time `json:"autoUpdateStartDate"`
	ServiceUpdateName   string    `json:"serviceUpdateName"`
	ClusterName         string    `json:"clusterName,omitempty"`
	NodesUpdated        string    `json:"nodesUpdated,omitempty"`
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
// (float64), matching the real ServiceUpdate TStamp shapes. ClusterName/
// NodesUpdated/Engine confirmed real fields via deserializers.go's
// awsAwsjson11_deserializeDocumentServiceUpdate.
type serviceUpdateObject struct {
	ServiceUpdateName   string  `json:"ServiceUpdateName,omitempty"`
	ClusterName         string  `json:"ClusterName,omitempty"`
	NodesUpdated        string  `json:"NodesUpdated,omitempty"`
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
