package gcpspanner

import (
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
)

// API groups the Spanner admin services for in-process callers (dashboard proxy).
type API struct {
	Instances instancepb.InstanceAdminServer
	Databases databasepb.DatabaseAdminServer
}

// API exposes the instance and database admin services.
func (h *Handler) API() *API {
	return &API{Instances: &instanceServer{b: h.Backend}, Databases: &databaseServer{b: h.Backend}}
}
