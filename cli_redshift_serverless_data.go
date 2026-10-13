package main

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	redshiftbackend "github.com/blackbirdworks/gopherstack/services/redshift"
	redshiftdatabackend "github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// wireRedshiftServerlessData lets the Redshift Data API validate WorkgroupName against live serverless workgroups.
func wireRedshiftServerlessData(byName map[string]service.Registerable) {
	serverless, ok := byName["RedshiftServerless"].(*redshiftbackend.ServerlessHandler)
	if !ok {
		return
	}

	if data, dok := byName["RedshiftData"].(*redshiftdatabackend.Handler); dok {
		data.SetWorkgroupResolver(serverless)
	}
}

// applyRedshiftServerless sets the namespace and workgroup dwell in transitional states.
func applyRedshiftServerless(reg service.Registerable, d time.Duration) {
	if h, ok := reg.(*redshiftbackend.ServerlessHandler); ok {
		h.Backend.SetServerlessLifecycleDelay(d)
	}
}
