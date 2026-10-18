package app

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

type redshiftClusterEngines struct{ h *redshiftbackend.Handler }

func (r redshiftClusterEngines) EngineLogin(region, clusterID string) (redshiftdatabackend.EngineLogin, bool) {
	l, ok := r.h.EngineLogin(region, clusterID)

	return redshiftdatabackend.EngineLogin(l), ok
}

// wireRedshiftClusterEngines lets the Redshift Data API run SQL on clusters backed by a live engine.
func wireRedshiftClusterEngines(byName map[string]service.Registerable) {
	cluster, ok := byName["Redshift"].(*redshiftbackend.Handler)
	if !ok {
		return
	}

	if data, dok := byName["RedshiftData"].(*redshiftdatabackend.Handler); dok {
		data.SetClusterEngines(redshiftClusterEngines{h: cluster})
	}
}

// applyRedshiftServerless sets the namespace and workgroup dwell in transitional states.
func applyRedshiftServerless(reg service.Registerable, d time.Duration) {
	if h, ok := reg.(*redshiftbackend.ServerlessHandler); ok {
		h.Backend.SetServerlessLifecycleDelay(d)
	}
}
