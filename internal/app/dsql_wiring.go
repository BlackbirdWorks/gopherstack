package app

import (
	"net/http"
	"path/filepath"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/dsql"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	dsqlCAPath      = "/_gopherstack/dsql/ca.pem"
	dsqlClusterPath = "/_gopherstack/dsql/clusters/:id"
)

// GetDSQLCADir persists the docker-mode DSQL CA under the data dir when --persist is on (dsql.CADirConfig).
func (c *CLI) GetDSQLCADir() string {
	if !c.Persist {
		return ""
	}

	return filepath.Join(c.resolvedDataDir(), "dsql-ca")
}

// wireDSQLAuth connects DSQL IAM-token authentication to IAM and STS and serves the data-plane CA and each
// cluster's connection address.
func wireDSQLAuth(e *echo.Echo, services []service.Registerable, enforceIAM bool) {
	var (
		dsqlH *dsql.Handler
		iamH  *iambackend.Handler
		stsH  *stsbackend.Handler
	)

	for _, svc := range services {
		switch h := svc.(type) {
		case *dsql.Handler:
			dsqlH = h
		case *iambackend.Handler:
			iamH = h
		case *stsbackend.Handler:
			stsH = h
		}
	}

	if dsqlH == nil {
		return
	}

	bk, ok := dsqlH.Backend.(*dsql.InMemoryBackend)
	if !ok {
		return
	}

	bk.SetIAMAuth(func() dsql.IAMAuthConfig { return dsqlIAMAuthConfig(iamH, stsH, enforceIAM) })

	e.GET(dsqlCAPath, func(c *echo.Context) error {
		pem := bk.EngineCAPEM()
		if len(pem) == 0 {
			return c.String(http.StatusNotFound, "DSQL docker engine is not enabled")
		}

		return c.Blob(http.StatusOK, "application/x-pem-file", pem)
	})

	e.GET(dsqlClusterPath, func(c *echo.Context) error {
		id := c.Param("id")

		host, port, found := bk.ConnectAddr(id)
		if !found {
			return c.String(http.StatusNotFound, "cluster has no running data plane")
		}

		cl, err := bk.GetCluster(id)
		if err != nil {
			return c.String(http.StatusNotFound, "cluster not found")
		}

		return c.JSON(http.StatusOK, map[string]any{"host": host, "port": port, "endpoint": cl.Endpoint})
	})
}

func dsqlIAMAuthConfig(iamH *iambackend.Handler, stsH *stsbackend.Handler, enforceIAM bool) dsql.IAMAuthConfig {
	cfg := dsql.IAMAuthConfig{Enforce: enforceIAM}

	if iamH != nil {
		if ids, ok := iamH.Backend.(dsql.IAMAuthIdentityProvider); ok {
			cfg.Identities = ids
		}
	}

	if stsH != nil {
		if ss, ok := stsH.Backend.(dsql.IAMAuthSessionProvider); ok {
			cfg.Sessions = ss
		}
	}

	return cfg
}
