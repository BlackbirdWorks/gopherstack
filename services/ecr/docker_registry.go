package ecr

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
)

// newDistributionRegistry creates the embedded Docker Registry v2 [http.Handler]: in-memory storage and no
// authentication. The result also satisfies the Shutdown() error interface the Handler releases it with.
func newDistributionRegistry(parent context.Context) http.Handler {
	return ociregistry.New(parent, ociregistry.Options{InstanceID: "gopherstack-ecr"})
}
