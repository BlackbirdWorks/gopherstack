package lambdaweb

import (
	"context"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// StorageBackend is the Lambda Web Functions backend.
type StorageBackend interface {
	CreateWebFunction(ctx context.Context, region string, in CreateFunctionInput) (*CreatedFunction, error)
	GetWebFunction(region, ref string) (*Function, error)
	ListWebFunctions(region string, opts ListOptions) (page.Page[*Function], error)
	DeleteWebFunction(region, ref string) error

	CreateWebFunctionRevision(ctx context.Context, region, ref string, in RevisionInput) (*Revision, error)
	GetWebFunctionRevision(region, fnRef, revRef string) (*Revision, error)
	ListWebFunctionRevisions(region, ref string, opts ListOptions) (page.Page[*Revision], error)
	DeleteWebFunctionRevision(region, fnRef, revRef string) error

	CreateWebFunctionEndpoint(region, ref string, in EndpointInput) (*Endpoint, error)
	GetWebFunctionEndpoint(region, fnRef, epRef string) (*Endpoint, error)
	ListWebFunctionEndpoints(region, ref string, opts ListOptions) (page.Page[*Endpoint], error)
	UpdateWebFunctionEndpoint(region, fnRef, epRef string, up EndpointUpdate) (*Endpoint, error)
	DeleteWebFunctionEndpoint(region, fnRef, epRef string) error

	PutResourcePolicy(resourceARN, policy, revisionID string) (*ResourcePolicy, error)
	GetResourcePolicy(resourceARN string) (*ResourcePolicy, error)
	DeleteResourcePolicy(resourceARN, revisionID string) error

	AccountSettings(region string) AccountSettings

	TagResource(resourceARN string, tags map[string]string) error
	UntagResource(resourceARN string, keys []string) error
	ListTags(resourceARN string) (map[string]string, error)

	// ServeEndpoint serves a data-plane request; it reports false when the Host names no endpoint domain.
	ServeEndpoint(w http.ResponseWriter, r *http.Request) bool
	// EndpointAuthType returns the authType of the endpoint a Host names, or "".
	EndpointAuthType(host string) string

	Reset()
}

var _ StorageBackend = (*InMemoryBackend)(nil)
