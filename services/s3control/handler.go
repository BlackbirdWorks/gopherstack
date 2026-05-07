package s3control

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// defaultAccountID is used when no account ID is provided in the request header.
	defaultAccountID = "default"

	// Path constants for S3 Control operations.
	pathPublicAccessBlock    = "/configuration/publicAccessBlock"
	pathAccessGrantsInstance = "/v20180820/accessgrantsinstance"
	pathIdentityCenter       = "/v20180820/accessgrantsinstance/identitycenter"
	pathAccessGrant          = "/v20180820/accessgrantsinstance/grant"
	pathAccessGrantsLocation = "/v20180820/accessgrantsinstance/location"
	pathAccessPointPrefix    = "/v20180820/accesspoint/"
	pathObjectLambdaPrefix   = "/v20180820/accesspointforobjectlambda/"
	pathBucketPrefix         = "/v20180820/bucket/"
	pathJobs                 = "/v20180820/jobs"
	pathMRAPCreate           = "/v20180820/async-requests/mrap/create"
	pathStorageLensGroup     = "/v20180820/storagelensgroup"

	// Additional path constants for stub operations.
	pathAccessGrantsInstanceResourcePolicy = "/v20180820/accessgrantsinstance/resourcepolicy"
	pathAccessGrantsInstancePrefix         = "/v20180820/accessgrantsinstance/"
	pathAccessGrantsLocationPrefix         = "/v20180820/accessgrantsinstance/location/"
	pathAccessGrantPrefix                  = "/v20180820/accessgrantsinstance/grant/"
	pathDataAccess                         = "/v20180820/accessgrantsinstance/dataaccess"
	pathCallerAccessGrants                 = "/v20180820/accessgrantsinstance/caller-grants"
	pathMRAPPrefix                         = "/v20180820/async-requests/mrap/"
	pathMRAPDeletePrefix                   = "/v20180820/async-requests/mrap/delete"
	pathMRAPPutPolicyPrefix                = "/v20180820/async-requests/mrap/put_policy"
	pathMRAPList                           = "/v20180820/mrap/instances"
	pathMRAPInstancePrefix                 = "/v20180820/mrap/instances/"
	pathStorageLensPrefix                  = "/v20180820/storagelens/"
	pathStorageLensList                    = "/v20180820/storagelens"
	pathStorageLensGroupPrefix             = "/v20180820/storagelensgroup/"
	pathTagsPrefix                         = "/v20180820/tags/"
	pathJobPrefix                          = "/v20180820/jobs/"
	pathAccessPointsDirectoryBuckets       = "/v20180820/accesspointfordirectories"
	pathAccessPointsForObjectLambdaList    = "/v20180820/accesspointforobjectlambda"
	pathRegionalBuckets                    = "/v20180820/bucket"

	// opDeleteMRAP is the operation name for DeleteMultiRegionAccessPoint.
	// It is used in multiple dispatch cases to avoid a goconst violation.
	opDeleteMRAP = "DeleteMultiRegionAccessPoint"
)

// Handler is the Echo HTTP handler for S3 Control operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new S3 Control handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Reset clears all backend state.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Name returns the service name.
func (h *Handler) Name() string { return "S3Control" }

// GetSupportedOperations returns the list of supported S3 Control operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		// Public access block
		"DeletePublicAccessBlock",
		"GetPublicAccessBlock",
		"PutPublicAccessBlock",
		// Access Grants Instance
		"AssociateAccessGrantsIdentityCenter",
		"CreateAccessGrantsInstance",
		"DeleteAccessGrantsInstance",
		"DeleteAccessGrantsInstanceResourcePolicy",
		"DissociateAccessGrantsIdentityCenter",
		"GetAccessGrantsInstance",
		"GetAccessGrantsInstanceForPrefix",
		"GetAccessGrantsInstanceResourcePolicy",
		"ListAccessGrantsInstances",
		"PutAccessGrantsInstanceResourcePolicy",
		// Access Grants
		"CreateAccessGrant",
		"DeleteAccessGrant",
		"GetAccessGrant",
		"GetDataAccess",
		"ListAccessGrants",
		"ListCallerAccessGrants",
		// Access Grants Locations
		"CreateAccessGrantsLocation",
		"DeleteAccessGrantsLocation",
		"GetAccessGrantsLocation",
		"ListAccessGrantsLocations",
		"UpdateAccessGrantsLocation",
		// Access Points
		"CreateAccessPoint",
		"DeleteAccessPoint",
		"GetAccessPoint",
		"GetAccessPointPolicy",
		"GetAccessPointPolicyStatus",
		"GetAccessPointScope",
		"ListAccessPoints",
		"ListAccessPointsForDirectoryBuckets",
		"PutAccessPointPolicy",
		"PutAccessPointScope",
		"DeleteAccessPointPolicy",
		"DeleteAccessPointScope",
		// Object Lambda Access Points
		"CreateAccessPointForObjectLambda",
		"DeleteAccessPointForObjectLambda",
		"DeleteAccessPointPolicyForObjectLambda",
		"GetAccessPointConfigurationForObjectLambda",
		"GetAccessPointForObjectLambda",
		"GetAccessPointPolicyForObjectLambda",
		"GetAccessPointPolicyStatusForObjectLambda",
		"ListAccessPointsForObjectLambda",
		"PutAccessPointConfigurationForObjectLambda",
		"PutAccessPointPolicyForObjectLambda",
		// Outposts Buckets
		"CreateBucket",
		"DeleteBucket",
		"DeleteBucketLifecycleConfiguration",
		"DeleteBucketPolicy",
		"DeleteBucketReplication",
		"DeleteBucketTagging",
		"GetBucket",
		"GetBucketLifecycleConfiguration",
		"GetBucketPolicy",
		"GetBucketReplication",
		"GetBucketTagging",
		"GetBucketVersioning",
		"ListRegionalBuckets",
		"PutBucketLifecycleConfiguration",
		"PutBucketPolicy",
		"PutBucketReplication",
		"PutBucketTagging",
		"PutBucketVersioning",
		// Batch Jobs
		"CreateJob",
		"DeleteJobTagging",
		"DescribeJob",
		"GetJobTagging",
		"ListJobs",
		"PutJobTagging",
		"UpdateJobPriority",
		"UpdateJobStatus",
		// MRAP
		"CreateMultiRegionAccessPoint",
		"DeleteMultiRegionAccessPoint",
		"DescribeMultiRegionAccessPointOperation",
		"GetMultiRegionAccessPoint",
		"GetMultiRegionAccessPointPolicy",
		"GetMultiRegionAccessPointPolicyStatus",
		"GetMultiRegionAccessPointRoutes",
		"ListMultiRegionAccessPoints",
		"PutMultiRegionAccessPointPolicy",
		"SubmitMultiRegionAccessPointRoutes",
		// Storage Lens
		"DeleteStorageLensConfiguration",
		"DeleteStorageLensConfigurationTagging",
		"GetStorageLensConfiguration",
		"GetStorageLensConfigurationTagging",
		"ListStorageLensConfigurations",
		"PutStorageLensConfiguration",
		"PutStorageLensConfigurationTagging",
		// Storage Lens Groups
		"CreateStorageLensGroup",
		"DeleteStorageLensGroup",
		"GetStorageLensGroup",
		"ListStorageLensGroups",
		"UpdateStorageLensGroup",
		// Resource Tags
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "s3" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this S3 Control instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function that matches S3 Control requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, "/v20180820/")
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation extracts the S3 Control operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	r := c.Request()
	path := r.URL.Path
	method := r.Method

	if strings.HasSuffix(path, pathPublicAccessBlock) {
		return extractPublicAccessBlockOp(method)
	}

	return extractNewOpsOperation(path, method)
}

func extractPublicAccessBlockOp(method string) string {
	switch method {
	case http.MethodGet:
		return "GetPublicAccessBlock"
	case http.MethodPut:
		return "PutPublicAccessBlock"
	case http.MethodDelete:
		return "DeletePublicAccessBlock"
	}

	return "Unknown"
}

// isSimplePath returns true if path has the given prefix and the remainder
// contains no "/" (i.e., it is a direct resource path, not a sub-resource).
func isSimplePath(prefix, path string) bool {
	return strings.HasPrefix(path, prefix) &&
		!strings.Contains(strings.TrimPrefix(path, prefix), "/")
}

// isGrantsLocationPath returns true if path has the access grants location prefix
// and the remainder does NOT start with "s" (avoiding "storagelensgroup" collisions).
func isGrantsLocationPath(path string) bool {
	return strings.HasPrefix(path, pathAccessGrantsLocationPrefix) &&
		!strings.HasPrefix(strings.TrimPrefix(path, pathAccessGrantsLocationPrefix), "s")
}

// isPrefixSuffix returns true if path has the given prefix AND suffix.
func isPrefixSuffix(prefix, path, suffix string) bool {
	return strings.HasPrefix(path, prefix) && strings.HasSuffix(path, suffix)
}

func extractNewOpsOperation(path, method string) string { //nolint:cyclop,gocognit,gocyclo // path dispatch table
	switch {
	case path == pathAccessGrantsInstance && method == http.MethodPost:
		return "CreateAccessGrantsInstance"
	case path == pathAccessGrantsInstance && method == http.MethodGet:
		return "GetAccessGrantsInstance"
	case path == pathAccessGrantsInstance && method == http.MethodDelete:
		return "DeleteAccessGrantsInstance"
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodGet:
		return "GetAccessGrantsInstanceResourcePolicy"
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodPut:
		return "PutAccessGrantsInstanceResourcePolicy"
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodDelete:
		return "DeleteAccessGrantsInstanceResourcePolicy"
	case path == pathIdentityCenter && method == http.MethodPost:
		return "AssociateAccessGrantsIdentityCenter"
	case path == pathIdentityCenter && method == http.MethodDelete:
		return "DissociateAccessGrantsIdentityCenter"
	case path == pathAccessGrant && method == http.MethodPost:
		return "CreateAccessGrant"
	case path == pathAccessGrant && method == http.MethodGet:
		return "ListAccessGrants"
	case path == pathCallerAccessGrants && method == http.MethodGet:
		return "ListCallerAccessGrants"
	case path == pathDataAccess && method == http.MethodGet:
		return "GetDataAccess"
	case path == pathAccessGrantsLocation && method == http.MethodPost:
		return "CreateAccessGrantsLocation"
	case path == pathAccessGrantsLocation && method == http.MethodGet:
		return "ListAccessGrantsLocations"
	case strings.HasPrefix(path, pathAccessGrantsInstancePrefix+"prefix") && method == http.MethodGet:
		return "GetAccessGrantsInstanceForPrefix"
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodGet:
		return "GetAccessGrant"
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodDelete:
		return "DeleteAccessGrant"
	case isGrantsLocationPath(path) && method == http.MethodGet:
		return "GetAccessGrantsLocation"
	case isGrantsLocationPath(path) && method == http.MethodDelete:
		return "DeleteAccessGrantsLocation"
	case isGrantsLocationPath(path) && method == http.MethodPut:
		return "UpdateAccessGrantsLocation"
	}

	return extractAccessPointOpsOperation(path, method)
}

//nolint:cyclop,gocognit,gocyclo // path dispatch table for access point and object lambda ops
func extractAccessPointOpsOperation(path, method string) string {
	switch {
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodPut:
		return "CreateAccessPoint"
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodGet:
		return "GetAccessPoint"
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodDelete:
		return "DeleteAccessPoint"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodGet:
		return "GetAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodPut:
		return "PutAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodDelete:
		return "DeleteAccessPointPolicy"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetAccessPointPolicyStatus"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodGet:
		return "GetAccessPointScope"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodPut:
		return "PutAccessPointScope"
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodDelete:
		return "DeleteAccessPointScope"
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodPut:
		return "CreateAccessPointForObjectLambda"
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodGet:
		return "GetAccessPointForObjectLambda"
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodDelete:
		return "DeleteAccessPointForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodGet:
		return "GetAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodPut:
		return "PutAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodDelete:
		return "DeleteAccessPointPolicyForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetAccessPointPolicyStatusForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodGet:
		return "GetAccessPointConfigurationForObjectLambda"
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodPut:
		return "PutAccessPointConfigurationForObjectLambda"
	case path == "/v20180820/accesspoint" && method == http.MethodGet:
		return "ListAccessPoints"
	case path == pathAccessPointsDirectoryBuckets && method == http.MethodGet:
		return "ListAccessPointsForDirectoryBuckets"
	case path == pathAccessPointsForObjectLambdaList && method == http.MethodGet:
		return "ListAccessPointsForObjectLambda"
	}

	return extractBucketOpsOperation(path, method)
}

func extractBucketOpsOperation(path, method string) string { //nolint:cyclop,gocyclo // path dispatch table
	switch {
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodPut:
		return "CreateBucket"
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodGet:
		return "GetBucket"
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodDelete:
		return "DeleteBucket"
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodGet:
		return "GetBucketLifecycleConfiguration"
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodPut:
		return "PutBucketLifecycleConfiguration"
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodDelete:
		return "DeleteBucketLifecycleConfiguration"
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodGet:
		return "GetBucketPolicy"
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodPut:
		return "PutBucketPolicy"
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodDelete:
		return "DeleteBucketPolicy"
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodGet:
		return "GetBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodPut:
		return "PutBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodDelete:
		return "DeleteBucketReplication"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodGet:
		return "GetBucketTagging"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodPut:
		return "PutBucketTagging"
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodDelete:
		return "DeleteBucketTagging"
	case isPrefixSuffix(pathBucketPrefix, path, "/versioning") && method == http.MethodGet:
		return "GetBucketVersioning"
	case isPrefixSuffix(pathBucketPrefix, path, "/versioning") && method == http.MethodPut:
		return "PutBucketVersioning"
	case path == pathRegionalBuckets && method == http.MethodGet:
		return "ListRegionalBuckets"
	}

	return extractJobMRAPStorageLensOps(path, method)
}

func extractJobMRAPStorageLensOps(path, method string) string { //nolint:cyclop,gocyclo // path dispatch table
	switch {
	case path == pathJobs && method == http.MethodPost:
		return "CreateJob"
	case path == pathJobs && method == http.MethodGet:
		return "ListJobs"
	case isSimplePath(pathJobPrefix, path) && method == http.MethodGet:
		return "DescribeJob"
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodGet:
		return "GetJobTagging"
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodPut:
		return "PutJobTagging"
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodDelete:
		return "DeleteJobTagging"
	case isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPut:
		return "UpdateJobPriority"
	case isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPut:
		return "UpdateJobStatus"
	case path == pathMRAPCreate && method == http.MethodPost:
		return "CreateMultiRegionAccessPoint"
	case strings.HasPrefix(path, pathMRAPDeletePrefix) && method == http.MethodPost:
		return "DeleteMultiRegionAccessPoint"
	case strings.HasPrefix(path, pathMRAPPutPolicyPrefix) && method == http.MethodPost:
		return "PutMultiRegionAccessPointPolicy"
	case strings.HasPrefix(path, pathMRAPPrefix) && method == http.MethodGet:
		return "DescribeMultiRegionAccessPointOperation"
	case path == pathMRAPList && method == http.MethodGet:
		return "ListMultiRegionAccessPoints"
	case isSimplePath(pathMRAPInstancePrefix, path) && method == http.MethodGet:
		return "GetMultiRegionAccessPoint"
	case isSimplePath(pathMRAPInstancePrefix, path) && method == http.MethodDelete:
		return opDeleteMRAP
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicy"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicyStatus"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return "GetMultiRegionAccessPointRoutes"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return "SubmitMultiRegionAccessPointRoutes"
	}

	return extractStorageLensTagOps(path, method)
}

func extractStorageLensTagOps(path, method string) string { //nolint:cyclop,gocyclo // path dispatch table
	switch {
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodGet:
		return "GetStorageLensConfiguration"
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodPut:
		return "PutStorageLensConfiguration"
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodDelete:
		return "DeleteStorageLensConfiguration"
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodGet:
		return "GetStorageLensConfigurationTagging"
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodPut:
		return "PutStorageLensConfigurationTagging"
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodDelete:
		return "DeleteStorageLensConfigurationTagging"
	case path == pathStorageLensList && method == http.MethodGet:
		return "ListStorageLensConfigurations"
	case path == pathStorageLensGroup && method == http.MethodPost:
		return "CreateStorageLensGroup"
	case path == pathStorageLensGroup && method == http.MethodGet:
		return "ListStorageLensGroups"
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodGet:
		return "GetStorageLensGroup"
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodPut:
		return "UpdateStorageLensGroup"
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodDelete:
		return "DeleteStorageLensGroup"
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodGet:
		return "ListTagsForResource"
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodPost:
		return "TagResource"
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodDelete:
		return "UntagResource"
	}

	return "Unknown"
}

// ExtractResource returns the account ID from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().Header.Get("X-Amz-Account-Id")
}

// Handler returns the Echo handler function.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method

		if strings.HasSuffix(path, pathPublicAccessBlock) {
			return h.dispatchPublicAccessBlock(c, method)
		}

		return h.dispatchNewOps(c, path, method)
	}
}

func (h *Handler) dispatchPublicAccessBlock(c *echo.Context, method string) error {
	switch method {
	case http.MethodGet:
		return h.handleGetPublicAccessBlock(c)
	case http.MethodPut:
		return h.handlePutPublicAccessBlock(c)
	case http.MethodDelete:
		return h.handleDeletePublicAccessBlock(c)
	}

	return c.String(http.StatusNotFound, "not found")
}

//nolint:cyclop,gocognit,gocyclo // dispatch table for access grants ops
func (h *Handler) dispatchNewOps(c *echo.Context, path, method string) error {
	switch {
	case path == pathAccessGrantsInstance && method == http.MethodPost:
		return h.handleCreateAccessGrantsInstance(c)
	case path == pathAccessGrantsInstance && method == http.MethodGet:
		return h.handleStub(c, "GetAccessGrantsInstance")
	case path == pathAccessGrantsInstance && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessGrantsInstance")
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodGet:
		return h.handleStub(c, "GetAccessGrantsInstanceResourcePolicy")
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodPut:
		return h.handleStub(c, "PutAccessGrantsInstanceResourcePolicy")
	case path == pathAccessGrantsInstanceResourcePolicy && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessGrantsInstanceResourcePolicy")
	case path == pathIdentityCenter && method == http.MethodPost:
		return h.handleAssociateAccessGrantsIdentityCenter(c)
	case path == pathIdentityCenter && method == http.MethodDelete:
		return h.handleStub(c, "DissociateAccessGrantsIdentityCenter")
	case path == pathAccessGrant && method == http.MethodPost:
		return h.handleCreateAccessGrant(c)
	case path == pathAccessGrant && method == http.MethodGet:
		return h.handleStub(c, "ListAccessGrants")
	case path == pathCallerAccessGrants && method == http.MethodGet:
		return h.handleStub(c, "ListCallerAccessGrants")
	case path == pathDataAccess && method == http.MethodGet:
		return h.handleStub(c, "GetDataAccess")
	case path == pathAccessGrantsLocation && method == http.MethodPost:
		return h.handleCreateAccessGrantsLocation(c)
	case path == pathAccessGrantsLocation && method == http.MethodGet:
		return h.handleStub(c, "ListAccessGrantsLocations")
	case strings.HasPrefix(path, pathAccessGrantsInstancePrefix+"prefix") && method == http.MethodGet:
		return h.handleStub(c, "GetAccessGrantsInstanceForPrefix")
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodGet:
		return h.handleStub(c, "GetAccessGrant")
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessGrant")
	case isGrantsLocationPath(path) && method == http.MethodGet:
		return h.handleStub(c, "GetAccessGrantsLocation")
	case isGrantsLocationPath(path) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessGrantsLocation")
	case isGrantsLocationPath(path) && method == http.MethodPut:
		return h.handleStub(c, "UpdateAccessGrantsLocation")
	}

	return h.dispatchAccessPointOps(c, path, method)
}

//nolint:cyclop,gocognit,gocyclo // dispatch table for access point ops
func (h *Handler) dispatchAccessPointOps(c *echo.Context, path, method string) error {
	switch {
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodPut:
		return h.handleCreateAccessPoint(c)
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodGet:
		return h.handleGetAccessPoint(c)
	case isSimplePath(pathAccessPointPrefix, path) && method == http.MethodDelete:
		return h.handleDeleteAccessPoint(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodGet:
		return h.handleGetAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodPut:
		return h.handlePutAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodDelete:
		return h.handleDeleteAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policyStatus") && method == http.MethodGet:
		return h.handleGetAccessPointPolicyStatus(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodGet:
		return h.handleStub(c, "GetAccessPointScope")
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodPut:
		return h.handleStub(c, "PutAccessPointScope")
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessPointScope")
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodPut:
		return h.handleCreateAccessPointForObjectLambda(c)
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodGet:
		return h.handleStub(c, "GetAccessPointForObjectLambda")
	case isSimplePath(pathObjectLambdaPrefix, path) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessPointForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodGet:
		return h.handleStub(c, "GetAccessPointPolicyForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodPut:
		return h.handleStub(c, "PutAccessPointPolicyForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteAccessPointPolicyForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policyStatus") && method == http.MethodGet:
		return h.handleStub(c, "GetAccessPointPolicyStatusForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodGet:
		return h.handleStub(c, "GetAccessPointConfigurationForObjectLambda")
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration") && method == http.MethodPut:
		return h.handleStub(c, "PutAccessPointConfigurationForObjectLambda")
	case path == "/v20180820/accesspoint" && method == http.MethodGet:
		return h.handleListAccessPoints(c)
	case path == pathAccessPointsDirectoryBuckets && method == http.MethodGet:
		return h.handleStub(c, "ListAccessPointsForDirectoryBuckets")
	case path == pathAccessPointsForObjectLambdaList && method == http.MethodGet:
		return h.handleStub(c, "ListAccessPointsForObjectLambda")
	}

	return h.dispatchBucketOps(c, path, method)
}

//nolint:cyclop,gocyclo // dispatch table for outposts bucket ops
func (h *Handler) dispatchBucketOps(c *echo.Context, path, method string) error {
	switch {
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodPut:
		return h.handleCreateBucket(c)
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodGet:
		return h.handleStub(c, "GetBucket")
	case isSimplePath(pathBucketPrefix, path) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteBucket")
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodGet:
		return h.handleStub(c, "GetBucketLifecycleConfiguration")
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodPut:
		return h.handleStub(c, "PutBucketLifecycleConfiguration")
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteBucketLifecycleConfiguration")
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodGet:
		return h.handleStub(c, "GetBucketPolicy")
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodPut:
		return h.handleStub(c, "PutBucketPolicy")
	case isPrefixSuffix(pathBucketPrefix, path, "/policy") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteBucketPolicy")
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodGet:
		return h.handleStub(c, "GetBucketReplication")
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodPut:
		return h.handleStub(c, "PutBucketReplication")
	case isPrefixSuffix(pathBucketPrefix, path, "/replication") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteBucketReplication")
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodGet:
		return h.handleStub(c, "GetBucketTagging")
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodPut:
		return h.handleStub(c, "PutBucketTagging")
	case isPrefixSuffix(pathBucketPrefix, path, "/tagging") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteBucketTagging")
	case isPrefixSuffix(pathBucketPrefix, path, "/versioning") && method == http.MethodGet:
		return h.handleStub(c, "GetBucketVersioning")
	case isPrefixSuffix(pathBucketPrefix, path, "/versioning") && method == http.MethodPut:
		return h.handleStub(c, "PutBucketVersioning")
	case path == pathRegionalBuckets && method == http.MethodGet:
		return h.handleStub(c, "ListRegionalBuckets")
	}

	return h.dispatchJobMRAPStorageLensOps(c, path, method)
}

//nolint:cyclop,gocyclo // dispatch table for job and MRAP ops
func (h *Handler) dispatchJobMRAPStorageLensOps(c *echo.Context, path, method string) error {
	switch {
	case path == pathJobs && method == http.MethodPost:
		return h.handleCreateJob(c)
	case path == pathJobs && method == http.MethodGet:
		return h.handleListJobs(c)
	case isSimplePath(pathJobPrefix, path) && method == http.MethodGet:
		return h.handleDescribeJob(c)
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodGet:
		return h.handleStub(c, "GetJobTagging")
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodPut:
		return h.handleStub(c, "PutJobTagging")
	case isPrefixSuffix(pathJobPrefix, path, "/tagging") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteJobTagging")
	case isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPut:
		return h.handleUpdateJobPriority(c)
	case isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPut:
		return h.handleUpdateJobStatus(c)
	case path == pathMRAPCreate && method == http.MethodPost:
		return h.handleCreateMultiRegionAccessPoint(c)
	case strings.HasPrefix(path, pathMRAPDeletePrefix) && method == http.MethodPost:
		return h.handleDeleteMultiRegionAccessPointAsync(c)
	case strings.HasPrefix(path, pathMRAPPutPolicyPrefix) && method == http.MethodPost:
		return h.handlePutMultiRegionAccessPointPolicy(c)
	case strings.HasPrefix(path, pathMRAPPrefix) && method == http.MethodGet:
		return h.handleStub(c, "DescribeMultiRegionAccessPointOperation")
	case path == pathMRAPList && method == http.MethodGet:
		return h.handleListMultiRegionAccessPoints(c)
	case isSimplePath(pathMRAPInstancePrefix, path) && method == http.MethodGet:
		return h.handleGetMultiRegionAccessPoint(c)
	case isSimplePath(pathMRAPInstancePrefix, path) && method == http.MethodDelete:
		return h.handleDeleteMultiRegionAccessPoint(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return h.handleStub(c, "GetMultiRegionAccessPointPolicy")
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policyStatus") && method == http.MethodGet:
		return h.handleStub(c, "GetMultiRegionAccessPointPolicyStatus")
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return h.handleStub(c, "GetMultiRegionAccessPointRoutes")
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return h.handleStub(c, "SubmitMultiRegionAccessPointRoutes")
	}

	return h.dispatchStorageLensTagOps(c, path, method)
}

//nolint:cyclop,gocyclo // dispatch table for storage lens and tag ops
func (h *Handler) dispatchStorageLensTagOps(c *echo.Context, path, method string) error {
	switch {
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodGet:
		return h.handleStub(c, "GetStorageLensConfiguration")
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodPut:
		return h.handleStub(c, "PutStorageLensConfiguration")
	case isSimplePath(pathStorageLensPrefix, path) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteStorageLensConfiguration")
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodGet:
		return h.handleStub(c, "GetStorageLensConfigurationTagging")
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodPut:
		return h.handleStub(c, "PutStorageLensConfigurationTagging")
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodDelete:
		return h.handleStub(c, "DeleteStorageLensConfigurationTagging")
	case path == pathStorageLensList && method == http.MethodGet:
		return h.handleStub(c, "ListStorageLensConfigurations")
	case path == pathStorageLensGroup && method == http.MethodPost:
		return h.handleCreateStorageLensGroup(c)
	case path == pathStorageLensGroup && method == http.MethodGet:
		return h.handleStub(c, "ListStorageLensGroups")
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodGet:
		return h.handleStub(c, "GetStorageLensGroup")
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodPut:
		return h.handleStub(c, "UpdateStorageLensGroup")
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodDelete:
		return h.handleStub(c, "DeleteStorageLensGroup")
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodGet:
		return h.handleStub(c, "ListTagsForResource")
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodPost:
		return h.handleStub(c, "TagResource")
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodDelete:
		return h.handleStub(c, "UntagResource")
	}

	return c.String(http.StatusNotFound, "not found")
}

func accountIDFromRequest(c *echo.Context) string {
	accountID := c.Request().Header.Get("X-Amz-Account-Id")
	if accountID == "" {
		return defaultAccountID
	}

	return accountID
}

// handleBackendError maps backend sentinel errors to appropriate HTTP responses.
func handleBackendError(c *echo.Context, err error) error {
	if err == nil {
		return nil
	}

	switch {
	case errors.Is(err, awserr.ErrNotFound):
		return c.String(http.StatusNotFound, err.Error())
	case errors.Is(err, awserr.ErrInvalidParameter):
		return c.String(http.StatusBadRequest, err.Error())
	case errors.Is(err, awserr.ErrAlreadyExists):
		return c.String(http.StatusConflict, err.Error())
	default:
		return c.String(http.StatusInternalServerError, err.Error())
	}
}

// decodeXML decodes the request body into v, treating EOF as an empty-body (not an error).
func decodeXML(c *echo.Context, v any) error {
	if err := xml.NewDecoder(c.Request().Body).Decode(v); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

func writeXML(c *echo.Context, v any) error {
	data, err := xml.Marshal(v)
	if err != nil {
		return c.String(http.StatusInternalServerError, "marshal error")
	}

	return c.Blob(http.StatusOK, "application/xml", append([]byte(xml.Header), data...))
}

// handleStub returns 501 Not Implemented for operations that are stubbed.
// The operation name is included in the response body for debugging.
func (h *Handler) handleStub(c *echo.Context, operation string) error {
	return c.String(http.StatusNotImplemented, operation+" not implemented")
}

// --- public access block handlers ---

type publicAccessBlockConfigurationXML struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

func (h *Handler) handleGetPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	cfg, err := h.Backend.GetPublicAccessBlock(accountID)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	out := publicAccessBlockConfigurationXML{
		BlockPublicAcls:       cfg.BlockPublicAcls,
		IgnorePublicAcls:      cfg.IgnorePublicAcls,
		BlockPublicPolicy:     cfg.BlockPublicPolicy,
		RestrictPublicBuckets: cfg.RestrictPublicBuckets,
	}

	return writeXML(c, out)
}

func (h *Handler) handlePutPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body publicAccessBlockConfigurationXML
	if err := xml.NewDecoder(c.Request().Body).Decode(&body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.PutPublicAccessBlock(PublicAccessBlock{
		AccountID:             accountID,
		BlockPublicAcls:       body.BlockPublicAcls,
		IgnorePublicAcls:      body.IgnorePublicAcls,
		BlockPublicPolicy:     body.BlockPublicPolicy,
		RestrictPublicBuckets: body.RestrictPublicBuckets,
	})

	return c.NoContent(http.StatusCreated)
}

func (h *Handler) handleDeletePublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	if err := h.Backend.DeletePublicAccessBlock(accountID); err != nil {
		if errors.Is(err, ErrNotFound) {
			return c.String(http.StatusNotFound, "NoSuchPublicAccessBlockConfiguration")
		}

		return c.String(http.StatusInternalServerError, err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Access Grants Instance handlers ---

type createAccessGrantsInstanceRequestXML struct {
	XMLName           xml.Name `xml:"CreateAccessGrantsInstanceRequest"`
	IdentityCenterArn string   `xml:"IdentityCenterArn"`
}

type createAccessGrantsInstanceResponseXML struct {
	XMLName                      xml.Name `xml:"CreateAccessGrantsInstanceResult"`
	AccessGrantsInstanceArn      string   `xml:"AccessGrantsInstanceArn"`
	AccessGrantsInstanceID       string   `xml:"AccessGrantsInstanceId"`
	IdentityCenterArn            string   `xml:"IdentityCenterArn,omitempty"`
	IdentityCenterInstanceArn    string   `xml:"IdentityCenterInstanceArn,omitempty"`
	IdentityCenterApplicationArn string   `xml:"IdentityCenterApplicationArn,omitempty"`
}

func (h *Handler) handleCreateAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantsInstanceRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	inst := h.Backend.CreateAccessGrantsInstance(accountID, body.IdentityCenterArn)

	return writeXML(c, createAccessGrantsInstanceResponseXML{
		AccessGrantsInstanceArn:   inst.AccessGrantsInstanceArn,
		AccessGrantsInstanceID:    inst.AccessGrantsInstanceID,
		IdentityCenterArn:         inst.IdentityCenterArn,
		IdentityCenterInstanceArn: inst.IdentityCenterInstanceArn,
	})
}

// --- AssociateAccessGrantsIdentityCenter handler ---

type associateAccessGrantsIdentityCenterRequestXML struct {
	XMLName           xml.Name `xml:"AssociateAccessGrantsIdentityCenterRequest"`
	IdentityCenterArn string   `xml:"IdentityCenterArn"`
}

func (h *Handler) handleAssociateAccessGrantsIdentityCenter(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body associateAccessGrantsIdentityCenterRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.AssociateAccessGrantsIdentityCenter(accountID, body.IdentityCenterArn)

	return c.NoContent(http.StatusOK)
}

// --- CreateAccessGrant handler ---

type createAccessGrantGranteeXML struct {
	GranteeType       string `xml:"GranteeType"`
	GranteeIdentifier string `xml:"GranteeIdentifier"`
}

type createAccessGrantRequestXML struct {
	XMLName                xml.Name                    `xml:"CreateAccessGrantRequest"`
	AccessGrantsLocationID string                      `xml:"AccessGrantsLocationId"`
	Permission             string                      `xml:"Permission"`
	Grantee                createAccessGrantGranteeXML `xml:"Grantee"`
	ApplicationArn         string                      `xml:"ApplicationArn"`
}

type createAccessGrantResponseXML struct {
	XMLName                xml.Name                    `xml:"CreateAccessGrantResult"`
	AccessGrantArn         string                      `xml:"AccessGrantArn"`
	AccessGrantID          string                      `xml:"AccessGrantId"`
	AccessGrantsLocationID string                      `xml:"AccessGrantsLocationId"`
	GrantScope             string                      `xml:"GrantScope"`
	Permission             string                      `xml:"Permission"`
	Grantee                createAccessGrantGranteeXML `xml:"Grantee,omitempty"`
	ApplicationArn         string                      `xml:"ApplicationArn,omitempty"`
}

func (h *Handler) handleCreateAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	grant, err := h.Backend.CreateAccessGrant(
		accountID,
		body.AccessGrantsLocationID,
		body.Grantee.GranteeType,
		body.Grantee.GranteeIdentifier,
		body.Permission,
		body.ApplicationArn,
	)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, createAccessGrantResponseXML{
		AccessGrantArn:         grant.AccessGrantArn,
		AccessGrantID:          grant.AccessGrantID,
		AccessGrantsLocationID: grant.AccessGrantsLocationID,
		GrantScope:             grant.GrantScope,
		Permission:             grant.Permission,
		Grantee: createAccessGrantGranteeXML{
			GranteeType:       grant.GranteeType,
			GranteeIdentifier: grant.GranteeIdentifier,
		},
		ApplicationArn: grant.ApplicationArn,
	})
}

// --- CreateAccessGrantsLocation handler ---

type createAccessGrantsLocationRequestXML struct {
	XMLName       xml.Name `xml:"CreateAccessGrantsLocationRequest"`
	LocationScope string   `xml:"LocationScope"`
	IAMRoleArn    string   `xml:"IAMRoleArn"`
}

type createAccessGrantsLocationResponseXML struct {
	XMLName                 xml.Name `xml:"CreateAccessGrantsLocationResult"`
	AccessGrantsLocationArn string   `xml:"AccessGrantsLocationArn"`
	AccessGrantsLocationID  string   `xml:"AccessGrantsLocationId"`
	LocationScope           string   `xml:"LocationScope"`
	IAMRoleArn              string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleCreateAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createAccessGrantsLocationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	loc := h.Backend.CreateAccessGrantsLocation(accountID, body.LocationScope, body.IAMRoleArn)

	return writeXML(c, createAccessGrantsLocationResponseXML{
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

// --- CreateAccessPoint handler ---

type createAccessPointRequestXML struct {
	XMLName xml.Name `xml:"CreateAccessPointRequest"`
	Bucket  string   `xml:"Bucket"`
}

type createAccessPointResponseXML struct {
	XMLName        xml.Name `xml:"CreateAccessPointResult"`
	AccessPointArn string   `xml:"AccessPointArn"`
	Alias          string   `xml:"Alias,omitempty"`
}

func (h *Handler) handleCreateAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	var body createAccessPointRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	ap := h.Backend.CreateAccessPoint(accountID, name, body.Bucket)

	return writeXML(c, createAccessPointResponseXML{
		AccessPointArn: ap.AccessPointArn,
		Alias:          ap.Alias,
	})
}

// --- CreateAccessPointForObjectLambda handler ---

type createAccessPointForObjectLambdaResponseXML struct {
	XMLName                    xml.Name `xml:"CreateAccessPointForObjectLambdaResult"`
	ObjectLambdaAccessPointArn string   `xml:"ObjectLambdaAccessPointArn"`
}

func (h *Handler) handleCreateAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	ap := h.Backend.CreateAccessPointForObjectLambda(accountID, name)

	return writeXML(c, createAccessPointForObjectLambdaResponseXML{
		ObjectLambdaAccessPointArn: ap.ObjectLambdaAccessPointArn,
	})
}

// --- CreateBucket handler ---

type createBucketResponseXML struct {
	XMLName   xml.Name `xml:"CreateBucketResult"`
	BucketArn string   `xml:"BucketArn"`
}

func (h *Handler) handleCreateBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	bkt := h.Backend.CreateBucket(accountID, bucketName)

	c.Response().Header().Set("Location", bkt.Location)

	return writeXML(c, createBucketResponseXML{
		BucketArn: bkt.BucketArn,
	})
}

// --- CreateJob handler ---

type createJobRequestXML struct {
	XMLName            xml.Name `xml:"CreateJobRequest"`
	ClientRequestToken string   `xml:"ClientRequestToken"`
	Description        string   `xml:"Description"`
	RoleArn            string   `xml:"RoleArn"`
	Priority           int32    `xml:"Priority"`
}

type createJobResponseXML struct {
	XMLName xml.Name `xml:"CreateJobResult"`
	JobID   string   `xml:"JobId"`
}

func (h *Handler) handleCreateJob(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createJobRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	job, err := h.Backend.CreateJob(accountID, body.RoleArn, body.Priority)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, createJobResponseXML{
		JobID: job.JobID,
	})
}

// --- CreateMultiRegionAccessPoint handler ---

type createMRAPDetailsXML struct {
	Name string `xml:"Name"`
}

type createMRAPRequestXML struct {
	XMLName     xml.Name             `xml:"CreateMultiRegionAccessPointRequest"`
	ClientToken string               `xml:"ClientToken"`
	Details     createMRAPDetailsXML `xml:"Details"`
}

type createMRAPResponseXML struct {
	XMLName         xml.Name `xml:"CreateMultiRegionAccessPointResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handleCreateMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createMRAPRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	req := h.Backend.CreateMultiRegionAccessPoint(accountID, body.Details.Name, body.ClientToken)

	return writeXML(c, createMRAPResponseXML{
		RequestTokenARN: req.RequestTokenARN,
	})
}

// --- GetAccessPoint handler ---

type getAccessPointResponseXML struct {
	XMLName        xml.Name `xml:"GetAccessPointResult"`
	Name           string   `xml:"Name"`
	Bucket         string   `xml:"Bucket"`
	NetworkOrigin  string   `xml:"NetworkOrigin"`
	AccessPointArn string   `xml:"AccessPointArn,omitempty"`
	Alias          string   `xml:"Alias,omitempty"`
}

func (h *Handler) handleGetAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	ap, err := h.Backend.GetAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessPointResponseXML{
		Name:           ap.Name,
		Bucket:         ap.Bucket,
		NetworkOrigin:  "Internet",
		AccessPointArn: ap.AccessPointArn,
		Alias:          ap.Alias,
	})
}

func (h *Handler) handleDeleteAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	if err := h.Backend.DeleteAccessPoint(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- ListAccessPoints handler ---

type listAccessPointItemXML struct {
	Name           string `xml:"Name"`
	Bucket         string `xml:"Bucket"`
	NetworkOrigin  string `xml:"NetworkOrigin"`
	AccessPointArn string `xml:"AccessPointArn,omitempty"`
}

type listAccessPointsResponseXML struct {
	XMLName      xml.Name                 `xml:"ListAccessPointsResult"`
	AccessPoints []listAccessPointItemXML `xml:"AccessPointList>AccessPoint"`
}

func (h *Handler) handleListAccessPoints(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	aps := h.Backend.ListAccessPoints(accountID)

	items := make([]listAccessPointItemXML, 0, len(aps))
	for _, ap := range aps {
		items = append(items, listAccessPointItemXML{
			Name:           ap.Name,
			Bucket:         ap.Bucket,
			NetworkOrigin:  "Internet",
			AccessPointArn: ap.AccessPointArn,
		})
	}

	return writeXML(c, listAccessPointsResponseXML{AccessPoints: items})
}

// --- Access point policy handlers ---

type putAccessPointPolicyRequestXML struct {
	XMLName xml.Name `xml:"PutAccessPointPolicyRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	var body putAccessPointPolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.PutAccessPointPolicy(accountID, name, body.Policy); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusCreated)
}

type getAccessPointPolicyResponseXML struct {
	XMLName xml.Name `xml:"GetAccessPointPolicyResult"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handleGetAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	policy, err := h.Backend.GetAccessPointPolicy(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessPointPolicyResponseXML{Policy: policy})
}

func (h *Handler) handleDeleteAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	name := strings.TrimSuffix(strings.TrimPrefix(path, pathAccessPointPrefix), "/policy")

	if err := h.Backend.DeleteAccessPointPolicy(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type getAccessPointPolicyStatusResponseXML struct {
	XMLName  xml.Name `xml:"GetAccessPointPolicyStatusResult"`
	IsPublic bool     `xml:"PolicyStatus>IsPublic"`
}

func (h *Handler) handleGetAccessPointPolicyStatus(c *echo.Context) error {
	return writeXML(c, getAccessPointPolicyStatusResponseXML{IsPublic: false})
}

// --- Batch job read/update handlers ---

type describeJobDescriptorXML struct {
	JobArn   string `xml:"JobArn"`
	JobID    string `xml:"JobId"`
	RoleArn  string `xml:"RoleArn"`
	Status   string `xml:"Status"`
	Priority int32  `xml:"Priority"`
}

type describeJobResponseXML struct {
	XMLName xml.Name                 `xml:"DescribeJobResult"`
	Job     describeJobDescriptorXML `xml:"Job"`
}

func (h *Handler) handleDescribeJob(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix)

	job, err := h.Backend.GetJob(accountID, jobID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, describeJobResponseXML{
		Job: describeJobDescriptorXML{
			JobID:    job.JobID,
			JobArn:   job.JobArn,
			Status:   job.Status,
			Priority: job.Priority,
			RoleArn:  job.RoleArn,
		},
	})
}

type listJobsJobXML struct {
	JobID    string `xml:"JobId"`
	Status   string `xml:"Status"`
	Priority int32  `xml:"Priority"`
}

type listJobsResponseXML struct {
	XMLName xml.Name         `xml:"ListJobsResult"`
	Jobs    []listJobsJobXML `xml:"Jobs>member"`
}

func (h *Handler) handleListJobs(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	jobs := h.Backend.ListJobs(accountID)

	items := make([]listJobsJobXML, 0, len(jobs))
	for _, j := range jobs {
		items = append(items, listJobsJobXML{
			JobID:    j.JobID,
			Status:   j.Status,
			Priority: j.Priority,
		})
	}

	return writeXML(c, listJobsResponseXML{Jobs: items})
}

type updateJobPriorityRequestXML struct {
	XMLName  xml.Name `xml:"UpdateJobPriorityRequest"`
	Priority int32    `xml:"Priority"`
}

type updateJobPriorityResponseXML struct {
	XMLName  xml.Name `xml:"UpdateJobPriorityResult"`
	JobID    string   `xml:"JobId"`
	Priority int32    `xml:"Priority"`
}

func (h *Handler) handleUpdateJobPriority(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	jobID := strings.TrimSuffix(strings.TrimPrefix(path, pathJobPrefix), "/priority")

	var body updateJobPriorityRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	job, err := h.Backend.UpdateJobPriority(accountID, jobID, body.Priority)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, updateJobPriorityResponseXML{
		JobID:    job.JobID,
		Priority: job.Priority,
	})
}

type updateJobStatusRequestXML struct {
	XMLName            xml.Name `xml:"UpdateJobStatusRequest"`
	RequestedJobStatus string   `xml:"RequestedJobStatus"`
}

type updateJobStatusResponseXML struct {
	XMLName xml.Name `xml:"UpdateJobStatusResult"`
	JobID   string   `xml:"JobId"`
	Status  string   `xml:"Status"`
}

func (h *Handler) handleUpdateJobStatus(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	path := c.Request().URL.Path
	jobID := strings.TrimSuffix(strings.TrimPrefix(path, pathJobPrefix), "/status")

	var body updateJobStatusRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	job, err := h.Backend.UpdateJobStatus(accountID, jobID, body.RequestedJobStatus)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, updateJobStatusResponseXML{
		JobID:  job.JobID,
		Status: job.Status,
	})
}

// --- MRAP handlers ---

type getMRAPResponseXML struct {
	XMLName xml.Name `xml:"GetMultiRegionAccessPointResult"`
	Name    string   `xml:"AccessPoint>Name"`
	Alias   string   `xml:"AccessPoint>Alias"`
	Status  string   `xml:"AccessPoint>Status"`
}

func (h *Handler) handleGetMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix)

	mrap, err := h.Backend.GetMultiRegionAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getMRAPResponseXML{
		Name:   mrap.Name,
		Alias:  mrap.Alias,
		Status: mrap.Status,
	})
}

func (h *Handler) handleDeleteMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix)

	if err := h.Backend.DeleteMultiRegionAccessPoint(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type deleteMRAPAsyncRequestXML struct {
	XMLName xml.Name `xml:"DeleteMultiRegionAccessPointRequest"`
	Details struct {
		Name string `xml:"Name"`
	} `xml:"Details"`
}

type deleteMRAPAsyncResponseXML struct {
	XMLName         xml.Name `xml:"DeleteMultiRegionAccessPointResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handleDeleteMultiRegionAccessPointAsync(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body deleteMRAPAsyncRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.DeleteMultiRegionAccessPoint(accountID, body.Details.Name); err != nil {
		return handleBackendError(c, err)
	}

	tokenARN := "arn:aws:s3::" + accountID + ":async-request/mrap/delete/1"

	return writeXML(c, deleteMRAPAsyncResponseXML{RequestTokenARN: tokenARN})
}

type listMRAPItemXML struct {
	Name   string `xml:"Name"`
	Alias  string `xml:"Alias"`
	Status string `xml:"Status"`
}

type listMRAPsResponseXML struct {
	XMLName      xml.Name          `xml:"ListMultiRegionAccessPointsResult"`
	AccessPoints []listMRAPItemXML `xml:"AccessPoints>item"`
}

func (h *Handler) handleListMultiRegionAccessPoints(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	mraps := h.Backend.ListMultiRegionAccessPoints(accountID)

	items := make([]listMRAPItemXML, 0, len(mraps))
	for _, m := range mraps {
		items = append(items, listMRAPItemXML{
			Name:   m.Name,
			Alias:  m.Alias,
			Status: m.Status,
		})
	}

	return writeXML(c, listMRAPsResponseXML{AccessPoints: items})
}

type putMRAPPolicyRequestXML struct {
	XMLName xml.Name `xml:"PutMultiRegionAccessPointPolicyRequest"`
	Details struct {
		Name   string `xml:"Name"`
		Policy string `xml:"Policy"`
	} `xml:"Details"`
}

type putMRAPPolicyResponseXML struct {
	XMLName         xml.Name `xml:"PutMultiRegionAccessPointPolicyResult"`
	RequestTokenARN string   `xml:"RequestTokenARN"`
}

func (h *Handler) handlePutMultiRegionAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body putMRAPPolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.PutMultiRegionAccessPointPolicy(accountID, body.Details.Name, body.Details.Policy); err != nil {
		return handleBackendError(c, err)
	}

	policyTokenARN := "arn:aws:s3::" + accountID + ":async-request/mrap/put_policy/1"

	return writeXML(c, putMRAPPolicyResponseXML{RequestTokenARN: policyTokenARN})
}

// --- CreateStorageLensGroup handler ---

type createStorageLensGroupStorageLensGroupXML struct {
	Name string `xml:"Name"`
}

type createStorageLensGroupRequestXML struct {
	XMLName          xml.Name                                  `xml:"CreateStorageLensGroupRequest"`
	StorageLensGroup createStorageLensGroupStorageLensGroupXML `xml:"StorageLensGroup"`
}

func (h *Handler) handleCreateStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createStorageLensGroupRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.CreateStorageLensGroup(accountID, body.StorageLensGroup.Name)

	return c.NoContent(http.StatusCreated)
}
