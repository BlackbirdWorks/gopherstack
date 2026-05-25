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
		"GetAccessPointPublicAccessBlock",
		"GetAccessPointScope",
		"ListAccessPoints",
		"ListAccessPointsForDirectoryBuckets",
		"PutAccessPointPolicy",
		"PutAccessPointPublicAccessBlock",
		"PutAccessPointScope",
		"DeleteAccessPointPolicy",
		"DeleteAccessPointPublicAccessBlock",
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

// extractNewOpsOperation routes access grants and access point operations.
func extractNewOpsOperation(path, method string) string {
	if op := extractAccessGrantsInstanceOp(path, method); op != "" {
		return op
	}

	if op := extractAccessGrantsOp(path, method); op != "" {
		return op
	}

	return extractAccessPointOpsOperation(path, method)
}

// extractAccessGrantsInstanceOp handles access grants instance and identity center operations.
func extractAccessGrantsInstanceOp(path, method string) string {
	switch path {
	case pathAccessGrantsInstance:
		switch method {
		case http.MethodPost:
			return "CreateAccessGrantsInstance"
		case http.MethodGet:
			return "GetAccessGrantsInstance"
		case http.MethodDelete:
			return "DeleteAccessGrantsInstance"
		}
	case pathAccessGrantsInstanceResourcePolicy:
		switch method {
		case http.MethodGet:
			return "GetAccessGrantsInstanceResourcePolicy"
		case http.MethodPut:
			return "PutAccessGrantsInstanceResourcePolicy"
		case http.MethodDelete:
			return "DeleteAccessGrantsInstanceResourcePolicy"
		}
	case pathIdentityCenter:
		switch method {
		case http.MethodPost:
			return "AssociateAccessGrantsIdentityCenter"
		case http.MethodDelete:
			return "DissociateAccessGrantsIdentityCenter"
		}
	}

	return ""
}

// extractAccessGrantsOp handles access grants, locations, and data access operations.
func extractAccessGrantsOp(path, method string) string {
	if op := extractAccessGrantsExactOp(path, method); op != "" {
		return op
	}

	return extractAccessGrantsPrefixOp(path, method)
}

// extractAccessGrantsExactOp handles exact-path access grants operations.
func extractAccessGrantsExactOp(path, method string) string {
	switch path {
	case pathAccessGrant:
		switch method {
		case http.MethodPost:
			return "CreateAccessGrant"
		case http.MethodGet:
			return "ListAccessGrants"
		}
	case pathCallerAccessGrants:
		if method == http.MethodGet {
			return "ListCallerAccessGrants"
		}
	case pathDataAccess:
		if method == http.MethodGet {
			return "GetDataAccess"
		}
	case pathAccessGrantsLocation:
		switch method {
		case http.MethodPost:
			return "CreateAccessGrantsLocation"
		case http.MethodGet:
			return "ListAccessGrantsLocations"
		}
	}

	return ""
}

// extractAccessGrantsPrefixOp handles prefix-based access grants operations.
func extractAccessGrantsPrefixOp(path, method string) string {
	switch {
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

	return ""
}

// extractAccessPointOpsOperation routes access point and object lambda operations.
func extractAccessPointOpsOperation(path, method string) string {
	if op := extractAccessPointCRUDOp(path, method); op != "" {
		return op
	}

	if op := extractObjectLambdaOp(path, method); op != "" {
		return op
	}

	return extractBucketOpsOperation(path, method)
}

// extractAccessPointCRUDOp handles standard access point CRUD and listing.
func extractAccessPointCRUDOp(path, method string) string {
	if op := extractAccessPointBasicOp(path, method); op != "" {
		return op
	}

	return extractAccessPointSubResourceOp(path, method)
}

// extractAccessPointBasicOp handles access point CRUD and list operations.
func extractAccessPointBasicOp(path, method string) string {
	if isSimplePath(pathAccessPointPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateAccessPoint"
		case http.MethodGet:
			return "GetAccessPoint"
		case http.MethodDelete:
			return "DeleteAccessPoint"
		}

		return ""
	}

	switch path {
	case "/v20180820/accesspoint":
		if method == http.MethodGet {
			return "ListAccessPoints"
		}
	case pathAccessPointsDirectoryBuckets:
		if method == http.MethodGet {
			return "ListAccessPointsForDirectoryBuckets"
		}
	case pathAccessPointsForObjectLambdaList:
		if method == http.MethodGet {
			return "ListAccessPointsForObjectLambda"
		}
	}

	return ""
}

// extractAccessPointSubResourceOp handles access point policy, status, and scope operations.
func extractAccessPointSubResourceOp(path, method string) string {
	switch {
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
	}

	return ""
}

// extractObjectLambdaOp handles object lambda access point operations.
func extractObjectLambdaOp(path, method string) string {
	if isSimplePath(pathObjectLambdaPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateAccessPointForObjectLambda"
		case http.MethodGet:
			return "GetAccessPointForObjectLambda"
		case http.MethodDelete:
			return "DeleteAccessPointForObjectLambda"
		}

		return ""
	}

	return extractObjectLambdaPolicyOp(path, method)
}

// extractObjectLambdaPolicyOp handles object lambda policy and configuration operations.
func extractObjectLambdaPolicyOp(path, method string) string {
	switch {
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
	}

	return ""
}

// extractBucketOpsOperation routes bucket and sub-resource operations.
func extractBucketOpsOperation(path, method string) string {
	if op := extractBucketCRUDOps(path, method); op != "" {
		return op
	}

	if op := extractBucketSubResourceOps(path, method); op != "" {
		return op
	}

	return extractJobMRAPStorageLensOps(path, method)
}

// extractBucketCRUDOps handles bucket CRUD, lifecycle, and policy operations.
func extractBucketCRUDOps(path, method string) string {
	if isSimplePath(pathBucketPrefix, path) {
		switch method {
		case http.MethodPut:
			return "CreateBucket"
		case http.MethodGet:
			return "GetBucket"
		case http.MethodDelete:
			return "DeleteBucket"
		}

		return ""
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/lifecycle") {
		switch method {
		case http.MethodGet:
			return "GetBucketLifecycleConfiguration"
		case http.MethodPut:
			return "PutBucketLifecycleConfiguration"
		case http.MethodDelete:
			return "DeleteBucketLifecycleConfiguration"
		}

		return ""
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/policy") {
		switch method {
		case http.MethodGet:
			return "GetBucketPolicy"
		case http.MethodPut:
			return "PutBucketPolicy"
		case http.MethodDelete:
			return "DeleteBucketPolicy"
		}
	}

	return ""
}

// extractBucketSubResourceOps handles bucket replication, tagging, versioning, and listing.
func extractBucketSubResourceOps(path, method string) string {
	if op := extractBucketReplicationTaggingOp(path, method); op != "" {
		return op
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/versioning") {
		switch method {
		case http.MethodGet:
			return "GetBucketVersioning"
		case http.MethodPut:
			return "PutBucketVersioning"
		}
	}

	if path == pathRegionalBuckets && method == http.MethodGet {
		return "ListRegionalBuckets"
	}

	return ""
}

// extractBucketReplicationTaggingOp handles bucket replication and tagging operations.
func extractBucketReplicationTaggingOp(path, method string) string {
	switch {
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
	}

	return ""
}

// extractJobMRAPStorageLensOps routes job, MRAP, and storage lens operations.
func extractJobMRAPStorageLensOps(path, method string) string {
	if op := extractJobOps(path, method); op != "" {
		return op
	}

	if op := extractMRAPOps(path, method); op != "" {
		return op
	}

	return extractStorageLensTagOps(path, method)
}

// extractJobOps handles S3 Batch Operations job operations.
func extractJobOps(path, method string) string {
	if path == pathJobs {
		switch method {
		case http.MethodPost:
			return "CreateJob"
		case http.MethodGet:
			return "ListJobs"
		}

		return ""
	}

	if isSimplePath(pathJobPrefix, path) && method == http.MethodGet {
		return "DescribeJob"
	}

	return extractJobSubResourceOp(path, method)
}

// extractJobSubResourceOp handles job tagging, priority, and status operations.
func extractJobSubResourceOp(path, method string) string {
	if isPrefixSuffix(pathJobPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return "GetJobTagging"
		case http.MethodPut:
			return "PutJobTagging"
		case http.MethodDelete:
			return "DeleteJobTagging"
		}

		return ""
	}

	if isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPut {
		return "UpdateJobPriority"
	}

	if isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPut {
		return "UpdateJobStatus"
	}

	return ""
}

// extractMRAPOps handles Multi-Region Access Point operations.
func extractMRAPOps(path, method string) string {
	if op := extractMRAPCreateListOp(path, method); op != "" {
		return op
	}

	return extractMRAPInstanceOp(path, method)
}

// extractMRAPCreateListOp handles MRAP create, delete, policy, and list operations.
func extractMRAPCreateListOp(path, method string) string {
	switch {
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
	}

	return ""
}

// extractMRAPInstanceOp handles MRAP instance CRUD and sub-resource operations.
func extractMRAPInstanceOp(path, method string) string {
	if isSimplePath(pathMRAPInstancePrefix, path) {
		switch method {
		case http.MethodGet:
			return "GetMultiRegionAccessPoint"
		case http.MethodDelete:
			return opDeleteMRAP
		}

		return ""
	}

	switch {
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicy"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policyStatus") && method == http.MethodGet:
		return "GetMultiRegionAccessPointPolicyStatus"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return "GetMultiRegionAccessPointRoutes"
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return "SubmitMultiRegionAccessPointRoutes"
	}

	return ""
}

// extractStorageLensTagOps routes storage lens and tagging operations.
func extractStorageLensTagOps(path, method string) string {
	if op := extractStorageLensOps(path, method); op != "" {
		return op
	}

	return extractTagOps(path, method)
}

// extractStorageLensOps handles storage lens configuration and group operations.
func extractStorageLensOps(path, method string) string {
	if op := extractStorageLensConfigOp(path, method); op != "" {
		return op
	}

	return extractStorageLensGroupOp(path, method)
}

// extractStorageLensConfigOp handles storage lens configuration operations.
func extractStorageLensConfigOp(path, method string) string {
	if isSimplePath(pathStorageLensPrefix, path) {
		switch method {
		case http.MethodGet:
			return "GetStorageLensConfiguration"
		case http.MethodPut:
			return "PutStorageLensConfiguration"
		case http.MethodDelete:
			return "DeleteStorageLensConfiguration"
		}

		return ""
	}

	switch {
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodGet:
		return "GetStorageLensConfigurationTagging"
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodPut:
		return "PutStorageLensConfigurationTagging"
	case isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") && method == http.MethodDelete:
		return "DeleteStorageLensConfigurationTagging"
	case path == pathStorageLensList && method == http.MethodGet:
		return "ListStorageLensConfigurations"
	}

	return ""
}

// extractStorageLensGroupOp handles storage lens group operations.
func extractStorageLensGroupOp(path, method string) string {
	if path == pathStorageLensGroup {
		switch method {
		case http.MethodPost:
			return "CreateStorageLensGroup"
		case http.MethodGet:
			return "ListStorageLensGroups"
		}
	}

	switch {
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodGet:
		return "GetStorageLensGroup"
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodPut:
		return "UpdateStorageLensGroup"
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodDelete:
		return "DeleteStorageLensGroup"
	}

	return ""
}

// extractTagOps handles resource tagging operations.
func extractTagOps(path, method string) string {
	switch {
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

// dispatchNewOps handles access grants instance and access point operations.
func (h *Handler) dispatchNewOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchAccessGrantsInstanceOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchAccessGrantsOps(c, path, method); handled {
		return err
	}

	return h.dispatchAccessPointOps(c, path, method)
}

// dispatchAccessGrantsInstanceOps handles access grants instance and identity center operations.
func (h *Handler) dispatchAccessGrantsInstanceOps(c *echo.Context, path, method string) (bool, error) {
	switch path {
	case pathAccessGrantsInstance:
		switch method {
		case http.MethodPost:
			return true, h.handleCreateAccessGrantsInstance(c)
		case http.MethodGet:
			return true, h.handleGetAccessGrantsInstance(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessGrantsInstance(c)
		}
	case pathAccessGrantsInstanceResourcePolicy:
		switch method {
		case http.MethodGet:
			return true, h.handleGetAccessGrantsInstanceResourcePolicy(c)
		case http.MethodPut:
			return true, h.handlePutAccessGrantsInstanceResourcePolicy(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessGrantsInstanceResourcePolicy(c)
		}
	case pathIdentityCenter:
		switch method {
		case http.MethodPost:
			return true, h.handleAssociateAccessGrantsIdentityCenter(c)
		case http.MethodDelete:
			return true, h.handleDissociateAccessGrantsIdentityCenter(c)
		}
	}

	return false, nil
}

// dispatchAccessGrantsOps handles access grants, locations, and data access operations.
func (h *Handler) dispatchAccessGrantsOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchAccessGrantsExactOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchAccessGrantsPrefixOps(c, path, method)
}

// dispatchAccessGrantsExactOps handles exact-path access grants dispatch.
func (h *Handler) dispatchAccessGrantsExactOps(c *echo.Context, path, method string) (bool, error) {
	switch path {
	case pathAccessGrant:
		switch method {
		case http.MethodPost:
			return true, h.handleCreateAccessGrant(c)
		case http.MethodGet:
			return true, h.handleListAccessGrants(c)
		}
	case pathCallerAccessGrants:
		if method == http.MethodGet {
			return true, h.handleListCallerAccessGrants(c)
		}
	case pathDataAccess:
		if method == http.MethodGet {
			return true, h.handleGetDataAccess(c)
		}
	case pathAccessGrantsLocation:
		switch method {
		case http.MethodPost:
			return true, h.handleCreateAccessGrantsLocation(c)
		case http.MethodGet:
			return true, h.handleListAccessGrantsLocations(c)
		}
	}

	return false, nil
}

// dispatchAccessGrantsPrefixOps handles prefix-based access grants dispatch.
func (h *Handler) dispatchAccessGrantsPrefixOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case strings.HasPrefix(path, pathAccessGrantsInstancePrefix+"prefix") && method == http.MethodGet:
		return true, h.handleGetAccessGrantsInstanceForPrefix(c)
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodGet:
		return true, h.handleGetAccessGrant(c)
	case strings.HasPrefix(path, pathAccessGrantPrefix) && method == http.MethodDelete:
		return true, h.handleDeleteAccessGrant(c)
	case isGrantsLocationPath(path) && method == http.MethodGet:
		return true, h.handleGetAccessGrantsLocation(c)
	case isGrantsLocationPath(path) && method == http.MethodDelete:
		return true, h.handleDeleteAccessGrantsLocation(c)
	case isGrantsLocationPath(path) && method == http.MethodPut:
		return true, h.handleUpdateAccessGrantsLocation(c)
	}

	return false, nil
}

// dispatchAccessPointOps handles access point and object lambda operations.
func (h *Handler) dispatchAccessPointOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchAccessPointCRUDOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchObjectLambdaOps(c, path, method); handled {
		return err
	}

	return h.dispatchBucketOps(c, path, method)
}

// dispatchAccessPointCRUDOps handles standard access point CRUD and listing.
func (h *Handler) dispatchAccessPointCRUDOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchAccessPointBasicOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchAccessPointSubResourceOps(c, path, method)
}

// dispatchAccessPointBasicOps handles access point CRUD and list dispatch.
func (h *Handler) dispatchAccessPointBasicOps(c *echo.Context, path, method string) (bool, error) {
	if isSimplePath(pathAccessPointPrefix, path) {
		switch method {
		case http.MethodPut:
			return true, h.handleCreateAccessPoint(c)
		case http.MethodGet:
			return true, h.handleGetAccessPoint(c)
		case http.MethodDelete:
			return true, h.handleDeleteAccessPoint(c)
		}

		return false, nil
	}

	switch path {
	case "/v20180820/accesspoint":
		if method == http.MethodGet {
			return true, h.handleListAccessPoints(c)
		}
	case pathAccessPointsDirectoryBuckets:
		if method == http.MethodGet {
			return true, h.handleListAccessPointsForDirectoryBuckets(c)
		}
	case pathAccessPointsForObjectLambdaList:
		if method == http.MethodGet {
			return true, h.handleListAccessPointsForObjectLambda(c)
		}
	}

	return false, nil
}

// dispatchAccessPointSubResourceOps handles access point policy, status, scope, and PAB dispatch.
func (h *Handler) dispatchAccessPointSubResourceOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodGet:
		return true, h.handleGetAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodPut:
		return true, h.handlePutAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policy") && method == http.MethodDelete:
		return true, h.handleDeleteAccessPointPolicy(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/policyStatus") && method == http.MethodGet:
		return true, h.handleGetAccessPointPolicyStatus(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodGet:
		return true, h.handleGetAccessPointScope(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodPut:
		return true, h.handlePutAccessPointScope(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/scope") && method == http.MethodDelete:
		return true, h.handleDeleteAccessPointScope(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/publicAccessBlock") && method == http.MethodGet:
		return true, h.handleGetAccessPointPublicAccessBlock(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/publicAccessBlock") && method == http.MethodPut:
		return true, h.handlePutAccessPointPublicAccessBlock(c)
	case isPrefixSuffix(pathAccessPointPrefix, path, "/publicAccessBlock") && method == http.MethodDelete:
		return true, h.handleDeleteAccessPointPublicAccessBlock(c)
	}

	return false, nil
}

// dispatchObjectLambdaOps handles object lambda access point operations.
func (h *Handler) dispatchObjectLambdaOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isSimplePath(pathObjectLambdaPrefix, path):
		return h.dispatchObjectLambdaRootMethod(c, method)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policy"):
		return h.dispatchObjectLambdaPolicyMethod(c, method)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/policyStatus") && method == http.MethodGet:
		return true, h.handleGetAccessPointPolicyStatusForObjectLambda(c)
	case isPrefixSuffix(pathObjectLambdaPrefix, path, "/configuration"):
		return h.dispatchObjectLambdaConfigMethod(c, method)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaRootMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodPut:
		return true, h.handleCreateAccessPointForObjectLambda(c)
	case http.MethodGet:
		return true, h.handleGetAccessPointForObjectLambda(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointForObjectLambda(c)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaPolicyMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointPolicyForObjectLambda(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointPolicyForObjectLambda(c)
	case http.MethodDelete:
		return true, h.handleDeleteAccessPointPolicyForObjectLambda(c)
	}

	return false, nil
}

func (h *Handler) dispatchObjectLambdaConfigMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetAccessPointConfigurationForObjectLambda(c)
	case http.MethodPut:
		return true, h.handlePutAccessPointConfigurationForObjectLambda(c)
	}

	return false, nil
}

// dispatchBucketOps handles bucket CRUD and sub-resource operations.
func (h *Handler) dispatchBucketOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchBucketCRUDStubs(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchBucketSubResourceStubs(c, path, method); handled {
		return err
	}

	return h.dispatchJobMRAPStorageLensOps(c, path, method)
}

// dispatchBucketCRUDStubs handles bucket CRUD, lifecycle, and policy stub operations.
func (h *Handler) dispatchBucketCRUDStubs(c *echo.Context, path, method string) (bool, error) {
	switch {
	case isSimplePath(pathBucketPrefix, path):
		return h.dispatchBucketBaseMethod(c, method)
	case isPrefixSuffix(pathBucketPrefix, path, "/lifecycle"):
		return h.dispatchBucketLifecycleMethod(c, method)
	case isPrefixSuffix(pathBucketPrefix, path, "/policy"):
		return h.dispatchBucketPolicyMethod(c, method)
	}

	return false, nil
}

func (h *Handler) dispatchBucketBaseMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodPut:
		return true, h.handleCreateBucket(c)
	case http.MethodGet:
		return true, h.handleGetBucket(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucket(c)
	}

	return false, nil
}

func (h *Handler) dispatchBucketLifecycleMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetBucketLifecycleConfiguration(c)
	case http.MethodPut:
		return true, h.handlePutBucketLifecycleConfiguration(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucketLifecycleConfiguration(c)
	}

	return false, nil
}

func (h *Handler) dispatchBucketPolicyMethod(c *echo.Context, method string) (bool, error) {
	switch method {
	case http.MethodGet:
		return true, h.handleGetBucketPolicy(c)
	case http.MethodPut:
		return true, h.handlePutBucketPolicy(c)
	case http.MethodDelete:
		return true, h.handleDeleteBucketPolicy(c)
	}

	return false, nil
}

// dispatchBucketSubResourceStubs handles bucket replication, tagging, versioning, and listing stubs.
func (h *Handler) dispatchBucketSubResourceStubs(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathBucketPrefix, path, "/replication") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketReplication(c)
		case http.MethodPut:
			return true, h.handlePutBucketReplication(c)
		case http.MethodDelete:
			return true, h.handleDeleteBucketReplication(c)
		}

		return false, nil
	}

	return h.dispatchBucketTagVersionStubs(c, path, method)
}

// dispatchBucketTagVersionStubs handles bucket tagging, versioning, and listing stubs.
func (h *Handler) dispatchBucketTagVersionStubs(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathBucketPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketTagging(c)
		case http.MethodPut:
			return true, h.handlePutBucketTagging(c)
		case http.MethodDelete:
			return true, h.handleDeleteBucketTagging(c)
		}

		return false, nil
	}

	if isPrefixSuffix(pathBucketPrefix, path, "/versioning") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetBucketVersioning(c)
		case http.MethodPut:
			return true, h.handlePutBucketVersioning(c)
		}

		return false, nil
	}

	if path == pathRegionalBuckets && method == http.MethodGet {
		return true, h.handleListRegionalBuckets(c)
	}

	return false, nil
}

// dispatchJobMRAPStorageLensOps handles job, MRAP, and storage lens dispatch.
func (h *Handler) dispatchJobMRAPStorageLensOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchJobOps(c, path, method); handled {
		return err
	}

	if handled, err := h.dispatchMRAPDispatchOps(c, path, method); handled {
		return err
	}

	return h.dispatchStorageLensTagOps(c, path, method)
}

// dispatchJobOps handles S3 Batch Operations job dispatch.
func (h *Handler) dispatchJobOps(c *echo.Context, path, method string) (bool, error) {
	if path == pathJobs {
		switch method {
		case http.MethodPost:
			return true, h.handleCreateJob(c)
		case http.MethodGet:
			return true, h.handleListJobs(c)
		}

		return false, nil
	}

	if isSimplePath(pathJobPrefix, path) && method == http.MethodGet {
		return true, h.handleDescribeJob(c)
	}

	return h.dispatchJobSubResourceOps(c, path, method)
}

// dispatchJobSubResourceOps handles job tagging, priority, and status dispatch.
func (h *Handler) dispatchJobSubResourceOps(c *echo.Context, path, method string) (bool, error) {
	if isPrefixSuffix(pathJobPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetJobTagging(c)
		case http.MethodPut:
			return true, h.handlePutJobTagging(c)
		case http.MethodDelete:
			return true, h.handleDeleteJobTagging(c)
		}

		return false, nil
	}

	if isPrefixSuffix(pathJobPrefix, path, "/priority") && method == http.MethodPut {
		return true, h.handleUpdateJobPriority(c)
	}

	if isPrefixSuffix(pathJobPrefix, path, "/status") && method == http.MethodPut {
		return true, h.handleUpdateJobStatus(c)
	}

	return false, nil
}

// dispatchMRAPDispatchOps handles Multi-Region Access Point dispatch.
func (h *Handler) dispatchMRAPDispatchOps(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchMRAPCreateListOps(c, path, method); handled {
		return true, err
	}

	return h.dispatchMRAPInstanceDispatch(c, path, method)
}

// dispatchMRAPCreateListOps handles MRAP create, delete, policy, and list dispatch.
func (h *Handler) dispatchMRAPCreateListOps(c *echo.Context, path, method string) (bool, error) {
	switch {
	case path == pathMRAPCreate && method == http.MethodPost:
		return true, h.handleCreateMultiRegionAccessPoint(c)
	case strings.HasPrefix(path, pathMRAPDeletePrefix) && method == http.MethodPost:
		return true, h.handleDeleteMultiRegionAccessPointAsync(c)
	case strings.HasPrefix(path, pathMRAPPutPolicyPrefix) && method == http.MethodPost:
		return true, h.handlePutMultiRegionAccessPointPolicy(c)
	case strings.HasPrefix(path, pathMRAPPrefix) && method == http.MethodGet:
		return true, h.handleDescribeMultiRegionAccessPointOperation(c)
	case path == pathMRAPList && method == http.MethodGet:
		return true, h.handleListMultiRegionAccessPoints(c)
	}

	return false, nil
}

// dispatchMRAPInstanceDispatch handles MRAP instance CRUD and sub-resource dispatch.
func (h *Handler) dispatchMRAPInstanceDispatch(c *echo.Context, path, method string) (bool, error) {
	if isSimplePath(pathMRAPInstancePrefix, path) {
		switch method {
		case http.MethodGet:
			return true, h.handleGetMultiRegionAccessPoint(c)
		case http.MethodDelete:
			return true, h.handleDeleteMultiRegionAccessPoint(c)
		}

		return false, nil
	}

	switch {
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policy") && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointPolicy(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/policyStatus") && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointPolicyStatus(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodGet:
		return true, h.handleGetMultiRegionAccessPointRoutes(c)
	case isPrefixSuffix(pathMRAPInstancePrefix, path, "/routes") && method == http.MethodPatch:
		return true, h.handleSubmitMultiRegionAccessPointRoutes(c)
	}

	return false, nil
}

// dispatchStorageLensTagOps handles storage lens and tagging dispatch.
func (h *Handler) dispatchStorageLensTagOps(c *echo.Context, path, method string) error {
	if handled, err := h.dispatchStorageLensDispatch(c, path, method); handled {
		return err
	}

	return h.dispatchTagDispatch(c, path, method)
}

// dispatchStorageLensDispatch handles storage lens configuration and group operations.
func (h *Handler) dispatchStorageLensDispatch(c *echo.Context, path, method string) (bool, error) {
	if handled, err := h.dispatchStorageLensConfigDispatch(c, path, method); handled {
		return true, err
	}

	return h.dispatchStorageLensGroupDispatch(c, path, method)
}

// dispatchStorageLensConfigDispatch handles storage lens configuration dispatch.
func (h *Handler) dispatchStorageLensConfigDispatch(c *echo.Context, path, method string) (bool, error) {
	if isSimplePath(pathStorageLensPrefix, path) {
		switch method {
		case http.MethodGet:
			return true, h.handleGetStorageLensConfiguration(c)
		case http.MethodPut:
			return true, h.handlePutStorageLensConfiguration(c)
		case http.MethodDelete:
			return true, h.handleDeleteStorageLensConfiguration(c)
		}

		return false, nil
	}

	if isPrefixSuffix(pathStorageLensPrefix, path, "/tagging") {
		switch method {
		case http.MethodGet:
			return true, h.handleGetStorageLensConfigurationTagging(c)
		case http.MethodPut:
			return true, h.handlePutStorageLensConfigurationTagging(c)
		case http.MethodDelete:
			return true, h.handleDeleteStorageLensConfigurationTagging(c)
		}

		return false, nil
	}

	if path == pathStorageLensList && method == http.MethodGet {
		return true, h.handleListStorageLensConfigurations(c)
	}

	return false, nil
}

// dispatchStorageLensGroupDispatch handles storage lens group dispatch.
func (h *Handler) dispatchStorageLensGroupDispatch(c *echo.Context, path, method string) (bool, error) {
	if path == pathStorageLensGroup {
		switch method {
		case http.MethodPost:
			return true, h.handleCreateStorageLensGroup(c)
		case http.MethodGet:
			return true, h.handleListStorageLensGroups(c)
		}
	}

	switch {
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodGet:
		return true, h.handleGetStorageLensGroup(c)
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodPut:
		return true, h.handleUpdateStorageLensGroup(c)
	case strings.HasPrefix(path, pathStorageLensGroupPrefix) && method == http.MethodDelete:
		return true, h.handleDeleteStorageLensGroup(c)
	}

	return false, nil
}

// dispatchTagDispatch handles resource tagging dispatch.
func (h *Handler) dispatchTagDispatch(c *echo.Context, path, method string) error {
	switch {
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodGet:
		return h.handleListTagsForResource(c)
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodPost:
		return h.handleTagResource(c)
	case strings.HasPrefix(path, pathTagsPrefix) && method == http.MethodDelete:
		return h.handleUntagResource(c)
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

type apVpcConfigurationXML struct {
	VpcId string `xml:"VpcId"`
}

type apPublicAccessBlockXML struct {
	BlockPublicAcls       bool `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool `xml:"RestrictPublicBuckets"`
}

type createAccessPointRequestXML struct {
	XMLName                    xml.Name               `xml:"CreateAccessPointRequest"`
	Bucket                     string                 `xml:"Bucket"`
	BucketAccountId            string                 `xml:"BucketAccountId"`
	VpcConfiguration           apVpcConfigurationXML  `xml:"VpcConfiguration"`
	PublicAccessBlockConfiguration apPublicAccessBlockXML `xml:"PublicAccessBlockConfiguration"`
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

	// Persist VPC config and bucket account ID when provided.
	if body.VpcConfiguration.VpcId != "" || body.BucketAccountId != "" {
		if err := h.Backend.SetAccessPointVpcConfig(
			accountID, name,
			body.VpcConfiguration.VpcId,
			body.BucketAccountId,
		); err != nil {
			return handleBackendError(c, err)
		}
		// Refresh ap after update to get correct NetworkOrigin and Alias.
		var errGet error
		ap, errGet = h.Backend.GetAccessPoint(accountID, name)
		if errGet != nil {
			return handleBackendError(c, errGet)
		}
	}

	// Store per-AP PAB when provided.
	if body.PublicAccessBlockConfiguration != (apPublicAccessBlockXML{}) {
		pab := PublicAccessBlock{
			AccountID:             accountID,
			BlockPublicAcls:       body.PublicAccessBlockConfiguration.BlockPublicAcls,
			IgnorePublicAcls:      body.PublicAccessBlockConfiguration.IgnorePublicAcls,
			BlockPublicPolicy:     body.PublicAccessBlockConfiguration.BlockPublicPolicy,
			RestrictPublicBuckets: body.PublicAccessBlockConfiguration.RestrictPublicBuckets,
		}
		_ = h.Backend.PutAccessPointPublicAccessBlock(accountID, name, pab)
	}

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

type createJobXMLCapture struct {
	Raw string `xml:",innerxml"`
}

type createJobRequestXML struct {
	XMLName              xml.Name             `xml:"CreateJobRequest"`
	ClientRequestToken   string               `xml:"ClientRequestToken"`
	Description          string               `xml:"Description"`
	RoleArn              string               `xml:"RoleArn"`
	Priority             int32                `xml:"Priority"`
	ConfirmationRequired bool                 `xml:"ConfirmationRequired"`
	Manifest             createJobXMLCapture  `xml:"Manifest"`
	Operation            createJobXMLCapture  `xml:"Operation"`
	Report               createJobXMLCapture  `xml:"Report"`
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

	// Persist extended fields if present.
	if body.Description != "" || body.Manifest.Raw != "" || body.Operation.Raw != "" ||
		body.Report.Raw != "" || body.ConfirmationRequired {
		_ = h.Backend.UpdateJobDetails(
			accountID, job.JobID,
			body.Description,
			body.Manifest.Raw,
			body.Operation.Raw,
			body.Report.Raw,
			body.ConfirmationRequired,
		)
	}

	return writeXML(c, createJobResponseXML{
		JobID: job.JobID,
	})
}

// --- CreateMultiRegionAccessPoint handler ---

type createMRAPRegionXML struct {
	Bucket string `xml:"Bucket"`
}

type createMRAPDetailsXML struct {
	Name    string               `xml:"Name"`
	Regions []createMRAPRegionXML `xml:"Regions>Region"`
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

	// Persist regions if provided.
	if len(body.Details.Regions) > 0 {
		buckets := make([]string, 0, len(body.Details.Regions))
		for _, r := range body.Details.Regions {
			buckets = append(buckets, r.Bucket)
		}
		_ = h.Backend.SetMRAPRegions(accountID, body.Details.Name, buckets)
	}

	return writeXML(c, createMRAPResponseXML{
		RequestTokenARN: req.RequestTokenARN,
	})
}

// --- GetAccessPoint handler ---

type getAccessPointVpcConfigXML struct {
	VpcId string `xml:"VpcId"`
}

type getAccessPointResponseXML struct {
	XMLName          xml.Name                `xml:"GetAccessPointResult"`
	Name             string                  `xml:"Name"`
	Bucket           string                  `xml:"Bucket"`
	NetworkOrigin    string                  `xml:"NetworkOrigin"`
	AccessPointArn   string                  `xml:"AccessPointArn,omitempty"`
	Alias            string                  `xml:"Alias,omitempty"`
	BucketAccountId  string                  `xml:"BucketAccountId,omitempty"`
	VpcConfiguration *getAccessPointVpcConfigXML `xml:"VpcConfiguration,omitempty"`
	CreationDate     string                  `xml:"CreationDate,omitempty"`
}

func (h *Handler) handleGetAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix)

	ap, err := h.Backend.GetAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getAccessPointResponseXML{
		Name:            ap.Name,
		Bucket:          ap.Bucket,
		NetworkOrigin:   ap.NetworkOrigin,
		AccessPointArn:  ap.AccessPointArn,
		Alias:           ap.Alias,
		BucketAccountId: ap.BucketAccountId,
		CreationDate:    ap.CreationDate,
	}

	if ap.VpcID != "" {
		resp.VpcConfiguration = &getAccessPointVpcConfigXML{VpcId: ap.VpcID}
	}

	return writeXML(c, resp)
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

type listAccessPointVpcConfigXML struct {
	VpcId string `xml:"VpcId"`
}

type listAccessPointItemXML struct {
	Name             string                 `xml:"Name"`
	Bucket           string                 `xml:"Bucket"`
	NetworkOrigin    string                 `xml:"NetworkOrigin"`
	AccessPointArn   string                 `xml:"AccessPointArn,omitempty"`
	Alias            string                 `xml:"Alias,omitempty"`
	BucketAccountId  string                 `xml:"BucketAccountId,omitempty"`
	VpcConfiguration *listAccessPointVpcConfigXML `xml:"VpcConfiguration,omitempty"`
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
		item := listAccessPointItemXML{
			Name:            ap.Name,
			Bucket:          ap.Bucket,
			NetworkOrigin:   ap.NetworkOrigin,
			AccessPointArn:  ap.AccessPointArn,
			Alias:           ap.Alias,
			BucketAccountId: ap.BucketAccountId,
		}
		if ap.VpcID != "" {
			item.VpcConfiguration = &listAccessPointVpcConfigXML{VpcId: ap.VpcID}
		}
		items = append(items, item)
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

type describeJobInnerXML struct {
	XMLName xml.Name
	Raw     string `xml:",innerxml"`
}

type describeJobDescriptorXML struct {
	JobArn               string               `xml:"JobArn"`
	JobID                string               `xml:"JobId"`
	RoleArn              string               `xml:"RoleArn"`
	Status               string               `xml:"Status"`
	Priority             int32                `xml:"Priority"`
	Description          string               `xml:"Description,omitempty"`
	ConfirmationRequired bool                 `xml:"ConfirmationRequired,omitempty"`
	CreationTime         string               `xml:"CreationTime,omitempty"`
	TerminationDate      string               `xml:"TerminationDate,omitempty"`
	StatusUpdateReason   string               `xml:"StatusUpdateReason,omitempty"`
	Manifest             *describeJobInnerXML `xml:"Manifest,omitempty"`
	Operation            *describeJobInnerXML `xml:"Operation,omitempty"`
	Report               *describeJobInnerXML `xml:"Report,omitempty"`
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

	desc := describeJobDescriptorXML{
		JobID:                job.JobID,
		JobArn:               job.JobArn,
		Status:               job.Status,
		Priority:             job.Priority,
		RoleArn:              job.RoleArn,
		Description:          job.Description,
		ConfirmationRequired: job.ConfirmationRequired,
		CreationTime:         job.CreationTime,
		TerminationDate:      job.TerminationDate,
		StatusUpdateReason:   job.StatusUpdateReason,
	}

	if job.Manifest != "" {
		desc.Manifest = &describeJobInnerXML{Raw: job.Manifest}
	}

	if job.Operation != "" {
		desc.Operation = &describeJobInnerXML{Raw: job.Operation}
	}

	if job.Report != "" {
		desc.Report = &describeJobInnerXML{Raw: job.Report}
	}

	return writeXML(c, describeJobResponseXML{Job: desc})
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
	XMLName              xml.Name `xml:"UpdateJobStatusRequest"`
	RequestedJobStatus   string   `xml:"RequestedJobStatus"`
	StatusUpdateReason   string   `xml:"StatusUpdateReason"`
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

	job, err := h.Backend.UpdateJobStatusValidated(accountID, jobID, body.RequestedJobStatus, body.StatusUpdateReason)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, updateJobStatusResponseXML{
		JobID:  job.JobID,
		Status: job.Status,
	})
}

// --- MRAP handlers ---

type getMRAPRegionXML struct {
	Bucket string `xml:"Bucket"`
}

type getMRAPAccessPointXML struct {
	Name      string             `xml:"Name"`
	Alias     string             `xml:"Alias"`
	Status    string             `xml:"Status"`
	CreatedAt string             `xml:"CreatedAt,omitempty"`
	Regions   []getMRAPRegionXML `xml:"Regions>Region,omitempty"`
}

type getMRAPResponseXML struct {
	XMLName     xml.Name              `xml:"GetMultiRegionAccessPointResult"`
	AccessPoint getMRAPAccessPointXML `xml:"AccessPoint"`
}

func (h *Handler) handleGetMultiRegionAccessPoint(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix)

	mrap, err := h.Backend.GetMultiRegionAccessPoint(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	regionItems := make([]getMRAPRegionXML, 0, len(mrap.Regions))
	for _, bucket := range mrap.Regions {
		regionItems = append(regionItems, getMRAPRegionXML{Bucket: bucket})
	}

	return writeXML(c, getMRAPResponseXML{
		AccessPoint: getMRAPAccessPointXML{
			Name:      mrap.Name,
			Alias:     mrap.Alias,
			Status:    mrap.Status,
			CreatedAt: mrap.CreatedAt,
			Regions:   regionItems,
		},
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
	Name   string                `xml:"Name"`
	Filter createJobXMLCapture   `xml:"Filter"`
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

	grp := h.Backend.CreateStorageLensGroup(accountID, body.StorageLensGroup.Name)

	if body.StorageLensGroup.Filter.Raw != "" {
		_ = h.Backend.UpdateStorageLensGroupFilter(accountID, grp.Name, body.StorageLensGroup.Filter.Raw)
	}

	return c.NoContent(http.StatusCreated)
}
