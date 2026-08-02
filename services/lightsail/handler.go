package lightsail

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// lightsailTargetPrefix is the literal X-Amz-Target prefix every one of
// this service's 161 operations carries (PARITY.md: "Lightsail_20161128.").
// Like mgn/directconnect, this is awsjson1.1: every request is a literal
// POST / dispatched purely by this header, with zero path-based routing.
const lightsailTargetPrefix = "Lightsail_20161128."

var errUnknownOperation = errors.New("unknown Lightsail operation")

// Handler is the HTTP handler for the Amazon Lightsail API.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Lightsail handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "Lightsail" }

// GetSupportedOperations returns every operation this handler routes -- all
// 161 operations on the aws-sdk-go-v2/service/lightsail client, per
// sdk_completeness_test.go's empty exception list.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		// A. Static reference data (9)
		"GetBlueprints", "GetBundles", "GetRelationalDatabaseBlueprints",
		"GetRelationalDatabaseBundles", "GetBucketBundles", "GetDistributionBundles",
		"GetContainerServicePowers", "GetRegions", "GetContainerAPIMetadata",
		// B. Instances core (9)
		"CreateInstances", "CreateInstancesFromSnapshot", "DeleteInstance",
		"GetInstance", "GetInstances", "GetInstanceState", "RebootInstance",
		"StartInstance", "StopInstance",
		// C. Instance access/ports/HTTPS (8)
		"GetInstanceAccessDetails", "GetInstancePortStates", "OpenInstancePublicPorts",
		"CloseInstancePublicPorts", "PutInstancePublicPorts", "SetupInstanceHttps",
		"GetSetupHistory", "DeleteKnownHostKeys",
		// D. Instance snapshots (4)
		"CreateInstanceSnapshot", "DeleteInstanceSnapshot", "GetInstanceSnapshot", "GetInstanceSnapshots",
		// E. Instance metrics/metadata (2)
		"GetInstanceMetricData", "UpdateInstanceMetadataOptions",
		// F. Add-ons (4)
		"GetAutoSnapshots", "DeleteAutoSnapshot", "EnableAddOn", "DisableAddOn",
		// G. Key pairs (6)
		"CreateKeyPair", "DeleteKeyPair", "DownloadDefaultKeyPair", "GetKeyPair", "GetKeyPairs", "ImportKeyPair",
		// H. Static IPs (6)
		"AllocateStaticIp", "AttachStaticIp", "DetachStaticIp", "GetStaticIp", "GetStaticIps", "ReleaseStaticIp",
		// I. Disks (7)
		"AttachDisk", "DetachDisk", "CreateDisk", "CreateDiskFromSnapshot", "DeleteDisk", "GetDisk", "GetDisks",
		// J. Disk snapshots + CopySnapshot (5)
		"CreateDiskSnapshot", "DeleteDiskSnapshot", "GetDiskSnapshot", "GetDiskSnapshots", "CopySnapshot",
		// K. Export/CloudFormation (4)
		"ExportSnapshot", "GetExportSnapshotRecords", "CreateCloudFormationStack", "GetCloudFormationStackRecords",
		// L. Load balancers (9)
		"CreateLoadBalancer", "DeleteLoadBalancer", "GetLoadBalancer", "GetLoadBalancers",
		"AttachInstancesToLoadBalancer", "DetachInstancesFromLoadBalancer", "UpdateLoadBalancerAttribute",
		"SetIpAddressType", "GetLoadBalancerMetricData",
		// M. LB TLS certificates (5)
		"CreateLoadBalancerTlsCertificate", "DeleteLoadBalancerTlsCertificate", "AttachLoadBalancerTlsCertificate",
		"GetLoadBalancerTlsCertificates", "GetLoadBalancerTlsPolicies",
		// N. Relational databases core (9)
		"CreateRelationalDatabase", "CreateRelationalDatabaseFromSnapshot", "DeleteRelationalDatabase",
		"GetRelationalDatabase", "GetRelationalDatabases", "StartRelationalDatabase", "StopRelationalDatabase",
		"RebootRelationalDatabase", "UpdateRelationalDatabase",
		// O. Relational databases ops (7)
		"GetRelationalDatabaseEvents", "GetRelationalDatabaseLogEvents", "GetRelationalDatabaseLogStreams",
		"GetRelationalDatabaseMasterUserPassword", "GetRelationalDatabaseMetricData",
		"GetRelationalDatabaseParameters", "UpdateRelationalDatabaseParameters",
		// P. Relational database snapshots (4)
		"CreateRelationalDatabaseSnapshot", "DeleteRelationalDatabaseSnapshot",
		"GetRelationalDatabaseSnapshot", "GetRelationalDatabaseSnapshots",
		// Q. Container services core (5)
		"CreateContainerService", "UpdateContainerService", "DeleteContainerService",
		"GetContainerServices", "GetContainerServiceMetricData",
		// R. Container deployments/images (7)
		"CreateContainerServiceDeployment", "GetContainerServiceDeployments",
		"CreateContainerServiceRegistryLogin", "RegisterContainerImage", "GetContainerImages",
		"DeleteContainerImage", "GetContainerLog",
		// S. Buckets (10)
		"CreateBucket", "DeleteBucket", "UpdateBucket", "UpdateBucketBundle", "GetBuckets",
		"SetResourceAccessForBucket", "GetBucketMetricData", "CreateBucketAccessKey",
		"DeleteBucketAccessKey", "GetBucketAccessKeys",
		// T. Distributions (10)
		"CreateDistribution", "UpdateDistribution", "DeleteDistribution", "GetDistributions",
		"UpdateDistributionBundle", "ResetDistributionCache", "GetDistributionLatestCacheReset",
		"GetDistributionMetricData", "AttachCertificateToDistribution", "DetachCertificateFromDistribution",
		// U. Domains/DNS (7)
		"CreateDomain", "DeleteDomain", "GetDomain", "GetDomains",
		"CreateDomainEntry", "DeleteDomainEntry", "UpdateDomainEntry",
		// V. Certificates (3)
		"CreateCertificate", "DeleteCertificate", "GetCertificates",
		// W. Alarms/contacts (8)
		"PutAlarm", "DeleteAlarm", "GetAlarms", "TestAlarm", "CreateContactMethod",
		"DeleteContactMethod", "GetContactMethods", "SendContactMethodVerification",
		// X. VPC peering (3)
		"PeerVpc", "UnpeerVpc", "IsVpcPeered",
		// Y. Tagging (2)
		"TagResource", "UntagResource",
		// Z. Operations (3)
		"GetOperation", "GetOperations", "GetOperationsForResource",
		// AA. GUI sessions (3)
		"CreateGUISessionAccessDetails", "StartGUISession", "StopGUISession",
		// BB. Misc (2)
		"GetActiveNames", "GetCostEstimate",
	}
}

// Reset clears all stored state in the backend.
func (h *Handler) Reset() { h.Backend.Reset() }

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "lightsail" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a matcher that reports true iff the request carries
// this service's exact X-Amz-Target prefix.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, lightsailTargetPrefix)
	}
}

// MatchPriority returns the routing priority -- matched by an exact
// X-Amz-Target prefix, the same tier DynamoDB/directconnect/mgn use.
func (h *Handler) MatchPriority() int { return service.PriorityHeaderExact }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return strings.TrimPrefix(c.Request().Header.Get("X-Amz-Target"), lightsailTargetPrefix)
}

// ExtractResource extracts the primary resource identifier from the
// request body -- whichever of this service's common name/id field names
// is present, matching services/directconnect's extractFirstResourceByKeys
// pattern.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var m map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &m); unmarshalErr != nil {
		return ""
	}

	return extractFirstResourceByKeys(
		m,
		"resourceName", "instanceName", "diskName", "loadBalancerName",
		"relationalDatabaseName", "serviceName", "bucketName", "distributionName",
		"domainName", "certificateName", "alarmName", "keyPairName", "staticIpName",
	)
}

func extractFirstResourceByKeys(body map[string]json.RawMessage, keys ...string) string {
	for _, key := range keys {
		if raw, ok := body[key]; ok {
			var v string
			if err := json.Unmarshal(raw, &v); err == nil && v != "" {
				return v
			}
		}
	}

	return ""
}

// Handler returns the Echo handler function for Lightsail requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		return service.HandleTarget(
			c, log,
			"Lightsail", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			func(dispatchCtx context.Context, action string, body []byte) ([]byte, error) {
				return h.dispatch(dispatchCtx, action, body)
			},
			h.handleError,
		)
	}
}

// dispatch routes action to its handler via the flat opTable() map.
func (h *Handler) dispatch(ctx context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.opTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w: %s", errUnknownOperation, action)
	}

	return fn(ctx, body)
}

type opFunc func(ctx context.Context, body []byte) ([]byte, error)

// opTable merges every op-family's own handler map -- see handler_*.go,
// each contributing one or more families.
func (h *Handler) opTable() map[string]opFunc {
	table := make(map[string]opFunc, len(h.GetSupportedOperations()))

	maps.Copy(table, h.referenceDataOps())
	maps.Copy(table, h.instanceOps())
	maps.Copy(table, h.instanceAccessOps())
	maps.Copy(table, h.instanceExtrasOps())
	maps.Copy(table, h.keyPairStaticIPOps())
	maps.Copy(table, h.diskOps())
	maps.Copy(table, h.exportCfnOps())
	maps.Copy(table, h.loadBalancerOps())
	maps.Copy(table, h.databaseOps())
	maps.Copy(table, h.containerOps())
	maps.Copy(table, h.bucketOps())
	maps.Copy(table, h.distributionCertOps())
	maps.Copy(table, h.domainOps())
	maps.Copy(table, h.alarmContactOps())
	maps.Copy(table, h.taggingVpcMiscOps())
	maps.Copy(table, h.operationOps())

	return table
}

// handleError renders err as one of Lightsail's 8 awsjson1.1 exception
// shapes: {"__type": "...Exception", "message": "..."}.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, err error) error {
	status, errType := classifyLightsailError(err)

	if status == http.StatusInternalServerError {
		logger.Load(ctx).ErrorContext(ctx, "lightsail: operation failed", "op", action, "error", err)
	}

	body := map[string]string{"__type": errType, "message": err.Error()}
	payload, _ := json.Marshal(body)

	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	return c.JSONBlob(status, payload)
}

// decodeBody unmarshals body into a fresh *T, treating an empty body as a
// no-op (several ops in this service take no input fields at all, e.g.
// GetContainerAPIMetadata/DownloadDefaultKeyPair/PeerVpc/IsVpcPeered).
func decodeBody[T any](body []byte) (*T, error) {
	var v T

	if len(body) > 0 {
		if err := json.Unmarshal(body, &v); err != nil {
			return nil, validationError("malformed request body: " + err.Error())
		}
	}

	return &v, nil
}

// marshalResponse marshals v, wrapping any error as a server-fault
// exception (marshaling our own well-typed wire structs should never
// fail).
func marshalResponse(v any) ([]byte, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, serviceError("lightsail: marshal response: " + err.Error())
	}

	return data, nil
}
