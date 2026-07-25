package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/labstack/echo/v5"
)

type serverlessCacheEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

// securityGroupIDsXML/subnetIDsXML wrap ServerlessCache.SecurityGroupIds/
// SubnetIds. Unlike User's UserGroupIds (locationName "member"), the real
// types.ServerlessCache's list items use dedicated per-list element names --
// verified against aws-sdk-go-v2/service/elasticache@v1.51.11/deserializers.go's
// awsAwsquery_deserializeDocumentSecurityGroupIdsList/
// awsAwsquery_deserializeDocumentSubnetIdsList ("SecurityGroupId"/"SubnetId",
// not "member").
type securityGroupIDsXML struct {
	SecurityGroupID []string `xml:"SecurityGroupId"`
}

type subnetIDsXML struct {
	SubnetID []string `xml:"SubnetId"`
}

// dataStorageLimitXML/ecpuPerSecondLimitXML/cacheUsageLimitsXML are the wire
// shape for ServerlessCache.CacheUsageLimits, verified against
// aws-sdk-go-v2/service/elasticache@v1.51.11's
// awsAwsquery_deserializeDocumentCacheUsageLimits/DataStorage/ECPUPerSecond.
// Previously entirely unmodeled -- see PARITY.md gaps (fixed this pass).
type dataStorageLimitXML struct {
	Unit    string `xml:"Unit,omitempty"`
	Maximum int32  `xml:"Maximum,omitempty"`
	Minimum int32  `xml:"Minimum,omitempty"`
}

type ecpuPerSecondLimitXML struct {
	Maximum int32 `xml:"Maximum,omitempty"`
	Minimum int32 `xml:"Minimum,omitempty"`
}

type cacheUsageLimitsXML struct {
	DataStorage   *dataStorageLimitXML   `xml:"DataStorage,omitempty"`
	ECPUPerSecond *ecpuPerSecondLimitXML `xml:"ECPUPerSecond,omitempty"`
}

func cacheUsageLimitsToXML(l *CacheUsageLimits) *cacheUsageLimitsXML {
	if l == nil {
		return nil
	}

	x := &cacheUsageLimitsXML{}
	if l.DataStorage != nil {
		x.DataStorage = &dataStorageLimitXML{
			Unit:    l.DataStorage.Unit,
			Maximum: l.DataStorage.Maximum,
			Minimum: l.DataStorage.Minimum,
		}
	}

	if l.ECPUPerSecond != nil {
		x.ECPUPerSecond = &ecpuPerSecondLimitXML{
			Maximum: l.ECPUPerSecond.Maximum,
			Minimum: l.ECPUPerSecond.Minimum,
		}
	}

	return x
}

// parseCacheUsageLimitsForm extracts CacheUsageLimits.DataStorage.*/
// CacheUsageLimits.ECPUPerSecond.* from a CreateServerlessCache/
// ModifyServerlessCache request form (field names verified against
// awsAwsquery_serializeDocumentCacheUsageLimits/DataStorage/ECPUPerSecond).
// Returns nil when the caller supplied neither nested object, matching real
// AWS's "absent means unchanged/unset" semantics for optional structs.
func parseCacheUsageLimitsForm(form url.Values) *CacheUsageLimits {
	var out *CacheUsageLimits

	if unit := form.Get("CacheUsageLimits.DataStorage.Unit"); unit != "" ||
		form.Has("CacheUsageLimits.DataStorage.Maximum") || form.Has("CacheUsageLimits.DataStorage.Minimum") {
		out = &CacheUsageLimits{DataStorage: &DataStorageLimit{
			Unit:    unit,
			Maximum: parseFormInt32(form, "CacheUsageLimits.DataStorage.Maximum"),
			Minimum: parseFormInt32(form, "CacheUsageLimits.DataStorage.Minimum"),
		}}
	}

	if form.Has("CacheUsageLimits.ECPUPerSecond.Maximum") || form.Has("CacheUsageLimits.ECPUPerSecond.Minimum") {
		if out == nil {
			out = &CacheUsageLimits{}
		}

		out.ECPUPerSecond = &ECPUPerSecondLimit{
			Maximum: parseFormInt32(form, "CacheUsageLimits.ECPUPerSecond.Maximum"),
			Minimum: parseFormInt32(form, "CacheUsageLimits.ECPUPerSecond.Minimum"),
		}
	}

	return out
}

// parseFormInt32 returns the parsed int32 value of a form field, or 0 if
// absent/non-numeric (numeric validation for these optional usage-limit
// fields is out of scope -- AWS itself validates ranges server-side; a
// non-numeric value here simply surfaces as 0, matching how the rest of
// this handler treats malformed optional numeric input).
func parseFormInt32(form url.Values, key string) int32 {
	n, err := strconv.ParseInt(form.Get(key), 10, 32)
	if err != nil {
		return 0
	}

	return int32(n)
}

// serverlessCacheXML is the wire shape for ServerlessCache, verified against
// aws-sdk-go-v2/service/elasticache@v1.51.11's
// awsAwsquery_deserializeDocumentServerlessCache. A prior revision of this
// struct only wired ARN/ServerlessCacheName/Description/Status/Engine/
// Endpoint/ReaderEndpoint -- CreateTime/DailySnapshotTime/FullEngineVersion/
// KmsKeyId/MajorEngineVersion/SecurityGroupIds/SnapshotRetentionLimit/
// SubnetIds/UserGroupId were all silently dropped from every
// CreateServerlessCache/ModifyServerlessCache/DeleteServerlessCache/
// DescribeServerlessCaches response despite the domain ServerlessCache model
// already storing all of them -- this was purely a missing-wire-mapping bug,
// not a missing-data gap. CacheUsageLimits is now also wired (was the one
// real field left unmodeled by the prior pass -- see PARITY.md).
type serverlessCacheXML struct {
	Endpoint               *serverlessCacheEndpointXML `xml:"Endpoint,omitempty"`
	ReaderEndpoint         *serverlessCacheEndpointXML `xml:"ReaderEndpoint,omitempty"`
	SecurityGroupIDs       *securityGroupIDsXML        `xml:"SecurityGroupIds,omitempty"`
	SubnetIDs              *subnetIDsXML               `xml:"SubnetIds,omitempty"`
	CacheUsageLimits       *cacheUsageLimitsXML        `xml:"CacheUsageLimits,omitempty"`
	ARN                    string                      `xml:"ARN"`
	Name                   string                      `xml:"ServerlessCacheName"`
	Description            string                      `xml:"Description,omitempty"`
	Status                 string                      `xml:"Status"`
	Engine                 string                      `xml:"Engine,omitempty"`
	CreateTime             string                      `xml:"CreateTime,omitempty"`
	DailySnapshotTime      string                      `xml:"DailySnapshotTime,omitempty"`
	FullEngineVersion      string                      `xml:"FullEngineVersion,omitempty"`
	KmsKeyID               string                      `xml:"KmsKeyId,omitempty"`
	MajorEngineVersion     string                      `xml:"MajorEngineVersion,omitempty"`
	UserGroupID            string                      `xml:"UserGroupId,omitempty"`
	SnapshotRetentionLimit int32                       `xml:"SnapshotRetentionLimit,omitempty"`
}

func serverlessCacheToXML(sc *ServerlessCache) serverlessCacheXML {
	// FullEngineVersion (real AWS's combined "engine+majorVersion" display
	// string, e.g. "redis7") is deliberately left unset: the domain model
	// has no field it could be derived from without guessing a format this
	// pass could not verify, and a fabricated-but-wrong value would be worse
	// than the field being absent (parity-principles.md's no-fabrication
	// rule). Unchanged from before this pass.
	x := serverlessCacheXML{
		ARN:                    sc.ARN,
		Name:                   sc.Name,
		Description:            sc.Description,
		Status:                 sc.Status,
		Engine:                 sc.Engine,
		DailySnapshotTime:      sc.DailySnapshotTime,
		KmsKeyID:               sc.KmsKeyID,
		MajorEngineVersion:     sc.MajorEngineVersion,
		UserGroupID:            sc.UserGroupID,
		SnapshotRetentionLimit: sc.SnapshotRetentionLimit,
		CacheUsageLimits:       cacheUsageLimitsToXML(sc.CacheUsageLimits),
	}
	if !sc.CreatedAt.IsZero() {
		x.CreateTime = sc.CreatedAt.UTC().Format(time.RFC3339)
	}

	if sc.Endpoint != nil {
		x.Endpoint = &serverlessCacheEndpointXML{Address: sc.Endpoint.Address, Port: sc.Endpoint.Port}
	}

	if sc.ReaderEndpoint != nil {
		x.ReaderEndpoint = &serverlessCacheEndpointXML{Address: sc.ReaderEndpoint.Address, Port: sc.ReaderEndpoint.Port}
	}

	if len(sc.SecurityGroupIDs) > 0 {
		x.SecurityGroupIDs = &securityGroupIDsXML{SecurityGroupID: sc.SecurityGroupIDs}
	}

	if len(sc.SubnetIDs) > 0 {
		x.SubnetIDs = &subnetIDsXML{SubnetID: sc.SubnetIDs}
	}

	return x
}

// createServerlessCache parses the full CreateServerlessCacheInput request
// shape and creates the cache via CreateServerlessCacheFull.
//
// This previously called the crippled 3-arg CreateServerlessCache backend
// method, which only ever read ServerlessCacheName/Description/Engine from
// the request -- every other real CreateServerlessCacheInput member
// (CacheUsageLimits, DailySnapshotTime, KmsKeyId, MajorEngineVersion,
// SecurityGroupIds, SnapshotRetentionLimit, SubnetIds, Tags, UserGroupId)
// was silently dropped on the actual wire-routed create path, even though
// serverlessCacheXML's response mapping (above) already surfaced them
// correctly -- so a real client's create request would lose the data, and
// its own create response (plus every subsequent Describe) would just
// reflect the empty defaults back. This is exactly the "asserted against
// backend structs, not what a real client receives" bug class called out in
// PARITY.md: TestBackend_CreateServerlessCacheFull_AllFields exercised
// CreateServerlessCacheFull directly and passed, while the real wire op
// silently discarded everything. Found and fixed alongside the
// CacheUsageLimits gap this pass; see PARITY.md and
// TestHandler_CreateServerlessCache_WireRequestFieldsThreaded (a real SDK
// client round trip through *this* handler, not a direct backend call).
func (h *Handler) createServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	sc, err := h.Backend.CreateServerlessCacheFull(ctx, ServerlessCreateOpts{
		Name:                   form.Get("ServerlessCacheName"),
		Description:            form.Get("Description"),
		Engine:                 form.Get("Engine"),
		KmsKeyID:               form.Get("KmsKeyId"),
		UserGroupID:            form.Get("UserGroupId"),
		DailySnapshotTime:      form.Get("DailySnapshotTime"),
		MajorEngineVersion:     form.Get("MajorEngineVersion"),
		SecurityGroupIDs:       parseRepeatedField(form, "SecurityGroupIds.SecurityGroupId"),
		SubnetIDs:              parseRepeatedField(form, "SubnetIds.SubnetId"),
		SnapshotRetentionLimit: parseFormInt32(form, "SnapshotRetentionLimit"),
		CacheUsageLimits:       parseCacheUsageLimitsForm(form),
		Tags:                   parseFormTags(form),
	})
	if err != nil {
		if errors.Is(err, ErrServerlessCacheAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheAlreadyExistsFault",
				"Serverless cache already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name           `xml:"CreateServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"CreateServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}

// ----------------------------------------
// CreateServerlessCacheSnapshot
// ----------------------------------------

// serverlessCacheConfigurationXML is the wire shape for
// ServerlessCacheSnapshot.ServerlessCacheConfiguration, verified against
// awsAwsquery_deserializeDocumentServerlessCacheConfiguration.
type serverlessCacheConfigurationXML struct {
	ServerlessCacheName string `xml:"ServerlessCacheName,omitempty"`
	Engine              string `xml:"Engine,omitempty"`
	MajorEngineVersion  string `xml:"MajorEngineVersion,omitempty"`
}

// serverlessCacheSnapshotXML now also wires ExpiryTime/KmsKeyId/
// BytesUsedForCache/ServerlessCacheConfiguration (real
// types.ServerlessCacheSnapshot fields, verified against
// awsAwsquery_deserializeDocumentServerlessCacheSnapshot/
// ServerlessCacheConfiguration) -- previously entirely unmodeled, see
// PARITY.md gaps (fixed this pass). ExpiryTime stays unset for every
// snapshot this emulator ever produces (see the doc comment on
// [ServerlessCacheSnapshot]); BytesUsedForCache is always "0" for the same
// documented "no real data-plane engine backs a serverless cache here"
// reason, not a fabricated number.
type serverlessCacheSnapshotXML struct {
	ServerlessCacheConfiguration *serverlessCacheConfigurationXML `xml:"ServerlessCacheConfiguration,omitempty"`
	ARN                          string                           `xml:"ARN"`
	Name                         string                           `xml:"ServerlessCacheSnapshotName"`
	Status                       string                           `xml:"Status"`
	ServerlessCacheName          string                           `xml:"ServerlessCacheName,omitempty"`
	SnapshotType                 string                           `xml:"SnapshotType,omitempty"`
	CreateTime                   string                           `xml:"CreateTime,omitempty"`
	ExpiryTime                   string                           `xml:"ExpiryTime,omitempty"`
	KmsKeyID                     string                           `xml:"KmsKeyId,omitempty"`
	BytesUsedForCache            string                           `xml:"BytesUsedForCache,omitempty"`
}

func serverlessCacheSnapshotToXML(snap *ServerlessCacheSnapshot) serverlessCacheSnapshotXML {
	x := serverlessCacheSnapshotXML{
		ARN:                 snap.ARN,
		Name:                snap.Name,
		Status:              snap.Status,
		ServerlessCacheName: snap.ServerlessCacheName,
		SnapshotType:        snap.SnapshotType,
		KmsKeyID:            snap.KmsKeyID,
		BytesUsedForCache:   snap.BytesUsedForCache,
	}
	if !snap.CreatedAt.IsZero() {
		x.CreateTime = snap.CreatedAt.UTC().Format(time.RFC3339)
	}

	if !snap.ExpiryTime.IsZero() {
		x.ExpiryTime = snap.ExpiryTime.UTC().Format(time.RFC3339)
	}

	if cfg := snap.ServerlessCacheConfiguration; cfg != nil {
		x.ServerlessCacheConfiguration = &serverlessCacheConfigurationXML{
			ServerlessCacheName: cfg.ServerlessCacheName,
			Engine:              cfg.Engine,
			MajorEngineVersion:  cfg.MajorEngineVersion,
		}
	}

	return x
}

func (h *Handler) createServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	serverlessCacheName := form.Get("ServerlessCacheName")
	kmsKeyID := form.Get("KmsKeyId")

	snap, err := h.Backend.CreateServerlessCacheSnapshot(ctx, snapshotName, serverlessCacheName, kmsKeyID)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotAlreadyExistsFault",
				"Serverless cache snapshot already exists",
			)
		}
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusNotFound, "ServerlessCacheNotFoundFault", "Serverless cache not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"CreateServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"CreateServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

// ----------------------------------------
// CopyServerlessCacheSnapshot
// ----------------------------------------

func (h *Handler) copyServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	sourceSnapshotName := form.Get("SourceServerlessCacheSnapshotName")
	targetSnapshotName := form.Get("TargetServerlessCacheSnapshotName")

	snap, err := h.Backend.CopyServerlessCacheSnapshot(ctx, sourceSnapshotName, targetSnapshotName)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ServerlessCacheSnapshotNotFoundFault",
				"Source serverless cache snapshot not found",
			)
		}
		if errors.Is(err, ErrServerlessCacheSnapshotExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"ServerlessCacheSnapshotAlreadyExistsFault",
				"Target serverless cache snapshot already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"CopyServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"CopyServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

// ----------------------------------------
// CreateUser
// ----------------------------------------

// describeServerlessCachesResultXML is the XML envelope for DescribeServerlessCaches responses.
type describeServerlessCachesResultXML struct {
	XMLName          xml.Name `xml:"DescribeServerlessCachesResponse"`
	Xmlns            string   `xml:"xmlns,attr"`
	Marker           string   `xml:"DescribeServerlessCachesResult>Marker,omitempty"`
	ServerlessCaches struct {
		Member []serverlessCacheXML `xml:"member"`
	} `xml:"DescribeServerlessCachesResult>ServerlessCaches"`
}

func (h *Handler) deleteServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")

	sc, err := h.Backend.DeleteServerlessCache(ctx, name)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusNotFound, "ServerlessCacheNotFoundFault", "Serverless cache not found")
		}
		if errors.Is(err, ErrServerlessCacheNotAvailable) {
			return xmlError(c, http.StatusBadRequest, "InvalidServerlessCacheStateFault", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name           `xml:"DeleteServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"DeleteServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}

func (h *Handler) deleteServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheSnapshotName")

	snap, err := h.Backend.DeleteServerlessCacheSnapshot(ctx, name)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"DeleteServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"DeleteServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

func (h *Handler) describeServerlessCaches(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")

	p, err := describeListChecked(c, form,
		func(marker string, maxRecords int) (page.Page[ServerlessCache], error) {
			return h.Backend.DescribeServerlessCaches(ctx, name, marker, maxRecords)
		},
		ErrServerlessCacheNotFound, http.StatusNotFound, "ServerlessCacheNotFoundFault", "Serverless cache not found")
	if err != nil {
		return err
	}

	var res describeServerlessCachesResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.ServerlessCaches.Member = append(res.ServerlessCaches.Member, serverlessCacheToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) describeServerlessCacheSnapshots(ctx context.Context, c *echo.Context, form url.Values) error {
	serverlessCacheName := form.Get("ServerlessCacheName")
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeServerlessCacheSnapshots(ctx, serverlessCacheName, snapshotName, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]serverlessCacheSnapshotXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, serverlessCacheSnapshotToXML(&p.Data[i]))
	}

	type scsListXML struct {
		ServerlessCacheSnapshot []serverlessCacheSnapshotXML `xml:"ServerlessCacheSnapshot"`
	}

	type result struct {
		XMLName                  xml.Name   `xml:"DescribeServerlessCacheSnapshotsResponse"`
		Xmlns                    string     `xml:"xmlns,attr"`
		Marker                   string     `xml:"DescribeServerlessCacheSnapshotsResult>Marker,omitempty"`
		ServerlessCacheSnapshots scsListXML `xml:"DescribeServerlessCacheSnapshotsResult>ServerlessCacheSnapshots"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                    elasticacheNS,
		Marker:                   p.Next,
		ServerlessCacheSnapshots: scsListXML{ServerlessCacheSnapshot: items},
	})
}

func (h *Handler) exportServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	s3BucketName := form.Get("S3BucketName")

	snap, err := h.Backend.ExportServerlessCacheSnapshot(ctx, snapshotName, s3BucketName)
	if err != nil {
		if errors.Is(err, ErrServerlessCacheSnapshotNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"ServerlessCacheSnapshotNotFoundFault",
				"Serverless cache snapshot not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name                   `xml:"ExportServerlessCacheSnapshotResponse"`
		Xmlns                   string                     `xml:"xmlns,attr"`
		ServerlessCacheSnapshot serverlessCacheSnapshotXML `xml:"ExportServerlessCacheSnapshotResult>ServerlessCacheSnapshot"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		ServerlessCacheSnapshot: serverlessCacheSnapshotToXML(snap),
	})
}

// modifyServerlessCache parses the full ModifyServerlessCacheInput request
// shape and applies it via ModifyServerlessCacheFull. Previously called the
// crippled 2-arg ModifyServerlessCache backend method, which only read
// Description -- see the doc comment on createServerlessCache for the same
// bug class (UserGroupId/DailySnapshotTime/SnapshotRetentionLimit/
// SecurityGroupIds/CacheUsageLimits were all silently dropped on modify too).
func (h *Handler) modifyServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")

	var snapshotRetentionLimit *int32
	if s := form.Get("SnapshotRetentionLimit"); s != "" {
		v := parseFormInt32(form, "SnapshotRetentionLimit")
		snapshotRetentionLimit = &v
	}

	sc, err := h.Backend.ModifyServerlessCacheFull(ctx, name, ServerlessModifyOpts{
		Description:            form.Get("Description"),
		UserGroupID:            form.Get("UserGroupId"),
		DailySnapshotTime:      form.Get("DailySnapshotTime"),
		SecurityGroupIDs:       parseRepeatedField(form, "SecurityGroupIds.SecurityGroupId"),
		RemoveUserGroup:        form.Get("RemoveUserGroup") == "true",
		SnapshotRetentionLimit: snapshotRetentionLimit,
		CacheUsageLimits:       parseCacheUsageLimitsForm(form),
	})
	if err != nil {
		if errors.Is(err, ErrServerlessCacheNotFound) {
			return xmlError(c, http.StatusNotFound, "ServerlessCacheNotFoundFault", "Serverless cache not found")
		}
		if errors.Is(err, ErrServerlessCacheNotAvailable) {
			return xmlError(c, http.StatusBadRequest, "InvalidServerlessCacheStateFault", err.Error())
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName         xml.Name           `xml:"ModifyServerlessCacheResponse"`
		Xmlns           string             `xml:"xmlns,attr"`
		ServerlessCache serverlessCacheXML `xml:"ModifyServerlessCacheResult>ServerlessCache"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:           elasticacheNS,
		ServerlessCache: serverlessCacheToXML(sc),
	})
}
