package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
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
// not a missing-data gap. CacheUsageLimits is the one real field
// deliberately NOT added: it is not modeled anywhere in the domain type, and
// modeling data/ECPU usage limits is a larger feature than a wire-mapping
// fix (see PARITY.md).
type serverlessCacheXML struct {
	Endpoint               *serverlessCacheEndpointXML `xml:"Endpoint,omitempty"`
	ReaderEndpoint         *serverlessCacheEndpointXML `xml:"ReaderEndpoint,omitempty"`
	SecurityGroupIDs       *securityGroupIDsXML        `xml:"SecurityGroupIds,omitempty"`
	SubnetIDs              *subnetIDsXML               `xml:"SubnetIds,omitempty"`
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

func (h *Handler) createServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")
	engine := form.Get("Engine")

	sc, err := h.Backend.CreateServerlessCache(ctx, name, description, engine)
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

// serverlessCacheSnapshotXML's ExpiryTime/KmsKeyId/BytesUsedForCache/
// ServerlessCacheConfiguration (real types.ServerlessCacheSnapshot fields,
// verified against awsAwsquery_deserializeDocumentServerlessCacheSnapshot)
// are deliberately NOT added -- the domain ServerlessCacheSnapshot model has
// no fields to derive them from, unlike CreateTime (below), which the model
// already stores as CreatedAt and was simply never wired to the wire
// response. Modeling snapshot expiry/KMS/size/config tracking is new-feature
// scope, not a wire-mapping fix; see PARITY.md.
type serverlessCacheSnapshotXML struct {
	ARN                 string `xml:"ARN"`
	Name                string `xml:"ServerlessCacheSnapshotName"`
	Status              string `xml:"Status"`
	ServerlessCacheName string `xml:"ServerlessCacheName,omitempty"`
	SnapshotType        string `xml:"SnapshotType,omitempty"`
	CreateTime          string `xml:"CreateTime,omitempty"`
}

func serverlessCacheSnapshotToXML(snap *ServerlessCacheSnapshot) serverlessCacheSnapshotXML {
	x := serverlessCacheSnapshotXML{
		ARN:                 snap.ARN,
		Name:                snap.Name,
		Status:              snap.Status,
		ServerlessCacheName: snap.ServerlessCacheName,
		SnapshotType:        snap.SnapshotType,
	}
	if !snap.CreatedAt.IsZero() {
		x.CreateTime = snap.CreatedAt.UTC().Format(time.RFC3339)
	}

	return x
}

func (h *Handler) createServerlessCacheSnapshot(ctx context.Context, c *echo.Context, form url.Values) error {
	snapshotName := form.Get("ServerlessCacheSnapshotName")
	serverlessCacheName := form.Get("ServerlessCacheName")

	snap, err := h.Backend.CreateServerlessCacheSnapshot(ctx, snapshotName, serverlessCacheName)
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

func (h *Handler) modifyServerlessCache(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("ServerlessCacheName")
	description := form.Get("Description")

	sc, err := h.Backend.ModifyServerlessCache(ctx, name, description)
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
