package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/labstack/echo/v5"
)

type serverlessCacheEndpointXML struct {
	Address string `xml:"Address"`
	Port    int    `xml:"Port"`
}

type serverlessCacheXML struct {
	Endpoint       *serverlessCacheEndpointXML `xml:"Endpoint,omitempty"`
	ReaderEndpoint *serverlessCacheEndpointXML `xml:"ReaderEndpoint,omitempty"`
	ARN            string                      `xml:"ARN"`
	Name           string                      `xml:"ServerlessCacheName"`
	Description    string                      `xml:"Description,omitempty"`
	Status         string                      `xml:"Status"`
	Engine         string                      `xml:"Engine,omitempty"`
}

func serverlessCacheToXML(sc *ServerlessCache) serverlessCacheXML {
	x := serverlessCacheXML{
		ARN:         sc.ARN,
		Name:        sc.Name,
		Description: sc.Description,
		Status:      sc.Status,
		Engine:      sc.Engine,
	}
	if sc.Endpoint != nil {
		x.Endpoint = &serverlessCacheEndpointXML{Address: sc.Endpoint.Address, Port: sc.Endpoint.Port}
	}
	if sc.ReaderEndpoint != nil {
		x.ReaderEndpoint = &serverlessCacheEndpointXML{Address: sc.ReaderEndpoint.Address, Port: sc.ReaderEndpoint.Port}
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

type serverlessCacheSnapshotXML struct {
	ARN                 string `xml:"ARN"`
	Name                string `xml:"ServerlessCacheSnapshotName"`
	Status              string `xml:"Status"`
	ServerlessCacheName string `xml:"ServerlessCacheName,omitempty"`
	SnapshotType        string `xml:"SnapshotType,omitempty"`
}

func serverlessCacheSnapshotToXML(snap *ServerlessCacheSnapshot) serverlessCacheSnapshotXML {
	return serverlessCacheSnapshotXML{
		ARN:                 snap.ARN,
		Name:                snap.Name,
		Status:              snap.Status,
		ServerlessCacheName: snap.ServerlessCacheName,
		SnapshotType:        snap.SnapshotType,
	}
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
