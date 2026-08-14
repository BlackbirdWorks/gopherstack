package route53

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type xmlCidrCollection struct {
	ID      string `xml:"Id"`
	Name    string `xml:"Name"`
	ARN     string `xml:"Arn,omitempty"`
	Version int64  `xml:"Version"`
}

type xmlCreateCidrCollectionRequest struct {
	XMLName         xml.Name `xml:"CreateCidrCollectionRequest"`
	Name            string   `xml:"Name"`
	CallerReference string   `xml:"CallerReference,omitempty"`
}

type xmlCreateCidrCollectionResponse struct {
	XMLName    xml.Name          `xml:"CreateCidrCollectionResponse"`
	Xmlns      string            `xml:"xmlns,attr"`
	Collection xmlCidrCollection `xml:"Collection"`
}

type xmlChangeCidrCollectionResponse struct {
	XMLName xml.Name `xml:"ChangeCidrCollectionResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	ID      string   `xml:"Id"`
	Version int64    `xml:"Version"`
}

type xmlCidrChangeEntry struct {
	LocationName string   `xml:"LocationName"`
	Action       string   `xml:"Action"`
	CidrList     []string `xml:"CidrList>Cidr"`
}

type xmlChangeCidrCollectionRequest struct {
	CollectionVersion *int64   `xml:"CollectionVersion"`
	XMLName           xml.Name `xml:"ChangeCidrCollectionRequest"`
	// The real serializer (awsRestxml_serializeDocumentCidrCollectionChanges,
	// route53@v1.65.6) sends each entry as <member>, not <Change>.
	Changes []xmlCidrChangeEntry `xml:"Changes>member"`
}

type xmlListCidrCollectionsResponse struct {
	XMLName         xml.Name                   `xml:"ListCidrCollectionsResponse"`
	Xmlns           string                     `xml:"xmlns,attr"`
	NextToken       string                     `xml:"NextToken,omitempty"`
	CidrCollections []xmlCidrCollectionSummary `xml:"CidrCollections>member"`
	IsTruncated     bool                       `xml:"IsTruncated"`
}

type xmlCidrCollectionSummary struct {
	ARN     string `xml:"Arn"`
	ID      string `xml:"Id"`
	Name    string `xml:"Name"`
	Version int64  `xml:"Version"`
}

func (h *Handler) routeCidrCollectionRoot(c *echo.Context, method string) error {
	switch method {
	case http.MethodPost:
		return h.createCidrCollection(c)
	case http.MethodGet:
		return h.listCidrCollections(c)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on /cidrcollection",
		)
	}
}

func (h *Handler) routeCidrCollection(c *echo.Context, path, method string) error {
	switch method {
	case http.MethodPost:
		return h.changeCidrCollection(c, path)
	case http.MethodDelete:
		return h.deleteCidrCollection(c, path)
	case http.MethodGet:
		// ListCidrLocations → GET /cidrcollection/{Id}
		// ListCidrBlocks → GET /cidrcollection/{Id}/cidrblocks
		if strings.HasSuffix(path, "/cidrblocks") {
			return h.listCidrBlocks(c, path)
		}

		return h.listCidrLocations(c, path)
	default:
		return xmlError(
			c,
			http.StatusNotFound,
			"NoSuchOperation",
			"unsupported method on cidrcollection",
		)
	}
}

func (h *Handler) createCidrCollection(c *echo.Context) error {
	ctx := c.Request().Context()

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlCreateCidrCollectionRequest
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(
			c,
			http.StatusBadRequest,
			"InvalidInput",
			"failed to parse XML: "+err.Error(),
		)
	}

	col, err := h.Backend.CreateCidrCollection(req.Name, req.CallerReference)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).
		DebugContext(ctx, "Route53 CreateCidrCollection", "id", col.ID, "name", col.Name)

	resp := xmlCreateCidrCollectionResponse{
		Xmlns: route53Namespace,
		Collection: xmlCidrCollection{
			ID:      col.ID,
			Name:    col.Name,
			Version: col.Version,
			ARN:     col.ARN,
		},
	}

	c.Response().Header().Set("Location", "/2013-04-01/cidrcollection/"+col.ID)

	return writeXML(c, http.StatusCreated, resp)
}

func (h *Handler) changeCidrCollection(c *echo.Context, path string) error {
	ctx := c.Request().Context()
	collectionID := strings.TrimPrefix(path, route53CidrCollectionPrefix)

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req xmlChangeCidrCollectionRequest
	if len(body) > 0 {
		if err = xml.Unmarshal(body, &req); err != nil {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidInput",
				"failed to parse XML: "+err.Error(),
			)
		}
	}

	changes := make([]CidrCollectionChange, 0, len(req.Changes))
	for _, ch := range req.Changes {
		changes = append(changes, CidrCollectionChange(ch))
	}

	col, err := h.Backend.ChangeCidrCollection(collectionID, changes, req.CollectionVersion)
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ChangeCidrCollection", "id", collectionID)

	return writeXML(c, http.StatusOK, xmlChangeCidrCollectionResponse{
		Xmlns:   route53Namespace,
		ID:      col.ID,
		Version: col.Version,
	})
}

func (h *Handler) listCidrCollections(c *echo.Context) error {
	ctx := c.Request().Context()

	collections, err := h.Backend.ListCidrCollections()
	if err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 ListCidrCollections", "count", len(collections))

	summaries := make([]xmlCidrCollectionSummary, 0, len(collections))
	for _, col := range collections {
		summaries = append(summaries, xmlCidrCollectionSummary{
			ARN:     col.ARN,
			ID:      col.ID,
			Name:    col.Name,
			Version: col.Version,
		})
	}

	return writeXML(c, http.StatusOK, xmlListCidrCollectionsResponse{
		Xmlns:           route53Namespace,
		CidrCollections: summaries,
		IsTruncated:     false,
	})
}

func (h *Handler) deleteCidrCollection(c *echo.Context, path string) error {
	ctx := c.Request().Context()

	id := strings.TrimPrefix(path, route53CidrCollectionPrefix)

	if err := h.Backend.DeleteCidrCollection(id); err != nil {
		return handleBackendError(c, err)
	}

	logger.Load(ctx).DebugContext(ctx, "Route53 DeleteCidrCollection", "id", id)

	return writeXML(c, http.StatusOK, struct {
		XMLName xml.Name `xml:"DeleteCidrCollectionResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}{Xmlns: route53Namespace})
}

// xmlCidrBlockSummary mirrors types.CidrBlockSummary (route53@v1.65.6
// deserializers.go: awsRestxml_deserializeDocumentCidrBlockSummary) — each
// member nests CidrBlock and LocationName, it is never a bare string.
type xmlCidrBlockSummary struct {
	CidrBlock    string `xml:"CidrBlock"`
	LocationName string `xml:"LocationName"`
}

type listCidrBlocksResponse struct {
	XMLName     xml.Name              `xml:"ListCidrBlocksResponse"`
	Xmlns       string                `xml:"xmlns,attr"`
	CidrBlocks  []xmlCidrBlockSummary `xml:"CidrBlocks>member"`
	IsTruncated bool                  `xml:"IsTruncated"`
}

func (h *Handler) listCidrBlocks(c *echo.Context, path string) error {
	// path: /2013-04-01/cidrcollection/{id}/cidrblocks[?location=...]
	trimmed := strings.TrimPrefix(path, route53CidrCollectionPrefix)
	collectionID, _, _ := strings.Cut(trimmed, "/")
	locationName := c.Request().URL.Query().Get("location")

	blocks, err := h.Backend.ListCidrBlocks(collectionID, locationName)
	if err != nil {
		return handleBackendError(c, err)
	}

	summaries := make([]xmlCidrBlockSummary, 0, len(blocks))
	for _, b := range blocks {
		summaries = append(summaries, xmlCidrBlockSummary{CidrBlock: b, LocationName: locationName})
	}

	return writeXML(c, http.StatusOK, listCidrBlocksResponse{
		Xmlns:       route53Namespace,
		CidrBlocks:  summaries,
		IsTruncated: false,
	})
}

// xmlCidrLocationSummary mirrors types.LocationSummary (route53@v1.65.6
// deserializers.go: awsRestxml_deserializeDocumentLocationSummary) — the
// member nests LocationName, it is never a bare string.
type xmlCidrLocationSummary struct {
	LocationName string `xml:"LocationName"`
}

type listCidrLocationsResponse struct {
	XMLName       xml.Name                 `xml:"ListCidrLocationsResponse"`
	Xmlns         string                   `xml:"xmlns,attr"`
	CidrLocations []xmlCidrLocationSummary `xml:"CidrLocations>member"`
	IsTruncated   bool                     `xml:"IsTruncated"`
}

func (h *Handler) listCidrLocations(c *echo.Context, path string) error {
	// path: /2013-04-01/cidrcollection/{id}[/cidrlocations]
	trimmed := strings.TrimPrefix(path, route53CidrCollectionPrefix)
	collectionID, _, _ := strings.Cut(trimmed, "/")

	locations, err := h.Backend.ListCidrLocations(collectionID)
	if err != nil {
		return handleBackendError(c, err)
	}

	summaries := make([]xmlCidrLocationSummary, 0, len(locations))
	for _, l := range locations {
		summaries = append(summaries, xmlCidrLocationSummary{LocationName: l})
	}

	return writeXML(c, http.StatusOK, listCidrLocationsResponse{
		Xmlns:         route53Namespace,
		CidrLocations: summaries,
		IsTruncated:   false,
	})
}
