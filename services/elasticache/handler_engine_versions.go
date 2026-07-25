package elasticache

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

type cacheEngineVersionXML struct {
	Engine                        string `xml:"Engine"`
	EngineVersion                 string `xml:"EngineVersion"`
	CacheParameterGroupFamily     string `xml:"CacheParameterGroupFamily"`
	CacheEngineDescription        string `xml:"CacheEngineDescription"`
	CacheEngineVersionDescription string `xml:"CacheEngineVersionDescription"`
}

func (h *Handler) describeCacheEngineVersions(ctx context.Context, c *echo.Context, form url.Values) error {
	engine := form.Get("Engine")
	family := form.Get("CacheParameterGroupFamily")
	engineVersion := form.Get("EngineVersion")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeCacheEngineVersions(ctx, engine, family, engineVersion, marker, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]cacheEngineVersionXML, 0, len(p.Data))
	for _, v := range p.Data {
		items = append(items, cacheEngineVersionXML(v))
	}

	type cevListXML struct {
		CacheEngineVersion []cacheEngineVersionXML `xml:"CacheEngineVersion"`
	}

	type result struct {
		XMLName             xml.Name   `xml:"DescribeCacheEngineVersionsResponse"`
		Xmlns               string     `xml:"xmlns,attr"`
		Marker              string     `xml:"DescribeCacheEngineVersionsResult>Marker,omitempty"`
		CacheEngineVersions cevListXML `xml:"DescribeCacheEngineVersionsResult>CacheEngineVersions"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:               elasticacheNS,
		Marker:              p.Next,
		CacheEngineVersions: cevListXML{CacheEngineVersion: items},
	})
}
