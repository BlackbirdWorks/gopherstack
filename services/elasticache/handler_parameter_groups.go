package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/labstack/echo/v5"
)

// cacheParameterGroupXML is the XML representation of a cache parameter group.
type cacheParameterGroupXML struct {
	ARN                       string `xml:"ARN"`
	CacheParameterGroupFamily string `xml:"CacheParameterGroupFamily"`
	CacheParameterGroupName   string `xml:"CacheParameterGroupName"`
	Description               string `xml:"Description"`
	IsGlobal                  bool   `xml:"IsGlobal"`
}

func paramGroupToXML(pg *CacheParameterGroup) cacheParameterGroupXML {
	return cacheParameterGroupXML{
		ARN:                       pg.ARN,
		CacheParameterGroupFamily: pg.Family,
		CacheParameterGroupName:   pg.Name,
		Description:               pg.Description,
		IsGlobal:                  pg.IsGlobal,
	}
}

func (h *Handler) createCacheParameterGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	family := form.Get("CacheParameterGroupFamily")
	desc := form.Get("Description")

	pg, err := h.Backend.CreateParameterGroup(ctx, name, family, desc)
	if err != nil {
		if errors.Is(err, ErrParameterGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheParameterGroupAlreadyExists",
				"Cache parameter group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	if initialTags := parseFormTags(form); len(initialTags) > 0 {
		_ = h.Backend.AddTagsToResource(ctx, pg.ARN, initialTags)
	}

	type result struct {
		XMLName             xml.Name               `xml:"CreateCacheParameterGroupResponse"`
		Xmlns               string                 `xml:"xmlns,attr"`
		CacheParameterGroup cacheParameterGroupXML `xml:"CreateCacheParameterGroupResult>CacheParameterGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:               elasticacheNS,
		CacheParameterGroup: paramGroupToXML(pg),
	})
}

func (h *Handler) deleteCacheParameterGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")

	if err := h.Backend.DeleteParameterGroup(ctx, name); err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be deleted",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName   xml.Name `xml:"DeleteCacheParameterGroupResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS, RequestID: newRequestID()})
}

// describeCacheParameterGroupsResultXML is the XML result for DescribeCacheParameterGroups.
type describeCacheParameterGroupsResultXML struct {
	XMLName              xml.Name                    `xml:"DescribeCacheParameterGroupsResponse"`
	Xmlns                string                      `xml:"xmlns,attr"`
	Marker               string                      `xml:"DescribeCacheParameterGroupsResult>Marker,omitempty"`
	CacheParameterGroups cacheParameterGroupsListXML `xml:"DescribeCacheParameterGroupsResult>CacheParameterGroups"`
}

// cacheParameterGroupsListXML holds the list of cache parameter groups.
type cacheParameterGroupsListXML struct {
	CacheParameterGroup []cacheParameterGroupXML `xml:"CacheParameterGroup"`
}

func (h *Handler) describeCacheParameterGroups(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")

	p, err := describeListChecked(c, form,
		func(marker string, maxRecords int) (page.Page[CacheParameterGroup], error) {
			return h.Backend.DescribeParameterGroups(ctx, name, marker, maxRecords)
		},
		ErrParameterGroupNotFound, http.StatusNotFound,
		"CacheParameterGroupNotFound", "Cache parameter group not found")
	if err != nil {
		return err
	}

	items := make([]cacheParameterGroupXML, 0, len(p.Data))
	for i := range p.Data {
		items = append(items, paramGroupToXML(&p.Data[i]))
	}

	return xmlResp(c, http.StatusOK, describeCacheParameterGroupsResultXML{
		Xmlns:                elasticacheNS,
		Marker:               p.Next,
		CacheParameterGroups: cacheParameterGroupsListXML{CacheParameterGroup: items},
	})
}

func (h *Handler) modifyCacheParameterGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")

	params := make(map[string]string)

	for i := 1; ; i++ {
		pname := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterName", i))
		if pname == "" {
			break
		}
		pval := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterValue", i))
		params[pname] = pval
	}

	pg, err := h.Backend.ModifyParameterGroup(ctx, name, params)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be modified",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name `xml:"ModifyCacheParameterGroupResponse"`
		Xmlns                   string   `xml:"xmlns,attr"`
		CacheParameterGroupName string   `xml:"ModifyCacheParameterGroupResult>CacheParameterGroupName"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		CacheParameterGroupName: pg.Name,
	})
}

func (h *Handler) resetCacheParameterGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	resetAll := form.Get("ResetAllParameters") == "true"

	var paramNames []string
	if !resetAll {
		for i := 1; ; i++ {
			pname := form.Get(fmt.Sprintf("ParameterNameValues.ParameterNameValue.%d.ParameterName", i))
			if pname == "" {
				break
			}
			paramNames = append(paramNames, pname)
		}
	}

	pg, err := h.Backend.ResetParameterGroup(ctx, name, paramNames, resetAll)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}
		if errors.Is(err, ErrParameterGroupDefaultNotModifiable) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"InvalidCacheParameterGroupState",
				"The default parameter group cannot be reset",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                 xml.Name `xml:"ResetCacheParameterGroupResponse"`
		Xmlns                   string   `xml:"xmlns,attr"`
		CacheParameterGroupName string   `xml:"ResetCacheParameterGroupResult>CacheParameterGroupName"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                   elasticacheNS,
		CacheParameterGroupName: pg.Name,
	})
}

// parameterXML is the XML representation of a single cache parameter.
type parameterXML struct {
	ParameterName  string `xml:"ParameterName"`
	ParameterValue string `xml:"ParameterValue"`
	DataType       string `xml:"DataType"`
	IsModifiable   bool   `xml:"IsModifiable"`
}

// describeCacheParametersResultXML is the XML result for DescribeCacheParameters.
type describeCacheParametersResultXML struct {
	XMLName    xml.Name          `xml:"DescribeCacheParametersResponse"`
	Xmlns      string            `xml:"xmlns,attr"`
	Marker     string            `xml:"DescribeCacheParametersResult>Marker,omitempty"`
	Parameters parametersListXML `xml:"DescribeCacheParametersResult>Parameters"`
}

// parametersListXML holds the list of parameters.
type parametersListXML struct {
	Parameter []parameterXML `xml:"Parameter"`
}

// buildParameterItems converts CacheParameter backend items to XML.
func buildParameterItems(params []CacheParameter) []parameterXML {
	items := make([]parameterXML, 0, len(params))
	for _, param := range params {
		items = append(items, parameterXML{
			ParameterName:  param.Name,
			ParameterValue: param.Value,
			DataType:       param.DataType,
			IsModifiable:   param.IsModifiable,
		})
	}

	return items
}

func (h *Handler) describeCacheParameters(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheParameterGroupName")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeParameters(ctx, name, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrParameterGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheParameterGroupNotFound", "Cache parameter group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	return xmlResp(c, http.StatusOK, describeCacheParametersResultXML{
		Xmlns:      elasticacheNS,
		Marker:     p.Next,
		Parameters: parametersListXML{Parameter: buildParameterItems(p.Data)},
	})
}

func (h *Handler) describeEngineDefaultParameters(ctx context.Context, c *echo.Context, form url.Values) error {
	family := form.Get("CacheParameterGroupFamily")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}

	p, err := h.Backend.DescribeEngineDefaultParameters(ctx, family, marker, maxRecords)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type paramsListXML struct {
		Parameter []parameterXML `xml:"Parameter"`
	}

	type result struct {
		XMLName xml.Name      `xml:"DescribeEngineDefaultParametersResponse"`
		Xmlns   string        `xml:"xmlns,attr"`
		Family  string        `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>CacheParameterGroupFamily"`
		Marker  string        `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>Marker,omitempty"`
		Params  paramsListXML `xml:"DescribeEngineDefaultParametersResult>EngineDefaults>Parameters"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:  elasticacheNS,
		Family: family,
		Marker: p.Next,
		Params: paramsListXML{Parameter: buildParameterItems(p.Data)},
	})
}
