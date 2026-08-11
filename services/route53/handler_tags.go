package route53

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type r53Tag struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

func (h *Handler) deleteTagsForResource(_ string) {
	// Let backend handle deletion on resource deletion
}

func (h *Handler) routeTags(c *echo.Context, path, method string) error {
	// Real Route 53 REST URIs are:
	//   GET/POST /2013-04-01/tags/{ResourceType}/{ResourceId}  → ListTagsForResource / ChangeTagsForResource
	//   POST     /2013-04-01/tags/{ResourceType}               → ListTagsForResources (batch)
	// i.e. the batch op carries only a ResourceType segment and no ResourceId.
	// (This function is only reached for paths with the route53TagsPrefix, so
	// rest is always non-empty here.)
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	if method == http.MethodPost && !strings.Contains(rest, "/") {
		return h.listTagsForResources(c, rest)
	}

	switch method {
	case http.MethodGet:
		return h.listTagsForResource(c, path)
	case http.MethodPost:
		return h.changeTagsForResource(c)
	default:
		return xmlError(c, http.StatusNotFound, "NoSuchOperation",
			"unsupported method on tags")
	}
}

func (h *Handler) listTagsForResource(c *echo.Context, path string) error {
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split into type + id
	resourceType := ""
	resourceID := ""

	if len(parts) >= 1 {
		resourceType = parts[0]
	}

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	tags, err := h.Backend.ListTagsForResource(resourceType, resourceID)
	if err != nil {
		return handleBackendError(c, err)
	}

	tagList := make([]r53Tag, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, r53Tag{Key: k, Value: v})
	}

	type resourceTagSet struct {
		ResourceType string   `xml:"ResourceType"`
		ResourceID   string   `xml:"ResourceId"`
		Tags         []r53Tag `xml:"Tags>Tag"`
	}
	type tagsResp struct {
		XMLName        xml.Name       `xml:"ListTagsForResourceResponse"`
		Xmlns          string         `xml:"xmlns,attr"`
		ResourceTagSet resourceTagSet `xml:"ResourceTagSet"`
	}

	resp := tagsResp{Xmlns: route53Namespace}
	resp.ResourceTagSet.ResourceType = resourceType
	resp.ResourceTagSet.ResourceID = resourceID
	resp.ResourceTagSet.Tags = tagList

	return writeXML(c, http.StatusOK, resp)
}

func (h *Handler) changeTagsForResource(c *echo.Context) error {
	path := c.Request().URL.Path
	rest := strings.TrimPrefix(path, route53TagsPrefix)
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // path has two segments: type + id
	resourceType := ""
	resourceID := ""

	if len(parts) >= 1 {
		resourceType = parts[0]
	}

	if len(parts) >= 2 { //nolint:mnd // path has two segments: type + id
		resourceID = parts[1]
	}

	if err := h.applyTagChanges(resourceType, resourceID, c.Request()); err != nil {
		return handleBackendError(c, err)
	}

	type changeTagsResp struct {
		XMLName xml.Name `xml:"ChangeTagsForResourceResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return writeXML(c, http.StatusOK, changeTagsResp{Xmlns: route53Namespace})
}

type applyTagChangesInput struct {
	AddTags       []svcTags.KV `xml:"AddTags>Tag"`
	RemoveTagKeys []string     `xml:"RemoveTagKeys>Key"`
}

// applyTagChanges reads a ChangeTagsForResource XML body and applies the add/remove
// operations via a single backend call. Routing through one ChangeTagsForResource call
// (even when the request carries no tag mutations) ensures a nonexistent or
// wrong-resourceType target always surfaces the AWS NoSuchHostedZone/NoSuchHealthCheck
// error instead of silently no-oping.
func (h *Handler) applyTagChanges(resourceType, resourceID string, r *http.Request) error {
	body, err := httputils.ReadBody(r)
	if err != nil {
		return fmt.Errorf("%w: failed to read request body: %s", ErrInvalidInput, err.Error())
	}

	var req applyTagChangesInput

	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return fmt.Errorf("%w: failed to parse XML: %s", ErrInvalidInput, xmlErr.Error())
		}
	}

	var addTags map[string]string
	if len(req.AddTags) > 0 {
		addTags = make(map[string]string, len(req.AddTags))
		for _, t := range req.AddTags {
			addTags[t.Key] = t.Value
		}
	}

	return h.Backend.ChangeTagsForResource(resourceType, resourceID, addTags, req.RemoveTagKeys)
}

// listTagsForResources is a batch version of listTagsForResource.
type listTagsForResourcesResponse struct {
	XMLName         xml.Name            `xml:"ListTagsForResourcesResponse"`
	Xmlns           string              `xml:"xmlns,attr"`
	ResourceTagSets []xmlResourceTagSet `xml:"ResourceTagSets>ResourceTagSet"`
}

type xmlResourceTagSet struct {
	ResourceType string   `xml:"ResourceType"`
	ResourceID   string   `xml:"ResourceId"`
	Tags         []r53Tag `xml:"Tags>Tag,omitempty"`
}

// listTagsReq's ResourceType is unused: real ListTagsForResourcesInput binds
// ResourceType as a URI path segment (httpLabel), not a body field
// (route53@v1.65.6 serializers.go: awsRestxml_serializeOpHttpBindingsListTagsForResourcesInput
// sets it via encoder.SetURI, while awsRestxml_serializeOpDocumentListTagsForResourcesInput
// only ever writes ResourceIds) -- resourceType comes from the path instead,
// see routeTags's rest segment.
type listTagsReq struct {
	XMLName     xml.Name `xml:"ListTagsForResourcesRequest"`
	ResourceIDs []string `xml:"ResourceIds>ResourceId"`
}

func (h *Handler) listTagsForResources(c *echo.Context, resourceType string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to read request body")
	}

	var req listTagsReq
	if err = xml.Unmarshal(body, &req); err != nil {
		return xmlError(c, http.StatusBadRequest, "InvalidInput", "failed to parse XML: "+err.Error())
	}

	tagsMap, err := h.Backend.ListTagsForResources(resourceType, req.ResourceIDs)
	if err != nil {
		return handleBackendError(c, err)
	}

	var resourceTagSets []xmlResourceTagSet
	for _, id := range req.ResourceIDs {
		tags := tagsMap[id]
		var tagList []r53Tag
		for k, v := range tags {
			tagList = append(tagList, r53Tag{Key: k, Value: v})
		}
		if len(tagList) == 0 {
			tagList = nil
		}
		resourceTagSets = append(resourceTagSets, xmlResourceTagSet{
			ResourceType: resourceType,
			ResourceID:   id,
			Tags:         tagList,
		})
	}

	return writeXML(c, http.StatusOK, listTagsForResourcesResponse{
		Xmlns:           route53Namespace,
		ResourceTagSets: resourceTagSets,
	})
}
