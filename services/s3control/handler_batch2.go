package s3control

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---- Bucket Replication ----

type replicationConfigurationXML struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Rules   string   `xml:"Rules,omitempty"`
}

type getReplicationResultXML struct {
	XMLName                  xml.Name                    `xml:"GetBucketReplicationResult"`
	ReplicationConfiguration replicationConfigurationXML `xml:"ReplicationConfiguration"`
}

func (h *Handler) handleGetBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	cfg, err := h.Backend.GetBucketReplication(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getReplicationResultXML{
		ReplicationConfiguration: replicationConfigurationXML{Rules: cfg},
	})
}

type putReplicationRequestXML struct {
	XMLName xml.Name `xml:"ReplicationConfiguration"`
	Rules   string   `xml:"Rules,omitempty"`
}

func (h *Handler) handlePutBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	var body putReplicationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.PutBucketReplication(accountID, bucketName, body.Rules); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteBucketReplication(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/replication",
	)

	if err := h.Backend.DeleteBucketReplication(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---- MRAP Routes (submit) ----

type submitMRAPRoutesRequestXML struct {
	XMLName xml.Name `xml:"SubmitMultiRegionAccessPointRoutesRequest"`
	Routes  string   `xml:"Routes,omitempty"`
}

func (h *Handler) handleSubmitMultiRegionAccessPointRoutes(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	mrapName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/routes",
	)

	var body submitMRAPRoutesRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.SubmitMultiRegionAccessPointRoutes(accountID, mrapName, body.Routes); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// ---- Storage Lens Configuration ----

type storageLensConfigurationXML struct {
	XMLName xml.Name `xml:"StorageLensConfiguration"`
	ID      string   `xml:"Id,omitempty"`
	Config  string   `xml:"Config,omitempty"`
}

func (h *Handler) handleGetStorageLensConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix)

	cfg, err := h.Backend.GetStorageLensConfiguration(accountID, configName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name                    `xml:"GetStorageLensConfigurationResult"`
		Config  storageLensConfigurationXML `xml:"StorageLensConfiguration"`
	}{
		Config: storageLensConfigurationXML{ID: configName, Config: cfg},
	})
}

type putStorageLensConfigRequestXML struct {
	XMLName xml.Name `xml:"PutStorageLensConfigurationRequest"`
	Config  string   `xml:"Config,omitempty"`
}

func (h *Handler) handlePutStorageLensConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix)

	var body putStorageLensConfigRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	if err := h.Backend.PutStorageLensConfiguration(accountID, configName, body.Config); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteStorageLensConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix)

	if err := h.Backend.DeleteStorageLensConfiguration(accountID, configName); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---- Storage Lens Configuration Tagging ----

type storageLensTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type storageLensTagsXML struct {
	Tags []storageLensTagXML `xml:"Tag"`
}

type getStorageLensConfigTaggingResultXML struct {
	XMLName xml.Name           `xml:"GetStorageLensConfigurationTaggingResult"`
	Tags    storageLensTagsXML `xml:"Tags"`
}

func (h *Handler) handleGetStorageLensConfigurationTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix),
		"/tagging",
	)

	tags, err := h.Backend.GetStorageLensConfigurationTagging(accountID, configName)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getStorageLensConfigTaggingResultXML{}
	for k, v := range tags {
		resp.Tags.Tags = append(resp.Tags.Tags, storageLensTagXML{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

type putStorageLensConfigTaggingRequestXML struct {
	XMLName xml.Name           `xml:"PutStorageLensConfigurationTaggingRequest"`
	Tags    storageLensTagsXML `xml:"Tags"`
}

func (h *Handler) handlePutStorageLensConfigurationTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix),
		"/tagging",
	)

	var body putStorageLensConfigTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	tags := make(TagSet, len(body.Tags.Tags))
	for _, t := range body.Tags.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutStorageLensConfigurationTagging(accountID, configName, tags); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutStorageLensConfigurationTaggingResult"`
	}{})
}

func (h *Handler) handleDeleteStorageLensConfigurationTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix),
		"/tagging",
	)

	if err := h.Backend.DeleteStorageLensConfigurationTagging(accountID, configName); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---- List Storage Lens Configurations ----

type listStorageLensConfigItemXML struct {
	ID string `xml:"Id"`
}

type listStorageLensConfigurationsResultXML struct {
	XMLName xml.Name                       `xml:"ListStorageLensConfigurationsResult"`
	Configs []listStorageLensConfigItemXML `xml:"StorageLensConfigurationList>StorageLensConfiguration"`
}

func (h *Handler) handleListStorageLensConfigurations(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	names := h.Backend.ListStorageLensConfigurations(accountID)
	items := make([]listStorageLensConfigItemXML, 0, len(names))

	for _, n := range names {
		items = append(items, listStorageLensConfigItemXML{ID: n})
	}

	return writeXML(c, listStorageLensConfigurationsResultXML{Configs: items})
}

// ---- Storage Lens Groups ----

// slgFilterWrapXML captures the raw inner XML of a StorageLensGroup Filter element.
type slgFilterWrapXML struct {
	Raw string `xml:",innerxml"`
}

type storageLensGroupItemXML struct {
	Name                string            `xml:"Name"`
	StorageLensGroupArn string            `xml:"StorageLensGroupArn,omitempty"`
	CreatedAt           string            `xml:"CreatedAt,omitempty"`
	Filter              *slgFilterWrapXML `xml:"Filter,omitempty"`
}

// buildSLGItem converts a StorageLensGroup backend struct to the XML response item.
func buildSLGItem(grp *StorageLensGroup) storageLensGroupItemXML {
	item := storageLensGroupItemXML{
		Name:                grp.Name,
		StorageLensGroupArn: grp.StorageLensGroupArn,
		CreatedAt:           grp.CreatedAt,
	}

	if grp.Filter != "" {
		item.Filter = &slgFilterWrapXML{Raw: grp.Filter}
	}

	return item
}

type getStorageLensGroupResultXML struct {
	XMLName          xml.Name                `xml:"GetStorageLensGroupResult"`
	StorageLensGroup storageLensGroupItemXML `xml:"StorageLensGroup"`
}

func (h *Handler) handleGetStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix)

	grp, err := h.Backend.GetStorageLensGroup(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getStorageLensGroupResultXML{
		StorageLensGroup: buildSLGItem(grp),
	})
}

type updateStorageLensGroupRequestXML struct {
	XMLName xml.Name         `xml:"StorageLensGroup"`
	Name    string           `xml:"Name"`
	Filter  slgFilterWrapXML `xml:"Filter"`
}

func (h *Handler) handleUpdateStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix)

	var body updateStorageLensGroupRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	_, err := h.Backend.UpdateStorageLensGroup(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	if body.Filter.Raw != "" {
		_ = h.Backend.UpdateStorageLensGroupFilter(accountID, name, body.Filter.Raw)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensGroupPrefix)

	if err := h.Backend.DeleteStorageLensGroup(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type listStorageLensGroupsResultXML struct {
	XMLName xml.Name                  `xml:"ListStorageLensGroupsResult"`
	Groups  []storageLensGroupItemXML `xml:"StorageLensGroupList>StorageLensGroup"`
}

func (h *Handler) handleListStorageLensGroups(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	groups := h.Backend.ListStorageLensGroups(accountID)
	items := make([]storageLensGroupItemXML, 0, len(groups))

	for _, g := range groups {
		items = append(items, buildSLGItem(g))
	}

	return writeXML(c, listStorageLensGroupsResultXML{Groups: items})
}

// ---- Resource Tags ----

type resourceTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type listTagsForResourceResultXML struct {
	XMLName xml.Name         `xml:"ListTagsForResourceResult"`
	Tags    []resourceTagXML `xml:"Tags>Tag"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)

	tags := h.Backend.ListTagsForResource(arn)
	items := make([]resourceTagXML, 0, len(tags))

	for k, v := range tags {
		items = append(items, resourceTagXML{Key: k, Value: v})
	}

	return writeXML(c, listTagsForResourceResultXML{Tags: items})
}

type tagResourceRequestXML struct {
	XMLName xml.Name         `xml:"TagResourceRequest"`
	Tags    []resourceTagXML `xml:"Tags>Tag"`
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)

	var body tagResourceRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	tags := make(map[string]string, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	h.Backend.TagResource(arn, tags)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"TagResourceResult"`
	}{})
}

type untagResourceRequestXML struct {
	XMLName xml.Name `xml:"UntagResourceRequest"`
	TagKeys []string `xml:"TagKeys>TagKey"`
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	arn := strings.TrimPrefix(c.Request().URL.Path, pathTagsPrefix)

	var body untagResourceRequestXML
	if err := decodeXML(c, &body); err != nil {
		return c.String(http.StatusBadRequest, "invalid request body")
	}

	h.Backend.UntagResource(arn, body.TagKeys)

	return c.NoContent(http.StatusNoContent)
}
