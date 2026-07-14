package s3control

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	pathStorageLensGroup       = "/v20180820/storagelensgroup"
	pathStorageLensPrefix      = "/v20180820/storagelens/"
	pathStorageLensList        = "/v20180820/storagelens"
	pathStorageLensGroupPrefix = "/v20180820/storagelensgroup/"
)

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

// --- CreateStorageLensGroup handler ---

type createStorageLensGroupStorageLensGroupXML struct {
	Name   string              `xml:"Name"`
	Filter createJobXMLCapture `xml:"Filter"`
}

type createStorageLensGroupRequestXML struct {
	XMLName          xml.Name                                  `xml:"CreateStorageLensGroupRequest"`
	StorageLensGroup createStorageLensGroupStorageLensGroupXML `xml:"StorageLensGroup"`
}

func (h *Handler) handleCreateStorageLensGroup(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body createStorageLensGroupRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	grp := h.Backend.CreateStorageLensGroup(accountID, body.StorageLensGroup.Name)

	if body.StorageLensGroup.Filter.Raw != "" {
		_ = h.Backend.UpdateStorageLensGroupFilter(accountID, grp.Name, body.StorageLensGroup.Filter.Raw)
	}

	return c.NoContent(http.StatusCreated)
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
	XMLName xml.Name
	Config  string `xml:"Config,omitempty"`
}

func (h *Handler) handlePutStorageLensConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	configName := strings.TrimPrefix(c.Request().URL.Path, pathStorageLensPrefix)

	var body putStorageLensConfigRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
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
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
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
	XMLName   xml.Name                       `xml:"ListStorageLensConfigurationsResult"`
	NextToken string                         `xml:"NextToken,omitempty"`
	Configs   []listStorageLensConfigItemXML `xml:"StorageLensConfigurationList>StorageLensConfiguration"`
}

func (h *Handler) handleListStorageLensConfigurations(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	nextToken := c.Request().URL.Query().Get("nextToken")

	names := h.Backend.ListStorageLensConfigurations(accountID)
	items := make([]listStorageLensConfigItemXML, 0, len(names))

	for _, n := range names {
		items = append(items, listStorageLensConfigItemXML{ID: n})
	}

	page, tok := s3cPaginate(items, nextToken, 0)

	return writeXML(c, listStorageLensConfigurationsResultXML{Configs: page, NextToken: tok})
}

// ---- Storage Lens Groups ----

// slgFilterWrapXML captures the raw inner XML of a StorageLensGroup Filter element.
type slgFilterWrapXML struct {
	Raw string `xml:",innerxml"`
}

type storageLensGroupItemXML struct {
	Filter              *slgFilterWrapXML `xml:"Filter,omitempty"`
	Name                string            `xml:"Name"`
	StorageLensGroupArn string            `xml:"StorageLensGroupArn,omitempty"`
	CreatedAt           string            `xml:"CreatedAt,omitempty"`
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
	StorageLensGroup storageLensGroupItemXML `xml:"StorageLensGroup"`
	XMLName          xml.Name                `xml:"GetStorageLensGroupResult"`
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
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
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
	XMLName   xml.Name                  `xml:"ListStorageLensGroupsResult"`
	NextToken string                    `xml:"NextToken,omitempty"`
	Groups    []storageLensGroupItemXML `xml:"StorageLensGroupList>StorageLensGroup"`
}

func (h *Handler) handleListStorageLensGroups(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	nextToken := c.Request().URL.Query().Get("nextToken")

	groups := h.Backend.ListStorageLensGroups(accountID)
	items := make([]storageLensGroupItemXML, 0, len(groups))

	for _, g := range groups {
		items = append(items, buildSLGItem(g))
	}

	page, tok := s3cPaginate(items, nextToken, 0)

	return writeXML(c, listStorageLensGroupsResultXML{Groups: page, NextToken: tok})
}
