package elasticache

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

type updateActionResultXML struct {
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ServiceUpdateName  string `xml:"ServiceUpdateName"`
	UpdateActionStatus string `xml:"UpdateActionStatus"`
}

type processedUpdateActionsXML struct {
	ProcessedUpdateAction []updateActionResultXML `xml:"ProcessedUpdateAction"`
}

type unprocessedUpdateActionsXML struct {
	UnprocessedUpdateAction []updateActionResultXML `xml:"UnprocessedUpdateAction"`
}

// toBatchUpdateActionXMLLists converts a BatchUpdateResult to the XML list types used in responses.
func toBatchUpdateActionXMLLists(result *BatchUpdateResult) (processedUpdateActionsXML, unprocessedUpdateActionsXML) {
	processed := make([]updateActionResultXML, 0, len(result.ProcessedUpdateActions))
	for _, a := range result.ProcessedUpdateActions {
		processed = append(processed, updateActionResultXML(a))
	}

	unprocessed := make([]updateActionResultXML, 0, len(result.UnprocessedUpdateActions))
	for _, a := range result.UnprocessedUpdateActions {
		unprocessed = append(unprocessed, updateActionResultXML(a))
	}

	return processedUpdateActionsXML{ProcessedUpdateAction: processed},
		unprocessedUpdateActionsXML{UnprocessedUpdateAction: unprocessed}
}

// ----------------------------------------
// BatchApplyUpdateAction
// ----------------------------------------

func (h *Handler) batchApplyUpdateAction(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	replicationGroupIDs := parseRepeatedField(form, "ReplicationGroupIds.member")
	cacheClusterIDs := parseRepeatedField(form, "CacheClusterIds.member")

	result, err := h.Backend.BatchApplyUpdateAction(ctx, replicationGroupIDs, cacheClusterIDs, serviceUpdateName)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	processed, unprocessed := toBatchUpdateActionXMLLists(result)

	type resp struct {
		XMLName                  xml.Name                    `xml:"BatchApplyUpdateActionResponse"`
		Xmlns                    string                      `xml:"xmlns,attr"`
		ProcessedUpdateActions   processedUpdateActionsXML   `xml:"BatchApplyUpdateActionResult>ProcessedUpdateActions"`
		UnprocessedUpdateActions unprocessedUpdateActionsXML `xml:"BatchApplyUpdateActionResult>UnprocessedUpdateActions"`
	}

	return xmlResp(c, http.StatusOK, resp{
		Xmlns:                    elasticacheNS,
		ProcessedUpdateActions:   processed,
		UnprocessedUpdateActions: unprocessed,
	})
}

// ----------------------------------------
// BatchStopUpdateAction
// ----------------------------------------

func (h *Handler) batchStopUpdateAction(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	replicationGroupIDs := parseRepeatedField(form, "ReplicationGroupIds.member")
	cacheClusterIDs := parseRepeatedField(form, "CacheClusterIds.member")

	result, err := h.Backend.BatchStopUpdateAction(ctx, replicationGroupIDs, cacheClusterIDs, serviceUpdateName)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	processed, unprocessed := toBatchUpdateActionXMLLists(result)

	type resp struct {
		XMLName                  xml.Name                    `xml:"BatchStopUpdateActionResponse"`
		Xmlns                    string                      `xml:"xmlns,attr"`
		ProcessedUpdateActions   processedUpdateActionsXML   `xml:"BatchStopUpdateActionResult>ProcessedUpdateActions"`
		UnprocessedUpdateActions unprocessedUpdateActionsXML `xml:"BatchStopUpdateActionResult>UnprocessedUpdateActions"`
	}

	return xmlResp(c, http.StatusOK, resp{
		Xmlns:                    elasticacheNS,
		ProcessedUpdateActions:   processed,
		UnprocessedUpdateActions: unprocessed,
	})
}

// ----------------------------------------
// CompleteMigration
// ----------------------------------------

type serviceUpdateXML struct {
	ServiceUpdateName   string `xml:"ServiceUpdateName"`
	ServiceUpdateStatus string `xml:"ServiceUpdateStatus"`
}

type updateActionXML struct {
	ReplicationGroupID string `xml:"ReplicationGroupId,omitempty"`
	CacheClusterID     string `xml:"CacheClusterId,omitempty"`
	ServiceUpdateName  string `xml:"ServiceUpdateName"`
	UpdateActionStatus string `xml:"UpdateActionStatus"`
}

func (h *Handler) describeServiceUpdates(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}
	statusList := parseRepeatedField(form, "ServiceUpdateStatus.member")

	p, err := h.Backend.DescribeServiceUpdates(ctx, serviceUpdateName, marker, maxRecords, statusList)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]serviceUpdateXML, 0, len(p.Data))
	for _, su := range p.Data {
		items = append(items, serviceUpdateXML{
			ServiceUpdateName:   su.ServiceUpdateName,
			ServiceUpdateStatus: su.Status,
		})
	}

	type suListXML struct {
		ServiceUpdate []serviceUpdateXML `xml:"ServiceUpdate"`
	}

	type result struct {
		XMLName        xml.Name  `xml:"DescribeServiceUpdatesResponse"`
		Xmlns          string    `xml:"xmlns,attr"`
		Marker         string    `xml:"DescribeServiceUpdatesResult>Marker,omitempty"`
		ServiceUpdates suListXML `xml:"DescribeServiceUpdatesResult>ServiceUpdates"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:          elasticacheNS,
		Marker:         p.Next,
		ServiceUpdates: suListXML{ServiceUpdate: items},
	})
}

func (h *Handler) describeUpdateActions(ctx context.Context, c *echo.Context, form url.Values) error {
	serviceUpdateName := form.Get("ServiceUpdateName")
	marker, maxRecords, err := parsePaginationChecked(c, form)
	if err != nil {
		return err
	}
	cacheClusterIDs := parseRepeatedField(form, "CacheClusterIds.member")
	replicationGroupIDs := parseRepeatedField(form, "ReplicationGroupIds.member")
	updateActionStatus := parseRepeatedField(form, "UpdateActionStatus.member")

	p, err := h.Backend.DescribeUpdateActions(
		ctx, serviceUpdateName, marker, maxRecords, cacheClusterIDs, replicationGroupIDs, updateActionStatus,
	)
	if err != nil {
		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	items := make([]updateActionXML, 0, len(p.Data))
	for _, ua := range p.Data {
		items = append(items, updateActionXML(ua))
	}

	type uaListXML struct {
		UpdateAction []updateActionXML `xml:"UpdateAction"`
	}

	type result struct {
		XMLName       xml.Name  `xml:"DescribeUpdateActionsResponse"`
		Xmlns         string    `xml:"xmlns,attr"`
		Marker        string    `xml:"DescribeUpdateActionsResult>Marker,omitempty"`
		UpdateActions uaListXML `xml:"DescribeUpdateActionsResult>UpdateActions"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:         elasticacheNS,
		Marker:        p.Next,
		UpdateActions: uaListXML{UpdateAction: items},
	})
}
