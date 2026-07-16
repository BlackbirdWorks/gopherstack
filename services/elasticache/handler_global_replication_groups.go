package elasticache

import (
	"context"
	"encoding/xml"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

type globalReplicationGroupXML struct {
	GlobalReplicationGroupID          string `xml:"GlobalReplicationGroupId"`
	GlobalReplicationGroupDescription string `xml:"GlobalReplicationGroupDescription,omitempty"`
	Status                            string `xml:"Status"`
	ARN                               string `xml:"ARN"`
	Engine                            string `xml:"Engine,omitempty"`
	EngineVersion                     string `xml:"EngineVersion,omitempty"`
	NodeGroupCount                    int32  `xml:"NodeGroupCount,omitempty"`
}

func globalRGToXML(grg *GlobalReplicationGroup) globalReplicationGroupXML {
	return globalReplicationGroupXML{
		GlobalReplicationGroupID:          grg.GlobalReplicationGroupID,
		GlobalReplicationGroupDescription: grg.Description,
		Status:                            grg.Status,
		ARN:                               grg.ARN,
		Engine:                            grg.Engine,
		EngineVersion:                     grg.EngineVersion,
		NodeGroupCount:                    grg.NodeGroupCount,
	}
}

func (h *Handler) createGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	suffix := form.Get("GlobalReplicationGroupIdSuffix")
	description := form.Get("GlobalReplicationGroupDescription")
	primaryReplicationGroupID := form.Get("PrimaryReplicationGroupId")

	grg, err := h.Backend.CreateGlobalReplicationGroup(ctx, suffix, description, primaryReplicationGroupID)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"GlobalReplicationGroupAlreadyExistsFault",
				"Global replication group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"CreateGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"CreateGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

// ----------------------------------------
// CreateServerlessCache
// ----------------------------------------

// describeGlobalRGsResultXML is the XML envelope for DescribeGlobalReplicationGroups responses.
type describeGlobalRGsResultXML struct {
	XMLName                 xml.Name `xml:"DescribeGlobalReplicationGroupsResponse"`
	Xmlns                   string   `xml:"xmlns,attr"`
	Marker                  string   `xml:"DescribeGlobalReplicationGroupsResult>Marker,omitempty"`
	GlobalReplicationGroups struct {
		GlobalReplicationGroup []globalReplicationGroupXML `xml:"GlobalReplicationGroup"`
	} `xml:"DescribeGlobalReplicationGroupsResult>GlobalReplicationGroups"`
}

func (h *Handler) deleteGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	retainPrimary := strings.EqualFold(form.Get("RetainPrimaryReplicationGroup"), "true")

	grg, err := h.Backend.DeleteGlobalReplicationGroup(ctx, id, retainPrimary)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"DeleteGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"DeleteGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) describeGlobalReplicationGroups(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	marker, maxRecords := parsePagination(form)

	p, err := h.Backend.DescribeGlobalReplicationGroups(ctx, id, marker, maxRecords)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	var res describeGlobalRGsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.GlobalReplicationGroups.GlobalReplicationGroup = append(
			res.GlobalReplicationGroups.GlobalReplicationGroup,
			globalRGToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) disassociateGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	replicationGroupID := form.Get("ReplicationGroupId")
	replicationGroupRegion := form.Get("ReplicationGroupRegion")

	grg, err := h.Backend.DisassociateGlobalReplicationGroup(ctx, id, replicationGroupID, replicationGroupRegion)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"DisassociateGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"DisassociateGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) failoverGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	primaryRegion := form.Get("PrimaryRegion")
	primaryReplicationGroupID := form.Get("PrimaryReplicationGroupId")

	grg, err := h.Backend.FailoverGlobalReplicationGroup(ctx, id, primaryRegion, primaryReplicationGroupID)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"FailoverGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"FailoverGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) increaseNodeGroupsInGlobalReplicationGroup(
	ctx context.Context,
	c *echo.Context,
	form url.Values,
) error {
	id := form.Get("GlobalReplicationGroupId")
	nodeGroupCount, _ := strconv.ParseInt(form.Get("NodeGroupCount"), 10, 32)

	grg, err := h.Backend.IncreaseNodeGroupsInGlobalReplicationGroup(ctx, id, int32(nodeGroupCount))
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"IncreaseNodeGroupsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"IncreaseNodeGroupsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) decreaseNodeGroupsInGlobalReplicationGroup(
	ctx context.Context,
	c *echo.Context,
	form url.Values,
) error {
	id := form.Get("GlobalReplicationGroupId")
	nodeGroupCount, _ := strconv.ParseInt(form.Get("NodeGroupCount"), 10, 32)

	grg, err := h.Backend.DecreaseNodeGroupsInGlobalReplicationGroup(ctx, id, int32(nodeGroupCount))
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"DecreaseNodeGroupsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"DecreaseNodeGroupsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}

func (h *Handler) modifyGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")
	description := form.Get("GlobalReplicationGroupDescription")
	engineVersion := form.Get("EngineVersion")
	automaticFailoverEnabled := strings.EqualFold(form.Get("AutomaticFailoverEnabled"), "true")

	grg, err := h.Backend.ModifyGlobalReplicationGroup(ctx, id, description, engineVersion, automaticFailoverEnabled)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName                xml.Name                  `xml:"ModifyGlobalReplicationGroupResponse"`
		Xmlns                  string                    `xml:"xmlns,attr"`
		GlobalReplicationGroup globalReplicationGroupXML `xml:"ModifyGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:                  elasticacheNS,
		GlobalReplicationGroup: globalRGToXML(grg),
	})
}

func (h *Handler) rebalanceSlotsInGlobalReplicationGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	id := form.Get("GlobalReplicationGroupId")

	grg, err := h.Backend.RebalanceSlotsInGlobalReplicationGroup(ctx, id)
	if err != nil {
		if errors.Is(err, ErrGlobalReplicationGroupNotFound) {
			return xmlError(
				c,
				http.StatusNotFound,
				"GlobalReplicationGroupNotFoundFault",
				"Global replication group not found",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name                  `xml:"RebalanceSlotsInGlobalReplicationGroupResponse"`
		Xmlns   string                    `xml:"xmlns,attr"`
		GRG     globalReplicationGroupXML `xml:"RebalanceSlotsInGlobalReplicationGroupResult>GlobalReplicationGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns: elasticacheNS,
		GRG:   globalRGToXML(grg),
	})
}
