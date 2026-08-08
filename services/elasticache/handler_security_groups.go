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

type cacheSecurityGroupXML struct {
	ARN                    string `xml:"ARN"`
	CacheSecurityGroupName string `xml:"CacheSecurityGroupName"`
	Description            string `xml:"Description"`
	OwnerID                string `xml:"OwnerId"`
}

func cacheSecurityGroupToXML(sg *CacheSecurityGroup) cacheSecurityGroupXML {
	return cacheSecurityGroupXML{
		ARN:                    sg.ARN,
		CacheSecurityGroupName: sg.Name,
		Description:            sg.Description,
		OwnerID:                sg.OwnerID,
	}
}

func (h *Handler) createCacheSecurityGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	description := form.Get("Description")

	sg, err := h.Backend.CreateCacheSecurityGroup(ctx, name, description)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupAlreadyExists) {
			return xmlError(
				c,
				http.StatusBadRequest,
				"CacheSecurityGroupAlreadyExists",
				"Cache security group already exists",
			)
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	h.applyCreateTimeTags(ctx, form, sg.ARN)

	type result struct {
		XMLName            xml.Name              `xml:"CreateCacheSecurityGroupResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"CreateCacheSecurityGroupResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}

// ----------------------------------------
// AuthorizeCacheSecurityGroupIngress
// ----------------------------------------

func (h *Handler) authorizeCacheSecurityGroupIngress(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	ec2SecurityGroupName := form.Get("EC2SecurityGroupName")
	ec2SecurityGroupOwnerID := form.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.AuthorizeCacheSecurityGroupIngress(ctx, name, ec2SecurityGroupName, ec2SecurityGroupOwnerID)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name              `xml:"AuthorizeCacheSecurityGroupIngressResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"AuthorizeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}

// ----------------------------------------
// CreateGlobalReplicationGroup
// ----------------------------------------

// describeSGsResultXML is the XML envelope for DescribeCacheSecurityGroups responses.
type describeSGsResultXML struct {
	XMLName             xml.Name `xml:"DescribeCacheSecurityGroupsResponse"`
	Xmlns               string   `xml:"xmlns,attr"`
	Marker              string   `xml:"DescribeCacheSecurityGroupsResult>Marker,omitempty"`
	CacheSecurityGroups struct {
		CacheSecurityGroup []cacheSecurityGroupXML `xml:"CacheSecurityGroup"`
	} `xml:"DescribeCacheSecurityGroupsResult>CacheSecurityGroups"`
}

func (h *Handler) deleteCacheSecurityGroup(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")

	err := h.Backend.DeleteCacheSecurityGroup(ctx, name)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName xml.Name `xml:"DeleteCacheSecurityGroupResponse"`
		Xmlns   string   `xml:"xmlns,attr"`
	}

	return xmlResp(c, http.StatusOK, result{Xmlns: elasticacheNS})
}

func (h *Handler) describeCacheSecurityGroups(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")

	p, err := describeListChecked(c, form,
		func(marker string, maxRecords int) (page.Page[CacheSecurityGroup], error) {
			return h.Backend.DescribeCacheSecurityGroups(ctx, name, marker, maxRecords)
		},
		ErrCacheSecurityGroupNotFound, http.StatusNotFound,
		"CacheSecurityGroupNotFound", "Cache security group not found")
	if err != nil {
		return err
	}

	var res describeSGsResultXML
	res.Xmlns = elasticacheNS
	res.Marker = p.Next

	for i := range p.Data {
		res.CacheSecurityGroups.CacheSecurityGroup = append(
			res.CacheSecurityGroups.CacheSecurityGroup,
			cacheSecurityGroupToXML(&p.Data[i]),
		)
	}

	return xmlResp(c, http.StatusOK, res)
}

func (h *Handler) revokeCacheSecurityGroupIngress(ctx context.Context, c *echo.Context, form url.Values) error {
	name := form.Get("CacheSecurityGroupName")
	ec2SecurityGroupName := form.Get("EC2SecurityGroupName")
	ec2SecurityGroupOwnerID := form.Get("EC2SecurityGroupOwnerId")

	sg, err := h.Backend.RevokeCacheSecurityGroupIngress(ctx, name, ec2SecurityGroupName, ec2SecurityGroupOwnerID)
	if err != nil {
		if errors.Is(err, ErrCacheSecurityGroupNotFound) {
			return xmlError(c, http.StatusNotFound, "CacheSecurityGroupNotFound", "Cache security group not found")
		}

		return xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type result struct {
		XMLName            xml.Name              `xml:"RevokeCacheSecurityGroupIngressResponse"`
		Xmlns              string                `xml:"xmlns,attr"`
		CacheSecurityGroup cacheSecurityGroupXML `xml:"RevokeCacheSecurityGroupIngressResult>CacheSecurityGroup"`
	}

	return xmlResp(c, http.StatusOK, result{
		Xmlns:              elasticacheNS,
		CacheSecurityGroup: cacheSecurityGroupToXML(sg),
	})
}
