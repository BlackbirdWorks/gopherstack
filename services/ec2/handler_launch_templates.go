package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

func (h *Handler) handleDeleteLaunchTemplate(vals url.Values, reqID string) (any, error) {
	lt, err := h.Backend.DeleteLaunchTemplate(vals.Get("LaunchTemplateId"))
	if err != nil {
		return nil, err
	}

	return &deleteLaunchTemplateResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		LaunchTemplate: launchTemplateItem{
			ID:                   lt.ID,
			Name:                 lt.Name,
			CreateTime:           lt.CreateTime.Format(time.RFC3339),
			CreatedBy:            lt.CreatedBy,
			DefaultVersionNumber: lt.DefaultVersionNumber,
			LatestVersionNumber:  lt.LatestVersionNumber,
		},
	}, nil
}

func (h *Handler) handleDescribeLaunchTemplateVersions(vals url.Values, reqID string) (any, error) {
	versions, err := h.Backend.DescribeLaunchTemplateVersions(vals.Get("LaunchTemplateId"))
	if err != nil {
		return nil, err
	}

	items := make([]launchTemplateVersionItem, 0, len(versions))
	for _, lt := range versions {
		item := launchTemplateVersionItem{
			LaunchTemplateID:   lt.ID,
			LaunchTemplateName: lt.Name,
			CreatedBy:          lt.CreatedBy,
			CreateTime:         lt.CreateTime.UTC().Format("2006-01-02T15:04:05.000Z"),
			VersionNumber:      lt.LatestVersionNumber,
			DefaultVersion:     lt.DefaultVersionNumber == lt.LatestVersionNumber,
		}
		item.LaunchTemplateData.ImageID = lt.ImageID
		item.LaunchTemplateData.InstanceType = lt.InstanceType
		items = append(items, item)
	}

	return &describeLaunchTemplateVersionsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Versions:  launchTemplateVersionSet{Items: items},
	}, nil
}

// ---- VPC endpoint delete handler ----

type deleteLaunchTemplateResponse struct {
	XMLName        xml.Name           `xml:"DeleteLaunchTemplateResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	LaunchTemplate launchTemplateItem `xml:"launchTemplate"`
}

type describeLaunchTemplateVersionsResponse struct {
	XMLName   xml.Name                 `xml:"DescribeLaunchTemplateVersionsResponse"`
	Xmlns     string                   `xml:"xmlns,attr"`
	RequestID string                   `xml:"requestId"`
	Versions  launchTemplateVersionSet `xml:"launchTemplateVersionSet"`
}

type unsuccessfulEndpointItem struct {
	ID string `xml:"vpcEndpointId"`
}

type unsuccessfulEndpointSet struct {
	Items []unsuccessfulEndpointItem `xml:"item"`
}

// registerLaunchTemplatesOps registers the LaunchTemplates operation handlers.
func registerLaunchTemplatesOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["DeleteLaunchTemplate"] = h.handleDeleteLaunchTemplate
	ops["DescribeLaunchTemplateVersions"] = h.handleDescribeLaunchTemplateVersions
}

// launchTemplatesSupportedOperations lists the operation names registered by
// registerLaunchTemplatesOps, for GetSupportedOperations().
func launchTemplatesSupportedOperations() []string {
	return []string{
		"DeleteLaunchTemplate",
		"DescribeLaunchTemplateVersions",
	}
}

// handleDescribeLaunchTemplates returns launch templates.
func (h *Handler) handleDescribeLaunchTemplates(vals url.Values, reqID string) (any, error) {
	names := parseMemberList(vals, "LaunchTemplateName")
	templates := h.Backend.DescribeLaunchTemplates(names)
	items := make([]launchTemplateItem, 0, len(templates))
	for _, template := range templates {
		items = append(items, launchTemplateItem{
			ID:                   template.ID,
			Name:                 template.Name,
			CreateTime:           template.CreateTime.Format(time.RFC3339),
			CreatedBy:            template.CreatedBy,
			DefaultVersionNumber: template.DefaultVersionNumber,
			LatestVersionNumber:  template.LatestVersionNumber,
			TagSet:               tagItemsFromMap(h.Backend.TagsForResource(template.ID)),
		})
	}

	return &describeLaunchTemplatesResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		LaunchTemplateSet: launchTemplateSet{Items: items},
	}, nil
}

type launchTemplateItem struct {
	ID                   string          `xml:"launchTemplateId"`
	Name                 string          `xml:"launchTemplateName"`
	CreateTime           string          `xml:"createTime"`
	CreatedBy            string          `xml:"createdBy"`
	TagSet               []simpleTagItem `xml:"tagSet>item"`
	DefaultVersionNumber int64           `xml:"defaultVersionNumber"`
	LatestVersionNumber  int64           `xml:"latestVersionNumber"`
}

type launchTemplateSet struct {
	Items []launchTemplateItem `xml:"item"`
}

type describeLaunchTemplatesResponse struct {
	XMLName           xml.Name          `xml:"DescribeLaunchTemplatesResponse"`
	Xmlns             string            `xml:"xmlns,attr"`
	RequestID         string            `xml:"requestId"`
	LaunchTemplateSet launchTemplateSet `xml:"launchTemplates"`
}
