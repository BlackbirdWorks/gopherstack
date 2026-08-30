package ec2

import (
	"encoding/xml"
	"net/url"
	"slices"
	"strconv"
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

// handleDescribeLaunchTemplateVersions previously only ever read
// LaunchTemplateId, ignoring the wire's LaunchTemplateName (an alternative
// identifier), Versions (FlatKey "LaunchTemplateVersion" -- specific version
// numbers), and MinVersion/MaxVersion (a version range) --
// awsEc2query_serializeOpDocumentDescribeLaunchTemplateVersionsInput. This
// backend only ever stores one version snapshot per template, so Versions/
// MinVersion/MaxVersion are applied against that single item rather than a
// real multi-version history (which this mock does not model).
func (h *Handler) handleDescribeLaunchTemplateVersions(vals url.Values, reqID string) (any, error) {
	id := vals.Get("LaunchTemplateId")
	if id == "" {
		if name := vals.Get("LaunchTemplateName"); name != "" {
			if matches := h.Backend.DescribeLaunchTemplates([]string{name}); len(matches) > 0 {
				id = matches[0].ID
			}
		}
	}

	versions, err := h.Backend.DescribeLaunchTemplateVersions(id)
	if err != nil {
		return nil, err
	}

	requestedVersions := parseMemberList(vals, "LaunchTemplateVersion")

	var minVersion, maxVersion int64

	hasMin := vals.Get("MinVersion") != ""
	if hasMin {
		minVersion, _ = strconv.ParseInt(vals.Get("MinVersion"), 10, 64)
	}

	hasMax := vals.Get("MaxVersion") != ""
	if hasMax {
		maxVersion, _ = strconv.ParseInt(vals.Get("MaxVersion"), 10, 64)
	}

	items := make([]launchTemplateVersionItem, 0, len(versions))
	for _, lt := range versions {
		if len(requestedVersions) > 0 &&
			!slices.Contains(requestedVersions, strconv.FormatInt(lt.LatestVersionNumber, 10)) {
			continue
		}

		if hasMin && lt.LatestVersionNumber < minVersion {
			continue
		}

		if hasMax && lt.LatestVersionNumber > maxVersion {
			continue
		}

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

// handleDescribeLaunchTemplates previously only read LaunchTemplateName.N,
// silently ignoring LaunchTemplateId.N entirely
// (awsEc2query_serializeOpDocumentDescribeLaunchTemplatesInput both declare
// FlatKey("LaunchTemplateId")/FlatKey("LaunchTemplateName")) -- a client
// filtering by specific template IDs got every template back unfiltered.
func (h *Handler) handleDescribeLaunchTemplates(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "LaunchTemplateId")
	names := parseMemberList(vals, "LaunchTemplateName")

	var templates []*LaunchTemplate

	switch {
	case len(ids) > 0:
		idSet := make(map[string]bool, len(ids))
		for _, id := range ids {
			idSet[id] = true
		}

		for _, t := range h.Backend.DescribeLaunchTemplates(nil) {
			if idSet[t.ID] {
				templates = append(templates, t)
			}
		}
	default:
		templates = h.Backend.DescribeLaunchTemplates(names)
	}

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
