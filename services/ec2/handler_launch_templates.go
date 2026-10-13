package ec2

import (
	"encoding/xml"
	"net/url"
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
func parseVersionBound(raw string) (int64, bool) {
	if raw == "" {
		return 0, false
	}

	n, _ := strconv.ParseInt(raw, 10, 64)

	return n, true
}

func (h *Handler) handleDescribeLaunchTemplateVersions(vals url.Values, reqID string) (any, error) {
	var versions []LaunchTemplateVersion

	if ref := h.launchTemplateRef(vals); ref != "" {
		var err error

		if _, versions, err = h.Backend.LaunchTemplateVersions(ref); err != nil {
			return nil, err
		}
	} else {
		versions = h.Backend.AllLaunchTemplateVersions()
	}

	requested := parseMemberList(vals, "LaunchTemplateVersion")
	minVersion, hasMin := parseVersionBound(vals.Get("MinVersion"))
	maxVersion, hasMax := parseVersionBound(vals.Get("MaxVersion"))
	resolveAlias := vals.Get("ResolveAlias") == ec2BooleanTrue

	var latest int64
	for _, v := range versions {
		latest = max(latest, v.VersionNumber)
	}

	items := make([]launchTemplateVersionItem, 0, len(versions))

	for _, v := range versions {
		if len(requested) > 0 && !launchTemplateVersionRequested(requested, v, latest) {
			continue
		}

		if (hasMin && v.VersionNumber < minVersion) || (hasMax && v.VersionNumber > maxVersion) {
			continue
		}

		item, err := h.launchTemplateVersionItemFor(v, resolveAlias)
		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}

	items = applyLaunchTemplateVersionFilters(items, parseEC2Filters(vals))

	return finishDescribe(vals, &describeLaunchTemplateVersionsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Versions:  launchTemplateVersionSet{Items: items},
	}, describeOpts{spec: specLaunchTemplates()})
}

func launchTemplateVersionRequested(requested []string, v LaunchTemplateVersion, latest int64) bool {
	for _, r := range requested {
		switch r {
		case launchTemplateVersionLatest:
			if v.VersionNumber == latest {
				return true
			}
		case launchTemplateVersionDefault:
			if v.DefaultVersion {
				return true
			}
		default:
			if r == strconv.FormatInt(v.VersionNumber, 10) {
				return true
			}
		}
	}

	return false
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
	NextToken string                   `xml:"nextToken,omitempty"`
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

		if err := requireAllIDsPresent(
			ids, templates, func(t *LaunchTemplate) string { return t.ID }, ErrLaunchTemplateNotFound,
		); err != nil {
			return nil, err
		}
	default:
		templates = h.Backend.DescribeLaunchTemplates(names)

		if err := requireAllIDsPresent(
			names, templates, func(t *LaunchTemplate) string { return t.Name }, ErrLaunchTemplateNameNotFound,
		); err != nil {
			return nil, err
		}
	}

	templates = applyLaunchTemplateFilters(templates, parseEC2Filters(vals), h.Backend)

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

	return finishDescribe(vals, &describeLaunchTemplatesResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		LaunchTemplateSet: launchTemplateSet{Items: items},
	}, describeOpts{spec: specLaunchTemplates()})
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
	NextToken         string            `xml:"nextToken,omitempty"`
	LaunchTemplateSet launchTemplateSet `xml:"launchTemplates"`
}
