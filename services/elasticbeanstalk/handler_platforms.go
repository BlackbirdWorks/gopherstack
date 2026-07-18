package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
)

// platformVersionDescType is used in XML responses for platform versions.
type platformVersionDescType struct {
	PlatformArn     string `xml:"PlatformArn"`
	PlatformName    string `xml:"PlatformName"`
	PlatformVersion string `xml:"PlatformVersion"`
	PlatformStatus  string `xml:"PlatformStatus"`
}

func toPlatformVersionDesc(pv *PlatformVersion) platformVersionDescType {
	return platformVersionDescType{
		PlatformArn:     pv.PlatformArn,
		PlatformName:    pv.PlatformName,
		PlatformVersion: pv.PlatformVersion,
		PlatformStatus:  pv.PlatformStatus,
	}
}

// createPlatformVersionResult is the result body for CreatePlatformVersion.
type createPlatformVersionResult struct {
	PlatformSummary platformVersionDescType `xml:"PlatformSummary"`
}

// createPlatformVersionResponse is the XML response for CreatePlatformVersion.
type createPlatformVersionResponse struct {
	XMLName                     xml.Name                    `xml:"CreatePlatformVersionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	CreatePlatformVersionResult createPlatformVersionResult `xml:"CreatePlatformVersionResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

// handleCreatePlatformVersion creates a new custom platform version.
func (h *Handler) handleCreatePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformName := vals.Get("PlatformName")
	if platformName == "" {
		return nil, fmt.Errorf("%w: PlatformName is required", ErrInvalidParameter)
	}

	platformVersion := vals.Get("PlatformVersion")
	if platformVersion == "" {
		return nil, fmt.Errorf("%w: PlatformVersion is required", ErrInvalidParameter)
	}

	tags := parseTagList(vals, "Tags.member")

	pv, err := h.Backend.CreatePlatformVersion(ctx, platformName, platformVersion, tags)
	if err != nil {
		return nil, err
	}

	return &createPlatformVersionResponse{
		Xmlns: ebXMLNS,
		CreatePlatformVersionResult: createPlatformVersionResult{
			PlatformSummary: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-platform-ver"},
	}, nil
}

// deletePlatformVersionResponse is the XML response for DeletePlatformVersion.
type deletePlatformVersionResult struct {
	PlatformSummary platformVersionDescType `xml:"PlatformSummary"`
}

type deletePlatformVersionResponse struct {
	XMLName                     xml.Name                    `xml:"DeletePlatformVersionResponse"`
	Xmlns                       string                      `xml:"xmlns,attr"`
	DeletePlatformVersionResult deletePlatformVersionResult `xml:"DeletePlatformVersionResult"`
	ResponseMetadata            responseMetadata            `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeletePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformARN := vals.Get("PlatformArn")
	if platformARN == "" {
		return nil, fmt.Errorf("%w: PlatformArn is required", ErrInvalidParameter)
	}

	pv, err := h.Backend.DeletePlatformVersion(ctx, platformARN)
	if err != nil {
		return nil, err
	}

	return &deletePlatformVersionResponse{
		Xmlns: ebXMLNS,
		DeletePlatformVersionResult: deletePlatformVersionResult{
			PlatformSummary: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-platform-ver"},
	}, nil
}

// describePlatformVersionResponse is the XML response for DescribePlatformVersion.
type describePlatformVersionResult struct {
	PlatformDescription platformVersionDescType `xml:"PlatformDescription"`
}

type describePlatformVersionResponse struct {
	XMLName                       xml.Name                      `xml:"DescribePlatformVersionResponse"`
	Xmlns                         string                        `xml:"xmlns,attr"`
	DescribePlatformVersionResult describePlatformVersionResult `xml:"DescribePlatformVersionResult"`
	ResponseMetadata              responseMetadata              `xml:"ResponseMetadata"`
}

func (h *Handler) handleDescribePlatformVersion(ctx context.Context, vals url.Values) (any, error) {
	platformARN := vals.Get("PlatformArn")
	if platformARN == "" {
		return nil, fmt.Errorf("%w: PlatformArn is required", ErrInvalidParameter)
	}

	pv, err := h.Backend.DescribePlatformVersion(ctx, platformARN)
	if err != nil {
		return nil, err
	}

	return &describePlatformVersionResponse{
		Xmlns: ebXMLNS,
		DescribePlatformVersionResult: describePlatformVersionResult{
			PlatformDescription: toPlatformVersionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-platform-ver"},
	}, nil
}

// listPlatformBranchesResponse is the XML response for ListPlatformBranches.
type platformBranchSummary struct {
	PlatformName   string `xml:"PlatformName"`
	BranchName     string `xml:"BranchName"`
	LifecycleState string `xml:"LifecycleState"`
}

type listPlatformBranchesResult struct {
	PlatformBranchSummaryList []platformBranchSummary `xml:"PlatformBranchSummaryList>member"`
}

type listPlatformBranchesResponse struct {
	XMLName                    xml.Name                   `xml:"ListPlatformBranchesResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	ListPlatformBranchesResult listPlatformBranchesResult `xml:"ListPlatformBranchesResult"`
}

// allPlatformBranches is the full list of platform branches returned by ListPlatformBranches.
//
//nolint:gochecknoglobals // package-level constant slice
var allPlatformBranches = []platformBranchSummary{
	{
		PlatformName:   "Python",
		BranchName:     "Python 3.11 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Node.js",
		BranchName:     "Node.js 20 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Go",
		BranchName:     "Go 1 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "PHP",
		BranchName:     "PHP 8.3 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Docker",
		BranchName:     "Docker running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Ruby",
		BranchName:     "Ruby 3.3 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
	{
		PlatformName:   "Java",
		BranchName:     "Corretto 21 running on 64bit Amazon Linux 2023",
		LifecycleState: platformLifecycleSupported,
	},
}

// handleListPlatformBranches lists platform branches with optional filtering (improvement #3).
func (h *Handler) handleListPlatformBranches(_ context.Context, vals url.Values) (any, error) {
	// Collect filters: Filters.member.N.Attribute / Value
	type filterEntry struct{ attribute, value string }

	filters := make([]filterEntry, 0)

	for i := 1; ; i++ {
		attr := vals.Get(fmt.Sprintf("Filters.member.%d.Attribute", i))
		if attr == "" {
			break
		}

		value := vals.Get(fmt.Sprintf("Filters.member.%d.Values.member.1", i))
		filters = append(filters, filterEntry{attribute: attr, value: value})
	}

	branches := make([]platformBranchSummary, 0, len(allPlatformBranches))

	for _, b := range allPlatformBranches {
		match := true

		for _, f := range filters {
			switch f.attribute {
			case "PlatformName":
				if !strings.EqualFold(b.PlatformName, f.value) {
					match = false
				}
			case "LifecycleState":
				if !strings.EqualFold(b.LifecycleState, f.value) {
					match = false
				}
			}
		}

		if match {
			branches = append(branches, b)
		}
	}

	return &listPlatformBranchesResponse{
		Xmlns: ebXMLNS,
		ListPlatformBranchesResult: listPlatformBranchesResult{
			PlatformBranchSummaryList: branches,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-branches"},
	}, nil
}

// listPlatformVersionsResponse is the XML response for ListPlatformVersions.
type platformSummary struct {
	PlatformArn    string `xml:"PlatformArn"`
	PlatformStatus string `xml:"PlatformStatus"`
}

type listPlatformVersionsResult struct {
	PlatformSummaryList []platformSummary `xml:"PlatformSummaryList>member"`
}

type listPlatformVersionsResponse struct {
	XMLName                    xml.Name                   `xml:"ListPlatformVersionsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	ListPlatformVersionsResult listPlatformVersionsResult `xml:"ListPlatformVersionsResult"`
}

func (h *Handler) handleListPlatformVersions(ctx context.Context, _ url.Values) (any, error) {
	pvs := h.Backend.ListPlatformVersions(ctx)

	summaries := make([]platformSummary, 0, len(pvs))
	for _, pv := range pvs {
		summaries = append(summaries, platformSummary{
			PlatformArn:    pv.PlatformArn,
			PlatformStatus: pv.PlatformStatus,
		})
	}

	return &listPlatformVersionsResponse{
		Xmlns: ebXMLNS,
		ListPlatformVersionsResult: listPlatformVersionsResult{
			PlatformSummaryList: summaries,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-versions"},
	}, nil
}
