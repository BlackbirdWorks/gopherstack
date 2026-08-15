package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// platformSummaryDescType mirrors types.PlatformSummary
// (elasticbeanstalk@v1.37.4 types/types.go), used by CreatePlatformVersion's
// and DeletePlatformVersion's real PlatformSummary output member. Unlike
// PlatformDescription (see platformDescriptionDescType below), the real
// PlatformSummary shape has NO PlatformName member at all -- reusing one
// shared struct for both shapes previously fabricated a PlatformName field
// on these two ops' responses that real AWS never sends.
//
// OperatingSystemName/OperatingSystemVersion/PlatformBranchLifecycleState/
// PlatformBranchName/PlatformCategory/PlatformLifecycleState/
// SupportedAddonList/SupportedTierList are not modeled: this backend never
// parses the CreatePlatformVersion request's S3 platform-definition bundle
// (see handleCreatePlatformVersion's doc comment), so there is no real
// source for any platform metadata beyond the four fields the domain model
// (PlatformVersion) tracks -- a structural gap, not a dropped field.
type platformSummaryDescType struct {
	PlatformArn     string `xml:"PlatformArn"`
	PlatformOwner   string `xml:"PlatformOwner"`
	PlatformVersion string `xml:"PlatformVersion"`
	PlatformStatus  string `xml:"PlatformStatus"`
}

// platformDescriptionDescType mirrors types.PlatformDescription, the real
// output shape of DescribePlatformVersion only (types.PlatformSummary above
// is the shape CreatePlatformVersion/DeletePlatformVersion actually use).
// Same disclosed-gap set as platformSummaryDescType, plus CustomAmiList/
// DateCreated/DateUpdated/Description/Frameworks/Maintainer/
// ProgrammingLanguages/SolutionStackName -- all likewise unreachable without
// real bundle parsing.
type platformDescriptionDescType struct {
	PlatformArn     string `xml:"PlatformArn"`
	PlatformName    string `xml:"PlatformName"`
	PlatformOwner   string `xml:"PlatformOwner"`
	PlatformVersion string `xml:"PlatformVersion"`
	PlatformStatus  string `xml:"PlatformStatus"`
}

func toPlatformSummaryDesc(pv *PlatformVersion) platformSummaryDescType {
	return platformSummaryDescType{
		PlatformArn:     pv.PlatformArn,
		PlatformOwner:   platformOwnerSelf,
		PlatformVersion: pv.PlatformVersion,
		PlatformStatus:  pv.PlatformStatus,
	}
}

func toPlatformDescriptionDesc(pv *PlatformVersion) platformDescriptionDescType {
	return platformDescriptionDescType{
		PlatformArn:     pv.PlatformArn,
		PlatformName:    pv.PlatformName,
		PlatformOwner:   platformOwnerSelf,
		PlatformVersion: pv.PlatformVersion,
		PlatformStatus:  pv.PlatformStatus,
	}
}

// createPlatformVersionResult is the result body for CreatePlatformVersion.
type createPlatformVersionResult struct {
	PlatformSummary platformSummaryDescType `xml:"PlatformSummary"`
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

	// PlatformDefinitionBundle (an S3Location: This member is required) is
	// validated for presence only -- this backend has no S3 cross-service
	// wiring for elasticbeanstalk (unlike CreateApplicationVersion's
	// SourceBundle, which is stored-but-unvalidated; see PARITY.md), and
	// real AWS never echoes the bundle location back on any response type
	// (PlatformSummary/PlatformDescription/Builder all lack an S3 field), so
	// there is nothing to store or round-trip -- verifying the S3 object
	// actually exists and building a platform from its contents is a
	// genuine structural gap, not something to fake.
	if vals.Get("PlatformDefinitionBundle.S3Bucket") == "" || vals.Get("PlatformDefinitionBundle.S3Key") == "" {
		return nil, fmt.Errorf(
			"%w: PlatformDefinitionBundle.S3Bucket and PlatformDefinitionBundle.S3Key are required",
			ErrInvalidParameter,
		)
	}

	tags := parseTagList(vals, "Tags.member")

	pv, err := h.Backend.CreatePlatformVersion(ctx, platformName, platformVersion, tags)
	if err != nil {
		return nil, err
	}

	return &createPlatformVersionResponse{
		Xmlns: ebXMLNS,
		CreatePlatformVersionResult: createPlatformVersionResult{
			PlatformSummary: toPlatformSummaryDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-platform-ver"},
	}, nil
}

// deletePlatformVersionResponse is the XML response for DeletePlatformVersion.
type deletePlatformVersionResult struct {
	PlatformSummary platformSummaryDescType `xml:"PlatformSummary"`
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
			PlatformSummary: toPlatformSummaryDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-platform-ver"},
	}, nil
}

// describePlatformVersionResponse is the XML response for DescribePlatformVersion.
type describePlatformVersionResult struct {
	PlatformDescription platformDescriptionDescType `xml:"PlatformDescription"`
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
			PlatformDescription: toPlatformDescriptionDesc(pv),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-platform-ver"},
	}, nil
}

// listPlatformBranchesResponse is the XML response for ListPlatformBranches.
// platformBranchSummary mirrors types.PlatformBranchSummary; BranchOrder
// ("an ordering number... default branch is assigned 100") and
// SupportedTierList are not modeled -- this backend's allPlatformBranches
// below is a static, unordered curated list with no tier-compatibility
// concept, so there is no real value to derive either from.
type platformBranchSummary struct {
	PlatformName   string `xml:"PlatformName"`
	BranchName     string `xml:"BranchName"`
	LifecycleState string `xml:"LifecycleState"`
}

type listPlatformBranchesResult struct {
	NextToken                 string                  `xml:"NextToken,omitempty"`
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

	pg := page.New(branches, vals.Get("NextToken"), parseMaxRecords(vals, "MaxRecords"), defaultListLimit)

	return &listPlatformBranchesResponse{
		Xmlns: ebXMLNS,
		ListPlatformBranchesResult: listPlatformBranchesResult{
			PlatformBranchSummaryList: pg.Data,
			NextToken:                 pg.Next,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-branches"},
	}, nil
}

// listPlatformVersionsResponse is the XML response for ListPlatformVersions.
// platformSummary mirrors types.PlatformSummary -- see platformSummaryDescType's
// doc comment in this file for the same disclosed-gap set (this backend
// tracks no platform metadata beyond PlatformArn/PlatformName/
// PlatformVersion/PlatformStatus).
type platformSummary struct {
	PlatformArn     string `xml:"PlatformArn"`
	PlatformOwner   string `xml:"PlatformOwner"`
	PlatformVersion string `xml:"PlatformVersion"`
	PlatformStatus  string `xml:"PlatformStatus"`
}

type listPlatformVersionsResult struct {
	NextToken           string            `xml:"NextToken,omitempty"`
	PlatformSummaryList []platformSummary `xml:"PlatformSummaryList>member"`
}

type listPlatformVersionsResponse struct {
	XMLName                    xml.Name                   `xml:"ListPlatformVersionsResponse"`
	Xmlns                      string                     `xml:"xmlns,attr"`
	ResponseMetadata           responseMetadata           `xml:"ResponseMetadata"`
	ListPlatformVersionsResult listPlatformVersionsResult `xml:"ListPlatformVersionsResult"`
}

// listPlatformVersionsFilterValue applies a single PlatformFilter's Type
// against a *PlatformVersion, matching by equality only (this backend has no
// other filterable attribute -- OperatingSystemName/SupportedTier/
// SupportedAddon/ProgrammingLanguageName/PlatformBranchName/
// PlatformLifecycleState are all unmodeled, see platformSummaryDescType --
// and, matching handleListPlatformBranches's existing precedent, Operator is
// not honored beyond implicit equality).
func listPlatformVersionsFilterValue(pv *PlatformVersion, filterType string) (string, bool) {
	switch filterType {
	case "PlatformName":
		return pv.PlatformName, true
	case "PlatformVersion":
		return pv.PlatformVersion, true
	case "PlatformStatus":
		return pv.PlatformStatus, true
	case "PlatformArn":
		return pv.PlatformArn, true
	default:
		return "", false
	}
}

func (h *Handler) handleListPlatformVersions(ctx context.Context, vals url.Values) (any, error) {
	pvs := h.Backend.ListPlatformVersions(ctx)

	for i := 1; ; i++ {
		filterType := vals.Get(fmt.Sprintf("Filters.member.%d.Type", i))
		if filterType == "" {
			break
		}

		want := vals.Get(fmt.Sprintf("Filters.member.%d.Values.member.1", i))

		filtered := make([]*PlatformVersion, 0, len(pvs))

		for _, pv := range pvs {
			if got, known := listPlatformVersionsFilterValue(pv, filterType); !known || strings.EqualFold(got, want) {
				filtered = append(filtered, pv)
			}
		}

		pvs = filtered
	}

	pg := page.New(pvs, vals.Get("NextToken"), parseMaxRecords(vals, "MaxRecords"), defaultListLimit)

	summaries := make([]platformSummary, 0, len(pg.Data))
	for _, pv := range pg.Data {
		summaries = append(summaries, platformSummary{
			PlatformArn:     pv.PlatformArn,
			PlatformOwner:   platformOwnerSelf,
			PlatformVersion: pv.PlatformVersion,
			PlatformStatus:  pv.PlatformStatus,
		})
	}

	return &listPlatformVersionsResponse{
		Xmlns: ebXMLNS,
		ListPlatformVersionsResult: listPlatformVersionsResult{
			PlatformSummaryList: summaries,
			NextToken:           pg.Next,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-list-platform-versions"},
	}, nil
}
