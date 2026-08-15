package elasticbeanstalk

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// --- Application Version operations ---

// appVersionDescType mirrors types.ApplicationVersionDescription
// (elasticbeanstalk@v1.37.4 types/types.go). BuildArn ("The build ARN for
// the application version if it's an application deployed through AWS
// CodeBuild") is not modeled: this backend has no CodeBuild integration
// anywhere (SourceBuildInformation is stored-but-unvalidated, matching
// CreatePlatformVersion's S3 bundle precedent), so there is no real ARN to
// source -- a structural gap, not a dropped field.
type appVersionDescType struct {
	SourceBundle           *s3LocationType             `xml:"SourceBundle,omitempty"`
	SourceBuildInformation *sourceBuildInformationType `xml:"SourceBuildInformation,omitempty"`
	ApplicationName        string                      `xml:"ApplicationName"`
	VersionLabel           string                      `xml:"VersionLabel"`
	ApplicationVersionArn  string                      `xml:"ApplicationVersionArn"`
	Description            string                      `xml:"Description,omitempty"`
	DateCreated            string                      `xml:"DateCreated,omitempty"`
	DateUpdated            string                      `xml:"DateUpdated,omitempty"`
	Status                 string                      `xml:"Status"`
}

type s3LocationType struct {
	S3Bucket string `xml:"S3Bucket"`
	S3Key    string `xml:"S3Key"`
}

type sourceBuildInformationType struct {
	SourceType       string `xml:"SourceType"`
	SourceRepository string `xml:"SourceRepository"`
	SourceLocation   string `xml:"SourceLocation"`
}

func toAppVersionDesc(ver *ApplicationVersion) appVersionDescType {
	desc := appVersionDescType{
		ApplicationName:       ver.ApplicationName,
		VersionLabel:          ver.VersionLabel,
		ApplicationVersionArn: ver.ApplicationVersionARN,
		Description:           ver.Description,
		DateCreated:           ver.DateCreated,
		DateUpdated:           ver.DateUpdated,
		Status:                ver.Status,
	}
	if ver.S3Bucket != "" || ver.S3Key != "" {
		desc.SourceBundle = &s3LocationType{S3Bucket: ver.S3Bucket, S3Key: ver.S3Key}
	}
	if ver.SourceBuildInformation != nil {
		desc.SourceBuildInformation = &sourceBuildInformationType{
			SourceType:       ver.SourceBuildInformation.SourceType,
			SourceRepository: ver.SourceBuildInformation.SourceRepository,
			SourceLocation:   ver.SourceBuildInformation.SourceLocation,
		}
	}

	return desc
}

type createApplicationVersionResult struct {
	ApplicationVersion appVersionDescType `xml:"ApplicationVersion"`
}

type createApplicationVersionResponse struct {
	XMLName                        xml.Name                       `xml:"CreateApplicationVersionResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	CreateApplicationVersionResult createApplicationVersionResult `xml:"CreateApplicationVersionResult"`
	ResponseMetadata               responseMetadata               `xml:"ResponseMetadata"`
}

func (h *Handler) handleCreateApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabel := vals.Get("VersionLabel")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")
	tags := parseTagList(vals, "Tags.member")

	// Parse S3 source bundle (improvement #8)
	s3Bucket := vals.Get("SourceBundle.S3Bucket")
	s3Key := vals.Get("SourceBundle.S3Key")

	ver, err := h.Backend.CreateApplicationVersionWithParams(ctx, appName, versionLabel, ApplicationVersionParams{
		Description:            description,
		S3Bucket:               s3Bucket,
		S3Key:                  s3Key,
		Tags:                   tags,
		Process:                strings.EqualFold(vals.Get("Process"), "true"),
		AutoCreateApplication:  strings.EqualFold(vals.Get("AutoCreateApplication"), "true"),
		SourceBuildInformation: parseSourceBuildInformation(vals),
	})
	if err != nil {
		return nil, err
	}

	return &createApplicationVersionResponse{
		Xmlns: ebXMLNS,
		CreateApplicationVersionResult: createApplicationVersionResult{
			ApplicationVersion: toAppVersionDesc(ver),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-create-ver"},
	}, nil
}

type describeApplicationVersionsResult struct {
	NextToken           string               `xml:"NextToken,omitempty"`
	ApplicationVersions []appVersionDescType `xml:"ApplicationVersions>member"`
}

type describeApplicationVersionsResponse struct {
	XMLName                           xml.Name                          `xml:"DescribeApplicationVersionsResponse"`
	Xmlns                             string                            `xml:"xmlns,attr"`
	ResponseMetadata                  responseMetadata                  `xml:"ResponseMetadata"`
	DescribeApplicationVersionsResult describeApplicationVersionsResult `xml:"DescribeApplicationVersionsResult"`
}

func (h *Handler) handleDescribeApplicationVersions(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabels := parseMembers(vals, "VersionLabels.member")
	vers := h.Backend.DescribeApplicationVersions(ctx, appName, versionLabels)

	pg := page.New(vers, vals.Get("NextToken"), parseMaxRecords(vals, "MaxRecords"), defaultListLimit)

	members := make([]appVersionDescType, 0, len(pg.Data))

	for _, ver := range pg.Data {
		members = append(members, toAppVersionDesc(ver))
	}

	return &describeApplicationVersionsResponse{
		Xmlns: ebXMLNS,
		DescribeApplicationVersionsResult: describeApplicationVersionsResult{
			ApplicationVersions: members,
			NextToken:           pg.Next,
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-describe-vers"},
	}, nil
}

type deleteApplicationVersionResponse struct {
	XMLName          xml.Name         `xml:"DeleteApplicationVersionResponse"`
	Xmlns            string           `xml:"xmlns,attr"`
	ResponseMetadata responseMetadata `xml:"ResponseMetadata"`
}

func (h *Handler) handleDeleteApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	versionLabel := vals.Get("VersionLabel")

	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteApplicationVersion(ctx, appName, versionLabel); err != nil {
		return nil, err
	}

	return &deleteApplicationVersionResponse{
		Xmlns:            ebXMLNS,
		ResponseMetadata: responseMetadata{RequestID: "eb-delete-ver"},
	}, nil
}

// updateApplicationVersionResponse is the XML response for UpdateApplicationVersion.
type updateApplicationVersionResponse struct {
	XMLName                        xml.Name                       `xml:"UpdateApplicationVersionResponse"`
	Xmlns                          string                         `xml:"xmlns,attr"`
	UpdateApplicationVersionResult createApplicationVersionResult `xml:"UpdateApplicationVersionResult"`
	ResponseMetadata               responseMetadata               `xml:"ResponseMetadata"`
}

func (h *Handler) handleUpdateApplicationVersion(ctx context.Context, vals url.Values) (any, error) {
	appName := vals.Get("ApplicationName")
	if appName == "" {
		return nil, fmt.Errorf("%w: ApplicationName is required", ErrInvalidParameter)
	}

	versionLabel := vals.Get("VersionLabel")
	if versionLabel == "" {
		return nil, fmt.Errorf("%w: VersionLabel is required", ErrInvalidParameter)
	}

	description := vals.Get("Description")

	ver, err := h.Backend.UpdateApplicationVersion(ctx, appName, versionLabel, description)
	if err != nil {
		return nil, err
	}

	return &updateApplicationVersionResponse{
		Xmlns: ebXMLNS,
		UpdateApplicationVersionResult: createApplicationVersionResult{
			ApplicationVersion: toAppVersionDesc(ver),
		},
		ResponseMetadata: responseMetadata{RequestID: "eb-update-app-ver"},
	}, nil
}
