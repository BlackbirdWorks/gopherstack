package redshift

import (
	"encoding/xml"
	"net/url"
	"strconv"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ----- Qev2 IDC Application (Query Editor V2) -----
//
// Qev2IdcApplication is a distinct resource from RedshiftIdcApplication (see
// handler_idc_applications.go and the Qev2IdcApplication doc comment in
// models.go), so it gets its own wire types and store rather than reusing
// redshiftIdcAppXML.

type qev2IdcAppXML struct {
	Qev2IdcApplicationArn    string       `xml:"Qev2IdcApplicationArn"`
	Qev2IdcApplicationName   string       `xml:"Qev2IdcApplicationName"`
	IdcInstanceArn           string       `xml:"IdcInstanceArn,omitempty"`
	IdcDisplayName           string       `xml:"IdcDisplayName,omitempty"`
	IdcManagedApplicationArn string       `xml:"IdcManagedApplicationArn,omitempty"`
	IdcOnboardStatus         string       `xml:"IdcOnboardStatus,omitempty"`
	Tags                     []svcTags.KV `xml:"Tags>Tag,omitempty"`
}

func qev2IdcAppToXML(app *Qev2IdcApplication) qev2IdcAppXML {
	return qev2IdcAppXML{
		Qev2IdcApplicationArn:    app.Qev2IdcApplicationArn,
		Qev2IdcApplicationName:   app.Qev2IdcApplicationName,
		IdcInstanceArn:           app.IdcInstanceArn,
		IdcDisplayName:           app.IdcDisplayName,
		IdcManagedApplicationArn: app.IdcManagedApplicationArn,
		IdcOnboardStatus:         app.IdcOnboardStatus,
		Tags:                     tagMapToKVList(app.Tags),
	}
}

type createQev2IdcApplicationResponse struct {
	XMLName xml.Name      `xml:"CreateQev2IdcApplicationResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Result  qev2IdcAppXML `xml:"CreateQev2IdcApplicationResult>Qev2IdcApplication"`
}

// handleCreateQev2IdcApplication implements CreateQev2IdcApplication. Real
// aws-sdk-go-v2 clients send the application name as Qev2IdcApplicationName
// and tags as "Tags" (Tags.Tag.N.Key/Value), both confirmed against
// awsAwsquery_serializeOpDocumentCreateQev2IdcApplicationInput in
// aws-sdk-go-v2/service/redshift@v1.65.0/serializers.go -- unlike
// CreateIntegration, which sends its tag list under "TagList" instead.
func (h *Handler) handleCreateQev2IdcApplication(vals url.Values) (any, error) {
	tags := parseRedshiftTags(vals)

	app, err := h.Backend.CreateQev2IdcApplication(
		vals.Get("Qev2IdcApplicationName"),
		vals.Get("IdcInstanceArn"),
		vals.Get("IdcDisplayName"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createQev2IdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: qev2IdcAppToXML(app),
	}, nil
}

type deleteQev2IdcApplicationResponse struct {
	XMLName xml.Name `xml:"DeleteQev2IdcApplicationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// handleDeleteQev2IdcApplication implements DeleteQev2IdcApplication.
// DeleteQev2IdcApplicationOutput carries no fields beyond ResultMetadata, so
// the response is an empty envelope, matching deleteIdcApplicationResponse's
// own shape for the sibling RedshiftIdcApplication family.
func (h *Handler) handleDeleteQev2IdcApplication(vals url.Values) (any, error) {
	if err := h.Backend.DeleteQev2IdcApplication(vals.Get("Qev2IdcApplicationArn")); err != nil {
		return nil, err
	}

	return &deleteQev2IdcApplicationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeQev2IdcApplicationsResponse struct {
	XMLName xml.Name `xml:"DescribeQev2IdcApplicationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Marker              string          `xml:"Marker,omitempty"`
		Qev2IdcApplications []qev2IdcAppXML `xml:"Qev2IdcApplications>member"`
	} `xml:"DescribeQev2IdcApplicationsResult"`
}

// handleDescribeQev2IdcApplications implements DescribeQev2IdcApplications.
// This operation is plural and paginated in the real API (Marker/MaxRecords
// request fields, Marker response field -- confirmed against
// DescribeQev2IdcApplicationsInput/Output), so it follows the same
// Marker/MaxRecords convention as handleDescribeClusters (see handler.go)
// rather than the non-paginated shortcut the sibling
// handleDescribeIdcApplications takes for RedshiftIdcApplication.
func (h *Handler) handleDescribeQev2IdcApplications(vals url.Values) (any, error) {
	marker := vals.Get("Marker")

	maxRecords := 0
	if s := vals.Get("MaxRecords"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			maxRecords = n
		}
	}

	apps, nextMarker, err := h.Backend.DescribeQev2IdcApplications(
		vals.Get("Qev2IdcApplicationArn"), marker, maxRecords,
	)
	if err != nil {
		return nil, err
	}

	members := make([]qev2IdcAppXML, 0, len(apps))

	for i := range apps {
		members = append(members, qev2IdcAppToXML(&apps[i]))
	}

	resp := &describeQev2IdcApplicationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.Marker = nextMarker
	resp.Result.Qev2IdcApplications = members

	return resp, nil
}

type modifyQev2IdcApplicationResponse struct {
	XMLName xml.Name      `xml:"ModifyQev2IdcApplicationResponse"`
	Xmlns   string        `xml:"xmlns,attr"`
	Result  qev2IdcAppXML `xml:"ModifyQev2IdcApplicationResult>Qev2IdcApplication"`
}

// handleModifyQev2IdcApplication implements ModifyQev2IdcApplication. Real
// clients send the lookup key as Qev2IdcApplicationArn and the only mutable
// field as IdcDisplayName (confirmed against
// awsAwsquery_serializeOpDocumentModifyQev2IdcApplicationInput) -- there is no
// IamRoleArn on this resource, unlike ModifyRedshiftIdcApplication.
func (h *Handler) handleModifyQev2IdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.ModifyQev2IdcApplication(
		vals.Get("Qev2IdcApplicationArn"),
		vals.Get("IdcDisplayName"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyQev2IdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: qev2IdcAppToXML(app),
	}, nil
}
