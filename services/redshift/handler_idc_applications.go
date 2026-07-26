package redshift

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

// identityCenterTokenExpiryMinutes is the validity window for a generated IdentityCenter auth token.
const identityCenterTokenExpiryMinutes = 15

// ----- Redshift IDC Application -----

type redshiftIdcAppXML struct {
	IdcApplicationArn  string `xml:"RedshiftIdcApplicationArn"`
	IdcApplicationName string `xml:"RedshiftIdcApplicationName"`
	IdcInstanceArn     string `xml:"IdcInstanceArn,omitempty"`
	IdcDisplayName     string `xml:"IdcDisplayName,omitempty"`
	IamRoleArn         string `xml:"IamRoleArn,omitempty"`
}

func idcAppToXML(app *IdcApplication) redshiftIdcAppXML {
	return redshiftIdcAppXML{
		IdcApplicationArn:  app.IdcApplicationArn,
		IdcApplicationName: app.IdcApplicationName,
		IdcInstanceArn:     app.IdcInstanceArn,
		IdcDisplayName:     app.IdcDisplayName,
		IamRoleArn:         app.IamRoleArn,
	}
}

// createIdcApplicationResponse wraps the created application in a nested
// RedshiftIdcApplication element inside CreateRedshiftIdcApplicationResult
// (confirmed against awsAwsquery_deserializeOpDocumentCreateRedshiftIdcApplicationOutput
// in aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go, which looks for
// a "RedshiftIdcApplication" child element of the result -- mirroring
// createQev2IdcApplicationResponse's Qev2IdcApplication nesting).
type createIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"CreateRedshiftIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"CreateRedshiftIdcApplicationResult>RedshiftIdcApplication"`
}

// handleCreateIdcApplication implements CreateRedshiftIdcApplication. Real
// aws-sdk-go-v2 clients send the application name as RedshiftIdcApplicationName
// (confirmed against CreateRedshiftIdcApplicationInput), not IdcApplicationName.
func (h *Handler) handleCreateIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.CreateIdcApplication(
		vals.Get("RedshiftIdcApplicationName"),
		vals.Get("IdcInstanceArn"),
		vals.Get("IdcDisplayName"),
		vals.Get("IamRoleArn"),
	)
	if err != nil {
		return nil, err
	}

	return &createIdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: idcAppToXML(app),
	}, nil
}

type deleteIdcApplicationResponse struct {
	XMLName xml.Name `xml:"DeleteRedshiftIdcApplicationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// handleDeleteIdcApplication implements DeleteRedshiftIdcApplication. Real clients
// send the lookup key as RedshiftIdcApplicationArn (confirmed against
// DeleteRedshiftIdcApplicationInput), not IdcApplicationArn.
func (h *Handler) handleDeleteIdcApplication(vals url.Values) (any, error) {
	if err := h.Backend.DeleteIdcApplication(vals.Get("RedshiftIdcApplicationArn")); err != nil {
		return nil, err
	}

	return &deleteIdcApplicationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeIdcApplicationsResponse struct {
	XMLName xml.Name `xml:"DescribeRedshiftIdcApplicationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		IdcApplications []redshiftIdcAppXML `xml:"RedshiftIdcApplications>member"`
	} `xml:"DescribeRedshiftIdcApplicationsResult"`
}

// handleDescribeIdcApplications implements DescribeRedshiftIdcApplications. Real
// clients send the filter as RedshiftIdcApplicationArn (confirmed against
// DescribeRedshiftIdcApplicationsInput), not IdcApplicationArn.
func (h *Handler) handleDescribeIdcApplications(vals url.Values) (any, error) {
	apps, err := h.Backend.DescribeIdcApplications(vals.Get("RedshiftIdcApplicationArn"))
	if err != nil {
		return nil, err
	}

	members := make([]redshiftIdcAppXML, 0, len(apps))

	for i := range apps {
		members = append(members, idcAppToXML(&apps[i]))
	}

	resp := &describeIdcApplicationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.IdcApplications = members

	return resp, nil
}

// modifyIdcApplicationResponse wraps the modified application in a nested
// RedshiftIdcApplication element inside ModifyRedshiftIdcApplicationResult
// (confirmed against awsAwsquery_deserializeOpDocumentModifyRedshiftIdcApplicationOutput
// in aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go, same nesting as
// the Create response above).
type modifyIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"ModifyRedshiftIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"ModifyRedshiftIdcApplicationResult>RedshiftIdcApplication"`
}

// handleModifyIdcApplication implements ModifyRedshiftIdcApplication. Real clients
// send the lookup key as RedshiftIdcApplicationArn (confirmed against
// ModifyRedshiftIdcApplicationInput), not IdcApplicationArn.
func (h *Handler) handleModifyIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.ModifyIdcApplication(
		vals.Get("RedshiftIdcApplicationArn"),
		vals.Get("IdcDisplayName"),
		vals.Get("IamRoleArn"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIdcApplicationResponse{
		Xmlns:  redshiftXMLNS,
		Result: idcAppToXML(app),
	}, nil
}

type identityCenterAuthTokenResponse struct {
	XMLName xml.Name `xml:"GetIdentityCenterAuthTokenResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		AuthToken           string `xml:"AuthToken"`
		AuthTokenExpiration string `xml:"AuthTokenExpiration"`
	} `xml:"GetIdentityCenterAuthTokenResult"`
}

func (h *Handler) handleGetIdentityCenterAuthToken(params url.Values) (any, error) {
	appArn := params.Get("IdentityCenterApplicationArn")
	if appArn == "" {
		return nil, fmt.Errorf("%w: IdentityCenterApplicationArn is required", ErrInvalidParameter)
	}

	expiry := time.Now().UTC().Add(identityCenterTokenExpiryMinutes * time.Minute)

	hash := sha256.Sum256([]byte(appArn + expiry.Format(time.RFC3339)))
	token := "ict-" + hex.EncodeToString(hash[:16])

	resp := &identityCenterAuthTokenResponse{Xmlns: redshiftXMLNS}
	resp.Result.AuthToken = token
	resp.Result.AuthTokenExpiration = expiry.Format(time.RFC3339)

	return resp, nil
}
