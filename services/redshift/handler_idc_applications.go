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
	IdcApplicationArn  string `xml:"IdcApplicationArn"`
	IdcApplicationName string `xml:"IdcApplicationName"`
	IdcInstanceArn     string `xml:"IamRoleArn,omitempty"`
	IdcDisplayName     string `xml:"IdcDisplayName,omitempty"`
	IamRoleArn         string `xml:"IdcInstanceArn,omitempty"`
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

type createIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"CreateIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"CreateIdcApplicationResult"`
}

func (h *Handler) handleCreateIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.CreateIdcApplication(
		vals.Get("IdcApplicationName"),
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
	XMLName xml.Name `xml:"DeleteIdcApplicationResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func (h *Handler) handleDeleteIdcApplication(vals url.Values) (any, error) {
	if err := h.Backend.DeleteIdcApplication(vals.Get("IdcApplicationArn")); err != nil {
		return nil, err
	}

	return &deleteIdcApplicationResponse{Xmlns: redshiftXMLNS}, nil
}

type describeIdcApplicationsResponse struct {
	XMLName xml.Name `xml:"DescribeIdcApplicationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		IdcApplications []redshiftIdcAppXML `xml:"IdcApplications>IdcApplication"`
	} `xml:"DescribeIdcApplicationsResult"`
}

func (h *Handler) handleDescribeIdcApplications(vals url.Values) (any, error) {
	apps, err := h.Backend.DescribeIdcApplications(vals.Get("IdcApplicationArn"))
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

type modifyIdcApplicationResponse struct {
	XMLName xml.Name          `xml:"ModifyIdcApplicationResponse"`
	Xmlns   string            `xml:"xmlns,attr"`
	Result  redshiftIdcAppXML `xml:"ModifyIdcApplicationResult"`
}

func (h *Handler) handleModifyIdcApplication(vals url.Values) (any, error) {
	app, err := h.Backend.ModifyIdcApplication(
		vals.Get("IdcApplicationArn"),
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
