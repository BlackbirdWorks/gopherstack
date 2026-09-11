package redshift

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"net/url"
	"strings"
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
	ApplicationType    string `xml:"ApplicationType,omitempty"`
}

func idcAppToXML(app *IdcApplication) redshiftIdcAppXML {
	return redshiftIdcAppXML{
		IdcApplicationArn:  app.IdcApplicationArn,
		IdcApplicationName: app.IdcApplicationName,
		IdcInstanceArn:     app.IdcInstanceArn,
		IdcDisplayName:     app.IdcDisplayName,
		IamRoleArn:         app.IamRoleArn,
		ApplicationType:    app.ApplicationType,
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
		vals.Get("ApplicationType"),
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

// identityCenterAuthTokenResponse's Result members (Token/ExpirationTime) and
// ClusterIds.ClusterIdentifier.N wire shape are verified against
// redshift@v1.65.4 api_op_GetIdentityCenterAuthToken.go and
// serializers.go:13846-13858,10217-10225 (ClusterIdentifierList's member name
// is "ClusterIdentifier", not "member") and
// deserializers.go:52369-52390. The real operation scopes a token to
// ClusterIds, not an application ARN -- there is no IdentityCenterApplicationArn
// member on this input at all.
type identityCenterAuthTokenResponse struct {
	XMLName xml.Name `xml:"GetIdentityCenterAuthTokenResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Token          string `xml:"Token"`
		ExpirationTime string `xml:"ExpirationTime"`
	} `xml:"GetIdentityCenterAuthTokenResult"`
}

func (h *Handler) handleGetIdentityCenterAuthToken(params url.Values) (any, error) {
	clusterIDs := parseStringList(params, "ClusterIds.ClusterIdentifier.")
	if len(clusterIDs) == 0 {
		return nil, fmt.Errorf("%w: ClusterIds is required", ErrInvalidParameter)
	}

	for _, id := range clusterIDs {
		clusters, _, err := h.Backend.DescribeClusters(id, "", 0, nil, nil)
		if err != nil {
			return nil, err
		}
		if len(clusters) == 0 {
			return nil, fmt.Errorf("%w: %s", ErrClusterNotFound, id)
		}
	}

	expiry := time.Now().UTC().Add(identityCenterTokenExpiryMinutes * time.Minute)

	hash := sha256.Sum256([]byte(strings.Join(clusterIDs, ",") + expiry.Format(time.RFC3339)))
	token := "ict-" + hex.EncodeToString(hash[:16])

	resp := &identityCenterAuthTokenResponse{Xmlns: redshiftXMLNS}
	resp.Result.Token = token
	resp.Result.ExpirationTime = expiry.Format(time.RFC3339)

	return resp, nil
}
