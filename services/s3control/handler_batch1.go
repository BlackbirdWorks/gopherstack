package s3control

import (
	"encoding/xml"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---- Job Tagging ----

type jobTagSetXML struct {
	Tags []jobTagXML `xml:"Tag"`
}

type jobTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type putJobTaggingRequestXML struct {
	XMLName xml.Name     `xml:"PutJobTaggingRequest"`
	Tags    jobTagSetXML `xml:"Tags"`
}

type getJobTaggingResponseXML struct {
	XMLName xml.Name     `xml:"GetJobTaggingResult"`
	Tags    jobTagSetXML `xml:"Tags"`
}

func (h *Handler) handleGetJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	tags, err := h.Backend.GetJobTagging(accountID, jobID)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getJobTaggingResponseXML{}
	for k, v := range tags {
		resp.Tags.Tags = append(resp.Tags.Tags, jobTagXML{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

func (h *Handler) handlePutJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	var body putJobTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	tags := make(TagSet, len(body.Tags.Tags))
	for _, t := range body.Tags.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutJobTagging(accountID, jobID, tags); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutJobTaggingResult"`
	}{})
}

func (h *Handler) handleDeleteJobTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	jobID := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, pathJobPrefix), "/tagging")

	if err := h.Backend.DeleteJobTagging(accountID, jobID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

// ---- Access Grants Instance ----

type getAccessGrantsInstanceResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsInstanceResult"`
	AccessGrantsInstanceArn string   `xml:"AccessGrantsInstanceArn"`
	AccessGrantsInstanceID  string   `xml:"AccessGrantsInstanceId"`
	IdentityCenterArn       string   `xml:"IdentityCenterArn,omitempty"`
	CreatedAt               string   `xml:"CreatedAt,omitempty"`
}

func (h *Handler) handleGetAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	inst, err := h.Backend.GetAccessGrantsInstance(accountID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceResponseXML{
		AccessGrantsInstanceArn: inst.AccessGrantsInstanceArn,
		AccessGrantsInstanceID:  inst.AccessGrantsInstanceID,
		IdentityCenterArn:       inst.IdentityCenterArn,
		CreatedAt:               inst.CreatedAt,
	})
}

func (h *Handler) handleDeleteAccessGrantsInstance(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	if err := h.Backend.DeleteAccessGrantsInstance(accountID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type getAccessGrantsInstanceResourcePolicyResponseXML struct {
	XMLName xml.Name `xml:"GetAccessGrantsInstanceResourcePolicyResult"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handleGetAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	policy, err := h.Backend.GetAccessGrantsInstanceResourcePolicy(accountID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceResourcePolicyResponseXML{Policy: policy})
}

type putAccessGrantsInstanceResourcePolicyRequestXML struct {
	XMLName xml.Name `xml:"PutAccessGrantsInstanceResourcePolicyRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)

	var body putAccessGrantsInstanceResourcePolicyRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	h.Backend.PutAccessGrantsInstanceResourcePolicy(accountID, body.Policy)

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessGrantsInstanceResourcePolicyResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: body.Policy})
}

func (h *Handler) handleDeleteAccessGrantsInstanceResourcePolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	h.Backend.DeleteAccessGrantsInstanceResourcePolicy(accountID)

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleDissociateAccessGrantsIdentityCenter(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	h.Backend.DissociateAccessGrantsIdentityCenter(accountID)

	return c.String(http.StatusNoContent, "")
}

type getAccessGrantsInstanceForPrefixResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsInstanceForPrefixResult"`
	AccessGrantsInstanceArn string   `xml:"AccessGrantsInstanceArn"`
}

func (h *Handler) handleGetAccessGrantsInstanceForPrefix(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	prefix := c.Request().URL.Query().Get("s3prefix")

	inst, err := h.Backend.GetAccessGrantsInstanceForPrefix(accountID, prefix)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsInstanceForPrefixResponseXML{
		AccessGrantsInstanceArn: inst.AccessGrantsInstanceArn,
	})
}

// ---- Access Grants CRUD ----

type getAccessGrantResponseXML struct {
	XMLName           xml.Name `xml:"GetAccessGrantResult"`
	AccessGrantID     string   `xml:"AccessGrantId"`
	AccessGrantArn    string   `xml:"AccessGrantArn"`
	Permission        string   `xml:"Permission"`
	GranteeType       string   `xml:"Grantee>GranteeType"`
	GranteeIdentifier string   `xml:"Grantee>GranteeIdentifier"`
}

func (h *Handler) handleGetAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	grantID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantPrefix)

	grant, err := h.Backend.GetAccessGrant(accountID, grantID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantResponseXML{
		AccessGrantID:     grant.AccessGrantID,
		AccessGrantArn:    grant.AccessGrantArn,
		Permission:        grant.Permission,
		GranteeType:       grant.GranteeType,
		GranteeIdentifier: grant.GranteeIdentifier,
	})
}

func (h *Handler) handleDeleteAccessGrant(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	grantID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantPrefix)

	if err := h.Backend.DeleteAccessGrant(accountID, grantID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type listAccessGrantItemXML struct {
	AccessGrantID string `xml:"AccessGrantId"`
	Permission    string `xml:"Permission"`
	GrantScope    string `xml:"GrantScope,omitempty"`
}

type listAccessGrantsResponseXML struct {
	XMLName      xml.Name                 `xml:"ListAccessGrantsResult"`
	NextToken    string                   `xml:"NextToken,omitempty"`
	AccessGrants []listAccessGrantItemXML `xml:"AccessGrantsList>AccessGrant"`
}

func (h *Handler) handleListAccessGrants(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	locationScope := q.Get("locationscope")
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	grants := h.Backend.ListAccessGrants(accountID, locationScope)
	items := make([]listAccessGrantItemXML, 0, len(grants))
	for _, g := range grants {
		items = append(items, listAccessGrantItemXML{
			AccessGrantID: g.AccessGrantID,
			Permission:    g.Permission,
			GrantScope:    g.GrantScope,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, listAccessGrantsResponseXML{AccessGrants: page, NextToken: tok})
}

func (h *Handler) handleListCallerAccessGrants(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	grants := h.Backend.ListCallerAccessGrants(accountID)
	items := make([]listAccessGrantItemXML, 0, len(grants))
	for _, g := range grants {
		items = append(items, listAccessGrantItemXML{
			AccessGrantID: g.AccessGrantID,
			Permission:    g.Permission,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName      xml.Name                 `xml:"ListCallerAccessGrantsResult"`
		NextToken    string                   `xml:"NextToken,omitempty"`
		AccessGrants []listAccessGrantItemXML `xml:"AccessGrantsList>AccessGrant"`
	}{AccessGrants: page, NextToken: tok})
}

type getAccessGrantsLocationResponseXML struct {
	XMLName                 xml.Name `xml:"GetAccessGrantsLocationResult"`
	AccessGrantsLocationID  string   `xml:"AccessGrantsLocationId"`
	AccessGrantsLocationArn string   `xml:"AccessGrantsLocationArn"`
	LocationScope           string   `xml:"LocationScope"`
	IAMRoleArn              string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleGetAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	loc, err := h.Backend.GetAccessGrantsLocation(accountID, locationID)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsLocationResponseXML{
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

func (h *Handler) handleDeleteAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	if err := h.Backend.DeleteAccessGrantsLocation(accountID, locationID); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type updateAccessGrantsLocationRequestXML struct {
	XMLName    xml.Name `xml:"UpdateAccessGrantsLocationRequest"`
	IAMRoleArn string   `xml:"IAMRoleArn"`
}

func (h *Handler) handleUpdateAccessGrantsLocation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	locationID := strings.TrimPrefix(c.Request().URL.Path, pathAccessGrantsLocationPrefix)

	var body updateAccessGrantsLocationRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	loc, err := h.Backend.UpdateAccessGrantsLocation(accountID, locationID, body.IAMRoleArn)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAccessGrantsLocationResponseXML{
		AccessGrantsLocationID:  loc.AccessGrantsLocationID,
		AccessGrantsLocationArn: loc.AccessGrantsLocationArn,
		LocationScope:           loc.LocationScope,
		IAMRoleArn:              loc.IAMRoleArn,
	})
}

type listAccessGrantsLocationItemXML struct {
	AccessGrantsLocationID string `xml:"AccessGrantsLocationId"`
	LocationScope          string `xml:"LocationScope"`
}

func (h *Handler) handleListAccessGrantsLocations(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	locs := h.Backend.ListAccessGrantsLocations(accountID)
	items := make([]listAccessGrantsLocationItemXML, 0, len(locs))
	for _, loc := range locs {
		items = append(items, listAccessGrantsLocationItemXML{
			AccessGrantsLocationID: loc.AccessGrantsLocationID,
			LocationScope:          loc.LocationScope,
		})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName   xml.Name                          `xml:"ListAccessGrantsLocationsResult"`
		NextToken string                            `xml:"NextToken,omitempty"`
		Locations []listAccessGrantsLocationItemXML `xml:"AccessGrantsLocationsList>AccessGrantsLocation"`
	}{Locations: page, NextToken: tok})
}

func (h *Handler) handleGetDataAccess(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	target := c.Request().URL.Query().Get("target")
	permission := c.Request().URL.Query().Get("permission")

	url, err := h.Backend.GetDataAccess(accountID, target, permission)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName     xml.Name `xml:"GetDataAccessResult"`
		Credentials struct {
			AccessKeyID     string `xml:"AccessKeyId"`
			SecretAccessKey string `xml:"SecretAccessKey"`
		} `xml:"Credentials"`
		MatchedGrantTarget string `xml:"MatchedGrantTarget"`
	}{
		MatchedGrantTarget: url,
	})
}

// ---- Access Point Scope ----

func (h *Handler) handleGetAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	scope, err := h.Backend.GetAccessPointScope(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetAccessPointScopeResult"`
		Scope   string   `xml:"Scope,omitempty"`
	}{Scope: scope})
}

type putAccessPointScopeRequestXML struct {
	XMLName xml.Name `xml:"PutAccessPointScopeRequest"`
	Scope   string   `xml:"Scope"`
}

func (h *Handler) handlePutAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	var body putAccessPointScopeRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointScope(accountID, name, body.Scope); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointScopeResult"`
	}{})
}

func (h *Handler) handleDeleteAccessPointScope(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathAccessPointPrefix),
		"/scope",
	)

	if err := h.Backend.DeleteAccessPointScope(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleListAccessPointsForDirectoryBuckets(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	aps := h.Backend.ListAccessPointsForDirectoryBuckets(accountID)
	type apItem struct {
		Name           string `xml:"Name"`
		AccessPointArn string `xml:"AccessPointArn"`
		Bucket         string `xml:"Bucket"`
	}
	items := make([]apItem, 0, len(aps))
	for _, ap := range aps {
		items = append(
			items,
			apItem{Name: ap.Name, AccessPointArn: ap.AccessPointArn, Bucket: ap.Bucket},
		)
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName      xml.Name `xml:"ListAccessPointsForDirectoryBucketsResult"`
		NextToken    string   `xml:"NextToken,omitempty"`
		AccessPoints []apItem `xml:"AccessPointList>AccessPoint"`
	}{AccessPoints: page, NextToken: tok})
}

// ---- Object Lambda Access Points ----

func (h *Handler) handleGetAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	ap, err := h.Backend.GetAccessPointForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName                    xml.Name `xml:"GetAccessPointForObjectLambdaResult"`
		Name                       string   `xml:"Name"`
		ObjectLambdaAccessPointArn string   `xml:"ObjectLambdaAccessPointArn"`
	}{
		Name:                       ap.Name,
		ObjectLambdaAccessPointArn: ap.ObjectLambdaAccessPointArn,
	})
}

func (h *Handler) handleDeleteAccessPointForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix)

	if err := h.Backend.DeleteAccessPointForObjectLambda(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleListAccessPointsForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	aps := h.Backend.ListAccessPointsForObjectLambda(accountID)
	type olAPItem struct {
		Name                       string `xml:"Name"`
		ObjectLambdaAccessPointArn string `xml:"ObjectLambdaAccessPointArn"`
	}
	items := make([]olAPItem, 0, len(aps))
	for _, ap := range aps {
		items = append(
			items,
			olAPItem{Name: ap.Name, ObjectLambdaAccessPointArn: ap.ObjectLambdaAccessPointArn},
		)
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName                     xml.Name   `xml:"ListAccessPointsForObjectLambdaResult"`
		NextToken                   string     `xml:"NextToken,omitempty"`
		ObjectLambdaAccessPointList []olAPItem `xml:"ObjectLambdaAccessPointList>ObjectLambdaAccessPoint"`
	}{ObjectLambdaAccessPointList: page, NextToken: tok})
}

func (h *Handler) handleGetAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	policy, err := h.Backend.GetAccessPointPolicyForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetAccessPointPolicyForObjectLambdaResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: policy})
}

type putAccessPointPolicyForObjectLambdaRequestXML struct {
	XMLName xml.Name `xml:"PutAccessPointPolicyForObjectLambdaRequest"`
	Policy  string   `xml:"Policy"`
}

func (h *Handler) handlePutAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	var body putAccessPointPolicyForObjectLambdaRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointPolicyForObjectLambda(accountID, name, body.Policy); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointPolicyForObjectLambdaResult"`
	}{})
}

func (h *Handler) handleDeleteAccessPointPolicyForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policy",
	)

	if err := h.Backend.DeleteAccessPointPolicyForObjectLambda(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetAccessPointPolicyStatusForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/policyStatus",
	)

	isPublic, err := h.Backend.GetAccessPointPolicyStatusForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName  xml.Name `xml:"GetAccessPointPolicyStatusForObjectLambdaResult"`
		IsPublic bool     `xml:"PolicyStatus>IsPublic"`
	}{IsPublic: isPublic})
}

func (h *Handler) handleGetAccessPointConfigurationForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/configuration",
	)

	cfg, err := h.Backend.GetAccessPointConfigurationForObjectLambda(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName       xml.Name `xml:"GetAccessPointConfigurationForObjectLambdaResult"`
		Configuration string   `xml:"ObjectLambdaConfiguration,omitempty"`
	}{Configuration: cfg})
}

type putAPConfigForObjectLambdaRequestXML struct {
	XMLName       xml.Name `xml:"PutAccessPointConfigurationForObjectLambdaRequest"`
	Configuration string   `xml:"ObjectLambdaConfiguration"`
}

func (h *Handler) handlePutAccessPointConfigurationForObjectLambda(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathObjectLambdaPrefix),
		"/configuration",
	)

	var body putAPConfigForObjectLambdaRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutAccessPointConfigurationForObjectLambda(accountID, name, body.Configuration); err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"PutAccessPointConfigurationForObjectLambdaResult"`
	}{})
}

// ---- Outposts Bucket ----

func (h *Handler) handleGetBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	bucket, err := h.Backend.GetBucket(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName   xml.Name `xml:"GetBucketResult"`
		Bucket    string   `xml:"Bucket"`
		BucketArn string   `xml:"BucketArn"`
		Location  string   `xml:"OutpostId,omitempty"`
	}{
		Bucket:    bucket.Name,
		BucketArn: bucket.BucketArn,
		Location:  bucket.Location,
	})
}

func (h *Handler) handleDeleteBucket(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix)

	if err := h.Backend.DeleteBucket(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	config, err := h.Backend.GetBucketLifecycleConfiguration(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	if config == "" {
		return writeXML(c, struct {
			XMLName xml.Name `xml:"GetBucketLifecycleConfigurationResult"`
		}{})
	}

	return c.Blob(http.StatusOK, "application/xml", []byte(config))
}

func (h *Handler) handlePutBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	body := readBody(c)
	if err := h.Backend.PutBucketLifecycleConfiguration(accountID, bucketName, string(body)); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketLifecycleConfiguration(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/lifecycleconfiguration",
	)

	if err := h.Backend.DeleteBucketLifecycleConfiguration(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	policy, err := h.Backend.GetBucketPolicy(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketPolicyResult"`
		Policy  string   `xml:"Policy"`
	}{Policy: policy})
}

func (h *Handler) handlePutBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	body := readBody(c)
	if err := h.Backend.PutBucketPolicy(accountID, bucketName, string(body)); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/policy",
	)

	if err := h.Backend.DeleteBucketPolicy(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

type bucketTagXML struct {
	Key   string `xml:"Key"`
	Value string `xml:"Value"`
}

type getBucketTaggingResponseXML struct {
	XMLName xml.Name       `xml:"GetBucketTaggingResult"`
	Tags    []bucketTagXML `xml:"TagSet>Tag"`
}

func (h *Handler) handleGetBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	tags, err := h.Backend.GetBucketTagging(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	resp := getBucketTaggingResponseXML{}
	for k, v := range tags {
		resp.Tags = append(resp.Tags, bucketTagXML{Key: k, Value: v})
	}

	return writeXML(c, resp)
}

type putBucketTaggingRequestXML struct {
	XMLName xml.Name       `xml:"PutBucketTaggingRequest"`
	Tags    []bucketTagXML `xml:"Tagging>TagSet>Tag"`
}

func (h *Handler) handlePutBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	var body putBucketTaggingRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	tags := make(TagSet, len(body.Tags))
	for _, t := range body.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.PutBucketTagging(accountID, bucketName, tags); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

func (h *Handler) handleDeleteBucketTagging(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/tagging",
	)

	if err := h.Backend.DeleteBucketTagging(accountID, bucketName); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusNoContent, "")
}

func (h *Handler) handleGetBucketVersioning(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/versioning",
	)

	status, err := h.Backend.GetBucketVersioning(accountID, bucketName)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetBucketVersioningResult"`
		Status  string   `xml:"Status"`
	}{Status: status})
}

type putBucketVersioningRequestXML struct {
	XMLName xml.Name `xml:"PutBucketVersioningRequest"`
	Status  string   `xml:"VersioningConfiguration>Status"`
}

func (h *Handler) handlePutBucketVersioning(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	bucketName := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathBucketPrefix),
		"/versioning",
	)

	var body putBucketVersioningRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	if err := h.Backend.PutBucketVersioning(accountID, bucketName, body.Status); err != nil {
		return handleBackendError(c, err)
	}

	return c.String(http.StatusOK, "")
}

type listRegionalBucketItemXML struct {
	Bucket    string `xml:"Bucket"`
	BucketArn string `xml:"BucketArn"`
}

func (h *Handler) handleListRegionalBuckets(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	buckets := h.Backend.ListRegionalBuckets(accountID)
	items := make([]listRegionalBucketItemXML, 0, len(buckets))
	for _, b := range buckets {
		items = append(items, listRegionalBucketItemXML{Bucket: b.Name, BucketArn: b.BucketArn})
	}

	page, tok := s3cPaginate(items, nextToken, maxResults)

	return writeXML(c, struct {
		XMLName   xml.Name                    `xml:"ListRegionalBucketsResult"`
		NextToken string                      `xml:"NextToken,omitempty"`
		Buckets   []listRegionalBucketItemXML `xml:"RegionalBucketList>RegionalBucket"`
	}{Buckets: page, NextToken: tok})
}

// ---- MRAP ----

func (h *Handler) handleDescribeMultiRegionAccessPointOperation(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	requestToken := strings.TrimPrefix(c.Request().URL.Path, pathMRAPPrefix)

	req, err := h.Backend.DescribeMultiRegionAccessPointOperation(accountID, requestToken)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName        xml.Name `xml:"DescribeMultiRegionAccessPointOperationResult"`
		AsyncOperation struct {
			RequestTokenARN string `xml:"RequestTokenARN"`
			RequestStatus   string `xml:"RequestStatus"`
		} `xml:"AsyncOperation"`
	}{
		AsyncOperation: struct {
			RequestTokenARN string `xml:"RequestTokenARN"`
			RequestStatus   string `xml:"RequestStatus"`
		}{
			RequestTokenARN: req.RequestTokenARN,
			RequestStatus:   "SUCCEEDED",
		},
	})
}

func (h *Handler) handleGetMultiRegionAccessPointPolicy(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/policy",
	)

	policy, err := h.Backend.GetMultiRegionAccessPointPolicy(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetMultiRegionAccessPointPolicyResult"`
		Policy  struct {
			Established struct {
				Policy string `xml:"Policy"`
			} `xml:"Established"`
		} `xml:"Policy"`
	}{
		Policy: struct {
			Established struct {
				Policy string `xml:"Policy"`
			} `xml:"Established"`
		}{
			Established: struct {
				Policy string `xml:"Policy"`
			}{Policy: policy},
		},
	})
}

func (h *Handler) handleGetMultiRegionAccessPointPolicyStatus(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		pathMRAPPolicyStatusSuffix,
	)

	isPublic, err := h.Backend.GetMultiRegionAccessPointPolicyStatus(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName  xml.Name `xml:"GetMultiRegionAccessPointPolicyStatusResult"`
		IsPublic bool     `xml:"PolicyStatus>IsPublic"`
	}{IsPublic: isPublic})
}

func (h *Handler) handleGetMultiRegionAccessPointRoutes(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := strings.TrimSuffix(
		strings.TrimPrefix(c.Request().URL.Path, pathMRAPInstancePrefix),
		"/routes",
	)

	routes, err := h.Backend.GetMultiRegionAccessPointRoutes(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, struct {
		XMLName xml.Name `xml:"GetMultiRegionAccessPointRoutesResult"`
		Mrap    string   `xml:"Mrap"`
		Routes  string   `xml:"Routes,omitempty"`
	}{
		Mrap:   name,
		Routes: routes,
	})
}

// readBody reads and returns the request body as bytes.
func readBody(c *echo.Context) []byte {
	r := c.Request()
	if r.Body == nil {
		return nil
	}
	buf := make([]byte, 0, 4096) //nolint:mnd // 4KB initial buffer
	tmp := make([]byte, 512)     //nolint:mnd // 512B read chunk
	for {
		n, err := r.Body.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		if err != nil {
			break
		}
	}

	return buf
}
