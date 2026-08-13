package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateTrustStore(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals)
	s3Bucket := vals.Get("CaCertificatesBundleS3Bucket")
	s3Key := vals.Get("CaCertificatesBundleS3Key")
	s3ObjectVersion := vals.Get("CaCertificatesBundleS3ObjectVersion")

	ts, err := h.Backend.CreateTrustStore(name, kvs, s3Bucket, s3Key, s3ObjectVersion)
	if err != nil {
		return nil, err
	}

	return &createTrustStoreResponse{
		Xmlns: elbv2XMLNS,
		Result: createTrustStoreResult{
			TrustStores: xmlTrustStoreList{
				Members: []xmlTrustStore{toXMLTrustStore(ts)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-create-ts-" + name},
	}, nil
}

func (h *Handler) handleDeleteTrustStore(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteTrustStore(tsArn); err != nil {
		return nil, err
	}

	return &deleteTrustStoreResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-ts"},
	}, nil
}

func (h *Handler) handleDeleteSharedTrustStoreAssociation(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	resourceArn := vals.Get("ResourceArn")
	if resourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteSharedTrustStoreAssociation(tsArn, resourceArn); err != nil {
		return nil, err
	}

	return &deleteSharedTrustStoreAssociationResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-delete-ts-assoc"},
	}, nil
}

func (h *Handler) handleAddTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	contents := parseTrustStoreRevocationContents(vals)

	added, err := h.Backend.AddTrustStoreRevocations(tsArn, contents)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRevocationContent, 0, len(added))
	for _, r := range added {
		members = append(members, xmlRevocationContent{
			RevocationID:           r.RevocationID,
			RevocationType:         r.RevocationType,
			NumberOfRevokedEntries: r.NumberOfRevokedEntries,
			TrustStoreArn:          tsArn,
		})
	}

	return &addTrustStoreRevocationsResponse{
		Xmlns: elbv2XMLNS,
		Result: addTrustStoreRevocationsResult{
			TrustStoreRevocations: xmlRevocationContentList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-ts-revocations"},
	}, nil
}

// parseTrustStoreRevocationContents extracts RevocationContents from form values.
// Real AWS's RevocationContent shape (verified against aws-sdk-go-v2
// types.RevocationContent) is S3Bucket/S3Key/S3ObjectVersion/RevocationType only --
// there is no plain/inline revocation-content field on the wire, and RevocationId is
// never client-supplied (AWS assigns it when it parses the uploaded file). Iteration
// is capped at 1000 to prevent potential DoS from malicious input.
func parseTrustStoreRevocationContents(vals url.Values) []RevocationContentInput {
	const maxRevocations = 1000
	contents := make([]RevocationContentInput, 0)

	for i := 1; i <= maxRevocations; i++ {
		prefix := fmt.Sprintf("RevocationContents.member.%d.", i)
		s3Bucket := vals.Get(prefix + "S3Bucket")
		s3Key := vals.Get(prefix + "S3Key")
		s3Version := vals.Get(prefix + "S3ObjectVersion")
		revType := vals.Get(prefix + "RevocationType")

		if s3Bucket == "" && s3Key == "" && s3Version == "" && revType == "" {
			break
		}

		if revType == "" {
			revType = "CRL"
		}

		contents = append(contents, RevocationContentInput{
			S3Bucket:        s3Bucket,
			S3Key:           s3Key,
			S3ObjectVersion: s3Version,
			RevocationType:  revType,
		})
	}

	return contents
}

func (h *Handler) handleDescribeTrustStoreAssociations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	assocs, err := h.Backend.DescribeTrustStoreAssociations(tsArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrustStoreAssociation, 0, len(assocs))
	for _, resArn := range assocs {
		members = append(members, xmlTrustStoreAssociation{ResourceArn: resArn})
	}

	return &describeTrustStoreAssociationsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoreAssociationsResult{
			TrustStoreAssociations: xmlTrustStoreAssociationList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts-assocs"},
	}, nil
}

func (h *Handler) handleDescribeTrustStores(vals url.Values) (any, error) {
	arns := parseMembers(vals, "TrustStoreArns.member")
	names := parseMembers(vals, "Names.member")

	stores, err := h.Backend.DescribeTrustStores(arns, names)
	if err != nil {
		return nil, err
	}

	members := make([]xmlTrustStore, 0, len(stores))
	for i := range stores {
		members = append(members, toXMLTrustStore(&stores[i]))
	}

	return &describeTrustStoresResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoresResult{
			TrustStores: xmlTrustStoreList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts"},
	}, nil
}

func (h *Handler) handleModifyTrustStore(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	s3Bucket := vals.Get("CaCertificatesBundleS3Bucket")
	s3Key := vals.Get("CaCertificatesBundleS3Key")
	s3ObjectVersion := vals.Get("CaCertificatesBundleS3ObjectVersion")

	ts, err := h.Backend.ModifyTrustStore(tsArn, s3Bucket, s3Key, s3ObjectVersion)
	if err != nil {
		return nil, err
	}

	return &modifyTrustStoreResponse{
		Xmlns: elbv2XMLNS,
		Result: modifyTrustStoreResult{
			TrustStores: xmlTrustStoreList{
				Members: []xmlTrustStore{toXMLTrustStore(ts)},
			},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-modify-ts"},
	}, nil
}

func (h *Handler) handleDescribeTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocations, err := h.Backend.DescribeTrustStoreRevocations(tsArn)
	if err != nil {
		return nil, err
	}

	members := make([]xmlRevocationContent, 0, len(revocations))
	for _, r := range revocations {
		members = append(members, xmlRevocationContent{
			RevocationID:           r.RevocationID,
			RevocationType:         r.RevocationType,
			NumberOfRevokedEntries: r.NumberOfRevokedEntries,
			TrustStoreArn:          tsArn,
		})
	}

	return &describeTrustStoreRevocationsResponse{
		Xmlns: elbv2XMLNS,
		Result: describeTrustStoreRevocationsResult{
			TrustStoreRevocations: xmlRevocationContentList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-describe-ts-revocations"},
	}, nil
}

func (h *Handler) handleRemoveTrustStoreRevocations(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocationIDs, err := parseRevocationIDs(vals, "RevocationIds.member")
	if err != nil {
		return nil, err
	}

	if len(revocationIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one RevocationId is required", ErrInvalidParameter)
	}

	if removeErr := h.Backend.RemoveTrustStoreRevocations(tsArn, revocationIDs); removeErr != nil {
		return nil, removeErr
	}

	return &removeTrustStoreRevocationsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-remove-ts-revocations"},
	}, nil
}

// parseRevocationIDs parses RevocationIds.member.N into int64 values, matching the
// real wire type (aws-sdk-go-v2 RemoveTrustStoreRevocationsInput.RevocationIds
// []int64). A non-numeric entry is a client error on the real API too.
func parseRevocationIDs(vals url.Values, prefix string) ([]int64, error) {
	raw := parseMembers(vals, prefix)
	ids := make([]int64, 0, len(raw))

	for _, r := range raw {
		id, err := strconv.ParseInt(r, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid RevocationId %q", ErrInvalidParameter, r)
		}

		ids = append(ids, id)
	}

	return ids, nil
}

func (h *Handler) handleGetTrustStoreCaCertificatesBundle(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
	}

	return &getTrustStoreCaCertificatesBundleResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getTrustStoreCaCertificatesBundleResult{Location: ""},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-ts-ca-bundle"},
	}, nil
}

func (h *Handler) handleGetTrustStoreRevocationContent(vals url.Values) (any, error) {
	tsArn := vals.Get("TrustStoreArn")
	if tsArn == "" {
		return nil, fmt.Errorf("%w: TrustStoreArn is required", ErrInvalidParameter)
	}

	revocationIDStr := vals.Get("RevocationId")
	if revocationIDStr == "" {
		return nil, fmt.Errorf("%w: RevocationId is required", ErrInvalidParameter)
	}
	revocationID, err := strconv.ParseInt(revocationIDStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid RevocationId %q", ErrInvalidParameter, revocationIDStr)
	}

	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
	}

	found := false
	for _, r := range stores[0].Revocations {
		if r.RevocationID == revocationID {
			found = true

			break
		}
	}
	if !found {
		return nil, ErrRevocationIDNotFound
	}

	return &getTrustStoreRevocationContentResponse{
		Xmlns:            elbv2XMLNS,
		Result:           getTrustStoreRevocationContentResult{Location: ""},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-get-ts-revocation-content"},
	}, nil
}

type xmlTrustStore struct {
	TrustStoreArn       string `xml:"TrustStoreArn"`
	Name                string `xml:"Name"`
	Status              string `xml:"Status"`
	NumberOfCaCerts     int    `xml:"NumberOfCaCerts"`
	TotalRevokedEntries int64  `xml:"TotalRevokedEntries"`
}

type xmlTrustStoreList struct {
	Members []xmlTrustStore `xml:"member"`
}

type createTrustStoreResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type createTrustStoreResponse struct {
	XMLName          xml.Name               `xml:"CreateTrustStoreResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           createTrustStoreResult `xml:"CreateTrustStoreResult"`
}

type deleteTrustStoreResponse struct {
	XMLName          xml.Name            `xml:"DeleteTrustStoreResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type deleteSharedTrustStoreAssociationResponse struct {
	XMLName          xml.Name            `xml:"DeleteSharedTrustStoreAssociationResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type addTrustStoreRevocationsResponse struct {
	XMLName          xml.Name                       `xml:"AddTrustStoreRevocationsResponse"`
	Xmlns            string                         `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata            `xml:"ResponseMetadata"`
	Result           addTrustStoreRevocationsResult `xml:"AddTrustStoreRevocationsResult"`
}

type xmlTrustStoreAssociation struct {
	ResourceArn string `xml:"ResourceArn"`
}

type xmlTrustStoreAssociationList struct {
	Members []xmlTrustStoreAssociation `xml:"member"`
}

type describeTrustStoreAssociationsResult struct {
	TrustStoreAssociations xmlTrustStoreAssociationList `xml:"TrustStoreAssociations"`
}

type describeTrustStoreAssociationsResponse struct {
	XMLName          xml.Name                             `xml:"DescribeTrustStoreAssociationsResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           describeTrustStoreAssociationsResult `xml:"DescribeTrustStoreAssociationsResult"`
}

func toXMLTrustStore(ts *TrustStore) xmlTrustStore {
	return xmlTrustStore{
		TrustStoreArn:       ts.TrustStoreArn,
		Name:                ts.Name,
		Status:              ts.Status,
		NumberOfCaCerts:     0,
		TotalRevokedEntries: int64(len(ts.Revocations)),
	}
}

type describeTrustStoresResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type describeTrustStoresResponse struct {
	XMLName          xml.Name                  `xml:"DescribeTrustStoresResponse"`
	Xmlns            string                    `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata       `xml:"ResponseMetadata"`
	Result           describeTrustStoresResult `xml:"DescribeTrustStoresResult"`
}

type modifyTrustStoreResult struct {
	TrustStores xmlTrustStoreList `xml:"TrustStores"`
}

type modifyTrustStoreResponse struct {
	XMLName          xml.Name               `xml:"ModifyTrustStoreResponse"`
	Xmlns            string                 `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata    `xml:"ResponseMetadata"`
	Result           modifyTrustStoreResult `xml:"ModifyTrustStoreResult"`
}

// xmlRevocationContent mirrors both aws-sdk-go-v2 types.TrustStoreRevocation (used in
// AddTrustStoreRevocationsResult) and types.DescribeTrustStoreRevocation (used in
// DescribeTrustStoreRevocationsResult) — both have the same RevocationId/RevocationType/
// NumberOfRevokedEntries/TrustStoreArn fields on the wire.
type xmlRevocationContent struct {
	RevocationType         string `xml:"RevocationType,omitempty"`
	TrustStoreArn          string `xml:"TrustStoreArn,omitempty"`
	RevocationID           int64  `xml:"RevocationId"`
	NumberOfRevokedEntries int64  `xml:"NumberOfRevokedEntries,omitempty"`
}

type xmlRevocationContentList struct {
	Members []xmlRevocationContent `xml:"member"`
}

// describeTrustStoreRevocationsResult's list field is named TrustStoreRevocations on the
// wire (verified against aws-sdk-go-v2's deserializer) — NOT "RevocationContents", which
// is only the request-side field name for AddTrustStoreRevocations.
type describeTrustStoreRevocationsResult struct {
	TrustStoreRevocations xmlRevocationContentList `xml:"TrustStoreRevocations"`
}

type describeTrustStoreRevocationsResponse struct {
	XMLName          xml.Name                            `xml:"DescribeTrustStoreRevocationsResponse"`
	Xmlns            string                              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                 `xml:"ResponseMetadata"`
	Result           describeTrustStoreRevocationsResult `xml:"DescribeTrustStoreRevocationsResult"`
}

// addTrustStoreRevocationsResult reports the revocation files that were added, matching
// AWS's AddTrustStoreRevocationsOutput.TrustStoreRevocations.
type addTrustStoreRevocationsResult struct {
	TrustStoreRevocations xmlRevocationContentList `xml:"TrustStoreRevocations"`
}

type removeTrustStoreRevocationsResponse struct {
	XMLName          xml.Name            `xml:"RemoveTrustStoreRevocationsResponse"`
	Xmlns            string              `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata `xml:"ResponseMetadata"`
}

type getTrustStoreCaCertificatesBundleResult struct {
	Location string `xml:"Location,omitempty"`
}

type getTrustStoreCaCertificatesBundleResponse struct {
	XMLName          xml.Name                                `xml:"GetTrustStoreCaCertificatesBundleResponse"`
	Xmlns            string                                  `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                     `xml:"ResponseMetadata"`
	Result           getTrustStoreCaCertificatesBundleResult `xml:"GetTrustStoreCaCertificatesBundleResult"`
}

type getTrustStoreRevocationContentResult struct {
	Location string `xml:"Location,omitempty"`
}

type getTrustStoreRevocationContentResponse struct {
	XMLName          xml.Name                             `xml:"GetTrustStoreRevocationContentResponse"`
	Xmlns            string                               `xml:"xmlns,attr"`
	ResponseMetadata xmlResponseMetadata                  `xml:"ResponseMetadata"`
	Result           getTrustStoreRevocationContentResult `xml:"GetTrustStoreRevocationContentResult"`
}
