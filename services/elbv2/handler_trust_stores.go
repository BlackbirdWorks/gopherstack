package elbv2

import (
	"encoding/xml"
	"fmt"
	"net/url"

	"github.com/google/uuid"
)

func (h *Handler) handleCreateTrustStore(vals url.Values) (any, error) {
	name := vals.Get("Name")
	if name == "" {
		return nil, fmt.Errorf("%w: Name is required", ErrInvalidParameter)
	}

	kvs := parseTagKVs(vals)

	ts, err := h.Backend.CreateTrustStore(name, kvs)
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

	revocations := parseTrustStoreRevocations(vals)

	if err := h.Backend.AddTrustStoreRevocations(tsArn, revocations); err != nil {
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

	return &addTrustStoreRevocationsResponse{
		Xmlns: elbv2XMLNS,
		Result: addTrustStoreRevocationsResult{
			TrustStoreRevocations: xmlRevocationContentList{Members: members},
		},
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-add-ts-revocations"},
	}, nil
}

// parseTrustStoreRevocations extracts RevocationContents from form values.
// Supports both plain RevocationId fields and S3-structured entries.
// Iteration is capped at 1000 to prevent potential DoS from malicious input.
func parseTrustStoreRevocations(vals url.Values) []TrustStoreRevocation {
	const maxRevocations = 1000
	revocations := make([]TrustStoreRevocation, 0)

	for i := 1; i <= maxRevocations; i++ {
		prefix := fmt.Sprintf("RevocationContents.member.%d.", i)
		// S3-structured entry fields.
		s3Bucket := vals.Get(prefix + "S3Bucket")
		s3Key := vals.Get(prefix + "S3Key")
		revType := vals.Get(prefix + "RevocationType")
		plain := vals.Get(fmt.Sprintf("RevocationContents.member.%d", i))

		if s3Bucket == "" && s3Key == "" && revType == "" && plain == "" {
			break
		}

		if revType == "" {
			revType = "CRL"
		}

		revID := plain
		if revID == "" {
			// S3-format entries have no plain value; assign a unique ID server-side
			// so callers can reference the revocation in RemoveTrustStoreRevocations.
			revID = "s3-" + uuid.New().String()
		}

		revocations = append(revocations, TrustStoreRevocation{
			RevocationID:   revID,
			RevocationType: revType,
		})
	}

	return revocations
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

	ts, err := h.Backend.ModifyTrustStore(tsArn, vals.Get("Name"))
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

	revocationIDs := parseMembers(vals, "RevocationIds.member")
	if len(revocationIDs) == 0 {
		return nil, fmt.Errorf("%w: at least one RevocationId is required", ErrInvalidParameter)
	}

	if err := h.Backend.RemoveTrustStoreRevocations(tsArn, revocationIDs); err != nil {
		return nil, err
	}

	return &removeTrustStoreRevocationsResponse{
		Xmlns:            elbv2XMLNS,
		ResponseMetadata: xmlResponseMetadata{RequestID: "elbv2-remove-ts-revocations"},
	}, nil
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

	stores, err := h.Backend.DescribeTrustStores([]string{tsArn}, nil)
	if err != nil {
		return nil, err
	}

	if len(stores) == 0 {
		return nil, ErrTrustStoreNotFound
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
	RevocationID           string `xml:"RevocationId"`
	RevocationType         string `xml:"RevocationType,omitempty"`
	TrustStoreArn          string `xml:"TrustStoreArn,omitempty"`
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
