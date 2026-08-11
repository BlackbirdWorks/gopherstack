package rds

import (
	"encoding/xml"
	"net/url"
	"time"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type xmlIntegration struct {
	IntegrationName        string                  `xml:"IntegrationName"`
	IntegrationArn         string                  `xml:"IntegrationArn,omitempty"`
	Status                 string                  `xml:"Status,omitempty"`
	SourceArn              string                  `xml:"SourceArn,omitempty"`
	TargetArn              string                  `xml:"TargetArn,omitempty"`
	DataFilter             string                  `xml:"DataFilter,omitempty"`
	IntegrationDescription string                  `xml:"Description,omitempty"`
	KMSKeyID               string                  `xml:"KMSKeyId,omitempty"`
	CreateTime             string                  `xml:"CreateTime,omitempty"`
	Tags                   xmlTagList              `xml:"Tags,omitempty"`
	Errors                 xmlIntegrationErrorList `xml:"Errors,omitempty"`
}

type xmlIntegrationList struct {
	Members []xmlIntegration `xml:"Integration"`
}

// xmlIntegrationError / xmlIntegrationErrorList mirror aws-sdk-go-v2's
// types.IntegrationError and its IntegrationErrorList wire shape
// (<Errors><IntegrationError><ErrorCode>...<ErrorMessage>...) verified
// against aws-sdk-go-v2/service/rds@v1.116.2/deserializers.go's
// awsAwsquery_deserializeDocumentIntegrationErrorList.
type xmlIntegrationError struct {
	ErrorCode    string `xml:"ErrorCode"`
	ErrorMessage string `xml:"ErrorMessage,omitempty"`
}

type xmlIntegrationErrorList struct {
	Members []xmlIntegrationError `xml:"IntegrationError"`
}

// CreateIntegrationOutput, DeleteIntegrationOutput, and ModifyIntegrationOutput are
// flat shapes in the real RDS API (no nested <Integration> wrapper — see the comment
// on createCustomDBEngineVersionResponse for why each field below repeats the full
// result-element chain). DescribeIntegrations is different: it returns a real list,
// so describeIntegrationsResponse below correctly keeps the xmlIntegrationList nesting.
//
// All three of Create/Delete/ModifyIntegrationOutput carry the SAME full field set
// as types.Integration itself (verified against
// aws-sdk-go-v2/service/rds@v1.116.2's api_op_*Integration.go output structs),
// including KMSKeyId/CreateTime/Tags/Errors, which were previously missing from
// every one of these three responses (not just Create) — see the field-diff notes
// in PARITY.md.
type createIntegrationResponse struct {
	XMLName                xml.Name                `xml:"CreateIntegrationResponse"`
	Xmlns                  string                  `xml:"xmlns,attr"`
	IntegrationName        string                  `xml:"CreateIntegrationResult>IntegrationName"`
	IntegrationArn         string                  `xml:"CreateIntegrationResult>IntegrationArn,omitempty"`
	Status                 string                  `xml:"CreateIntegrationResult>Status,omitempty"`
	SourceArn              string                  `xml:"CreateIntegrationResult>SourceArn,omitempty"`
	TargetArn              string                  `xml:"CreateIntegrationResult>TargetArn,omitempty"`
	DataFilter             string                  `xml:"CreateIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string                  `xml:"CreateIntegrationResult>Description,omitempty"`
	KMSKeyID               string                  `xml:"CreateIntegrationResult>KMSKeyId,omitempty"`
	CreateTime             string                  `xml:"CreateIntegrationResult>CreateTime,omitempty"`
	Tags                   xmlTagList              `xml:"CreateIntegrationResult>Tags,omitempty"`
	Errors                 xmlIntegrationErrorList `xml:"CreateIntegrationResult>Errors,omitempty"`
}

type deleteIntegrationResponse struct {
	XMLName                xml.Name                `xml:"DeleteIntegrationResponse"`
	Xmlns                  string                  `xml:"xmlns,attr"`
	IntegrationName        string                  `xml:"DeleteIntegrationResult>IntegrationName"`
	IntegrationArn         string                  `xml:"DeleteIntegrationResult>IntegrationArn,omitempty"`
	Status                 string                  `xml:"DeleteIntegrationResult>Status,omitempty"`
	SourceArn              string                  `xml:"DeleteIntegrationResult>SourceArn,omitempty"`
	TargetArn              string                  `xml:"DeleteIntegrationResult>TargetArn,omitempty"`
	DataFilter             string                  `xml:"DeleteIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string                  `xml:"DeleteIntegrationResult>Description,omitempty"`
	KMSKeyID               string                  `xml:"DeleteIntegrationResult>KMSKeyId,omitempty"`
	CreateTime             string                  `xml:"DeleteIntegrationResult>CreateTime,omitempty"`
	Tags                   xmlTagList              `xml:"DeleteIntegrationResult>Tags,omitempty"`
	Errors                 xmlIntegrationErrorList `xml:"DeleteIntegrationResult>Errors,omitempty"`
}

type describeIntegrationsResponse struct {
	XMLName      xml.Name           `xml:"DescribeIntegrationsResponse"`
	Xmlns        string             `xml:"xmlns,attr"`
	Marker       string             `xml:"DescribeIntegrationsResult>Marker,omitempty"`
	Integrations xmlIntegrationList `xml:"DescribeIntegrationsResult>Integrations"`
}

type modifyIntegrationResponse struct {
	XMLName                xml.Name                `xml:"ModifyIntegrationResponse"`
	Xmlns                  string                  `xml:"xmlns,attr"`
	IntegrationName        string                  `xml:"ModifyIntegrationResult>IntegrationName"`
	IntegrationArn         string                  `xml:"ModifyIntegrationResult>IntegrationArn,omitempty"`
	Status                 string                  `xml:"ModifyIntegrationResult>Status,omitempty"`
	SourceArn              string                  `xml:"ModifyIntegrationResult>SourceArn,omitempty"`
	TargetArn              string                  `xml:"ModifyIntegrationResult>TargetArn,omitempty"`
	DataFilter             string                  `xml:"ModifyIntegrationResult>DataFilter,omitempty"`
	IntegrationDescription string                  `xml:"ModifyIntegrationResult>Description,omitempty"`
	KMSKeyID               string                  `xml:"ModifyIntegrationResult>KMSKeyId,omitempty"`
	CreateTime             string                  `xml:"ModifyIntegrationResult>CreateTime,omitempty"`
	Tags                   xmlTagList              `xml:"ModifyIntegrationResult>Tags,omitempty"`
	Errors                 xmlIntegrationErrorList `xml:"ModifyIntegrationResult>Errors,omitempty"`
}

// toXMLIntegration converts intg to its wire shape. tags comes from the
// caller (typically h.Backend.ListTagsForResource(intg.IntegrationArn)),
// not from the Integration struct itself: like every other RDS resource in
// this emulator, tags live in the backend's shared ARN-keyed tags map
// rather than inline on the resource, so a pure *Integration -> xmlIntegration
// conversion can't reach them on its own.
func toXMLIntegration(intg *Integration, tags []Tag) xmlIntegration {
	x := xmlIntegration{
		IntegrationName:        intg.IntegrationName,
		IntegrationArn:         intg.IntegrationArn,
		SourceArn:              intg.SourceArn,
		TargetArn:              intg.TargetArn,
		Status:                 intg.Status,
		DataFilter:             intg.DataFilter,
		IntegrationDescription: intg.IntegrationDescription,
		KMSKeyID:               intg.KmsKeyID,
		Errors:                 toXMLIntegrationErrors(intg.Errors),
		Tags:                   xmlTagList{Members: tagsToKV(tags)},
	}
	if !intg.CreatedAt.IsZero() {
		x.CreateTime = intg.CreatedAt.UTC().Format(time.RFC3339)
	}

	return x
}

// tagsToKV converts the backend's []Tag (this service's internal tag
// representation) to []svcTags.KV (the shared XML wire shape used by every
// tag-bearing response in this handler package).
func tagsToKV(tags []Tag) []svcTags.KV {
	if len(tags) == 0 {
		return nil
	}

	kv := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		kv = append(kv, svcTags.KV{Key: t.Key, Value: t.Value})
	}

	return kv
}

func toXMLIntegrationErrors(errs []IntegrationError) xmlIntegrationErrorList {
	if len(errs) == 0 {
		return xmlIntegrationErrorList{}
	}

	members := make([]xmlIntegrationError, 0, len(errs))
	for _, e := range errs {
		members = append(members, xmlIntegrationError(e))
	}

	return xmlIntegrationErrorList{Members: members}
}

func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	name := vals.Get("IntegrationName")
	sourceARN := vals.Get("SourceArn")
	targetARN := vals.Get("TargetArn")
	kmsKeyID := vals.Get("KMSKeyId")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.CreateIntegration(name, sourceARN, targetARN, kmsKeyID, dataFilter, description)
	if err != nil {
		return nil, err
	}

	h.applyCreateTags(vals, intg.IntegrationArn)

	x := toXMLIntegration(intg, h.Backend.ListTagsForResource(intg.IntegrationArn))

	return &createIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        x.IntegrationName,
		IntegrationArn:         x.IntegrationArn,
		Status:                 x.Status,
		SourceArn:              x.SourceArn,
		TargetArn:              x.TargetArn,
		DataFilter:             x.DataFilter,
		IntegrationDescription: x.IntegrationDescription,
		KMSKeyID:               x.KMSKeyID,
		CreateTime:             x.CreateTime,
		Tags:                   x.Tags,
		Errors:                 x.Errors,
	}, nil
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	intg, err := h.Backend.DeleteIntegration(identifier)
	if err != nil {
		return nil, err
	}

	x := toXMLIntegration(intg, h.Backend.ListTagsForResource(intg.IntegrationArn))

	return &deleteIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        x.IntegrationName,
		IntegrationArn:         x.IntegrationArn,
		Status:                 x.Status,
		SourceArn:              x.SourceArn,
		TargetArn:              x.TargetArn,
		DataFilter:             x.DataFilter,
		IntegrationDescription: x.IntegrationDescription,
		KMSKeyID:               x.KMSKeyID,
		CreateTime:             x.CreateTime,
		Tags:                   x.Tags,
		Errors:                 x.Errors,
	}, nil
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")

	integrations, err := h.Backend.DescribeIntegrations(identifier)
	if err != nil {
		return nil, err
	}

	members, marker, err := paginateDescribe(
		vals, integrations,
		func(a, b Integration) bool { return a.IntegrationName < b.IntegrationName },
		func(intg Integration) xmlIntegration {
			return toXMLIntegration(&intg, h.Backend.ListTagsForResource(intg.IntegrationArn))
		},
	)
	if err != nil {
		return nil, err
	}

	return &describeIntegrationsResponse{
		Xmlns:        rdsXMLNS,
		Marker:       marker,
		Integrations: xmlIntegrationList{Members: members},
	}, nil
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	identifier := vals.Get("IntegrationIdentifier")
	dataFilter := vals.Get("DataFilter")
	description := vals.Get("Description")

	intg, err := h.Backend.ModifyIntegration(identifier, dataFilter, description)
	if err != nil {
		return nil, err
	}

	x := toXMLIntegration(intg, h.Backend.ListTagsForResource(intg.IntegrationArn))

	return &modifyIntegrationResponse{
		Xmlns:                  rdsXMLNS,
		IntegrationName:        x.IntegrationName,
		IntegrationArn:         x.IntegrationArn,
		Status:                 x.Status,
		SourceArn:              x.SourceArn,
		TargetArn:              x.TargetArn,
		DataFilter:             x.DataFilter,
		IntegrationDescription: x.IntegrationDescription,
		KMSKeyID:               x.KMSKeyID,
		CreateTime:             x.CreateTime,
		Tags:                   x.Tags,
		Errors:                 x.Errors,
	}, nil
}
