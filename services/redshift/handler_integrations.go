package redshift

import (
	"encoding/xml"
	"net/url"
	"slices"
	"strconv"
	"time"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// ----- Integrations -----

type integrationXML struct {
	CreateTime      string       `xml:"CreateTime,omitempty"`
	IntegrationArn  string       `xml:"IntegrationArn"`
	IntegrationName string       `xml:"IntegrationName"`
	SourceArn       string       `xml:"SourceArn,omitempty"`
	TargetArn       string       `xml:"TargetArn,omitempty"`
	Status          string       `xml:"Status"`
	Description     string       `xml:"Description,omitempty"`
	KMSKeyID        string       `xml:"KMSKeyId,omitempty"`
	Tags            []svcTags.KV `xml:"Tags>Tag,omitempty"`
}

func integrationToXML(ig *Integration) integrationXML {
	x := integrationXML{
		IntegrationArn:  ig.IntegrationArn,
		IntegrationName: ig.IntegrationName,
		SourceArn:       ig.SourceArn,
		TargetArn:       ig.TargetArn,
		Status:          ig.Status,
		Description:     ig.Description,
		KMSKeyID:        ig.KmsKeyID,
		Tags:            tagMapToKVList(ig.Tags),
	}

	if !ig.CreateTime.IsZero() {
		x.CreateTime = ig.CreateTime.Format(time.RFC3339)
	}

	return x
}

type createIntegrationResponse struct {
	XMLName xml.Name       `xml:"CreateIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"CreateIntegrationResult"`
}

// handleCreateIntegration implements CreateIntegration. Real aws-sdk-go-v2 clients
// send the KMS key as "KMSKeyId" (confirmed against
// awsAwsquery_serializeOpDocumentCreateIntegrationInput) and tags as "TagList", not
// "KmsKeyId"/"Tags" -- both differ from this package's other Create* ops.
func (h *Handler) handleCreateIntegration(vals url.Values) (any, error) {
	tags := parseTagListPrefixed(vals, "TagList")

	ig, err := h.Backend.CreateIntegration(
		vals.Get("IntegrationName"),
		vals.Get("SourceArn"),
		vals.Get("TargetArn"),
		vals.Get("KMSKeyId"),
		vals.Get("Description"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type deleteIntegrationResponse struct {
	XMLName xml.Name       `xml:"DeleteIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"DeleteIntegrationResult"`
}

func (h *Handler) handleDeleteIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.DeleteIntegration(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	return &deleteIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}

type describeIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		Integrations []integrationXML `xml:"Integrations>Integration"`
	} `xml:"DescribeIntegrationsResult"`
}

// describeIntegrationsFilter mirrors one Filters.DescribeIntegrationsFilter.N entry:
// a Name (integration-arn/source-arn/source-types) and its Values.Value.M list
// (confirmed against awsAwsquery_serializeDocumentDescribeIntegrationsFilter,
// aws-sdk-go-v2/service/redshift@v1.65.4/serializers.go:10298).
type describeIntegrationsFilter struct {
	name   string
	values []string
}

// parseDescribeIntegrationsFilters extracts the indexed
// Filters.DescribeIntegrationsFilter.N.Name / .Values.Value.M filter list.
func parseDescribeIntegrationsFilters(vals url.Values) []describeIntegrationsFilter {
	var filters []describeIntegrationsFilter

	for i := 1; i <= maxListItems; i++ {
		prefix := "Filters.DescribeIntegrationsFilter." + strconv.Itoa(i) + "."

		name := vals.Get(prefix + "Name")
		if name == "" {
			break
		}

		filters = append(filters, describeIntegrationsFilter{
			name:   name,
			values: parseStringList(vals, prefix+"Values.Value."),
		})
	}

	return filters
}

// integrationMatchesFilters reports whether ig satisfies every filter. Real
// legal Name values are integration-arn, source-arn, source-types, status
// (DescribeIntegrationsFilterName, types/enums.go:194-202); source-types would
// need to classify SourceArn by AWS resource type, data this backend does not
// derive, so it is deliberately left unenforced (imposes no constraint) rather
// than guessed. status was previously in that same unenforced bucket by
// omission rather than by that deliberate reasoning -- Integration.Status is
// real, tracked data (integrations.go), so a status filter silently matched
// every integration instead of narrowing.
func integrationMatchesFilters(ig *Integration, filters []describeIntegrationsFilter) bool {
	for _, f := range filters {
		switch f.name {
		case "integration-arn":
			if !slices.Contains(f.values, ig.IntegrationArn) {
				return false
			}
		case "source-arn":
			if !slices.Contains(f.values, ig.SourceArn) {
				return false
			}
		case "status":
			if !slices.Contains(f.values, ig.Status) {
				return false
			}
		}
	}

	return true
}

func (h *Handler) handleDescribeIntegrations(vals url.Values) (any, error) {
	filters := parseDescribeIntegrationsFilters(vals)

	igs, err := h.Backend.DescribeIntegrations(vals.Get("IntegrationArn"))
	if err != nil {
		return nil, err
	}

	members := make([]integrationXML, 0, len(igs))

	for i := range igs {
		if !integrationMatchesFilters(&igs[i], filters) {
			continue
		}

		members = append(members, integrationToXML(&igs[i]))
	}

	resp := &describeIntegrationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.Integrations = members

	return resp, nil
}

// inboundIntegrationXML mirrors types.InboundIntegration (CreateTime, Errors,
// IntegrationArn, SourceArn, Status, TargetArn -- types/types.go:1160). It is a
// narrower shape than Integration/integrationXML: no IntegrationName,
// Description, KMSKeyId or Tags on the real wire, so integrationXML is not
// reused here.
type inboundIntegrationXML struct {
	CreateTime     string `xml:"CreateTime,omitempty"`
	IntegrationArn string `xml:"IntegrationArn"`
	SourceArn      string `xml:"SourceArn,omitempty"`
	TargetArn      string `xml:"TargetArn,omitempty"`
	Status         string `xml:"Status"`
}

func inboundIntegrationToXML(ig *Integration) inboundIntegrationXML {
	x := inboundIntegrationXML{
		IntegrationArn: ig.IntegrationArn,
		SourceArn:      ig.SourceArn,
		TargetArn:      ig.TargetArn,
		Status:         ig.Status,
	}

	if !ig.CreateTime.IsZero() {
		x.CreateTime = ig.CreateTime.Format(time.RFC3339)
	}

	return x
}

type describeInboundIntegrationsResponse struct {
	XMLName xml.Name `xml:"DescribeInboundIntegrationsResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
	Result  struct {
		InboundIntegrations []inboundIntegrationXML `xml:"InboundIntegrations>InboundIntegration"`
	} `xml:"DescribeInboundIntegrationsResult"`
}

// handleDescribeInboundIntegrations implements DescribeInboundIntegrations by
// filtering the same integrations store CreateIntegration/DescribeIntegrations
// populate -- every integration this backend can create already targets a
// Redshift resource, so it is real inbound-integration data, not fabricated.
func (h *Handler) handleDescribeInboundIntegrations(vals url.Values) (any, error) {
	integrationArn := vals.Get("IntegrationArn")
	targetArn := vals.Get("TargetArn")

	igs, err := h.Backend.DescribeIntegrations(integrationArn)
	if err != nil {
		return nil, err
	}

	members := make([]inboundIntegrationXML, 0, len(igs))

	for i := range igs {
		if targetArn != "" && igs[i].TargetArn != targetArn {
			continue
		}

		members = append(members, inboundIntegrationToXML(&igs[i]))
	}

	resp := &describeInboundIntegrationsResponse{Xmlns: redshiftXMLNS}
	resp.Result.InboundIntegrations = members

	return resp, nil
}

type modifyIntegrationResponse struct {
	XMLName xml.Name       `xml:"ModifyIntegrationResponse"`
	Xmlns   string         `xml:"xmlns,attr"`
	Result  integrationXML `xml:"ModifyIntegrationResult"`
}

func (h *Handler) handleModifyIntegration(vals url.Values) (any, error) {
	ig, err := h.Backend.ModifyIntegration(
		vals.Get("IntegrationArn"),
		vals.Get("Description"),
		vals.Get("IntegrationName"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyIntegrationResponse{
		Xmlns:  redshiftXMLNS,
		Result: integrationToXML(ig),
	}, nil
}
