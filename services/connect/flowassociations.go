package connect

import "slices"

var flowAssocTypes = []string{ //nolint:gochecknoglobals // immutable lookup table
	"SMS_PHONE_NUMBER", "INBOUND_EMAIL", "OUTBOUND_EMAIL", "ANALYTICS_CONNECTOR", "WHATSAPP_MESSAGING_PHONE_NUMBER",
}

const (
	voicePhoneNumber = "VOICE_PHONE_NUMBER"
	maxBatchAssoc    = 100
)

type flowAssocRec struct {
	InstanceID   string `json:"instanceId"`
	ResourceID   string `json:"resourceId"`
	ResourceType string `json:"resourceType"`
	FlowID       string `json:"flowId"`
}

// FlowAssociationSummary is the list shape of a flow association.
type FlowAssociationSummary struct {
	FlowID       string `json:"FlowId"`
	ResourceID   string `json:"ResourceId"`
	ResourceType string `json:"ResourceType"`
}

// FlowAssocRequest carries every flow association request.
type FlowAssocRequest struct {
	FlowID         string `json:"FlowId"`
	ResourceID     string `json:"ResourceId"`
	ResourceType   string `json:"ResourceType"`
	PathResourceID string `json:"-"            path:"ResourceId"`
	PathType       string `json:"-"            path:"ResourceType"`
	QueryType      string `json:"-"                                query:"ResourceType"`
	NextToken      string `json:"-"                                query:"nextToken"`
	instPath
	ResourceIDs []string `json:"ResourceIds"`
	MaxResults  int32    `json:"-"                                query:"maxResults"`
}

func assocKey(resourceType, resourceID string) string { return resourceType + "|" + resourceID }

// AssociateFlow associates a flow with a resource; re-associating replaces the flow.
func (b *Backend) AssociateFlow(in *FlowAssocRequest) (*empty, error) {
	if err := firstError(
		oneOf("ResourceType", in.ResourceType, flowAssocTypes...), checkName("ResourceId", in.ResourceID, maxARNLen),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "AssociateFlow", in.InstanceID, func(inst string) (*empty, error) {
		if !b.flows.has(inst, in.FlowID) {
			return nil, notFoundErr("ContactFlow", in.FlowID)
		}

		b.flowAssocs.put(&flowAssocRec{
			InstanceID: inst, ResourceID: in.ResourceID, ResourceType: in.ResourceType, FlowID: in.FlowID,
		})

		return &empty{}, nil
	})
}

// GetFlowAssociation returns the flow associated with a resource.
func (b *Backend) GetFlowAssociation(in *FlowAssocRequest) (*FlowAssociationSummary, error) {
	if err := oneOf("ResourceType", in.PathType, flowAssocTypes...); err != nil {
		return nil, err
	}

	return readInstance(b, "GetFlowAssociation", in.InstanceID, func(inst string) (*FlowAssociationSummary, error) {
		a, err := b.flowAssocs.get(inst, assocKey(in.PathType, in.PathResourceID))
		if err != nil {
			return nil, err
		}

		return &FlowAssociationSummary{FlowID: a.FlowID, ResourceID: a.ResourceID, ResourceType: a.ResourceType}, nil
	})
}

// DisassociateFlow removes the flow association of a resource.
func (b *Backend) DisassociateFlow(in *FlowAssocRequest) (*empty, error) {
	if err := oneOf("ResourceType", in.PathType, flowAssocTypes...); err != nil {
		return nil, err
	}

	return writeInstance(b, "DisassociateFlow", in.InstanceID, func(inst string) (*empty, error) {
		return nil, b.flowAssocs.del(inst, assocKey(in.PathType, in.PathResourceID))
	})
}

// flowAssociations lists associations of the given type; VOICE_PHONE_NUMBER reads the claimed phone numbers.
func (b *Backend) flowAssociations(inst, resourceType string) []FlowAssociationSummary {
	var out []FlowAssociationSummary

	for _, a := range b.flowAssocs.in(inst) {
		if resourceType == "" || a.ResourceType == resourceType {
			out = append(
				out,
				FlowAssociationSummary{FlowID: a.FlowID, ResourceID: a.ResourceID, ResourceType: a.ResourceType},
			)
		}
	}

	if resourceType == "" || resourceType == voicePhoneNumber {
		for _, p := range b.phoneNumbers.All() {
			if p.InstanceID == inst && p.FlowID != "" {
				out = append(out, FlowAssociationSummary{
					FlowID: p.FlowID, ResourceID: p.Phone.PhoneNumberArn, ResourceType: voicePhoneNumber,
				})
			}
		}
	}

	slices.SortFunc(out, func(a, c FlowAssociationSummary) int {
		if a.ResourceType != c.ResourceType {
			return compareStr(a.ResourceType, c.ResourceType)
		}

		return compareStr(a.ResourceID, c.ResourceID)
	})

	return out
}

func compareStr(a, b string) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}

	return 0
}

func checkListAssocType(t string) error {
	if t == "" {
		return nil
	}

	return oneOf("ResourceType", t, voicePhoneNumber, "WHATSAPP_MESSAGING_PHONE_NUMBER", "INBOUND_EMAIL",
		"OUTBOUND_EMAIL", "ANALYTICS_CONNECTOR")
}

// ListFlowAssociations pages through flow associations, optionally of one resource type.
func (b *Backend) ListFlowAssociations(in *FlowAssocRequest) ([]FlowAssociationSummary, string, error) {
	if err := checkListAssocType(in.QueryType); err != nil {
		return nil, "", err
	}

	all, err := readInstance(
		b,
		"ListFlowAssociations",
		in.InstanceID,
		func(inst string) ([]FlowAssociationSummary, error) {
			return b.flowAssociations(inst, in.QueryType), nil
		},
	)
	if err != nil {
		return nil, "", err
	}

	return paged(all, in.NextToken, in.MaxResults)
}

// BatchGetFlowAssociation returns the associations of up to 100 resources of one type.
func (b *Backend) BatchGetFlowAssociation(in *FlowAssocRequest) ([]FlowAssociationSummary, error) {
	if err := checkListAssocType(in.ResourceType); err != nil {
		return nil, err
	}

	if len(in.ResourceIDs) == 0 || len(in.ResourceIDs) > maxBatchAssoc {
		return nil, invalidParamErr("ResourceIds must contain 1-%d IDs", maxBatchAssoc)
	}

	return readInstance(
		b,
		"BatchGetFlowAssociation",
		in.InstanceID,
		func(inst string) ([]FlowAssociationSummary, error) {
			out := []FlowAssociationSummary{}

			for _, a := range b.flowAssociations(inst, in.ResourceType) {
				if slices.Contains(in.ResourceIDs, a.ResourceID) {
					out = append(out, a)
				}
			}

			return out, nil
		},
	)
}
