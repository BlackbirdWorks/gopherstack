package connect

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func (h *Handler) extractionOps(ops map[string]restroute.OpFunc) {
	b := h.Backend

	ops["CreateExtractionDefinition"] = replay(h, "CreateExtractionDefinition",
		func(in *CreateExtractionDefinitionInput) string { return in.ClientToken }, b.CreateExtractionDefinition)
	ops["DescribeExtractionDefinition"] = describeOp("ExtractionDefinitionId", "ExtractionDefinition",
		b.DescribeExtractionDefinition)
	ops["ListExtractionDefinitions"] = listOp(
		"ExtractionDefinitionSummaryList",
		func(r *restroute.Request, t string, m int32) ([]ExtractionDefinitionSummary, string, error) {
			return b.ListExtractionDefinitions(instOf(r), t, m)
		},
	)
	ops["UpdateExtractionDefinition"] = typedCtx(b.UpdateExtractionDefinition)
	ops["DeleteExtractionDefinition"] = deleteOp("ExtractionDefinitionId", b.DeleteExtractionDefinition)

	ops["DescribeAuthenticationProfile"] = func(ctx context.Context, req *restroute.Request) (any, int, error) {
		v, err := b.DescribeAuthenticationProfile(ctx, instOf(req), req.Params["AuthenticationProfileId"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"AuthenticationProfile": v}, 0, nil
	}
	ops["ListAuthenticationProfiles"] = func(ctx context.Context, req *restroute.Request) (any, int, error) {
		return listOp(
			"AuthenticationProfileSummaryList",
			func(r *restroute.Request, t string, m int32) ([]AuthenticationProfileSummary, string, error) {
				return b.ListAuthenticationProfiles(ctx, instOf(r), t, m)
			},
		)(ctx, req)
	}
	ops["UpdateAuthenticationProfile"] = typedCtx(b.UpdateAuthenticationProfile)

	ops["ListAssociatedContacts"] = listOp(
		"ContactSummaryList",
		func(r *restroute.Request, t string, m int32) ([]AssociatedContactSummary, string, error) {
			return b.ListAssociatedContacts(instOf(r), r.Query.Get("contactId"), t, m)
		},
	)
	ops["UpdateContactSchedule"] = typed(b.UpdateContactSchedule)
	ops["DeleteContactData"] = typed(b.DeleteContactData)
	ops["ListContactReferences"] = func(_ context.Context, req *restroute.Request) (any, int, error) {
		refs, err := b.ListContactReferences(instOf(req), req.Params["ContactId"], req.Query["referenceTypes"])
		if err != nil {
			return nil, 0, err
		}

		return map[string]any{"ReferenceSummaryList": refs}, 0, nil
	}
}
