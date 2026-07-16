package ssm

func (h *Handler) ssmAssociationOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeleteAssociation":                   jsonOp(h.Backend.DeleteAssociation),
		"DescribeAssociation":                 jsonOp(h.Backend.DescribeAssociation),
		"DescribeAssociationExecutionTargets": jsonOp(h.Backend.DescribeAssociationExecutionTargets),
		"DescribeAssociationExecutions":       jsonOp(h.Backend.DescribeAssociationExecutions),
		"ListAssociationVersions":             jsonOp(h.Backend.ListAssociationVersions),
		"ListAssociations":                    jsonOp(h.Backend.ListAssociations),
		"StartAssociationsOnce":               jsonOp(h.Backend.StartAssociationsOnce),
		"UpdateAssociation":                   jsonOp(h.Backend.UpdateAssociation),
		"UpdateAssociationStatus":             jsonOp(h.Backend.UpdateAssociationStatus),
		"CreateAssociation":                   jsonOp(h.Backend.CreateAssociation),
		"CreateAssociationBatch":              jsonOp(h.Backend.CreateAssociationBatch),
	}
}
