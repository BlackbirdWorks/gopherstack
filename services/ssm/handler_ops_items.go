package ssm

func (h *Handler) ssmOpsItemOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeleteOpsItem":                  jsonOp(h.Backend.DeleteOpsItem),
		"DeleteOpsMetadata":              jsonOp(h.Backend.DeleteOpsMetadata),
		"DescribeOpsItems":               jsonOp(h.Backend.DescribeOpsItems),
		"DisassociateOpsItemRelatedItem": jsonOp(h.Backend.DisassociateOpsItemRelatedItem),
		"GetOpsItem":                     jsonOp(h.Backend.GetOpsItem),
		"GetOpsMetadata":                 jsonOp(h.Backend.GetOpsMetadata),
		"GetOpsSummary":                  jsonOp(h.Backend.GetOpsSummary),
		"ListOpsItemEvents":              jsonOp(h.Backend.ListOpsItemEvents),
		"ListOpsItemRelatedItems":        jsonOp(h.Backend.ListOpsItemRelatedItems),
		"ListOpsMetadata":                jsonOp(h.Backend.ListOpsMetadata),
		"UpdateOpsItem":                  jsonOp(h.Backend.UpdateOpsItem),
		"UpdateOpsMetadata":              jsonOp(h.Backend.UpdateOpsMetadata),
		"CreateOpsItem":                  jsonOp(h.Backend.CreateOpsItem),
		"CreateOpsMetadata":              jsonOp(h.Backend.CreateOpsMetadata),
		"AssociateOpsItemRelatedItem":    jsonOp(h.Backend.AssociateOpsItemRelatedItem),
	}
}
