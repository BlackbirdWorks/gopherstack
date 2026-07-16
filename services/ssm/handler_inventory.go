package ssm

func (h *Handler) ssmInventoryOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeleteInventory":                 jsonOp(h.Backend.DeleteInventory),
		"DescribeInventoryDeletions":      jsonOp(h.Backend.DescribeInventoryDeletions),
		"GetInventory":                    jsonOp(h.Backend.GetInventory),
		"GetInventorySchema":              jsonOp(h.Backend.GetInventorySchema),
		"ListComplianceItems":             jsonOp(h.Backend.ListComplianceItems),
		"ListComplianceSummaries":         jsonOp(h.Backend.ListComplianceSummaries),
		"ListInventoryEntries":            jsonOp(h.Backend.ListInventoryEntries),
		"ListResourceComplianceSummaries": jsonOp(h.Backend.ListResourceComplianceSummaries),
		"PutComplianceItems":              jsonOp(h.Backend.PutComplianceItems),
		"PutInventory":                    jsonOp(h.Backend.PutInventory),
	}
}
