package ssm

func (h *Handler) ssmInstanceOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DescribeEffectiveInstanceAssociations":    jsonOp(h.Backend.DescribeEffectiveInstanceAssociations),
		"DescribeInstanceAssociationsStatus":       jsonOp(h.Backend.DescribeInstanceAssociationsStatus),
		"DescribeInstanceInformation":              jsonOp(h.Backend.DescribeInstanceInformation),
		"DescribeInstancePatchStates":              jsonOp(h.Backend.DescribeInstancePatchStates),
		"DescribeInstancePatchStatesForPatchGroup": jsonOp(h.Backend.DescribeInstancePatchStatesForPatchGroup),
		"DescribeInstancePatches":                  jsonOp(h.Backend.DescribeInstancePatches),
		"DescribeInstanceProperties":               jsonOp(h.Backend.DescribeInstanceProperties),
		"ListNodes":                                jsonOp(h.Backend.ListNodes),
		"ListNodesSummary":                         jsonOp(h.Backend.ListNodesSummary),
	}
}
