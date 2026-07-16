package ssm

func (h *Handler) ssmPatchBaselineOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeletePatchBaseline":                      jsonOp(h.Backend.DeletePatchBaseline),
		"DeregisterPatchBaselineForPatchGroup":     jsonOp(h.Backend.DeregisterPatchBaselineForPatchGroup),
		"DescribeAvailablePatches":                 jsonOp(h.Backend.DescribeAvailablePatches),
		"DescribeEffectivePatchesForPatchBaseline": jsonOp(h.Backend.DescribeEffectivePatchesForPatchBaseline),
		"DescribePatchBaselines":                   jsonOp(h.Backend.DescribePatchBaselines),
		"DescribePatchGroupState":                  jsonOp(h.Backend.DescribePatchGroupState),
		"DescribePatchGroups":                      jsonOp(h.Backend.DescribePatchGroups),
		"DescribePatchProperties":                  jsonOp(h.Backend.DescribePatchProperties),
		"GetDefaultPatchBaseline":                  jsonOp(h.Backend.GetDefaultPatchBaseline),
		"GetDeployablePatchSnapshotForInstance":    jsonOp(h.Backend.GetDeployablePatchSnapshotForInstance),
		"GetPatchBaseline":                         jsonOp(h.Backend.GetPatchBaseline),
		"GetPatchBaselineForPatchGroup":            jsonOp(h.Backend.GetPatchBaselineForPatchGroup),
		"RegisterDefaultPatchBaseline":             jsonOp(h.Backend.RegisterDefaultPatchBaseline),
		"RegisterPatchBaselineForPatchGroup":       jsonOp(h.Backend.RegisterPatchBaselineForPatchGroup),
		"UpdatePatchBaseline":                      jsonOp(h.Backend.UpdatePatchBaseline),
		"CreatePatchBaseline":                      jsonOp(h.Backend.CreatePatchBaseline),
	}
}
