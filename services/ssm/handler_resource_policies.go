package ssm

func (h *Handler) ssmResourcePolicyOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DeleteResourcePolicy": jsonOp(h.Backend.DeleteResourcePolicy),
		"GetResourcePolicies":  jsonOp(h.Backend.GetResourcePolicies),
		"PutResourcePolicy":    jsonOp(h.Backend.PutResourcePolicy),
	}
}
