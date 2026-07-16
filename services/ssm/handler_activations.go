package ssm

func (h *Handler) ssmActivationOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"CreateResourceDataSync":    jsonOp(h.Backend.CreateResourceDataSync),
		"DeleteActivation":          jsonOp(h.Backend.DeleteActivation),
		"DeleteResourceDataSync":    jsonOp(h.Backend.DeleteResourceDataSync),
		"DeregisterManagedInstance": jsonOp(h.Backend.DeregisterManagedInstance),
		"DescribeActivations":       jsonOp(h.Backend.DescribeActivations),
		"ListResourceDataSync":      jsonOp(h.Backend.ListResourceDataSync),
		"UpdateManagedInstanceRole": jsonOp(h.Backend.UpdateManagedInstanceRole),
		"UpdateResourceDataSync":    jsonOp(h.Backend.UpdateResourceDataSync),
		"CreateActivation":          jsonOp(h.Backend.CreateActivation),
	}
}
