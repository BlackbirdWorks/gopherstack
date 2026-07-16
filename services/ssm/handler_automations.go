package ssm

func (h *Handler) ssmAutomationOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DescribeAutomationExecutions":     jsonOp(h.Backend.DescribeAutomationExecutions),
		"DescribeAutomationStepExecutions": jsonOp(h.Backend.DescribeAutomationStepExecutions),
		"GetAutomationExecution":           jsonOp(h.Backend.GetAutomationExecution),
		"GetCalendarState":                 jsonOp(h.Backend.GetCalendarState),
		"GetExecutionPreview":              jsonOp(h.Backend.GetExecutionPreview),
		"SendAutomationSignal":             jsonOp(h.Backend.SendAutomationSignal),
		"StartAutomationExecution":         jsonOp(h.Backend.StartAutomationExecution),
		"StartChangeRequestExecution":      jsonOp(h.Backend.StartChangeRequestExecution),
		"StartExecutionPreview":            jsonOp(h.Backend.StartExecutionPreview),
		"StopAutomationExecution":          jsonOp(h.Backend.StopAutomationExecution),
	}
}
