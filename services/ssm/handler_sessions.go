package ssm

func (h *Handler) ssmSessionOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"DescribeSessions":    jsonOp(h.Backend.DescribeSessions),
		"GetAccessToken":      jsonOp(h.Backend.GetAccessToken),
		"GetConnectionStatus": jsonOp(h.Backend.GetConnectionStatus),
		"ResumeSession":       jsonOp(h.Backend.ResumeSession),
		"StartAccessRequest":  jsonOp(h.Backend.StartAccessRequest),
		"StartSession":        jsonOp(h.Backend.StartSession),
		"TerminateSession":    jsonOp(h.Backend.TerminateSession),
	}
}
