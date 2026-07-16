package ssm

func (h *Handler) ssmServiceSettingOps() map[string]ssmActionFn {
	return map[string]ssmActionFn{
		"GetServiceSetting":    jsonOp(h.Backend.GetServiceSetting),
		"ResetServiceSetting":  jsonOp(h.Backend.ResetServiceSetting),
		"UpdateServiceSetting": jsonOp(h.Backend.UpdateServiceSetting),
	}
}
