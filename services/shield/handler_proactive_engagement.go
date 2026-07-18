package shield

func (h *Handler) handleEnableProactiveEngagement() error {
	return h.Backend.EnableProactiveEngagement()
}

func (h *Handler) handleDisableProactiveEngagement() error {
	return h.Backend.DisableProactiveEngagement()
}
