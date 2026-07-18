package macie2

import "net/http"

func parseAdministratorPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /administrator
		if method == http.MethodGet {
			return opGetAdministratorAccount, ""
		}
	case depthResource: // /administrator/disassociate
		if parts[1] == segDisassociate && method == http.MethodPost {
			return opDisassociateFromAdministratorAccount, ""
		}
	}

	return opUnknown, ""
}

func parseMasterPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /master
		if method == http.MethodGet {
			return opGetMasterAccount, ""
		}
	case depthResource: // /master/disassociate
		if parts[1] == segDisassociate && method == http.MethodPost {
			return opDisassociateFromMasterAccount, ""
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchAdministratorOps(op string) (any, int, bool, error) {
	switch op {
	case opGetAdministratorAccount:
		result, code, err := h.handleGetAdministratorAccount()

		return result, code, true, err

	case opDisassociateFromAdministratorAccount:
		code, err := h.handleDisassociateFromAdministratorAccount()

		return nil, code, true, err

	case opGetMasterAccount:
		result, code, err := h.handleGetMasterAccount()

		return result, code, true, err

	case opDisassociateFromMasterAccount:
		code, err := h.handleDisassociateFromMasterAccount()

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetAdministratorAccount() (any, int, error) {
	acct, err := h.Backend.GetAdministratorAccount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"administrator": acct}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromAdministratorAccount() (int, error) {
	if err := h.Backend.DisassociateFromAdministratorAccount(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleGetMasterAccount() (any, int, error) {
	acct, err := h.Backend.GetMasterAccount()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"master": acct}, http.StatusOK, nil
}

func (h *Handler) handleDisassociateFromMasterAccount() (int, error) {
	if err := h.Backend.DisassociateFromMasterAccount(); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
