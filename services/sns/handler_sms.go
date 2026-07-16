package sns

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleCheckIfPhoneNumberIsOptedOut(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("phoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "phoneNumber is required")
	}

	optedOut, err := h.Backend.CheckIfPhoneNumberIsOptedOut(phoneNumber)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CheckIfPhoneNumberIsOptedOutResponse{
		CheckIfPhoneNumberIsOptedOutResult: CheckIfPhoneNumberIsOptedOutResult{
			IsOptedOut: optedOut,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleCreateSMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	languageCode := c.Request().FormValue("LanguageCode")

	if err := h.Backend.CreateSMSSandboxPhoneNumber(phoneNumber, languageCode); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, CreateSMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleDeleteSMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	if err := h.Backend.DeleteSMSSandboxPhoneNumber(phoneNumber); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, DeleteSMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetSMSAttributes(c *echo.Context) error {
	names := parseMemberList(c, "attributes")

	attrs, err := h.Backend.GetSMSAttributes(names)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetSMSAttributesResponse{
		GetSMSAttributesResult: GetSMSAttributesResult{Attributes: attrsToEntries(attrs)},
		ResponseMetadata:       ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleGetSMSSandboxAccountStatus(c *echo.Context) error {
	inSandbox, err := h.Backend.GetSMSSandboxAccountStatus()
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, GetSMSSandboxAccountStatusResponse{
		GetSMSSandboxAccountStatusResult: GetSMSSandboxAccountStatusResult{IsInSandbox: inSandbox},
		ResponseMetadata:                 ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListOriginationNumbers(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")
	maxResults := parseIntParam(c, "MaxResults", 0)

	nums, token, err := h.Backend.ListOriginationNumbers(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListOriginationNumbersResponse{
		ListOriginationNumbersResult: ListOriginationNumbersResult{
			PhoneNumbers: nums,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListPhoneNumbersOptedOut(c *echo.Context) error {
	nextToken := c.Request().FormValue("nextToken")
	maxResults := parseIntParam(c, "maxResults", 0)

	nums, token, err := h.Backend.ListPhoneNumbersOptedOut(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, ListPhoneNumbersOptedOutResponse{
		ListPhoneNumbersOptedOutResult: ListPhoneNumbersOptedOutResult{
			PhoneNumbers: nums,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleListSMSSandboxPhoneNumbers(c *echo.Context) error {
	nextToken := c.Request().FormValue("NextToken")
	maxResults := parseIntParam(c, "MaxResults", 0)

	nums, token, err := h.Backend.ListSMSSandboxPhoneNumbers(nextToken, maxResults)
	if err != nil {
		return h.handleBackendError(c, err)
	}

	members := make([]XMLSandboxPhoneNumber, len(nums))
	for i, n := range nums {
		members[i] = XMLSandboxPhoneNumber{
			PhoneNumber:  n.PhoneNumber,
			LanguageCode: n.LanguageCode,
			Status:       n.Status,
		}
	}

	return h.writeXML(c, ListSMSSandboxPhoneNumbersResponse{
		ListSMSSandboxPhoneNumbersResult: ListSMSSandboxPhoneNumbersResult{
			PhoneNumbers: members,
			NextToken:    token,
		},
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleVerifySMSSandboxPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("PhoneNumber")
	otp := c.Request().FormValue("OneTimePassword")

	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "PhoneNumber is required")
	}

	if err := h.Backend.VerifySMSSandboxPhoneNumber(phoneNumber, otp); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, VerifySMSSandboxPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

func (h *Handler) handleOptInPhoneNumber(c *echo.Context) error {
	phoneNumber := c.Request().FormValue("phoneNumber")
	if phoneNumber == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameter", "phoneNumber is required")
	}

	if err := h.Backend.OptInPhoneNumber(phoneNumber); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, OptInPhoneNumberResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}

// parseSetSMSAttributesForm reads Attributes.entry.N.key/value pairs for SetSMSAttributes.
func parseSetSMSAttributesForm(c *echo.Context) map[string]string {
	attrs := make(map[string]string)

	for i := 1; ; i++ {
		key := c.Request().FormValue(fmt.Sprintf("attributes.entry.%d.key", i))
		if key == "" {
			return attrs
		}

		attrs[key] = c.Request().FormValue(fmt.Sprintf("attributes.entry.%d.value", i))
	}
}

func (h *Handler) handleSetSMSAttributes(c *echo.Context) error {
	attrs := parseSetSMSAttributesForm(c)

	if err := h.Backend.SetSMSAttributes(attrs); err != nil {
		return h.handleBackendError(c, err)
	}

	return h.writeXML(c, SetSMSAttributesResponse{
		ResponseMetadata: ResponseMetadata{RequestID: uuid.New().String()},
	})
}
