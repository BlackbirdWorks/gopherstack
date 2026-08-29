package sesv2

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

type createContactInput struct {
	EmailAddress     string            `json:"EmailAddress"`
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
}

func (h *Handler) handleCreateContact(c *echo.Context, contactListName string) (any, error) {
	var in createContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContact(contactListName, in.EmailAddress, in.TopicPreferences); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// contact handlers

func (h *Handler) handleGetContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	c2, err := h.Backend.GetContact(contactListName, emailAddress)
	if err != nil {
		return nil, err
	}

	return toContactOutput(c2), nil
}

func (h *Handler) handleDeleteContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	if err := h.Backend.DeleteContact(contactListName, emailAddress); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateContactInput struct {
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
}

func (h *Handler) handleUpdateContact(c *echo.Context, contactListName string) (any, error) {
	segments := strings.Split(strings.TrimPrefix(c.Request().URL.Path, sesv2PathPrefix), "/")
	if len(segments) < 4 { //nolint:mnd // URL segment index is self-documenting in context
		return nil, fmt.Errorf("%w: invalid contact path", ErrInvalidInput)
	}

	emailAddress := segments[3]

	if decoded, err := url.PathUnescape(emailAddress); err == nil {
		emailAddress = decoded
	}

	var in updateContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateContact(contactListName, emailAddress, in.TopicPreferences); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type listContactsInput struct {
	NextToken string `json:"NextToken"`
	PageSize  int32  `json:"PageSize"`
}

// handleListContacts serves POST .../contacts/list. Real SES v2 carries
// NextToken/Filter/PageSize in the JSON body (not the query string) since
// ListContacts is a POST operation. Filter (FilteredStatus/TopicFilter) is
// not applied: TopicFilter.UseDefaultIfPreferenceUnavailable needs each
// topic's default subscription status, which ContactList (contact_lists.go)
// doesn't model, and the AWS doc for FilteredStatus alone (without a
// TopicFilter) doesn't say what it filters against.
func (h *Handler) handleListContacts(c *echo.Context, contactListName string) (any, error) {
	var in listContactsInput

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	pg, err := h.Backend.ListContacts(contactListName, in.NextToken, int(in.PageSize))
	if err != nil {
		return nil, err
	}

	items := make([]contactSummaryOutput, 0, len(pg.Data))
	for _, c2 := range pg.Data {
		items = append(items, toContactSummaryOutput(c2))
	}

	return map[string]any{
		"Contacts":   items,
		keyNextToken: pg.Next,
	}, nil
}
