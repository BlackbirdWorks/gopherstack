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
	AttributesData   string            `json:"AttributesData"`
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
	UnsubscribeAll   bool              `json:"UnsubscribeAll"`
}

func (h *Handler) handleCreateContact(c *echo.Context, contactListName string) (any, error) {
	var in createContactInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContact(
		contactListName, in.EmailAddress, in.AttributesData, in.TopicPreferences, in.UnsubscribeAll,
	); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// contact handlers

// contactListTopics returns the named contact list's Topics, or nil if the
// list can't be read -- GetContact/ListContacts already validate the list
// exists via their own backend call, so a lookup error here just means no
// TopicDefaultPreferences to report, not a request failure.
func (h *Handler) contactListTopics(contactListName string) []Topic {
	cl, err := h.Backend.GetContactList(contactListName)
	if err != nil {
		return nil
	}

	return cl.Topics
}

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

	return toContactOutput(c2, h.contactListTopics(contactListName)), nil
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
	AttributesData   string            `json:"AttributesData"`
	TopicPreferences []TopicPreference `json:"TopicPreferences"`
	UnsubscribeAll   bool              `json:"UnsubscribeAll"`
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

	if err := h.Backend.UpdateContact(
		contactListName, emailAddress, in.AttributesData, in.TopicPreferences, in.UnsubscribeAll,
	); err != nil {
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
// not applied: the AWS doc for FilteredStatus alone (without a TopicFilter)
// doesn't say what it filters against, and TopicFilter itself is a
// secondary refinement on top of that undocumented base behavior.
func (h *Handler) handleListContacts(c *echo.Context, contactListName string) (any, error) {
	var in listContactsInput

	_ = json.NewDecoder(c.Request().Body).Decode(&in)

	pg, err := h.Backend.ListContacts(contactListName, in.NextToken, int(in.PageSize))
	if err != nil {
		return nil, err
	}

	topics := h.contactListTopics(contactListName)

	items := make([]contactSummaryOutput, 0, len(pg.Data))
	for _, c2 := range pg.Data {
		items = append(items, toContactSummaryOutput(c2, topics))
	}

	return map[string]any{
		"Contacts":   items,
		keyNextToken: pg.Next,
	}, nil
}
