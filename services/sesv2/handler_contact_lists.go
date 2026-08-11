package sesv2

import (
	"encoding/json"
	"fmt"

	"github.com/labstack/echo/v5"
)

type createContactListInput struct {
	ContactListName string     `json:"ContactListName"`
	Description     string     `json:"Description"`
	Tags            []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateContactList(c *echo.Context) (any, error) {
	var in createContactListInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if _, err := h.Backend.CreateContactList(in.ContactListName, in.Description, tagsFromEntries(in.Tags)); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

// contact list handlers

func (h *Handler) handleGetContactList(name string) (any, error) {
	cl, err := h.Backend.GetContactList(name)
	if err != nil {
		return nil, err
	}

	return toContactListOutput(cl), nil
}

func (h *Handler) handleDeleteContactList(name string) (any, error) {
	if err := h.Backend.DeleteContactList(name); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

type updateContactListInput struct {
	Description string `json:"Description"`
}

func (h *Handler) handleUpdateContactList(c *echo.Context, name string) (any, error) {
	var in updateContactListInput

	if err := json.NewDecoder(c.Request().Body).Decode(&in); err != nil {
		return nil, fmt.Errorf("%w: invalid request body: %s", ErrInvalidInput, err.Error())
	}

	if err := h.Backend.UpdateContactList(name, in.Description); err != nil {
		return nil, err
	}

	return &emptyDeleteOutput{}, nil
}

func (h *Handler) handleListContactLists(c *echo.Context) (any, error) {
	nextToken := c.QueryParam("NextToken")
	pg := h.Backend.ListContactLists(nextToken, 0)

	items := make([]contactListSummaryOutput, 0, len(pg.Data))
	for _, cl := range pg.Data {
		items = append(items, toContactListSummaryOutput(cl))
	}

	return map[string]any{
		"ContactLists": items,
		keyNextToken:   pg.Next,
	}, nil
}
