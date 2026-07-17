package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateContact tests CreateContact.
func TestCreateContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*sesv2.InMemoryBackend)
		listName     string
		emailAddress string
		wantErr      bool
	}{
		{
			name: "happy_path",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddContactListInternal("my-list")
			},
			listName:     "my-list",
			emailAddress: "user@example.com",
		},
		{
			name:         "list_not_found",
			setup:        func(*sesv2.InMemoryBackend) {},
			listName:     "no-such-list",
			emailAddress: "user@example.com",
			wantErr:      true,
		},
		{
			name: "duplicate_contact",
			setup: func(b *sesv2.InMemoryBackend) {
				b.AddContactListInternal("my-list")
				_, _ = b.CreateContact("my-list", "user@example.com", nil)
			},
			listName:     "my-list",
			emailAddress: "user@example.com",
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateContact(tt.listName, tt.emailAddress, nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.ContactCount(backend, tt.listName))
		})
	}
}

// TestCreateContactHTTP tests CreateContact via HTTP.
func TestCreateContactHTTP(t *testing.T) {
	t.Parallel()

	h, backend := newSESv2TestHandler(t)
	backend.AddContactListInternal("cl-1")

	body := map[string]any{"EmailAddress": "user@example.com"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/contact-lists/cl-1/contacts", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetContact tests the GetContact operation.
func TestGetContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListGet",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListGet/contacts",
		map[string]any{
			"EmailAddress": "contact-get@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodGet,
		"/v2/email/contact-lists/ContactOpsListGet/contacts/contact-get%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteContact tests the DeleteContact operation.
func TestDeleteContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListDel",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListDel/contacts",
		map[string]any{
			"EmailAddress": "contact-del@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodDelete,
		"/v2/email/contact-lists/ContactOpsListDel/contacts/contact-del%40example.com",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateContact tests the UpdateContact operation.
func TestUpdateContact(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListUpd",
	})

	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListUpd/contacts",
		map[string]any{
			"EmailAddress": "contact-upd@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPut,
		"/v2/email/contact-lists/ContactOpsListUpd/contacts/contact-upd%40example.com",
		map[string]any{
			"UnsubscribeAll": true,
		},
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListContacts tests the ListContacts operation.
func TestListContacts(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "ContactOpsListList",
	})

	// Add a contact so that b.contacts["ContactOpsListList"] is initialized.
	doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListList/contacts",
		map[string]any{
			"EmailAddress": "seed@example.com",
		},
	)

	rec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/contact-lists/ContactOpsListList/contacts/list",
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)
}
