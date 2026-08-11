package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sesv2"
)

// TestCreateContactList tests CreateContactList.
func TestCreateContactList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(*sesv2.InMemoryBackend)
		list    string
		wantErr bool
	}{
		{
			name:  "create_new",
			setup: func(*sesv2.InMemoryBackend) {},
			list:  "my-list",
		},
		{
			name: "duplicate",
			setup: func(b *sesv2.InMemoryBackend) {
				_, _ = b.CreateContactList("my-list", "", nil)
			},
			list:    "my-list",
			wantErr: true,
		},
		{
			name:    "empty_name",
			setup:   func(*sesv2.InMemoryBackend) {},
			list:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := sesv2.NewInMemoryBackend()
			tt.setup(backend)

			_, err := backend.CreateContactList(tt.list, "desc", nil)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, sesv2.ContactListCount(backend))
		})
	}
}

// TestContactListSeedHelper verifies AddContactListInternal.
func TestContactListSeedHelper(t *testing.T) {
	t.Parallel()

	backend := sesv2.NewInMemoryBackend()
	cl := backend.AddContactListInternal("seed-list")
	require.NotNil(t, cl)
	assert.Equal(t, "seed-list", cl.Name)
	assert.Equal(t, 1, sesv2.ContactListCount(backend))
}

// TestCreateContactListHTTP tests CreateContactList via HTTP.
func TestCreateContactListHTTP(t *testing.T) {
	t.Parallel()

	h, _ := newSESv2TestHandler(t)
	body := map[string]any{"ContactListName": "my-list", "Description": "desc"}
	rec := doReq(t, h, http.MethodPost, "/v2/email/contact-lists", body)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestGetContactList tests the GetContactList operation.
func TestGetContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "TestList",
	})

	rec := doRequest(t, h, http.MethodGet, "/v2/email/contact-lists/TestList", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteContactList tests the DeleteContactList operation.
func TestDeleteContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "DeleteList",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/contact-lists/DeleteList", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestUpdateContactList tests the UpdateContactList operation.
func TestUpdateContactList(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/contact-lists", map[string]any{
		"ContactListName": "UpdateList",
	})

	rec := doRequest(t, h, http.MethodPut, "/v2/email/contact-lists/UpdateList", map[string]any{
		"Description": "Updated description",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListContactLists tests the ListContactLists operation.
func TestListContactLists(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/contact-lists", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}
