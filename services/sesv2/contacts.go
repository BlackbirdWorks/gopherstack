package sesv2

import (
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// TopicPreference represents a contact topic preference.
type TopicPreference struct {
	TopicName          string `json:"topicName"`
	SubscriptionStatus string `json:"subscriptionStatus"`
}

// Contact represents a contact in a contact list.
type Contact struct {
	CreatedAt        time.Time         `json:"createdAt"`
	LastUpdatedAt    time.Time         `json:"lastUpdatedAt"`
	EmailAddress     string            `json:"emailAddress"`
	ContactListName  string            `json:"contactListName"`
	TopicPreferences []TopicPreference `json:"topicPreferences"`
	UnsubscribeAll   bool              `json:"unsubscribeAll"`
}

// CreateContact adds a contact to a contact list.
func (b *InMemoryBackend) CreateContact(
	contactListName, emailAddress string,
	topicPreferences []TopicPreference,
) (*Contact, error) {
	b.mu.Lock("CreateContact")
	defer b.mu.Unlock()

	if !b.contactLists.Has(contactListName) {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	key := contactKey(contactListName, emailAddress)
	if b.contacts.Has(key) {
		return nil, fmt.Errorf(
			"%w: contact %s already exists in list %s",
			ErrAlreadyExists, emailAddress, contactListName,
		)
	}

	prefs := make([]TopicPreference, len(topicPreferences))
	copy(prefs, topicPreferences)

	now := time.Now()
	c := &Contact{
		EmailAddress:     emailAddress,
		ContactListName:  contactListName,
		TopicPreferences: prefs,
		CreatedAt:        now,
		LastUpdatedAt:    now,
	}
	b.contacts.Put(c)

	cp := *c

	return &cp, nil
}

// GetContact retrieves a contact from a contact list.
func (b *InMemoryBackend) GetContact(contactListName, emailAddress string) (*Contact, error) {
	b.mu.RLock("GetContact")
	defer b.mu.RUnlock()

	if !b.contactLists.Has(contactListName) {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	c, ok := b.contacts.Get(contactKey(contactListName, emailAddress))
	if !ok {
		return nil, fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	cp := *c

	return &cp, nil
}

// DeleteContact removes a contact from a contact list.
func (b *InMemoryBackend) DeleteContact(contactListName, emailAddress string) error {
	b.mu.Lock("DeleteContact")
	defer b.mu.Unlock()

	if !b.contactLists.Has(contactListName) {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	key := contactKey(contactListName, emailAddress)
	if !b.contacts.Has(key) {
		return fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	b.contacts.Delete(key)

	return nil
}

// UpdateContact updates a contact's topic preferences.
func (b *InMemoryBackend) UpdateContact(
	contactListName, emailAddress string,
	topicPreferences []TopicPreference,
) error {
	b.mu.Lock("UpdateContact")
	defer b.mu.Unlock()

	if !b.contactLists.Has(contactListName) {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, contactListName)
	}

	c, ok := b.contacts.Get(contactKey(contactListName, emailAddress))
	if !ok {
		return fmt.Errorf(
			"%w: contact %s not found in list %s",
			ErrNotFound,
			emailAddress,
			contactListName,
		)
	}

	prefs := make([]TopicPreference, len(topicPreferences))
	copy(prefs, topicPreferences)
	c.TopicPreferences = prefs
	c.LastUpdatedAt = time.Now()

	return nil
}

// ListContacts returns all contacts in a contact list.
func (b *InMemoryBackend) ListContacts(
	contactListName, nextToken string,
	pageSize int,
) (page.Page[*Contact], error) {
	b.mu.RLock("ListContacts")
	defer b.mu.RUnlock()

	if !b.contactLists.Has(contactListName) {
		return page.Page[*Contact]{}, fmt.Errorf(
			"%w: contact list %s not found",
			ErrNotFound,
			contactListName,
		)
	}

	listContacts := slices.Clone(b.contactsByList.Get(contactListName))
	sort.Slice(listContacts, func(i, j int) bool {
		return listContacts[i].EmailAddress < listContacts[j].EmailAddress
	})

	items := make([]*Contact, 0, len(listContacts))
	for _, c := range listContacts {
		cp := *c
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems), nil
}
