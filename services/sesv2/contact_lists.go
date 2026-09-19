package sesv2

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// Topic represents a contact list topic (types.Topic): a subscription
// category a contact can independently opt in or out of.
type Topic struct {
	TopicName                 string `json:"topicName"`
	DisplayName               string `json:"displayName"`
	DefaultSubscriptionStatus string `json:"defaultSubscriptionStatus"`
	Description               string `json:"description,omitempty"`
}

// ContactList represents a SES v2 contact list.
type ContactList struct {
	CreatedAt     time.Time         `json:"createdAt"`
	LastUpdatedAt time.Time         `json:"lastUpdatedAt"`
	Tags          map[string]string `json:"tags"`
	Name          string            `json:"name"`
	Description   string            `json:"description"`
	Topics        []Topic           `json:"topics,omitempty"`
}

// contactListARN builds the ARN for a contact list:
// arn:{partition}:ses:{region}:{account}:contact-list/{name}. Confirmed
// against terraform-provider-aws's resourceContactListRead
// (internal/service/sesv2/contact_list.go), which must construct this exact
// ARN to tag/import real contact lists.
func (b *InMemoryBackend) contactListARN(name string) string {
	return arn.Build("ses", b.region, b.accountID, "contact-list/"+name)
}

// validateTopics checks each Topic's required members (types.Topic:
// TopicName/DisplayName/DefaultSubscriptionStatus are required, Description
// is not).
func validateTopics(topics []Topic) error {
	for _, t := range topics {
		if t.TopicName == "" {
			return fmt.Errorf("%w: Topics[].TopicName is required", ErrInvalidInput)
		}

		if t.DisplayName == "" {
			return fmt.Errorf("%w: Topics[].DisplayName is required", ErrInvalidInput)
		}

		if t.DefaultSubscriptionStatus == "" {
			return fmt.Errorf("%w: Topics[].DefaultSubscriptionStatus is required", ErrInvalidInput)
		}
	}

	return nil
}

// CreateContactList creates a new contact list.
func (b *InMemoryBackend) CreateContactList(
	name, description string,
	tags map[string]string,
	topics []Topic,
) (*ContactList, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("%w: ContactListName is required", ErrInvalidInput)
	}

	if err := validateTopics(topics); err != nil {
		return nil, err
	}

	b.mu.Lock("CreateContactList")
	defer b.mu.Unlock()

	if b.contactLists.Has(name) {
		return nil, fmt.Errorf("%w: contact list %s already exists", ErrAlreadyExists, name)
	}

	now := time.Now()
	cl := &ContactList{
		Name:          name,
		Description:   description,
		Tags:          make(map[string]string),
		Topics:        slices.Clone(topics),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}

	if len(tags) > 0 {
		maps.Copy(cl.Tags, tags)
		b.putResourceTagsLocked(b.contactListARN(name), tags)
	}

	b.contactLists.Put(cl)

	cp := *cl

	return &cp, nil
}

// ---- contact list / contact ----

// GetContactList retrieves a contact list.
func (b *InMemoryBackend) GetContactList(name string) (*ContactList, error) {
	b.mu.RLock("GetContactList")
	defer b.mu.RUnlock()

	cl, ok := b.contactLists.Get(name)
	if !ok {
		return nil, fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	cp := *cl
	cp.Tags = b.liveTagsLocked(b.contactListARN(name))

	return &cp, nil
}

// DeleteContactList removes a contact list and all its contacts.
func (b *InMemoryBackend) DeleteContactList(name string) error {
	b.mu.Lock("DeleteContactList")
	defer b.mu.Unlock()

	if !b.contactLists.Has(name) {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	b.contactLists.Delete(name)
	delete(b.resourceTags, b.contactListARN(name))

	// Cascade-delete every contact of this contact list. slices.Clone the
	// index results first: deleting from b.contacts mutates
	// contactsByList's backing slice in place, which would otherwise
	// corrupt this very loop.
	for _, c := range slices.Clone(b.contactsByList.Get(name)) {
		b.contacts.Delete(contactKey(name, c.EmailAddress))
	}

	return nil
}

// UpdateContactList updates a contact list's description and topics.
func (b *InMemoryBackend) UpdateContactList(name, description string, topics []Topic) error {
	if err := validateTopics(topics); err != nil {
		return err
	}

	b.mu.Lock("UpdateContactList")
	defer b.mu.Unlock()

	cl, ok := b.contactLists.Get(name)
	if !ok {
		return fmt.Errorf("%w: contact list %s not found", ErrNotFound, name)
	}

	cl.Description = description
	cl.Topics = slices.Clone(topics)
	cl.LastUpdatedAt = time.Now()

	return nil
}

// ListContactLists returns all contact lists.
func (b *InMemoryBackend) ListContactLists(nextToken string, pageSize int) page.Page[*ContactList] {
	b.mu.RLock("ListContactLists")
	defer b.mu.RUnlock()

	snap := b.contactLists.Snapshot()

	items := make([]*ContactList, 0, len(snap))
	for _, cl := range snap {
		cp := *cl
		items = append(items, &cp)
	}

	return page.New(items, nextToken, pageSize, sesv2DefaultMaxItems)
}

// AddContactListInternal creates a contact list directly (for tests).
func (b *InMemoryBackend) AddContactListInternal(name string) *ContactList {
	b.mu.Lock("AddContactListInternal")
	defer b.mu.Unlock()

	now := time.Now()
	cl := &ContactList{
		Name:          name,
		Tags:          make(map[string]string),
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	b.contactLists.Put(cl)

	return cl
}
