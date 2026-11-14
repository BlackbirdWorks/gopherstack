package connect

import (
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	contactFieldEndpoint   = "CUSTOMER_ENDPOINT"
	contactFieldRecipients = "ADDITIONAL_EMAIL_RECIPIENTS"
	contactFieldSubject    = "EMAIL_SUBJECT"
)

// AssociatedContactSummary is one contact of a contact tree.
type AssociatedContactSummary struct {
	InitiationTimestamp *awstime.EpochTime `json:"InitiationTimestamp,omitempty"`
	DisconnectTimestamp *awstime.EpochTime `json:"DisconnectTimestamp,omitempty"`
	ContactID           string             `json:"ContactId"`
	ContactArn          string             `json:"ContactArn"`
	InitialContactID    string             `json:"InitialContactId"`
	PreviousContactID   string             `json:"PreviousContactId,omitempty"`
	RelatedContactID    string             `json:"RelatedContactId,omitempty"`
	Channel             string             `json:"Channel"`
	InitiationMethod    string             `json:"InitiationMethod"`
}

// UpdateContactScheduleInput is the UpdateContactSchedule request.
type UpdateContactScheduleInput struct {
	ScheduledTime *awstime.EpochTime
	ContactID     string
	InstanceID    string
}

// DeleteContactDataInput is the DeleteContactData request.
type DeleteContactDataInput struct {
	InstanceID    string `json:"-" path:"InstanceId"`
	ContactID     string `json:"-" path:"ContactId"`
	ContactFields []string
}

// ListAssociatedContacts lists the contacts that share the initial contact of the given one, oldest first.
func (b *Backend) ListAssociatedContacts(
	inst, contactID, token string, maxResults int32,
) ([]AssociatedContactSummary, string, error) {
	all, err := readInstance(b, "ListAssociatedContacts", inst, func(inst string) ([]AssociatedContactSummary, error) {
		c, err := b.contacts.get(inst, contactID)
		if err != nil {
			return nil, contactNotFoundErr(contactID)
		}

		var out []AssociatedContactSummary

		for _, r := range b.contacts.in(inst) {
			if r.Contact.InitialContactID != c.Contact.InitialContactID {
				continue
			}

			k := r.Contact
			out = append(out, AssociatedContactSummary{
				InitiationTimestamp: k.InitiationTimestamp, DisconnectTimestamp: k.DisconnectTimestamp,
				ContactID: k.ID, ContactArn: k.Arn, InitialContactID: k.InitialContactID,
				PreviousContactID: k.PreviousContactID, RelatedContactID: k.RelatedContactID,
				Channel: k.Channel, InitiationMethod: k.InitiationMethod,
			})
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	slices.SortStableFunc(all, func(a, c AssociatedContactSummary) int {
		return strings.Compare(tsKey(a.InitiationTimestamp), tsKey(c.InitiationTimestamp))
	})

	return paged(all, token, maxResults)
}

// UpdateContactSchedule moves the start time of a scheduled task contact.
func (b *Backend) UpdateContactSchedule(in *UpdateContactScheduleInput) (*EmptyOutput, error) {
	if in.ScheduledTime == nil {
		return nil, invalidParamErr("ScheduledTime is required")
	}

	b.mu.Lock("UpdateContactSchedule")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	now := b.now()
	if in.ScheduledTime.Before(now) || in.ScheduledTime.After(now.Add(maxScheduleAhead)) {
		return nil, invalidParamErr("ScheduledTime must be in the future and within 6 days")
	}

	if c.Contact.Channel != channelTask || c.Contact.ScheduledTimestamp == nil || !c.active() {
		return nil, invalidRequestErr("contact %q is not an active scheduled task", in.ContactID)
	}

	t := *in.ScheduledTime
	c.Contact.ScheduledTimestamp = &t
	b.touch(c)

	return &EmptyOutput{}, nil
}

// DeleteContactData redacts personal data fields from a terminated contact.
func (b *Backend) DeleteContactData(in *DeleteContactDataInput) (*EmptyOutput, error) {
	if len(in.ContactFields) == 0 {
		return nil, invalidParamErr("ContactFields is required")
	}

	for _, f := range in.ContactFields {
		if err := oneOf(
			"ContactFields",
			f,
			contactFieldEndpoint,
			contactFieldRecipients,
			contactFieldSubject,
		); err != nil {
			return nil, err
		}
	}

	b.mu.Lock("DeleteContactData")
	defer b.mu.Unlock()

	c, err := b.contactLocked(in.InstanceID, in.ContactID)
	if err != nil {
		return nil, err
	}

	if c.active() {
		return nil, invalidRequestErr("contact %q has not been disconnected", in.ContactID)
	}

	emailOnly := slices.ContainsFunc(in.ContactFields, func(f string) bool { return f != contactFieldEndpoint })
	if emailOnly && c.Contact.Channel != channelEmail {
		return nil, invalidRequestErr("ADDITIONAL_EMAIL_RECIPIENTS and EMAIL_SUBJECT apply to EMAIL contacts only")
	}

	for _, f := range in.ContactFields {
		switch f {
		case contactFieldEndpoint:
			c.Contact.CustomerEndpoint = nil
		case contactFieldRecipients:
			c.Contact.AdditionalEmailRecipients = nil
		case contactFieldSubject:
			for i := range c.Emails {
				c.Emails[i].Subject = ""
			}
		}
	}

	b.touch(c)

	return &EmptyOutput{}, nil
}

// referenceMembers maps a ReferenceType to its ReferenceSummary union member.
func referenceMembers() map[string]string {
	return map[string]string{
		"URL": "Url", "ATTACHMENT": "Attachment", "CONTACT_ANALYSIS": "ContactAnalysis", "NUMBER": "Number",
		"STRING": "String", "DATE": "Date", "EMAIL": "Email", "EMAIL_MESSAGE": "EmailMessage",
		"EMAIL_MESSAGE_PLAIN_TEXT": "EmailMessagePlainText", "EMAIL_MESSAGE_REDACTED": "EmailMessageRedacted",
		"EMAIL_MESSAGE_PLAIN_TEXT_REDACTED": "EmailMessagePlainTextRedacted",
	}
}

type storedReference struct {
	Type   string `json:"Type"`
	Value  string `json:"Value,omitempty"`
	Arn    string `json:"Arn,omitempty"`
	Status string `json:"Status,omitempty"`
}

func referenceSummary(name string, r storedReference) map[string]any {
	member := referenceMembers()[r.Type]
	body := map[string]any{"Name": name}

	switch {
	case strings.HasPrefix(r.Type, "EMAIL_MESSAGE"):
		body["Arn"] = r.Arn
	case r.Type == "ATTACHMENT" || r.Type == "CONTACT_ANALYSIS":
		body["Value"], body["Arn"], body["Status"] = r.Value, r.Arn, r.Status
	default:
		body["Value"] = r.Value
	}

	return map[string]any{member: body}
}

// ListContactReferences returns the references of a contact that have one of the requested types.
func (b *Backend) ListContactReferences(inst, contactID string, types []string) ([]map[string]any, error) {
	if len(types) == 0 {
		return nil, invalidParamErr("referenceTypes is required")
	}

	members := referenceMembers()

	for _, t := range types {
		if _, ok := members[t]; !ok {
			return nil, invalidParamErr("referenceTypes entry %q is not a reference type", t)
		}
	}

	return readInstance(b, "ListContactReferences", inst, func(inst string) ([]map[string]any, error) {
		c, err := b.contacts.get(inst, contactID)
		if err != nil {
			return nil, contactNotFoundErr(contactID)
		}

		var refs map[string]storedReference
		if len(c.References) > 0 {
			if err = json.Unmarshal(c.References, &refs, jsonx.V1()); err != nil {
				return nil, invalidRequestErr("the references of contact %q are not readable", contactID)
			}
		}

		names := make([]string, 0, len(refs))
		for n, r := range refs {
			if slices.Contains(types, r.Type) {
				names = append(names, n)
			}
		}

		slices.Sort(names)

		out := make([]map[string]any, 0, len(names))
		for _, n := range names {
			out = append(out, referenceSummary(n, refs[n]))
		}

		return out, nil
	})
}
