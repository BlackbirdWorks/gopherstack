package connect

import (
	"context"
	"net/mail"
	"slices"
)

const (
	maxEmailLen        = 255
	maxQueueEmails     = 49
	labelEmailAddress  = "EmailAddress"
	emailFieldAddress  = "emailaddress"
	emailFieldDisplay  = "displayname"
	emailTimestampForm = "2006-01-02T15:04:05.000Z"
)

// AliasConfiguration names an email address that forwards to a primary address.
type AliasConfiguration struct {
	EmailAddressID string `json:"EmailAddressId"`
}

type emailAddressRec struct {
	Tags        map[string]string `json:"tags,omitempty"`
	InstanceID  string            `json:"instanceId"`
	ID          string            `json:"id"`
	Arn         string            `json:"arn"`
	Address     string            `json:"address"`
	DisplayName string            `json:"displayName,omitempty"`
	Description string            `json:"description,omitempty"`
	Created     string            `json:"created"`
	Modified    string            `json:"modified"`
	AliasOf     string            `json:"aliasOf,omitempty"`
	Aliases     []string          `json:"aliases,omitempty"`
}

// EmailAddress is the DescribeEmailAddress response.
type EmailAddress struct {
	Tags                map[string]string    `json:"Tags,omitempty"`
	CreateTimestamp     string               `json:"CreateTimestamp"`
	Description         string               `json:"Description,omitempty"`
	DisplayName         string               `json:"DisplayName,omitempty"`
	EmailAddress        string               `json:"EmailAddress"`
	EmailAddressArn     string               `json:"EmailAddressArn"`
	EmailAddressID      string               `json:"EmailAddressId"`
	ModifiedTimestamp   string               `json:"ModifiedTimestamp"`
	AliasConfigurations []AliasConfiguration `json:"AliasConfigurations"`
}

// EmailAddressMetadata is the SearchEmailAddresses result shape.
type EmailAddressMetadata struct {
	Description         string               `json:"Description,omitempty"`
	DisplayName         string               `json:"DisplayName,omitempty"`
	EmailAddress        string               `json:"EmailAddress"`
	EmailAddressArn     string               `json:"EmailAddressArn"`
	EmailAddressID      string               `json:"EmailAddressId"`
	AliasConfigurations []AliasConfiguration `json:"AliasConfigurations"`
}

// EmailAddressSummary is the list shape of a queue's email address.
type EmailAddressSummary struct {
	Arn                    string `json:"Arn"`
	ID                     string `json:"Id"`
	IsDefaultOutboundEmail bool   `json:"IsDefaultOutboundEmail"`
}

type emailRef struct {
	EmailID string `json:"-" path:"EmailAddressId"`
	instPath
}

// CreateEmailAddressInput is the CreateEmailAddress request.
type CreateEmailAddressInput struct {
	Tags         map[string]string
	EmailAddress string
	Description  string
	DisplayName  string
	ClientToken  string
	instPath
}

// UpdateEmailAddressMetadataInput is the UpdateEmailAddressMetadata request.
type UpdateEmailAddressMetadataInput struct {
	Description *string
	DisplayName *string
	emailRef
}

// EmailAliasInput is the (Dis)AssociateEmailAddressAlias request.
type EmailAliasInput struct {
	AliasConfiguration *AliasConfiguration
	emailRef
}

type emailIDsOut struct {
	EmailAddressArn string `json:"EmailAddressArn"`
	EmailAddressID  string `json:"EmailAddressId"`
}

func checkEmailAddress(v string) error {
	if len(v) == 0 || len(v) > maxEmailLen {
		return invalidParamErr("EmailAddress must be 1-%d characters", maxEmailLen)
	}

	a, err := mail.ParseAddress(v)
	if err != nil || a.Address != v || a.Name != "" {
		return invalidParamErr("EmailAddress %q is not a valid email address", v)
	}

	return nil
}

func (b *Backend) emailTimestamp() string { return b.now().UTC().Format(emailTimestampForm) }

func (r *emailAddressRec) aliasConfigs() []AliasConfiguration {
	out := make([]AliasConfiguration, 0, len(r.Aliases))
	for _, id := range r.Aliases {
		out = append(out, AliasConfiguration{EmailAddressID: id})
	}

	return out
}

func (r *emailAddressRec) view() EmailAddress {
	return EmailAddress{
		Tags: cloneTags(r.Tags), CreateTimestamp: r.Created, Description: r.Description, DisplayName: r.DisplayName,
		EmailAddress: r.Address, EmailAddressArn: r.Arn, EmailAddressID: r.ID, ModifiedTimestamp: r.Modified,
		AliasConfigurations: r.aliasConfigs(),
	}
}

// CreateEmailAddress creates an email address; addresses are unique per instance.
func (b *Backend) CreateEmailAddress(ctx context.Context, in *CreateEmailAddressInput) (*emailIDsOut, error) {
	if err := firstError(
		checkEmailAddress(in.EmailAddress), checkDescription(in.Description), checkTags(in.Tags),
		checkOptionalName("DisplayName", in.DisplayName),
	); err != nil {
		return nil, err
	}

	return writeInstance(b, "CreateEmailAddress", in.InstanceID, func(inst string) (*emailIDsOut, error) {
		same := func(r *emailAddressRec) string { return r.Address }
		if nameExists(b.emails, inst, same, in.EmailAddress, "") {
			return nil, duplicateErr(labelEmailAddress, in.EmailAddress)
		}

		id := newID()
		now := b.emailTimestamp()
		rec := &emailAddressRec{
			InstanceID: inst, ID: id, Arn: b.buildARN(ctx, "instance/"+inst+"/email-address/"+id),
			Address: in.EmailAddress, DisplayName: in.DisplayName, Description: in.Description,
			Tags: cloneTags(in.Tags), Created: now, Modified: now,
		}
		b.emails.put(rec)

		return &emailIDsOut{EmailAddressArn: rec.Arn, EmailAddressID: id}, nil
	})
}

func checkOptionalName(field, v string) error {
	if v == "" {
		return nil
	}

	return checkName(field, v, maxNameLen)
}

// DescribeEmailAddress returns an email address with its aliases.
func (b *Backend) DescribeEmailAddress(inst, id string) (*EmailAddress, error) {
	return readInstance(b, "DescribeEmailAddress", inst, func(inst string) (*EmailAddress, error) {
		r, err := b.emails.get(inst, id)
		if err != nil {
			return nil, err
		}

		out := r.view()

		return &out, nil
	})
}

// UpdateEmailAddressMetadata changes the description or display name of an email address.
func (b *Backend) UpdateEmailAddressMetadata(in *UpdateEmailAddressMetadataInput) (*emailIDsOut, error) {
	if in.Description != nil {
		if err := checkDescription(*in.Description); err != nil {
			return nil, err
		}
	}

	if in.DisplayName != nil {
		if err := checkOptionalName("DisplayName", *in.DisplayName); err != nil {
			return nil, err
		}
	}

	return writeInstance(b, "UpdateEmailAddressMetadata", in.InstanceID, func(inst string) (*emailIDsOut, error) {
		r, err := b.emails.get(inst, in.EmailID)
		if err != nil {
			return nil, err
		}

		if in.Description != nil {
			r.Description = *in.Description
		}

		if in.DisplayName != nil {
			r.DisplayName = *in.DisplayName
		}

		r.Modified = b.emailTimestamp()

		return &emailIDsOut{EmailAddressArn: r.Arn, EmailAddressID: r.ID}, nil
	})
}

// emailInUse names what still references an email address, or "" when nothing does.
func (b *Backend) emailInUse(inst string, r *emailAddressRec) string {
	if len(r.Aliases) > 0 || r.AliasOf != "" {
		return "an alias relationship"
	}

	for _, q := range b.queues.in(inst) {
		if slices.Contains(q.EmailAddresses, r.ID) ||
			(q.Queue.OutboundEmailConfig != nil && q.Queue.OutboundEmailConfig.OutboundEmailAddressID == r.ID) {
			return "queue " + q.Queue.Name
		}
	}

	return ""
}

// DeleteEmailAddress removes an email address that no alias or queue references.
func (b *Backend) DeleteEmailAddress(inst, id string) (*empty, error) {
	return writeInstance(b, "DeleteEmailAddress", inst, func(inst string) (*empty, error) {
		r, err := b.emails.get(inst, id)
		if err != nil {
			return nil, err
		}

		if use := b.emailInUse(inst, r); use != "" {
			return nil, inUseErr("%s %q is used by %s", labelEmailAddress, r.Address, use)
		}

		return nil, b.emails.del(inst, id)
	})
}

// AssociateEmailAddressAlias makes an address forward to the primary address.
func (b *Backend) AssociateEmailAddressAlias(in *EmailAliasInput) (*empty, error) {
	if in.AliasConfiguration == nil || in.AliasConfiguration.EmailAddressID == "" {
		return nil, invalidParamErr("AliasConfiguration.EmailAddressId is required")
	}

	return writeInstance(b, "AssociateEmailAddressAlias", in.InstanceID, func(inst string) (*empty, error) {
		primary, err := b.emails.get(inst, in.EmailID)
		if err != nil {
			return nil, err
		}

		alias, err := b.emails.get(inst, in.AliasConfiguration.EmailAddressID)
		if err != nil {
			return nil, err
		}

		switch {
		case alias == primary:
			return nil, invalidRequestErr("an email address cannot be its own alias")
		case primary.AliasOf != "":
			return nil, resourceConflictErr("%s %q is itself an alias", labelEmailAddress, primary.Address)
		case alias.AliasOf != "" || len(alias.Aliases) > 0:
			return nil, resourceConflictErr("%s %q already is or has an alias", labelEmailAddress, alias.Address)
		}

		now := b.emailTimestamp()
		alias.AliasOf, alias.Modified = primary.ID, now
		primary.Aliases, primary.Modified = append(primary.Aliases, alias.ID), now

		return &empty{}, nil
	})
}

// DisassociateEmailAddressAlias removes an alias relationship.
func (b *Backend) DisassociateEmailAddressAlias(in *EmailAliasInput) (*empty, error) {
	if in.AliasConfiguration == nil || in.AliasConfiguration.EmailAddressID == "" {
		return nil, invalidParamErr("AliasConfiguration.EmailAddressId is required")
	}

	return writeInstance(b, "DisassociateEmailAddressAlias", in.InstanceID, func(inst string) (*empty, error) {
		primary, err := b.emails.get(inst, in.EmailID)
		if err != nil {
			return nil, err
		}

		id := in.AliasConfiguration.EmailAddressID
		if !slices.Contains(primary.Aliases, id) {
			return nil, notFoundErr("EmailAddressAlias", id)
		}

		now := b.emailTimestamp()
		primary.Aliases = slices.DeleteFunc(primary.Aliases, func(a string) bool { return a == id })
		primary.Modified = now

		if alias, ok := b.emails.table.Get(inst + "/" + id); ok {
			alias.AliasOf, alias.Modified = "", now
		}

		return &empty{}, nil
	})
}

// SearchEmailAddresses searches email addresses by address, display name and description.
func (b *Backend) SearchEmailAddresses(in *SearchRequest) (*SearchResult, error) {
	spec := searchSpec{fields: []string{emailFieldAddress, emailFieldDisplay, fieldDescription}}

	return runSearch(b, "SearchEmailAddresses", "EmailAddresses", in, b.emails, spec,
		func(r *emailAddressRec) searchSubject {
			return searchSubject{
				fields: map[string]string{
					emailFieldAddress: r.Address, emailFieldDisplay: r.DisplayName, fieldDescription: r.Description,
				},
				tags: r.Tags,
			}
		},
		func(r *emailAddressRec) EmailAddressMetadata {
			return EmailAddressMetadata{
				Description: r.Description, DisplayName: r.DisplayName, EmailAddress: r.Address,
				EmailAddressArn: r.Arn, EmailAddressID: r.ID, AliasConfigurations: r.aliasConfigs(),
			}
		})
}

// QueueEmailAddressesInput is the (Dis)AssociateQueueEmailAddresses request.
type QueueEmailAddressesInput struct {
	QueueID string `json:"-"                path:"QueueId"`
	instPath
	EmailAddressesConfig []struct {
		EmailAddressID string `json:"EmailAddressId"`
	}
	EmailAddressesID []string `json:"EmailAddressesId"`
}

func (b *Backend) queueForEmail(inst, queueID string) (*queueRec, error) {
	return b.queues.get(inst, queueID)
}

// AssociateQueueEmailAddresses adds "from" addresses to a queue; already associated ones are kept.
func (b *Backend) AssociateQueueEmailAddresses(in *QueueEmailAddressesInput) (*empty, error) {
	if len(in.EmailAddressesConfig) == 0 {
		return nil, invalidParamErr("EmailAddressesConfig is required")
	}

	return writeInstance(b, "AssociateQueueEmailAddresses", in.InstanceID, func(inst string) (*empty, error) {
		q, err := b.queueForEmail(inst, in.QueueID)
		if err != nil {
			return nil, err
		}

		next := slices.Clone(q.EmailAddresses)

		for _, c := range in.EmailAddressesConfig {
			if !b.emails.has(inst, c.EmailAddressID) {
				return nil, notFoundErr(labelEmailAddress, c.EmailAddressID)
			}

			if !slices.Contains(next, c.EmailAddressID) {
				next = append(next, c.EmailAddressID)
			}
		}

		if len(next) > maxQueueEmails {
			return nil, limitErr("a queue can have at most %d additional email addresses", maxQueueEmails)
		}

		q.EmailAddresses = next

		return &empty{}, nil
	})
}

// DisassociateQueueEmailAddresses removes "from" addresses from a queue.
func (b *Backend) DisassociateQueueEmailAddresses(in *QueueEmailAddressesInput) (*empty, error) {
	if len(in.EmailAddressesID) == 0 {
		return nil, invalidParamErr("EmailAddressesId is required")
	}

	return writeInstance(b, "DisassociateQueueEmailAddresses", in.InstanceID, func(inst string) (*empty, error) {
		q, err := b.queueForEmail(inst, in.QueueID)
		if err != nil {
			return nil, err
		}

		for _, id := range in.EmailAddressesID {
			if !slices.Contains(q.EmailAddresses, id) {
				return nil, notFoundErr("EmailAddress association", id)
			}
		}

		q.EmailAddresses = slices.DeleteFunc(q.EmailAddresses, func(id string) bool {
			return slices.Contains(in.EmailAddressesID, id)
		})

		return &empty{}, nil
	})
}

// ListQueueEmailAddresses pages through a queue's "from" addresses, the default outbound one first.
func (b *Backend) ListQueueEmailAddresses(
	inst, queueID, token string, maxResults int32,
) ([]EmailAddressSummary, Stamp, string, error) {
	type result struct {
		stamp Stamp
		items []EmailAddressSummary
	}

	res, err := readInstance(b, "ListQueueEmailAddresses", inst, func(inst string) (result, error) {
		q, err := b.queueForEmail(inst, queueID)
		if err != nil {
			return result{}, err
		}

		var items []EmailAddressSummary

		def := ""
		if c := q.Queue.OutboundEmailConfig; c != nil {
			def = c.OutboundEmailAddressID
		}

		if r, ok := b.emails.table.Get(inst + "/" + def); ok && def != "" {
			items = append(items, EmailAddressSummary{Arn: r.Arn, ID: r.ID, IsDefaultOutboundEmail: true})
		}

		for _, id := range q.EmailAddresses {
			if r, ok := b.emails.table.Get(inst + "/" + id); ok && id != def {
				items = append(items, EmailAddressSummary{Arn: r.Arn, ID: r.ID})
			}
		}

		return result{items: items, stamp: q.Queue.Stamp}, nil
	})
	if err != nil {
		return nil, Stamp{}, "", err
	}

	page, next, err := paged(res.items, token, maxResults)

	return page, res.stamp, next, err
}

func (b *Backend) checkOutboundEmail(inst string, c *OutboundEmailConfig) error {
	if c == nil || c.OutboundEmailAddressID == "" || b.emails.has(inst, c.OutboundEmailAddressID) {
		return nil
	}

	return notFoundErr(labelEmailAddress, c.OutboundEmailAddressID)
}
