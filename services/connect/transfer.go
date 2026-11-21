package connect

import (
	"context"
	"maps"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

const (
	maxContactTransfers = 11
	initiationTransfer  = "TRANSFER"
	flowAgentTransfer   = "AGENT_TRANSFER"
	flowQueueTransfer   = "QUEUE_TRANSFER"
)

// TransferContactInput is the TransferContact request.
type TransferContactInput struct {
	ClientToken   string
	ContactFlowID string `json:"ContactFlowId"`
	ContactID     string `json:"ContactId"`
	QueueID       string `json:"QueueId"`
	UserID        string `json:"UserId"`
	InstanceID    string `json:"InstanceId"`
}

// TransferContactOutput is the TransferContact response.
type TransferContactOutput struct {
	ContactArn string `json:"ContactArn"`
	ContactID  string `json:"ContactId"`
}

func (b *Backend) transferCount(inst string, c *contactRec) int {
	n := 0

	for prev := c; prev != nil && n <= maxContactTransfers; {
		if prev.Contact.InitiationMethod == initiationTransfer {
			n++
		}

		if prev.Contact.PreviousContactID == "" {
			break
		}

		prev, _ = b.contacts.table.Get(inst + "/" + prev.Contact.PreviousContactID)
	}

	return n
}

func (b *Backend) transferTarget(inst string, in *TransferContactInput) error {
	f, err := b.flows.get(inst, in.ContactFlowID)
	if err != nil {
		return err
	}

	if !slices.Contains([]string{endpointFlow, flowAgentTransfer, flowQueueTransfer}, f.Flow.Type) {
		return invalidRequestErr("ContactFlow %q of type %s cannot transfer contacts", in.ContactFlowID, f.Flow.Type)
	}

	if err = b.requireFlow(inst, in.ContactFlowID); err != nil {
		return err
	}

	if in.QueueID != "" && !b.queues.has(inst, in.QueueID) {
		return notFoundErr("Queue", in.QueueID)
	}

	if in.UserID != "" && !b.users.has(inst, userRefID(in.UserID)) {
		return notFoundErr("User", in.UserID)
	}

	return nil
}

// TransferContact hands an active TASK or EMAIL contact to another queue or agent through a flow.
func (b *Backend) TransferContact(ctx context.Context, in *TransferContactInput) (*TransferContactOutput, error) {
	if in.ContactID == "" || in.ContactFlowID == "" {
		return nil, invalidParamErr("ContactId and ContactFlowId are required")
	}

	if in.QueueID != "" && in.UserID != "" {
		return nil, invalidParamErr("QueueId and UserId cannot both be set")
	}

	return writeInstance(b, "TransferContact", in.InstanceID, func(inst string) (*TransferContactOutput, error) {
		c, err := b.contacts.get(inst, in.ContactID)
		if err != nil {
			return nil, contactNotFoundErr(in.ContactID)
		}

		if c.Contact.Channel != channelTask && c.Contact.Channel != emailValue {
			return nil, invalidRequestErr("only TASK and EMAIL contacts can be transferred")
		}

		if !c.active() {
			return nil, invalidRequestErr("contact %q is not active", in.ContactID)
		}

		if b.transferCount(inst, c) >= maxContactTransfers {
			return nil, limitErr("a contact cannot be transferred more than %d times", maxContactTransfers)
		}

		if err = b.transferTarget(inst, in); err != nil {
			return nil, err
		}

		return b.transferLocked(ctx, inst, c, in), nil
	})
}

func (b *Backend) transferLocked(
	ctx context.Context,
	inst string,
	c *contactRec,
	in *TransferContactInput,
) *TransferContactOutput {
	t := awstime.NewEpochTime(b.now())
	n := b.newContact(ctx, inst, c.Contact.Channel, initiationTransfer)
	n.FlowID, n.References, n.TaskTemplateID = in.ContactFlowID, c.References, c.TaskTemplateID
	n.Contact.InitialContactID = c.Contact.InitialContactID
	n.Contact.PreviousContactID = c.Contact.ID
	n.Contact.Name, n.Contact.Description = c.Contact.Name, c.Contact.Description
	n.Contact.Attributes = cloneTags(c.Contact.Attributes)
	n.Contact.SegmentAttributes = c.Contact.SegmentAttributes
	n.Contact.CustomerEndpoint, n.Contact.SystemEndpoint = c.Contact.CustomerEndpoint, c.Contact.SystemEndpoint

	if c.Contact.Tags != nil {
		n.Contact.Tags = make(map[string]string, len(c.Contact.Tags))
		maps.Copy(n.Contact.Tags, c.Contact.Tags)
	}

	switch {
	case in.QueueID != "":
		n.Contact.QueueInfo = &ContactQueueInfo{ID: in.QueueID, EnqueueTimestamp: &t}
	case in.UserID != "":
		n.Contact.AgentInfo = &ContactAgentInfo{ID: userRefID(in.UserID), ConnectedToAgentTimestamp: &t}
	}

	b.armExpiryLocked(n)
	b.contacts.put(n)

	c.Contact.DisconnectTimestamp, c.Contact.DisconnectReason = &t, reasonAPI
	c.TransferredAt = &t
	b.finishContactLocked(c, t)
	b.touch(c)

	return &TransferContactOutput{ContactArn: n.Contact.Arn, ContactID: n.Contact.ID}
}
