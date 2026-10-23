package connect

import (
	"context"
	"fmt"
)

var defaultFlows = []struct { //nolint:gochecknoglobals // immutable seed data
	name, typ, description, prompt string
}{
	{"Default agent hold", "AGENT_HOLD", "Plays while an agent holds a contact", ""},
	{"Default agent transfer", "AGENT_TRANSFER", "Runs when an agent transfers a contact to another agent", ""},
	{"Default agent whisper", "AGENT_WHISPER", "Played to the agent before the customer is connected", ""},
	{"Default customer hold", "CUSTOMER_HOLD", "Plays while a customer is on hold", ""},
	{"Default customer queue", "CUSTOMER_QUEUE", "Plays while a customer waits in a queue", "Thank you for calling."},
	{"Default customer whisper", "CUSTOMER_WHISPER", "Played to the customer before the agent is connected", ""},
	{"Default outbound", "OUTBOUND_WHISPER", "Played to the customer on outbound calls", ""},
	{"Default queue transfer", "QUEUE_TRANSFER", "Runs when a contact is transferred to a queue", ""},
	{
		"Sample inbound flow (first contact experience)", "CONTACT_FLOW",
		"Greets the customer and disconnects", "Thanks for calling. Goodbye.",
	},
}

// defaultFlowContent builds a flow that optionally speaks a prompt and then ends.
func defaultFlowContent(prompt string) string {
	if prompt == "" {
		return `{"Version":"2019-10-30","StartAction":"end","Actions":[` +
			`{"Identifier":"end","Type":"EndFlowExecution","Parameters":{},"Transitions":{}}]}`
	}

	return fmt.Sprintf(`{"Version":"2019-10-30","StartAction":"say","Actions":[`+
		`{"Identifier":"say","Type":"MessageParticipant","Parameters":{"Text":%q},`+
		`"Transitions":{"NextAction":"end","Errors":[{"NextAction":"end","ErrorType":"NoMatchingError"}]}},`+
		`{"Identifier":"end","Type":"DisconnectParticipant","Parameters":{},"Transitions":{}}]}`, prompt)
}

// provisionFlows creates the contact flows Amazon Connect ships with every new instance.
func (b *Backend) provisionFlows(ctx context.Context, inst string) {
	for _, d := range defaultFlows {
		content := defaultFlowContent(d.prompt)
		id := newID()
		version := int64(1)
		b.flows.put(&flowRec{InstanceID: inst, Flow: ContactFlow{
			Stamp: b.stamp(ctx), Version: &version, Arn: b.buildARN(ctx, "instance/"+inst+"/contact-flow/"+id),
			Content: content, Description: d.description, FlowContentSha256: sha256Hex(content), ID: id, Name: d.name,
			State: flowActive, Status: flowPublished, Type: d.typ,
		}})
		b.snapshotInitial(ctx, inst, kindFlow, id, flowPublished)
	}
}
