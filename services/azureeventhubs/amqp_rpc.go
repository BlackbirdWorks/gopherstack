package azureeventhubs

import (
	"context"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const (
	rpcOK       int32 = 200
	rpcAccepted int32 = 202
	rpcBadReq   int32 = 400
	rpcUnauth   int32 = 401
	rpcNotFound int32 = 404
	rpcNotImpl  int32 = 501

	replyBuffer = 64

	mgmtTypeHub       = "com.microsoft:eventhub"
	mgmtTypePartition = "com.microsoft:partition"
)

type rpcSender struct{ ch chan *amqp1.Outgoing }

func (s *rpcSender) Next(ctx context.Context) (*amqp1.Outgoing, error) {
	select {
	case out := <-s.ch:
		return out, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (*rpcSender) Settle(*amqp1.Outgoing, amqp1.DeliveryState) {}
func (*rpcSender) Release(*amqp1.Outgoing)                     {}
func (*rpcSender) Close()                                      {}

// rpcReceiver handles request messages for $cbs or a hub's $management.
type rpcReceiver struct {
	c   *amqpConn
	ref HubRef
	cbs bool
}

func (*rpcReceiver) Close() {}

type rpcResponse struct {
	body   any
	desc   string
	status int32
}

func (r *rpcReceiver) Deliver(_ context.Context, in *amqp1.Incoming) amqp1.DeliveryState {
	req, err := amqp1.DecodeMessage(in.Payload)
	if err != nil || req.Properties == nil {
		return rejected(amqp1.CondDecode, "malformed rpc request")
	}

	if r.cbs {
		r.reply(req, r.c.putToken(req))
	} else {
		r.reply(req, r.c.manage(r.ref, req))
	}

	return amqp1.DeliveryState{Kind: amqp1.StateAccepted}
}

func (r *rpcReceiver) reply(req *amqp1.Message, resp rpcResponse) {
	ch := r.c.replyChan(req.Properties.ReplyTo, false)
	if ch == nil {
		return
	}

	m := &amqp1.Message{
		Properties:            &amqp1.Properties{CorrelationID: req.Properties.MessageID, To: req.Properties.ReplyTo},
		ApplicationProperties: amqp1.Map{"status-code": resp.status, "status-description": resp.desc},
		Value:                 resp.body,
		HasValue:              true,
	}

	payload, err := m.Encode()
	if err != nil {
		return
	}

	select {
	case ch <- &amqp1.Outgoing{Tag: r.c.nextTag(), Payload: payload, Settled: true}:
	default:
	}
}

func (c *amqpConn) putToken(req *amqp1.Message) rpcResponse {
	if op, _ := req.ApplicationProperties["operation"].(string); op != "put-token" {
		return rpcResponse{status: rpcBadReq, desc: "unsupported cbs operation"}
	}

	if !c.h.ValidateSAS {
		c.grant([]string{RightListen, RightSend, RightManage})

		return rpcResponse{status: rpcAccepted, desc: "Accepted"}
	}

	token, _ := req.Value.(string)
	typ, _ := req.ApplicationProperties["type"].(string)

	rights, ok := c.b.claimRights(token)
	if typ != sasTokenType || !ok {
		return rpcResponse{status: rpcUnauth, desc: "invalid SharedAccessSignature"}
	}

	c.grant(rights)

	return rpcResponse{status: rpcAccepted, desc: "Accepted"}
}

// manage serves the READ operations of a hub's $management node.
func (c *amqpConn) manage(ref HubRef, req *amqp1.Message) rpcResponse {
	if !c.authenticated() {
		return rpcResponse{status: rpcUnauth, desc: noClaim}
	}

	props := req.ApplicationProperties
	if op, _ := props["operation"].(string); op != "READ" {
		return rpcResponse{status: rpcNotImpl, desc: "unsupported management operation: " + op}
	}

	name, _ := props["name"].(string)
	if ref.Hub != "" && name != ref.Hub {
		return rpcResponse{status: rpcNotFound, desc: "The messaging entity '" + name + "' could not be found."}
	}

	ref, found := c.b.ResolveHub(name)
	if !found {
		return rpcResponse{status: rpcNotFound, desc: "The messaging entity '" + name + "' could not be found."}
	}

	switch typ, _ := props["type"].(string); typ {
	case mgmtTypeHub:
		return c.hubProperties(ref)
	case mgmtTypePartition:
		pid, _ := props["partition"].(string)

		return c.partitionProperties(ref, pid)
	default:
		return rpcResponse{status: rpcNotImpl, desc: "unsupported management type: " + typ}
	}
}

func (c *amqpConn) hubProperties(ref HubRef) rpcResponse {
	info, err := c.b.Hub(ref)
	if err != nil {
		return rpcResponse{status: rpcNotFound, desc: err.Error()}
	}

	return rpcResponse{status: rpcOK, desc: "OK", body: map[string]any{
		"name":                  info.Name,
		"created_at":            info.CreatedAt,
		"partition_count":       int32(len(info.PartitionIDs)), //nolint:gosec // bounded by MaxPartitionCount
		"partition_ids":         info.PartitionIDs,
		"georeplication_factor": int32(1),
	}}
}

func (c *amqpConn) partitionProperties(ref HubRef, pid string) rpcResponse {
	n, err := strconv.Atoi(pid)
	if err != nil {
		return rpcResponse{status: rpcBadReq, desc: "invalid partition id"}
	}

	p, err := c.b.PartitionInfo(ref, n)
	if err != nil {
		return rpcResponse{status: rpcNotFound, desc: err.Error()}
	}

	return rpcResponse{status: rpcOK, desc: "OK", body: map[string]any{
		"name":                          p.Hub,
		"partition":                     p.Partition,
		"begin_sequence_number":         p.BeginSequence,
		"last_enqueued_sequence_number": p.LastSequence,
		"last_enqueued_offset":          strconv.FormatInt(p.LastOffset, 10),
		"last_enqueued_time_utc":        p.LastEnqueuedTime,
		"is_partition_empty":            p.Empty,
	}}
}
