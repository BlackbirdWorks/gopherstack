package azureservicebus

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

const maxMgmtBatch = 1000

const maxSeqArg = int64(1) << 62

const (
	opRenewLock         = "com.microsoft:renew-lock"
	opPeek              = "com.microsoft:peek-message"
	opReceiveBySeq      = "com.microsoft:receive-by-sequence-number"
	opSchedule          = "com.microsoft:schedule-message"
	opCancelScheduled   = "com.microsoft:cancel-scheduled-message"
	opUpdateDisposition = "com.microsoft:update-disposition"
)

func ok(body any) rpcResponse { return rpcResponse{status: rpcOK, desc: "OK", body: body} }

func fail(status int32, err error) rpcResponse {
	return rpcResponse{status: status, desc: err.Error()}
}

func statusFor(err error) int32 {
	switch {
	case errors.Is(err, ErrMessageNotFound), errors.Is(err, ErrLockTokenMismatch), errors.Is(err, ErrMessageNotLocked):
		return rpcLockLost
	case errors.Is(err, ErrQueueNotFound), errors.Is(err, ErrTopicNotFound), errors.Is(err, ErrSubscriptionNotFound):
		return rpcNotFound
	}

	return rpcBadReq
}

// manage executes one $management request against ref.
func (c *amqpConn) manage(ref EntityRef, req *amqp1.Message) rpcResponse {
	if !c.authorized() {
		return rpcResponse{status: rpcUnauth, desc: noClaim}
	}

	op, _ := req.ApplicationProperties["operation"].(string)
	body, _ := req.Value.(amqp1.Map)

	switch op {
	case opPeek:
		return c.mgmtPeek(ref, body)
	case opRenewLock:
		return c.mgmtRenew(ref, body)
	case opReceiveBySeq:
		return c.mgmtReceiveBySeq(ref, body)
	case opSchedule:
		return c.mgmtSchedule(ref, body)
	case opCancelScheduled:
		return c.mgmtCancel(ref, body)
	case opUpdateDisposition:
		return c.mgmtDisposition(ref, body)
	}

	return rpcResponse{status: rpcNotImpl, desc: "unsupported management operation: " + op}
}

func int64Arg(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int32:
		return int64(x)
	case uint64:
		return int64(min(x, uint64(maxSeqArg)))
	case uint32:
		return int64(x)
	}

	return 0
}

func (c *amqpConn) mgmtPeek(ref EntityRef, body amqp1.Map) rpcResponse {
	if ref.Topic != "" && ref.Subscription == "" {
		return rpcResponse{status: rpcBadReq, desc: "peek requires a queue or subscription"}
	}

	from := int64Arg(body["from-sequence-number"])
	count := int(min(max(int64Arg(body["message-count"]), 1), maxMgmtBatch))

	msgs, err := c.b.Peek(ref, false, from, count)
	if err != nil {
		return fail(statusFor(err), err)
	}

	if len(msgs) == 0 {
		return rpcResponse{status: rpcNoContent, desc: "No Content"}
	}

	return c.messageList(msgs, false)
}

func (c *amqpConn) messageList(msgs []MessageInfo, withToken bool) rpcResponse {
	now := time.Now()
	list := make(amqp1.List, 0, len(msgs))

	for _, info := range msgs {
		m := toAMQP(info, now)

		if withToken {
			if u, ok := parseUUID(info.LockToken); ok {
				m.DeliveryAnnotations = amqp1.Map{amqp1.Symbol(annLockToken): u}
			}
		}

		enc, eerr := m.Encode()
		if eerr != nil {
			return fail(rpcBadReq, eerr)
		}

		entry := map[string]any{"message": enc}
		if withToken {
			if u, ok := parseUUID(info.LockToken); ok {
				entry["lock-token"] = u
			}
		}

		list = append(list, entry)
	}

	return ok(map[string]any{"messages": list})
}

func uuidArgs(v any) []string {
	us, _ := v.([]amqp1.UUID)
	out := make([]string, 0, len(us))

	for _, u := range us {
		out = append(out, uuidText(u))
	}

	return out
}

func (c *amqpConn) mgmtRenew(ref EntityRef, body amqp1.Map) rpcResponse {
	tokens := uuidArgs(body["lock-tokens"])
	expirations := make([]time.Time, 0, len(tokens))

	for _, t := range tokens {
		until, rerr := c.b.RenewLock(ref, t)
		if rerr != nil {
			return fail(statusFor(rerr), fmt.Errorf("the lock supplied is invalid: %w", rerr))
		}

		expirations = append(expirations, until.UTC())
	}

	return ok(map[string]any{"expirations": expirations})
}

func (c *amqpConn) mgmtReceiveBySeq(ref EntityRef, body amqp1.Map) rpcResponse {
	seqs, _ := body["sequence-numbers"].([]int64)
	if len(seqs) > maxMgmtBatch {
		return rpcResponse{status: rpcBadReq, desc: "too many sequence numbers"}
	}

	lock := int64Arg(body["receiver-settle-mode"]) == 1

	msgs, err := c.b.ReceiveDeferred(ref, seqs, lock)
	if err != nil {
		return fail(statusFor(err), err)
	}

	if len(msgs) == 0 {
		return rpcResponse{status: rpcNoContent, desc: "No Content"}
	}

	return c.messageList(msgs, lock)
}

func (c *amqpConn) mgmtSchedule(ref EntityRef, body amqp1.Map) rpcResponse {
	items, _ := body["messages"].(amqp1.List)
	if len(items) == 0 || len(items) > maxMgmtBatch {
		return rpcResponse{status: rpcBadReq, desc: "expected 1.." + strconv.Itoa(maxMgmtBatch) + " messages"}
	}

	seqs := make([]int64, 0, len(items))

	for _, it := range items {
		entry, _ := it.(amqp1.Map)
		raw, _ := entry["message"].([]byte)

		m, derr := amqp1.DecodeMessage(raw)
		if derr != nil {
			return fail(rpcBadReq, derr)
		}

		info, serr := c.b.Send(ref, fromAMQP(m))
		if serr != nil {
			return fail(statusFor(serr), serr)
		}

		seqs = append(seqs, info.SequenceNumber)
	}

	return ok(map[string]any{"sequence-numbers": seqs})
}

func (c *amqpConn) mgmtCancel(ref EntityRef, body amqp1.Map) rpcResponse {
	seqs, _ := body["sequence-numbers"].([]int64)
	if len(seqs) > maxMgmtBatch {
		return rpcResponse{status: rpcBadReq, desc: "too many sequence numbers"}
	}

	if cerr := c.b.CancelScheduled(ref, seqs); cerr != nil {
		return fail(rpcNotFound, cerr)
	}

	return ok(nil)
}

func (c *amqpConn) mgmtDisposition(ref EntityRef, body amqp1.Map) rpcResponse {
	status, _ := body["disposition-status"].(string)
	reason, _ := body["deadletter-reason"].(string)
	desc, _ := body["deadletter-description"].(string)

	for _, tok := range uuidArgs(body["lock-tokens"]) {
		info, dl, ferr := c.b.FindByLockToken(ref, tok)
		if ferr != nil {
			return fail(statusFor(ferr), ferr)
		}

		if aerr := c.applyDisposition(ref, status, dl, info, reason, desc); aerr != nil {
			return fail(statusFor(aerr), aerr)
		}
	}

	return ok(nil)
}

func (c *amqpConn) applyDisposition(
	ref EntityRef,
	status string,
	dl bool,
	info MessageInfo,
	reason, desc string,
) error {
	switch status {
	case "completed":
		return c.b.Complete(ref, dl, info.MessageID, info.LockToken)
	case "abandoned":
		return c.b.Abandon(ref, dl, info.MessageID, info.LockToken)
	case "defered":
		return c.b.Defer(ref, info.MessageID, info.LockToken)
	case "suspended":
		return c.b.DeadLetter(ref, info.MessageID, info.LockToken, reason, desc)
	}

	return fmt.Errorf("%w: unknown disposition %q", ErrInvalidEntityRef, status)
}
