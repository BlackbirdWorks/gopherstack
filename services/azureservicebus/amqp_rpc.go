package azureservicebus

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/amqp1"
)

// RPC status codes used by the $cbs and $management nodes.
const (
	rpcOK        int32 = 200
	rpcAccepted  int32 = 202
	rpcNoContent int32 = 204
	rpcBadReq    int32 = 400
	rpcUnauth    int32 = 401
	rpcNotFound  int32 = 404
	rpcLockLost  int32 = 410
	rpcNotImpl   int32 = 501
)

const noClaim = "no valid claim"

const sasTokenType = "servicebus.windows.net:sastoken"

// rpcSender delivers queued RPC responses to the client's reply link.
type rpcSender struct {
	ch chan *amqp1.Outgoing
}

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

// rpcReceiver handles request messages for $cbs or an entity's $management.
type rpcReceiver struct {
	c   *amqpConn
	ref EntityRef
	cbs bool
}

func (*rpcReceiver) Close() {}

func (r *rpcReceiver) Deliver(_ context.Context, in *amqp1.Incoming) amqp1.DeliveryState {
	req, err := amqp1.DecodeMessage(in.Payload)
	if err != nil || req.Properties == nil {
		return rejected(amqp1.CondDecode, "malformed rpc request")
	}

	var resp rpcResponse
	if r.cbs {
		resp = r.c.putToken(req)
	} else {
		resp = r.c.manage(r.ref, req)
	}

	r.reply(req, resp)

	return amqp1.DeliveryState{Kind: amqp1.StateAccepted}
}

type rpcResponse struct {
	body   any
	desc   string
	status int32
}

func (r *rpcReceiver) reply(req *amqp1.Message, resp rpcResponse) {
	ch := r.c.replyChan(req.Properties.ReplyTo, false)
	if ch == nil {
		return
	}

	codeKey, descKey := "statusCode", "statusDescription"
	if r.cbs {
		codeKey, descKey = "status-code", "status-description"
	}

	m := &amqp1.Message{
		Properties:            &amqp1.Properties{CorrelationID: req.Properties.MessageID, To: req.Properties.ReplyTo},
		ApplicationProperties: amqp1.Map{codeKey: resp.status, descKey: resp.desc},
		Value:                 resp.body,
		HasValue:              true,
	}

	payload, err := m.Encode()
	if err != nil {
		return
	}

	var tag [8]byte
	binary.BigEndian.PutUint64(tag[:], r.c.tagSeq.Add(1))

	select {
	case ch <- &amqp1.Outgoing{Tag: tag[:], Payload: payload, Settled: true}:
	default:
	}
}

// putToken implements the CBS put-token operation.
func (c *amqpConn) putToken(req *amqp1.Message) rpcResponse {
	if op, _ := req.ApplicationProperties["operation"].(string); op != "put-token" {
		return rpcResponse{status: rpcBadReq, desc: "unsupported cbs operation"}
	}

	token, _ := req.Value.(string)
	typ, _ := req.ApplicationProperties["type"].(string)

	if c.h.ValidateSAS && typ == sasTokenType && !c.validSAS(token) {
		return rpcResponse{status: rpcUnauth, desc: "invalid SharedAccessSignature"}
	}

	c.markClaimed()

	return rpcResponse{status: rpcAccepted, desc: "Accepted"}
}

func (c *amqpConn) validSAS(token string) bool {
	token = strings.TrimPrefix(token, sasPrefix)

	key := c.h.SASKeyValue
	if key == "" {
		key = DefaultKeyValue
	}

	raw := map[string]string{}

	for pair := range strings.SplitSeq(token, "&") {
		if k, v, found := strings.Cut(pair, "="); found {
			raw[k] = v
		}
	}

	expiry, err := strconv.ParseInt(raw["se"], 10, 64)
	if err != nil || time.Now().Unix() > expiry {
		return false
	}

	sig, err := url.QueryUnescape(raw["sig"])
	if err != nil {
		return false
	}

	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(raw["sr"] + "\n" + raw["se"]))

	return hmac.Equal([]byte(base64.StdEncoding.EncodeToString(mac.Sum(nil))), []byte(sig))
}
