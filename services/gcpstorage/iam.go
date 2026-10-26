package gcpstorage

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"

	"cloud.google.com/go/iam/apiv1/iampb"
	"google.golang.org/genproto/googleapis/type/expr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type conditionJSON struct {
	Expression  string `json:"expression"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

func (b *InMemoryBackend) nextIAMEtag() []byte {
	b.iamSeq++

	var seq [8]byte

	binary.BigEndian.PutUint64(seq[:], b.iamSeq)

	return []byte(base64.StdEncoding.EncodeToString(seq[:]))
}

func policyToPB(p IAMPolicy) *iampb.Policy {
	out := &iampb.Policy{Etag: []byte(p.Etag), Version: int32(p.Version)} //nolint:gosec // policy versions are tiny

	for _, bd := range p.Bindings {
		pb := &iampb.Binding{Role: bd.Role, Members: bd.Members}

		if len(bd.Condition) > 0 && string(bd.Condition) != "null" {
			var c conditionJSON

			_ = json.Unmarshal(bd.Condition, &c)
			pb.Condition = &expr.Expr{Expression: c.Expression, Title: c.Title, Description: c.Description}
		}

		out.Bindings = append(out.Bindings, pb)
	}

	return out
}

func policyFromPB(p *iampb.Policy) IAMPolicy {
	out := IAMPolicy{Etag: string(p.GetEtag()), Version: int(p.GetVersion()), Bindings: []IAMBinding{}}

	for _, bd := range p.GetBindings() {
		ib := IAMBinding{Role: bd.GetRole(), Members: bd.GetMembers()}

		if c := bd.GetCondition(); c != nil {
			ib.Condition, _ = json.Marshal(conditionJSON{c.GetExpression(), c.GetTitle(), c.GetDescription()})
		}

		out.Bindings = append(out.Bindings, ib)
	}

	return out
}

func iamErr(err error) *APIError {
	if status.Code(err) == codes.Aborted {
		return errPrecondition()
	}

	return errInvalid(status.Convert(err).Message())
}
