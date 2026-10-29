package neptune

import (
	"context"
	"errors"
	"hash/fnv"
	"math"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	boltAgent    = "Neptune/1.4.0.0"
	boltDatabase = "neo4j"
)

// boltBackend serves one cluster's openCypher graph over Bolt.
type boltBackend struct {
	graph   *opencypher.Graph
	queries *queryTracker
	auth    func() clusterAuth
	caller  *iamCaller
}

func (b *boltBackend) NewSession() bolt.Session { return &boltSession{b: b} }

// Authenticate accepts the none and basic schemes; on a cluster with IAM authentication the basic password must be
// the signed-request JSON Neptune documents, verified like an HTTP request.
func (b *boltBackend) Authenticate(_ context.Context, auth map[string]any) error {
	scheme, _ := auth["scheme"].(string)

	if b.auth != nil {
		if ca := b.auth(); ca.enabled {
			caller, err := ca.authenticateBolt(auth, time.Now())
			if err != nil {
				return &bolt.Error{Code: bolt.CodeUnauthorized, Message: "AccessDeniedException: " + err.Error()}
			}

			b.caller = caller

			return nil
		}
	}

	switch scheme {
	case "", "none", "basic":
		return nil
	default:
		return &bolt.Error{Code: bolt.CodeUnauthorized, Message: "unsupported authentication scheme " + scheme}
	}
}

type boltSession struct {
	b  *boltBackend
	tx *opencypher.Tx
}

func (s *boltSession) Run(ctx context.Context, query string, params map[string]any) (*bolt.Result, error) {
	if c := s.b.caller; c != nil {
		if a := c.denied(langOpenCypher, cypherAccessActions(query)...); a != "" {
			return nil, boltDenied(accessDeniedDetail(a))
		}
	}

	ctx, finish := s.b.queries.begin(ctx, query)
	defer finish()

	var (
		res *opencypher.Result
		err error
	)

	if s.tx != nil {
		res, err = s.tx.Execute(ctx, query, params)
	} else {
		res, err = s.b.graph.Execute(ctx, query, params)
	}

	if err != nil {
		return nil, boltError(ctx, err)
	}

	return toBoltResult(res), nil
}

func (s *boltSession) Begin(ctx context.Context) error {
	tx, err := s.b.graph.Begin(ctx)
	if err != nil {
		return boltError(ctx, err)
	}

	s.tx = tx

	return nil
}

func (s *boltSession) Commit(context.Context) error {
	if s.tx != nil {
		s.tx.Commit()
		s.tx = nil
	}

	return nil
}

func (s *boltSession) Rollback(context.Context) error {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}

	return nil
}

func (s *boltSession) Close() { _ = s.Rollback(context.Background()) }

func boltError(ctx context.Context, err error) *bolt.Error {
	if errors.Is(context.Cause(ctx), errQueryCancelled) {
		return &bolt.Error{
			Code:    bolt.CodeTransient,
			Message: "CancelledByUserException: The query was cancelled by the user",
		}
	}

	if ce, ok := opencypher.AsError(err); ok {
		return &bolt.Error{Code: boltCode(ce.Code), Message: ce.Code + ": " + ce.Detail}
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return &bolt.Error{Code: bolt.CodeTransient, Message: "TimeLimitExceededException: " + err.Error()}
	}

	return &bolt.Error{Code: bolt.CodeDatabaseError, Message: "InternalFailureException: " + err.Error()}
}

func boltCode(code string) string {
	switch code {
	case opencypher.CodeMalformedQuery:
		return bolt.CodeSyntax
	case opencypher.CodeConstraintViolation:
		return bolt.CodeConstraint
	default:
		return bolt.CodeInvalid
	}
}

func toBoltResult(res *opencypher.Result) *bolt.Result {
	out := &bolt.Result{Fields: res.Columns, Write: res.Write, Rows: make([][]any, len(res.Rows))}
	if out.Fields == nil {
		out.Fields = []string{}
	}

	for i, row := range res.Rows {
		conv := make([]any, len(row))
		for j, v := range row {
			conv[j] = toBoltValue(v)
		}

		out.Rows[i] = conv
	}

	return out
}

func boltID(id string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))

	return int64(h.Sum64() & math.MaxInt64)
}

func toBoltValue(v any) any {
	switch t := v.(type) {
	case *opencypher.Node:
		return boltNode(t)
	case *opencypher.Rel:
		return bolt.Relationship{
			ID: boltID(t.ID), ElementID: t.ID, Type: t.Type, Props: boltProps(t.Props),
			StartID: boltID(t.Start), EndID: boltID(t.End), StartElementID: t.Start, EndElementID: t.End,
		}
	case *opencypher.Path:
		return boltPath(t)
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = toBoltValue(e)
		}

		return out
	case map[string]any:
		return boltProps(t)
	default:
		return v
	}
}

func boltNode(n *opencypher.Node) bolt.Node {
	return bolt.Node{ID: boltID(n.ID), ElementID: n.ID, Labels: n.Labels, Props: boltProps(n.Props)}
}

func boltProps(p map[string]any) map[string]any {
	out := make(map[string]any, len(p))
	for k, v := range p {
		out[k] = toBoltValue(v)
	}

	return out
}

func boltPath(p *opencypher.Path) bolt.Path {
	out := bolt.Path{}

	for _, n := range p.Nodes {
		out.Nodes = append(out.Nodes, boltNode(n))
	}

	for i, r := range p.Rels {
		out.Rels = append(out.Rels, bolt.UnboundRelationship{
			ID: boltID(r.ID), ElementID: r.ID, Type: r.Type, Props: boltProps(r.Props),
		})

		idx := int64(i + 1)
		if i < len(p.Nodes) && r.Start != p.Nodes[i].ID {
			idx = -idx
		}

		out.Indices = append(out.Indices, idx, int64(i+1))
	}

	return out
}
