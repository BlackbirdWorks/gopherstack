package gcprest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const maxBodyBytes = 64 << 20

//nolint:gochecknoglobals // immutable lookup table
var ignoredParams = map[string]bool{
	"alt": true, "$alt": true, "key": true, "access_token": true, "prettyPrint": true, "fields": true,
	"$.xgafv": true, "quotaUser": true, "userIp": true, "callback": true, "upload_protocol": true,
	"uploadType": true, "oauth_token": true, "$unique": true,
}

//nolint:gochecknoglobals // immutable lookup table
var hopHeaders = map[string]bool{
	"host": true, "content-length": true, "content-type": true, "connection": true, "te": true,
	"transfer-encoding": true, "accept-encoding": true, "user-agent": true, "keep-alive": true,
}

//nolint:gochecknoglobals // immutable marshal options
var (
	marshaler   = protojson.MarshalOptions{}
	unmarshaler = protojson.UnmarshalOptions{DiscardUnknown: true}
)

// Handler returns the REST-only HTTP handler.
func (s *Server) Handler() http.Handler { return http.HandlerFunc(s.serveREST) }

func (s *Server) serveREST(w http.ResponseWriter, r *http.Request) {
	rt, binds, ok := s.match(r)
	if !ok {
		writeError(w, status.Error(codes.NotFound, "Method not found."))

		return
	}

	req, err := rt.buildRequest(r, binds)
	if err != nil {
		writeError(w, err)

		return
	}

	out, err := s.invoke(r, rt.target, req)
	if err != nil {
		writeError(w, err)

		return
	}

	body, err := rt.render(out)
	if err != nil {
		writeError(w, status.Error(codes.Internal, err.Error()))

		return
	}

	writeRaw(w, http.StatusOK, body)
}

func (rt *route) buildRequest(r *http.Request, binds []binding) (proto.Message, error) {
	msg := newMessage(rt.target.in).ProtoReflect()

	if err := rt.readBody(r, msg); err != nil {
		return nil, err
	}

	bound := make(map[string]bool, len(binds))

	for _, b := range binds {
		bound[b.field] = true

		if err := setPath(msg, b.field, []string{b.value}); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	if rt.body != "*" {
		if err := applyQuery(msg, r, bound, rt.body); err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
	}

	return msg.Interface(), nil
}

func (rt *route) readBody(r *http.Request, msg protoreflect.Message) error {
	if rt.body == "" {
		return nil
	}

	raw, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, maxBodyBytes))
	if err != nil {
		return status.Error(codes.InvalidArgument, "read body: "+err.Error())
	}

	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}

	if rt.body != "*" {
		raw = []byte(fmt.Sprintf("{%q:%s}", rt.body, raw))
	}

	if err = unmarshaler.Unmarshal(raw, msg.Interface()); err != nil {
		return status.Error(codes.InvalidArgument, "Invalid JSON payload received. "+err.Error())
	}

	return nil
}

func applyQuery(msg protoreflect.Message, r *http.Request, bound map[string]bool, bodyField string) error {
	for key, vals := range r.URL.Query() {
		root, _, _ := strings.Cut(key, ".")
		if ignoredParams[key] || bound[key] || bound[root] || (bodyField != "" && root == bodyField) {
			continue
		}

		if err := setPath(msg, key, vals); err != nil {
			return err
		}
	}

	return nil
}

func (rt *route) render(out []proto.Message) ([]byte, error) {
	if rt.target.stream {
		return renderArray(out)
	}

	return rt.renderOne(out[0])
}

func (rt *route) renderOne(m proto.Message) ([]byte, error) {
	if rt.respBody != "" && rt.respBody != "*" {
		rm := m.ProtoReflect()
		if fd := fieldByName(rm.Descriptor(), rt.respBody); fd != nil && fd.Message() != nil && !fd.IsList() {
			return marshaler.Marshal(rm.Get(fd).Message().Interface())
		}
	}

	return marshaler.Marshal(m)
}

func renderArray(msgs []proto.Message) ([]byte, error) {
	parts := make([]string, 0, len(msgs))

	for _, m := range msgs {
		b, err := marshaler.Marshal(m)
		if err != nil {
			return nil, fmt.Errorf("marshal: %w", err)
		}

		parts = append(parts, string(b))
	}

	return []byte("[" + strings.Join(parts, ",") + "]"), nil
}

func (s *Server) invoke(r *http.Request, t *target, req proto.Message) ([]proto.Message, error) {
	ctx := outgoingContext(r)

	if t.stream {
		return s.runStream(ctx, t, req)
	}

	resp := newMessage(t.out)
	if err := s.conn.Invoke(ctx, t.full, req, resp); err != nil {
		return nil, err
	}

	return []proto.Message{resp}, nil
}

func (s *Server) runStream(ctx context.Context, t *target, req proto.Message) ([]proto.Message, error) {
	cs, err := s.conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true}, t.full)
	if err != nil {
		return nil, err
	}

	if err = cs.SendMsg(req); err != nil {
		return nil, err
	}

	if err = cs.CloseSend(); err != nil {
		return nil, err
	}

	out := []proto.Message{}

	for {
		m := newMessage(t.out)
		if err = cs.RecvMsg(m); errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return nil, err
		}

		out = append(out, m)
	}
}

func outgoingContext(r *http.Request) context.Context {
	md := metadata.MD{}

	for k, v := range r.Header {
		if lk := strings.ToLower(k); !hopHeaders[lk] {
			md.Append(lk, v...)
		}
	}

	return metadata.NewOutgoingContext(r.Context(), md)
}
