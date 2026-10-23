package gcpspanner

import (
	"context"
	"errors"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/dynamicpb"
)

// proxy forwards RPCs to the emulator without knowing their message types.
type proxy struct {
	dial func(ctx context.Context) (*grpc.ClientConn, error)
}

// proxied returns a copy of base whose methods, except those in local, forward to the emulator.
func (p *proxy) proxied(base *grpc.ServiceDesc, local map[string]bool) *grpc.ServiceDesc {
	out := *base
	out.Methods = append([]grpc.MethodDesc(nil), base.Methods...)
	out.Streams = append([]grpc.StreamDesc(nil), base.Streams...)

	d, err := protoregistry.GlobalFiles.FindDescriptorByName(protoreflect.FullName(base.ServiceName))
	if err != nil {
		return &out
	}

	svc, ok := d.(protoreflect.ServiceDescriptor)
	if !ok {
		return &out
	}

	for i, m := range out.Methods {
		if md := svc.Methods().ByName(protoreflect.Name(m.MethodName)); md != nil && !local[m.MethodName] {
			out.Methods[i].Handler = p.unary(md, "/"+base.ServiceName+"/"+m.MethodName)
		}
	}

	for i, s := range out.Streams {
		if md := svc.Methods().ByName(protoreflect.Name(s.StreamName)); md != nil && !local[s.StreamName] {
			out.Streams[i].Handler = p.stream(md, "/"+base.ServiceName+"/"+s.StreamName)
		}
	}

	return &out
}

// forwardCtx carries the caller's metadata (routing headers, resource prefix) to the emulator.
func forwardCtx(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	return metadata.NewOutgoingContext(ctx, md.Copy())
}

func (p *proxy) unary(md protoreflect.MethodDescriptor, full string) grpc.MethodHandler {
	return func(_ any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
		in := dynamicpb.NewMessage(md.Input())
		if err := dec(in); err != nil {
			return nil, err
		}

		cc, err := p.dial(ctx)
		if err != nil {
			return nil, err
		}

		out := dynamicpb.NewMessage(md.Output())
		if err = cc.Invoke(forwardCtx(ctx), full, in, out); err != nil {
			return nil, err //nolint:wrapcheck // emulator status passes through
		}

		return out, nil
	}
}

func (p *proxy) stream(md protoreflect.MethodDescriptor, full string) grpc.StreamHandler {
	return func(_ any, ss grpc.ServerStream) error {
		in := dynamicpb.NewMessage(md.Input())
		if err := ss.RecvMsg(in); err != nil {
			return err //nolint:wrapcheck // transport status
		}

		cc, err := p.dial(ss.Context())
		if err != nil {
			return err
		}

		cs, err := cc.NewStream(forwardCtx(ss.Context()), &grpc.StreamDesc{ServerStreams: true}, full)
		if err != nil {
			return err //nolint:wrapcheck // emulator status passes through
		}

		if err = cs.SendMsg(in); err != nil {
			return err //nolint:wrapcheck // emulator status passes through
		}

		if err = cs.CloseSend(); err != nil {
			return err //nolint:wrapcheck // emulator status passes through
		}

		return relay(ss, cs, md.Output())
	}
}

func relay(dst grpc.ServerStream, src grpc.ClientStream, out protoreflect.MessageDescriptor) error {
	for {
		msg := dynamicpb.NewMessage(out)

		err := src.RecvMsg(msg)
		if errors.Is(err, io.EOF) {
			return nil
		}

		if err != nil {
			return err //nolint:wrapcheck // emulator status passes through
		}

		if err = dst.SendMsg(msg); err != nil {
			return err //nolint:wrapcheck // transport status
		}
	}
}
