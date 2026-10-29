package gcpcontainer

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/runtime/protoiface"
)

func withDetails(code codes.Code, msg string, details ...protoiface.MessageV1) error {
	st := status.New(code, msg)
	if withD, err := st.WithDetails(details...); err == nil {
		st = withD
	}

	return st.Err()
}

func errNotFound(name string) error {
	return withDetails(codes.NotFound, fmt.Sprintf("Not found: %s.", name),
		&errdetails.ResourceInfo{ResourceName: name})
}

func errExists(name string) error {
	return withDetails(codes.AlreadyExists, fmt.Sprintf("Already exists: %s.", name),
		&errdetails.ResourceInfo{ResourceName: name})
}

func errInvalid(field, format string, a ...any) error {
	msg := fmt.Sprintf(format, a...)

	return withDetails(codes.InvalidArgument, msg,
		&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{
			{Field: field, Description: msg},
		}})
}

func errPrecondition(subject, format string, a ...any) error {
	msg := fmt.Sprintf(format, a...)

	return withDetails(codes.FailedPrecondition, msg,
		&errdetails.PreconditionFailure{Violations: []*errdetails.PreconditionFailure_Violation{
			{Type: "STATE", Subject: subject, Description: msg},
		}})
}

func errBusy(subject, op string) error {
	return errPrecondition(subject, "Cluster is running incompatible operation %s.", op)
}

func errUnsupported(field string) error {
	return errInvalid(field, "Update of %s is not supported by the emulator.", field)
}
