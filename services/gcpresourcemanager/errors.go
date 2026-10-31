package gcpresourcemanager

import (
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	apiHost      = "cloudresourcemanager.googleapis.com"
	reasonDenied = "IAM_PERMISSION_DENIED"
	domainIAM    = "iam.googleapis.com"
)

func errInvalid(format string, a ...any) error {
	return status.Error(codes.InvalidArgument, fmt.Sprintf(format, a...))
}

func errPrecondition(format string, a ...any) error {
	return status.Error(codes.FailedPrecondition, fmt.Sprintf(format, a...))
}

func errExists(format string, a ...any) error {
	return status.Error(codes.AlreadyExists, fmt.Sprintf(format, a...))
}

func errNotFound(format string, a ...any) error {
	return status.Error(codes.NotFound, fmt.Sprintf(format, a...))
}

func errAborted(format string, a ...any) error {
	return status.Error(codes.Aborted, fmt.Sprintf(format, a...))
}

// errDenied is what GCP answers for a resource that does not exist, so callers cannot probe for it.
func errDenied(permission, resource string) error {
	st := status.New(codes.PermissionDenied, fmt.Sprintf(
		"Permission '%s' denied on resource '//%s/%s' (or it may not exist).", permission, apiHost, resource))

	withInfo, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason: reasonDenied, Domain: domainIAM,
		Metadata: map[string]string{"permission": permission, "resource": resource},
	})
	if err != nil {
		return st.Err()
	}

	return withInfo.Err()
}

func preconditionFailure(msg, typ, subject, desc string) error {
	st := status.New(codes.FailedPrecondition, msg)

	withDetail, err := st.WithDetails(&errdetails.PreconditionFailure{
		Violations: []*errdetails.PreconditionFailure_Violation{{Type: typ, Subject: subject, Description: desc}},
	})
	if err != nil {
		return st.Err()
	}

	return withDetail.Err()
}
