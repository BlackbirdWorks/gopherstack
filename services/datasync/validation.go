package datasync

import (
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"

	dstypes "github.com/aws/aws-sdk-go-v2/service/datasync/types"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const maxTaskNameLen = 256

var (
	taskNameRe    = regexp.MustCompile(`^[a-zA-Z0-9\s+=._:@/-]+$`)
	s3BucketArnRe = regexp.MustCompile(`^arn:aws[a-z-]*:s3(-outposts)?:`)
	scheduleRe    = regexp.MustCompile(`^(cron|rate)\(.+\)$`)
)

func paginate[T any](all []T, token string, maxResults int32) (page.Page[T], error) {
	if err := page.ValidateToken(token); err != nil {
		return page.Page[T]{}, fmt.Errorf("%w: invalid NextToken", ErrInvalidParameter)
	}

	if maxResults < 0 {
		return page.Page[T]{}, fmt.Errorf("%w: MaxResults must be non-negative", ErrInvalidParameter)
	}

	return page.New(all, token, int(maxResults), defaultMaxResults), nil
}

func validateTaskName(name string) error {
	if name == "" {
		return nil
	}

	if len(name) > maxTaskNameLen || !taskNameRe.MatchString(name) {
		return fmt.Errorf("%w: Name %q must match %s and be at most %d characters",
			errInvalidRequest, name, taskNameRe.String(), maxTaskNameLen)
	}

	return nil
}

func validateS3BucketArn(v string) error {
	if !s3BucketArnRe.MatchString(v) {
		return fmt.Errorf("%w: S3BucketArn %q is not a valid S3 bucket ARN", errInvalidRequest, v)
	}

	return nil
}

func validateScheduleExpression(s *taskScheduleInput) error {
	if s == nil {
		return nil
	}

	if !scheduleRe.MatchString(strings.TrimSpace(s.ScheduleExpression)) {
		return fmt.Errorf("%w: ScheduleExpression %q must be a cron(...) or rate(...) expression",
			errInvalidRequest, s.ScheduleExpression)
	}

	return checkEnum("Schedule.Status", dstypes.ScheduleStatus(s.Status))
}

func enumValues[T ~string](vals []T) []string {
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = string(v)
	}

	return out
}

func validateTaskOptions(opts map[string]any) error {
	enums := map[string][]string{
		"Atime":                       enumValues(dstypes.Atime("").Values()),
		"Gid":                         enumValues(dstypes.Gid("").Values()),
		"LogLevel":                    enumValues(dstypes.LogLevel("").Values()),
		"Mtime":                       enumValues(dstypes.Mtime("").Values()),
		"ObjectTags":                  enumValues(dstypes.ObjectTags("").Values()),
		"OverwriteMode":               enumValues(dstypes.OverwriteMode("").Values()),
		"PosixPermissions":            enumValues(dstypes.PosixPermissions("").Values()),
		"PreserveDeletedFiles":        enumValues(dstypes.PreserveDeletedFiles("").Values()),
		"PreserveDevices":             enumValues(dstypes.PreserveDevices("").Values()),
		"SecurityDescriptorCopyFlags": enumValues(dstypes.SmbSecurityDescriptorCopyFlags("").Values()),
		"TaskQueueing":                enumValues(dstypes.TaskQueueing("").Values()),
		"TransferMode":                enumValues(dstypes.TransferMode("").Values()),
		"Uid":                         enumValues(dstypes.Uid("").Values()),
		"VerifyMode":                  enumValues(dstypes.VerifyMode("").Values()),
	}

	for field, allowed := range enums {
		v, ok := opts[field].(string)
		if !ok || v == "" {
			continue
		}

		if !slices.Contains(allowed, v) {
			return fmt.Errorf("%w: invalid Options.%s %q", errInvalidRequest, field, v)
		}
	}

	return nil
}

// describeNotFound gives a bare not-found error a message naming the ARN the
// request referenced.
func describeNotFound(err error, body []byte) error {
	if err.Error() != resourceNotFoundType {
		return err
	}

	var fields map[string]any
	if json.Unmarshal(body, &fields) != nil {
		return err
	}

	for _, key := range []string{"TaskExecutionArn", "TaskArn", "LocationArn", "AgentArn", "ResourceArn"} {
		if v, ok := fields[key].(string); ok && v != "" {
			return fmt.Errorf("%w: %s %s not found", err, strings.TrimSuffix(key, "Arn"), v)
		}
	}

	return err
}
