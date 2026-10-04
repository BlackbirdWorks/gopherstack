package pipes

import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// SetRoleAuthorizer makes source reads, enrichment and target calls run under the pipe's RoleArn policies.
func (r *Runner) SetRoleAuthorizer(a roleauth.Authorizer) { r.auth = a }

// sourceActions are the actions the pipe role needs to poll the source.
func sourceActions(source string) []string {
	switch {
	case isSQSARN(source):
		return []string{"sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes"}
	case isKinesisARN(source):
		return []string{
			"kinesis:DescribeStream", "kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:ListShards",
		}
	case isDynamoDBStreamARN(source):
		return []string{"dynamodb:DescribeStream", "dynamodb:GetRecords", "dynamodb:GetShardIterator"}
	default:
		return nil
	}
}

// authorizeSource checks the pipe role may read the source.
func (r *Runner) authorizeSource(p *Pipe) error {
	if r.auth == nil {
		return nil
	}

	for _, action := range sourceActions(p.Source) {
		if err := r.auth.AuthorizeRole(roleauth.PrincipalPipes, p.RoleARN, action, p.Source); err != nil {
			return err
		}
	}

	return nil
}

// authorizeCall checks the pipe role may call a target or enrichment ARN.
func (r *Runner) authorizeCall(p *Pipe, arn string, enrichment bool) error {
	if r.auth == nil {
		return nil
	}

	action, ok := roleauth.TargetAction(arn)
	if strings.HasPrefix(arn, "arn:aws:logs:") {
		action, ok = "logs:PutLogEvents", true
	}

	if !ok {
		return nil
	}

	if enrichment && strings.HasPrefix(arn, "arn:aws:states:") {
		action = "states:StartSyncExecution"
	}

	return r.auth.AuthorizeRole(roleauth.PrincipalPipes, p.RoleARN, action, arn)
}
