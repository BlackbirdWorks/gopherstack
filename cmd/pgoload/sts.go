package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// stsWorker repeatedly runs a mix of STS operations, staggered by workerID,
// until ctx is done.
func stsWorker(ctx context.Context, cl *sts.Client, res *resources, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, _, _ int) error { return stsGetCallerIdentityOp(ctx, cl) },
		func(ctx context.Context, _, _ int) error { return stsGetSessionTokenOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error {
			return stsAssumeRoleOp(ctx, cl, res.roleArn, workerID, i)
		},
	}

	runOpLoop(ctx, workerID, ops, c, "sts", log)
}

func stsGetCallerIdentityOp(ctx context.Context, cl *sts.Client) error {
	_, err := cl.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})

	return err
}

func stsGetSessionTokenOp(ctx context.Context, cl *sts.Client) error {
	_, err := cl.GetSessionToken(ctx, &sts.GetSessionTokenInput{})

	return err
}

func stsAssumeRoleOp(ctx context.Context, cl *sts.Client, roleArn string, workerID, i int) error {
	_, err := cl.AssumeRole(ctx, &sts.AssumeRoleInput{
		RoleArn:         aws.String(roleArn),
		RoleSessionName: aws.String(fmt.Sprintf("pgoload-%d-%d", workerID, i)),
	})

	return err
}
