package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
)

// ssmParamRotation bounds the number of distinct parameter names pgoload
// cycles through per worker. Deliberately not a multiple of len(ops) in
// ssmWorker (5): if it were, i%ssmParamRotation would determine i%len(ops)
// exactly, so every parameter name would always be reached by the same
// operation — e.g. always Get, never Put — producing permanent (not
// transient) ParameterNotFound errors instead of a realistic op mix.
const ssmParamRotation = 9

func ssmParamName(workerID, i int) string {
	return fmt.Sprintf("/pgoload/%d/%d", workerID, i%ssmParamRotation)
}

// ssmWorker repeatedly runs a mix of SSM Parameter Store operations,
// staggered by workerID, until ctx is done.
func ssmWorker(ctx context.Context, cl *ssm.Client, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, workerID, i int) error { return ssmPutParameterOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return ssmPutParameterOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return ssmGetParameterOp(ctx, cl, workerID, i) },
		func(ctx context.Context, workerID, i int) error { return ssmGetParametersOp(ctx, cl, workerID, i) },
		func(ctx context.Context, _, _ int) error { return ssmDescribeParametersOp(ctx, cl) },
	}

	runOpLoop(ctx, workerID, ops, c, "ssm", log)
}

func ssmPutParameterOp(ctx context.Context, cl *ssm.Client, workerID, i int) error {
	_, err := cl.PutParameter(ctx, &ssm.PutParameterInput{
		Name:      aws.String(ssmParamName(workerID, i)),
		Value:     aws.String(fmt.Sprintf("value-%d", i)),
		Type:      ssmtypes.ParameterTypeString,
		Overwrite: aws.Bool(true),
	})

	return err
}

func ssmGetParameterOp(ctx context.Context, cl *ssm.Client, workerID, i int) error {
	_, err := cl.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(ssmParamName(workerID, i))})

	return err
}

func ssmGetParametersOp(ctx context.Context, cl *ssm.Client, workerID, _ int) error {
	names := make([]string, 0, ssmParamRotation)
	for j := range ssmParamRotation {
		names = append(names, ssmParamName(workerID, j))
	}

	_, err := cl.GetParameters(ctx, &ssm.GetParametersInput{Names: names})

	return err
}

func ssmDescribeParametersOp(ctx context.Context, cl *ssm.Client) error {
	_, err := cl.DescribeParameters(ctx, &ssm.DescribeParametersInput{})

	return err
}
