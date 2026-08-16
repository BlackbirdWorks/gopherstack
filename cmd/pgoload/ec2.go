package main

import (
	"context"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

const (
	ec2ImageID      = "ami-12345678"
	ec2TagKey       = "Owner"
	ec2TagValue     = "pgoload"
	ec2TerminateCap = 5
)

// ec2Worker repeatedly runs a mix of EC2 operations, staggered by workerID,
// until ctx is done. Instance churn is self-balancing: run and terminate are
// both in the op mix, and terminate always clears out whatever the pgoload
// tag currently has, so the instance count stays bounded without any
// client-side bookkeeping across calls.
func ec2Worker(ctx context.Context, cl *ec2.Client, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, workerID, i int) error { return ec2RunInstancesOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ec2DescribeInstancesOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ec2DescribeInstancesOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ec2DescribeSecurityGroupsOp(ctx, cl) },
		func(ctx context.Context, workerID, i int) error { return ec2TerminateInstancesOp(ctx, cl) },
	}

	runOpLoop(ctx, workerID, ops, c, "ec2", log)
}

func ec2OwnerFilter() []ec2types.Filter {
	return []ec2types.Filter{
		{Name: aws.String("tag:" + ec2TagKey), Values: []string{ec2TagValue}},
	}
}

func ec2RunInstancesOp(ctx context.Context, cl *ec2.Client) error {
	const minMaxCount = 1

	_, err := cl.RunInstances(ctx, &ec2.RunInstancesInput{
		ImageId:      aws.String(ec2ImageID),
		InstanceType: ec2types.InstanceTypeT2Micro,
		MinCount:     aws.Int32(minMaxCount),
		MaxCount:     aws.Int32(minMaxCount),
		TagSpecifications: []ec2types.TagSpecification{
			{
				ResourceType: ec2types.ResourceTypeInstance,
				Tags:         []ec2types.Tag{{Key: aws.String(ec2TagKey), Value: aws.String(ec2TagValue)}},
			},
		},
	})

	return err
}

func ec2DescribeInstancesOp(ctx context.Context, cl *ec2.Client) error {
	_, err := cl.DescribeInstances(ctx, &ec2.DescribeInstancesInput{Filters: ec2OwnerFilter()})

	return err
}

func ec2DescribeSecurityGroupsOp(ctx context.Context, cl *ec2.Client) error {
	_, err := cl.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{})

	return err
}

// ec2TerminateInstancesOp describes up to ec2TerminateCap pgoload-tagged
// instances and terminates them, keeping the emulator's instance count from
// growing without bound over a long run.
func ec2TerminateInstancesOp(ctx context.Context, cl *ec2.Client) error {
	out, err := cl.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters:    ec2OwnerFilter(),
		MaxResults: aws.Int32(ec2TerminateCap),
	})
	if err != nil {
		return err
	}

	ids := make([]string, 0, ec2TerminateCap)

	for _, res := range out.Reservations {
		for _, inst := range res.Instances {
			ids = append(ids, aws.ToString(inst.InstanceId))
		}
	}

	if len(ids) == 0 {
		return nil
	}

	_, err = cl.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: ids})

	return err
}
