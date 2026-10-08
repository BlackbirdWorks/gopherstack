package main

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSWFScheduleLambdaFunctionWiring(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	lc := lambda.NewFromConfig(fx.cfg)
	sc := swf.NewFromConfig(fx.cfg)

	_, err := lc.CreateFunction(t.Context(), &lambda.CreateFunctionInput{
		FunctionName: aws.String("swf-fn"), PackageType: lambdatypes.PackageTypeImage,
		Code: &lambdatypes.FunctionCode{ImageUri: aws.String("x:latest")},
		Role: aws.String("arn:aws:iam::000000000000:role/r"),
	})
	require.NoError(t, err)

	_, err = lc.PutFunctionConcurrency(t.Context(), &lambda.PutFunctionConcurrencyInput{
		FunctionName: aws.String("swf-fn"), ReservedConcurrentExecutions: aws.Int32(0),
	})
	require.NoError(t, err)

	_, err = sc.RegisterDomain(t.Context(), &swf.RegisterDomainInput{
		Name: aws.String("dom"), WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = sc.RegisterWorkflowType(t.Context(), &swf.RegisterWorkflowTypeInput{
		Domain: aws.String("dom"), Name: aws.String("wt"), Version: aws.String("1"),
	})
	require.NoError(t, err)

	_, err = sc.StartWorkflowExecution(t.Context(), &swf.StartWorkflowExecutionInput{
		Domain: aws.String("dom"), WorkflowId: aws.String("wf"),
		WorkflowType: &swftypes.WorkflowType{Name: aws.String("wt"), Version: aws.String("1")},
		TaskList:     &swftypes.TaskList{Name: aws.String("tl")},
	})
	require.NoError(t, err)

	poll, err := sc.PollForDecisionTask(t.Context(), &swf.PollForDecisionTaskInput{
		Domain: aws.String("dom"), TaskList: &swftypes.TaskList{Name: aws.String("tl")},
	})
	require.NoError(t, err)

	_, err = sc.RespondDecisionTaskCompleted(t.Context(), &swf.RespondDecisionTaskCompletedInput{
		TaskToken: poll.TaskToken,
		Decisions: []swftypes.Decision{{
			DecisionType: swftypes.DecisionTypeScheduleLambdaFunction,
			ScheduleLambdaFunctionDecisionAttributes: &swftypes.ScheduleLambdaFunctionDecisionAttributes{
				Id: aws.String("l1"), Name: aws.String("swf-fn"), Input: aws.String("{}"),
			},
		}},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		h, herr := sc.GetWorkflowExecutionHistory(t.Context(), &swf.GetWorkflowExecutionHistoryInput{
			Domain: aws.String(
				"dom",
			),
			Execution: &swftypes.WorkflowExecution{WorkflowId: aws.String("wf"), RunId: poll.WorkflowExecution.RunId},
		})
		if herr != nil {
			return false
		}

		for _, ev := range h.Events {
			if ev.EventType == swftypes.EventTypeLambdaFunctionFailed {
				assert.Equal(t, "LambdaInvocationFailed", aws.ToString(ev.LambdaFunctionFailedEventAttributes.Reason))
				assert.Contains(t, aws.ToString(ev.LambdaFunctionFailedEventAttributes.Details), "reserved concurrency")

				return true
			}
		}

		return false
	}, 10*time.Second, 20*time.Millisecond)
}
