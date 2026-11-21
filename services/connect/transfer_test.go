package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transferFn func(mut func(*sdk.TransferContactInput)) (*sdk.TransferContactOutput, error)

func (e *contactEnv) flowOfType(t *testing.T, typ types.ContactFlowType) string {
	t.Helper()

	out, err := e.f.client.ListContactFlows(t.Context(), &sdk.ListContactFlowsInput{
		InstanceId: aws.String(e.inst), ContactFlowTypes: []types.ContactFlowType{typ},
	})
	require.NoError(t, err)
	require.NotEmpty(t, out.ContactFlowSummaryList)

	return aws.ToString(out.ContactFlowSummaryList[0].Id)
}

func transferToQueueCase(t *testing.T, e *contactEnv, transfer transferFn) {
	t.Helper()

	orig := e.task(t)
	out, err := transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(orig) })
	require.NoError(t, err)

	moved := e.describe(t, aws.ToString(out.ContactId))
	assert.Equal(t, types.ContactInitiationMethodTransfer, moved.InitiationMethod)
	assert.Equal(t, orig, aws.ToString(moved.PreviousContactId))
	assert.Equal(t, orig, aws.ToString(moved.InitialContactId))
	assert.Equal(t, e.queue, aws.ToString(moved.QueueInfo.Id))
	assert.Equal(t, "task", aws.ToString(moved.Name))
	assert.Nil(t, moved.DisconnectTimestamp)
	assert.NotNil(t, e.describe(t, orig).DisconnectTimestamp, "the source contact ends")

	_, err = transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(orig) })
	assert.Equal(t, "InvalidRequestException", apiCode(err), "the source is no longer active")
}

func transferToAgentCase(t *testing.T, e *contactEnv, transfer transferFn) {
	t.Helper()

	out, err := transfer(func(in *sdk.TransferContactInput) {
		in.ContactId, in.QueueId, in.UserId = aws.String(e.task(t)), nil, aws.String(e.user)
	})
	require.NoError(t, err)
	assert.Equal(t, e.user, aws.ToString(e.describe(t, aws.ToString(out.ContactId)).AgentInfo.Id))
}

func transferRejectionsCase(t *testing.T, e *contactEnv, transfer transferFn) {
	t.Helper()

	task := e.task(t)
	queueFlow := e.flowOfType(t, types.ContactFlowTypeCustomerQueue)

	cases := []struct {
		mut  func(*sdk.TransferContactInput)
		name string
		code string
	}{
		{
			name: "queue and user", code: "InvalidParameterException",
			mut: func(in *sdk.TransferContactInput) { in.UserId = aws.String(e.user) },
		},
		{
			name: "unknown queue", code: "ResourceNotFoundException",
			mut: func(in *sdk.TransferContactInput) { in.QueueId = aws.String("nope") },
		},
		{
			name: "unknown user", code: "ResourceNotFoundException",
			mut: func(in *sdk.TransferContactInput) { in.QueueId, in.UserId = nil, aws.String("nope") },
		},
		{
			name: "unknown flow", code: "ResourceNotFoundException",
			mut: func(in *sdk.TransferContactInput) { in.ContactFlowId = aws.String("nope") },
		},
		{
			name: "unknown contact", code: "ContactNotFoundException",
			mut: func(in *sdk.TransferContactInput) { in.ContactId = aws.String("nope") },
		},
		{
			name: "wrong flow type", code: "InvalidRequestException",
			mut: func(in *sdk.TransferContactInput) { in.ContactFlowId = aws.String(queueFlow) },
		},
	}

	for _, c := range cases {
		_, err := transfer(func(in *sdk.TransferContactInput) {
			in.ContactId = aws.String(task)
			c.mut(in)
		})
		assert.Equal(t, c.code, apiCode(err), c.name)
	}

	voice := e.outbound(t, "+442071230009", true)
	_, err := transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(voice) })
	assert.Equal(t, "InvalidRequestException", apiCode(err), "voice contacts cannot transfer")
}

func transferLimitCase(t *testing.T, e *contactEnv, transfer transferFn) {
	t.Helper()

	cur := e.task(t)

	for range 11 {
		out, err := transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(cur) })
		require.NoError(t, err)

		cur = aws.ToString(out.ContactId)
	}

	_, err := transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(cur) })
	assert.Equal(t, "LimitExceededException", apiCode(err))
}

func transferRestoreCase(t *testing.T, e *contactEnv, transfer transferFn) {
	t.Helper()

	out, err := transfer(func(in *sdk.TransferContactInput) { in.ContactId = aws.String(e.task(t)) })
	require.NoError(t, err)

	r := e.f.restored(t)
	got, err := r.client.DescribeContact(t.Context(),
		&sdk.DescribeContactInput{InstanceId: aws.String(e.inst), ContactId: out.ContactId})
	require.NoError(t, err)
	assert.Equal(t, types.ContactInitiationMethodTransfer, got.Contact.InitiationMethod)
}

func TestTransferContact(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, e *contactEnv, transfer transferFn)
		name string
	}{
		{name: "task moves to a queue as a new transfer contact", run: transferToQueueCase},
		{name: "task moves to an agent", run: transferToAgentCase},
		{name: "rejections", run: transferRejectionsCase},
		{name: "a contact transfers at most eleven times", run: transferLimitCase},
		{name: "transfers survive a restore", run: transferRestoreCase},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newContactEnv(t)
			flow := e.flowOfType(t, types.ContactFlowTypeQueueTransfer)
			tt.run(t, e, func(mut func(*sdk.TransferContactInput)) (*sdk.TransferContactOutput, error) {
				in := &sdk.TransferContactInput{
					InstanceId: aws.String(e.inst), ContactFlowId: aws.String(flow), QueueId: aws.String(e.queue),
				}
				mut(in)

				return e.f.client.TransferContact(t.Context(), in)
			})
		})
	}
}

func TestGetMetricDataV2TransferMetrics(t *testing.T) {
	t.Parallel()

	e, start := historyEnv(t)
	flow := e.flowOfType(t, types.ContactFlowTypeQueueTransfer)

	queued, err := e.f.client.TransferContact(t.Context(), &sdk.TransferContactInput{
		InstanceId: aws.String(e.inst), ContactId: aws.String(e.task(t)), ContactFlowId: aws.String(flow),
		QueueId: aws.String(e.queue),
	})
	require.NoError(t, err)

	_, err = e.f.client.TransferContact(t.Context(), &sdk.TransferContactInput{
		InstanceId: aws.String(e.inst), ContactId: queued.ContactId, ContactFlowId: aws.String(flow),
		UserId: aws.String(e.user),
	})
	require.NoError(t, err)

	names := []string{"CONTACTS_TRANSFERRED_IN", "CONTACTS_TRANSFERRED_OUT", "SUM_HANDLE_TIME"}
	want := []float64{1, 1, 70}

	metrics := make([]types.MetricV2, 0, len(names))
	for _, n := range names {
		metrics = append(metrics, v2Metric(n))
	}

	out, err := e.f.client.GetMetricDataV2(t.Context(), &sdk.GetMetricDataV2Input{
		ResourceArn: aws.String(e.instARN()), StartTime: aws.Time(start), EndTime: aws.Time(e.f.clock.now()),
		Filters: queueFilter(e.queue), Metrics: metrics,
	})
	require.NoError(t, err)
	require.Len(t, out.MetricResults, 1)
	require.Len(t, out.MetricResults[0].Collections, len(want))

	for i, c := range out.MetricResults[0].Collections {
		assert.InDelta(t, want[i], aws.ToFloat64(c.Value), 0.001, names[i])
	}
}
