package connect_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHoursOfOperation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "hours"))
	cfg := []types.HoursOfOperationConfig{{
		Day:       types.HoursOfOperationDaysMonday,
		StartTime: &types.HoursOfOperationTimeSlice{Hours: aws.Int32(9), Minutes: aws.Int32(0)},
		EndTime:   &types.HoursOfOperationTimeSlice{Hours: aws.Int32(17), Minutes: aws.Int32(30)},
	}}

	tests := []struct {
		mutate func(in *sdk.CreateHoursOfOperationInput)
		name   string
		code   string
	}{
		{
			name:   "bad zone",
			mutate: func(in *sdk.CreateHoursOfOperationInput) { in.TimeZone = aws.String("Mars/Base") },
			code:   "InvalidParameterException",
		},
		{name: "bad day", mutate: func(in *sdk.CreateHoursOfOperationInput) {
			in.Config = []types.HoursOfOperationConfig{
				{Day: "FUNDAY", StartTime: cfg[0].StartTime, EndTime: cfg[0].EndTime},
			}
		}, code: "InvalidParameterException"},
		{name: "bad time", mutate: func(in *sdk.CreateHoursOfOperationInput) {
			in.Config = []types.HoursOfOperationConfig{
				{
					Day:       "MONDAY",
					StartTime: &types.HoursOfOperationTimeSlice{Hours: aws.Int32(25), Minutes: aws.Int32(0)},
					EndTime:   cfg[0].EndTime,
				},
			}
		}, code: "InvalidParameterException"},
		{
			name:   "duplicate of default",
			mutate: func(in *sdk.CreateHoursOfOperationInput) { in.Name = aws.String("Basic Hours") },
			code:   "DuplicateResourceException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			in := &sdk.CreateHoursOfOperationInput{
				InstanceId: inst,
				Name:       aws.String("Office"),
				TimeZone:   aws.String("Europe/Berlin"),
				Config:     cfg,
			}
			tt.mutate(in)
			_, err := f.client.CreateHoursOfOperation(t.Context(), in)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}

	h, err := f.client.CreateHoursOfOperation(t.Context(), &sdk.CreateHoursOfOperationInput{
		InstanceId: inst, Name: aws.String("Office"), TimeZone: aws.String("Europe/Berlin"), Config: cfg,
	})
	require.NoError(t, err)

	_, err = f.client.UpdateHoursOfOperation(t.Context(), &sdk.UpdateHoursOfOperationInput{
		InstanceId:         inst,
		HoursOfOperationId: h.HoursOfOperationId,
		TimeZone:           aws.String("UTC"),
		Description:        aws.String("d"),
	})
	require.NoError(t, err)

	got, err := f.client.DescribeHoursOfOperation(
		t.Context(),
		&sdk.DescribeHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: h.HoursOfOperationId},
	)
	require.NoError(t, err)
	assert.Equal(t, "UTC", aws.ToString(got.HoursOfOperation.TimeZone))
	require.Len(t, got.HoursOfOperation.Config, 1)
	assert.EqualValues(t, 17, aws.ToInt32(got.HoursOfOperation.Config[0].EndTime.Hours))

	list, err := f.client.ListHoursOfOperations(t.Context(), &sdk.ListHoursOfOperationsInput{InstanceId: inst})
	require.NoError(t, err)
	assert.Len(t, list.HoursOfOperationSummaryList, 2)

	q, err := f.client.CreateQueue(
		t.Context(),
		&sdk.CreateQueueInput{InstanceId: inst, Name: aws.String("Uses"), HoursOfOperationId: h.HoursOfOperationId},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteHoursOfOperation(
		t.Context(),
		&sdk.DeleteHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: h.HoursOfOperationId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.DeleteQueue(t.Context(), &sdk.DeleteQueueInput{InstanceId: inst, QueueId: q.QueueId})
	require.NoError(t, err)

	_, err = f.client.DeleteHoursOfOperation(
		t.Context(),
		&sdk.DeleteHoursOfOperationInput{InstanceId: inst, HoursOfOperationId: h.HoursOfOperationId},
	)
	require.NoError(t, err)
}

func TestQueueLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "queues"))
	_, hours, _ := f.defaultQueue(t, aws.ToString(inst))

	qc, err := f.client.CreateQuickConnect(t.Context(), &sdk.CreateQuickConnectInput{
		InstanceId: inst,
		Name:       aws.String("Boss"),
		QuickConnectConfig: &types.QuickConnectConfig{
			QuickConnectType: types.QuickConnectTypePhoneNumber,
			PhoneConfig:      &types.PhoneNumberQuickConnectConfig{PhoneNumber: aws.String("+15551234567")},
		},
	})
	require.NoError(t, err)

	q, err := f.client.CreateQueue(t.Context(), &sdk.CreateQueueInput{
		InstanceId:         inst,
		Name:               aws.String("Sales"),
		HoursOfOperationId: aws.String(hours),
		Description:        aws.String("sales"),
		MaxContacts: aws.Int32(
			10,
		),
		QuickConnectIds: []string{aws.ToString(qc.QuickConnectId)},
		Tags:            map[string]string{"t": "1"},
	})
	require.NoError(t, err)

	got, err := f.client.DescribeQueue(t.Context(), &sdk.DescribeQueueInput{InstanceId: inst, QueueId: q.QueueId})
	require.NoError(t, err)
	assert.Equal(t, types.QueueStatusEnabled, got.Queue.Status)
	assert.EqualValues(t, 10, aws.ToInt32(got.Queue.MaxContacts))

	_, err = f.client.UpdateQueueStatus(
		t.Context(),
		&sdk.UpdateQueueStatusInput{InstanceId: inst, QueueId: q.QueueId, Status: types.QueueStatusDisabled},
	)
	require.NoError(t, err)

	_, err = f.client.UpdateQueueName(
		t.Context(),
		&sdk.UpdateQueueNameInput{InstanceId: inst, QueueId: q.QueueId, Name: aws.String("Sales2")},
	)
	require.NoError(t, err)

	_, err = f.client.UpdateQueueMaxContacts(
		t.Context(),
		&sdk.UpdateQueueMaxContactsInput{InstanceId: inst, QueueId: q.QueueId, MaxContacts: aws.Int32(5)},
	)
	require.NoError(t, err)

	_, err = f.client.UpdateQueueHoursOfOperation(
		t.Context(),
		&sdk.UpdateQueueHoursOfOperationInput{
			InstanceId:         inst,
			QueueId:            q.QueueId,
			HoursOfOperationId: aws.String("missing"),
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	got, err = f.client.DescribeQueue(t.Context(), &sdk.DescribeQueueInput{InstanceId: inst, QueueId: q.QueueId})
	require.NoError(t, err)
	assert.Equal(t, "Sales2", aws.ToString(got.Queue.Name))
	assert.Equal(t, types.QueueStatusDisabled, got.Queue.Status)
	assert.EqualValues(t, 5, aws.ToInt32(got.Queue.MaxContacts))

	qcs, err := f.client.ListQueueQuickConnects(
		t.Context(),
		&sdk.ListQueueQuickConnectsInput{InstanceId: inst, QueueId: q.QueueId},
	)
	require.NoError(t, err)
	require.Len(t, qcs.QuickConnectSummaryList, 1)
	assert.Equal(t, types.QuickConnectTypePhoneNumber, qcs.QuickConnectSummaryList[0].QuickConnectType)

	_, err = f.client.DeleteQuickConnect(
		t.Context(),
		&sdk.DeleteQuickConnectInput{InstanceId: inst, QuickConnectId: qc.QuickConnectId},
	)
	assert.Equal(t, "ResourceInUseException", apiCode(err))

	_, err = f.client.DisassociateQueueQuickConnects(t.Context(), &sdk.DisassociateQueueQuickConnectsInput{
		InstanceId: inst, QueueId: q.QueueId, QuickConnectIds: []string{aws.ToString(qc.QuickConnectId)},
	})
	require.NoError(t, err)

	_, err = f.client.DeleteQuickConnect(
		t.Context(),
		&sdk.DeleteQuickConnectInput{InstanceId: inst, QuickConnectId: qc.QuickConnectId},
	)
	require.NoError(t, err)

	std, err := f.client.ListQueues(
		t.Context(),
		&sdk.ListQueuesInput{InstanceId: inst, QueueTypes: []types.QueueType{types.QueueTypeStandard}},
	)
	require.NoError(t, err)
	assert.Len(t, std.QueueSummaryList, 2)

	agent, err := f.client.ListQueues(
		t.Context(),
		&sdk.ListQueuesInput{InstanceId: inst, QueueTypes: []types.QueueType{types.QueueTypeAgent}},
	)
	require.NoError(t, err)
	assert.Empty(t, agent.QueueSummaryList)

	_, err = f.client.CreateQueue(
		t.Context(),
		&sdk.CreateQueueInput{InstanceId: inst, Name: aws.String("sales2"), HoursOfOperationId: aws.String(hours)},
	)
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.DeleteQueue(t.Context(), &sdk.DeleteQueueInput{InstanceId: inst, QueueId: q.QueueId})
	require.NoError(t, err)
}

func TestQuickConnectConfigs(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := aws.String(f.newInstance(t, "qcs"))
	queue, _, _ := f.defaultQueue(t, aws.ToString(inst))

	tests := []struct {
		cfg  *types.QuickConnectConfig
		name string
		code string
	}{
		{
			name: "bad phone",
			cfg: &types.QuickConnectConfig{
				QuickConnectType: types.QuickConnectTypePhoneNumber,
				PhoneConfig:      &types.PhoneNumberQuickConnectConfig{PhoneNumber: aws.String("12345")},
			},
			code: "InvalidParameterException",
		},
		{
			name: "queue needs flow",
			cfg: &types.QuickConnectConfig{
				QuickConnectType: types.QuickConnectTypeQueue,
				QueueConfig: &types.QueueQuickConnectConfig{
					QueueId:       aws.String(queue),
					ContactFlowId: aws.String("nope"),
				},
			},
			code: "InvalidParameterException",
		},
		{
			name: "user missing",
			cfg:  &types.QuickConnectConfig{QuickConnectType: types.QuickConnectTypeUser},
			code: "InvalidParameterException",
		},
		{
			name: "ok phone",
			cfg: &types.QuickConnectConfig{
				QuickConnectType: types.QuickConnectTypePhoneNumber,
				PhoneConfig:      &types.PhoneNumberQuickConnectConfig{PhoneNumber: aws.String("+442071234567")},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.CreateQuickConnect(
				t.Context(),
				&sdk.CreateQuickConnectInput{InstanceId: inst, Name: aws.String(tt.name), QuickConnectConfig: tt.cfg},
			)
			assert.Equal(t, tt.code, apiCode(err))
		})
	}

	list, err := f.client.ListQuickConnects(
		t.Context(),
		&sdk.ListQuickConnectsInput{
			InstanceId:        inst,
			QuickConnectTypes: []types.QuickConnectType{types.QuickConnectTypeUser},
		},
	)
	require.NoError(t, err)
	assert.Empty(t, list.QuickConnectSummaryList)
}
