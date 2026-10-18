package connect_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/connect"
	"github.com/aws/aws-sdk-go-v2/service/connect/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cn "github.com/blackbirdworks/gopherstack/services/connect"
)

type fakeApps struct {
	events, apps map[string]string
}

func (f *fakeApps) CreateEventAssociation(_ context.Context, name, client string) (string, error) {
	id := "evt-" + name
	f.events[id] = client

	return id, nil
}

func (f *fakeApps) DeleteEventAssociation(_ context.Context, id string) error {
	delete(f.events, id)

	return nil
}

func (f *fakeApps) CreateApplicationAssociation(_ context.Context, app, client string) (string, error) {
	id := "app-" + app
	f.apps[id] = client

	return id, nil
}

func (f *fakeApps) DeleteApplicationAssociation(_ context.Context, id string) error {
	delete(f.apps, id)

	return nil
}

var _ cn.Integrations = (*fakeApps)(nil)

func TestIntegrationAssociations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	apps := &fakeApps{events: map[string]string{}, apps: map[string]string{}}
	f.backend.SetIntegrations(apps)
	inst := f.newInstance(t, "integ")
	instARN := "arn:aws:connect:us-east-1:" + testAccount + ":instance/" + inst
	eventARN := "arn:aws:app-integrations:us-east-1:" + testAccount + ":event-integration/partner"

	evt, err := f.client.CreateIntegrationAssociation(t.Context(), &sdk.CreateIntegrationAssociationInput{
		InstanceId: aws.String(
			inst,
		),
		IntegrationType: types.IntegrationTypeEvent,
		IntegrationArn:  aws.String(eventARN),
		Tags:            map[string]string{"t": "1"},
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"evt-partner": instARN}, apps.events)

	_, err = f.client.CreateIntegrationAssociation(t.Context(), &sdk.CreateIntegrationAssociationInput{
		InstanceId: aws.String(inst), IntegrationType: types.IntegrationTypeEvent, IntegrationArn: aws.String(eventARN),
	})
	assert.Equal(t, "DuplicateResourceException", apiCode(err))

	_, err = f.client.CreateIntegrationAssociation(t.Context(), &sdk.CreateIntegrationAssociationInput{
		InstanceId: aws.String(
			inst,
		),
		IntegrationType: types.IntegrationTypeEvent,
		IntegrationArn:  aws.String("arn:aws:s3:::nope"),
	})
	assert.Equal(t, "InvalidParameterException", apiCode(err))

	_, err = f.client.CreateIntegrationAssociation(t.Context(), &sdk.CreateIntegrationAssociationInput{
		InstanceId: aws.String(
			inst,
		),
		IntegrationType: types.IntegrationTypeApplication,
		IntegrationArn:  aws.String("arn:aws:app-integrations:us-east-1:" + testAccount + ":application/abc"),
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"app-abc": instARN}, apps.apps)

	list, err := f.client.ListIntegrationAssociations(
		t.Context(),
		&sdk.ListIntegrationAssociationsInput{
			InstanceId:      aws.String(inst),
			IntegrationType: types.IntegrationTypeEvent,
		},
	)
	require.NoError(t, err)
	require.Len(t, list.IntegrationAssociationSummaryList, 1)
	assert.Equal(t, eventARN, aws.ToString(list.IntegrationAssociationSummaryList[0].IntegrationArn))

	uc, err := f.client.CreateUseCase(t.Context(), &sdk.CreateUseCaseInput{
		InstanceId: aws.String(
			inst,
		),
		IntegrationAssociationId: evt.IntegrationAssociationId,
		UseCaseType:              types.UseCaseTypeRulesEvaluation,
		Tags:                     map[string]string{"u": "1"},
	})
	require.NoError(t, err)

	tags, err := f.client.ListTagsForResource(t.Context(), &sdk.ListTagsForResourceInput{ResourceArn: uc.UseCaseArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"u": "1"}, tags.Tags)

	ucs, err := f.client.ListUseCases(
		t.Context(),
		&sdk.ListUseCasesInput{InstanceId: aws.String(inst), IntegrationAssociationId: evt.IntegrationAssociationId},
	)
	require.NoError(t, err)
	require.Len(t, ucs.UseCaseSummaryList, 1)

	_, err = f.client.DeleteUseCase(
		t.Context(),
		&sdk.DeleteUseCaseInput{
			InstanceId:               aws.String(inst),
			IntegrationAssociationId: evt.IntegrationAssociationId,
			UseCaseId:                uc.UseCaseId,
		},
	)
	require.NoError(t, err)

	_, err = f.client.DeleteIntegrationAssociation(
		t.Context(),
		&sdk.DeleteIntegrationAssociationInput{
			InstanceId:               aws.String(inst),
			IntegrationAssociationId: evt.IntegrationAssociationId,
		},
	)
	require.NoError(t, err)
	assert.Empty(t, apps.events)

	_, err = f.client.DeleteInstance(t.Context(), &sdk.DeleteInstanceInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)
	assert.Empty(t, apps.apps, "deleting the instance releases its AppIntegrations associations")
}

func TestTagging(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	inst := f.newInstance(t, "tags")
	_, hours, _ := f.defaultQueue(t, inst)

	q, err := f.client.CreateQueue(
		t.Context(),
		&sdk.CreateQueueInput{
			InstanceId:         aws.String(inst),
			Name:               aws.String("tagged"),
			HoursOfOperationId: aws.String(hours),
		},
	)
	require.NoError(t, err)

	for _, arn := range []string{
		aws.ToString(q.QueueArn),
		"arn:aws:connect:us-east-1:" + testAccount + ":instance/" + inst,
	} {
		_, err = f.client.TagResource(
			t.Context(),
			&sdk.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"a": "1", "b": "2"}},
		)
		require.NoError(t, err)

		_, err = f.client.UntagResource(
			t.Context(),
			&sdk.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"a"}},
		)
		require.NoError(t, err)

		out, lerr := f.client.ListTagsForResource(
			t.Context(),
			&sdk.ListTagsForResourceInput{ResourceArn: aws.String(arn)},
		)
		require.NoError(t, lerr)
		assert.Equal(t, map[string]string{"b": "2"}, out.Tags, arn)
	}

	desc, err := f.client.DescribeQueue(
		t.Context(),
		&sdk.DescribeQueueInput{InstanceId: aws.String(inst), QueueId: q.QueueId},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, desc.Queue.Tags)

	_, err = f.client.TagResource(
		t.Context(),
		&sdk.TagResourceInput{
			ResourceArn: aws.String("arn:aws:connect:us-east-1:" + testAccount + ":instance/" + inst + "/queue/none"),
			Tags:        map[string]string{"a": "1"},
		},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.TagResource(
		t.Context(),
		&sdk.TagResourceInput{ResourceArn: q.QueueArn, Tags: map[string]string{"aws:reserved": "1"}},
	)
	assert.Equal(t, "InvalidParameterException", apiCode(err))
}

func TestInstanceDeleteCascadesAndPersists(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	inst := src.newInstance(t, "persist")
	queue, hours, profile := src.defaultQueue(t, inst)
	user := src.newUser(t, inst, "kept", profile)
	extra, err := src.client.CreateQueue(
		t.Context(),
		&sdk.CreateQueueInput{
			InstanceId:         aws.String(inst),
			Name:               aws.String("extra"),
			HoursOfOperationId: aws.String(hours),
		},
	)
	require.NoError(t, err)

	snap := src.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixture(t)
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got, err := dst.client.DescribeUser(
		t.Context(),
		&sdk.DescribeUserInput{InstanceId: aws.String(inst), UserId: user.UserId},
	)
	require.NoError(t, err)
	assert.Equal(t, "kept", aws.ToString(got.User.Username))
	assert.True(t, got.User.LastModifiedTime.Equal(*aws.Time(*got.User.LastModifiedTime)))

	q, err := dst.client.DescribeQueue(
		t.Context(),
		&sdk.DescribeQueueInput{InstanceId: aws.String(inst), QueueId: extra.QueueId},
	)
	require.NoError(t, err)
	assert.Equal(t, hours, aws.ToString(q.Queue.HoursOfOperationId))

	_, err = dst.client.DeleteInstance(t.Context(), &sdk.DeleteInstanceInput{InstanceId: aws.String(inst)})
	require.NoError(t, err)

	for _, call := range []func() error{
		func() error {
			_, e := dst.client.DescribeQueue(t.Context(),
				&sdk.DescribeQueueInput{InstanceId: aws.String(inst), QueueId: aws.String(queue)})

			return e
		},
		func() error {
			_, e := dst.client.DescribeUser(t.Context(),
				&sdk.DescribeUserInput{InstanceId: aws.String(inst), UserId: user.UserId})

			return e
		},
	} {
		assert.Equal(t, "ResourceNotFoundException", apiCode(call()))
	}

	dst.handler.Reset()
	require.Error(t, dst.handler.Restore(t.Context(), []byte("{")))
}
