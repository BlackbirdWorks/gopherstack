package cloudformation_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResourceCreator_S3Bucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantPhysID   string
		wantContains string
		doDelete     bool
	}{
		{
			name:       "explicit_name",
			logicalID:  "MyBucket",
			props:      map[string]any{"BucketName": "test-cfn-bucket"},
			wantPhysID: "test-cfn-bucket",
			doDelete:   true,
		},
		{
			name:         "auto_name",
			logicalID:    "MyBucket",
			props:        map[string]any{},
			wantContains: "mybucket",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::S3::Bucket",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)

			if tt.wantPhysID != "" {
				assert.Equal(t, tt.wantPhysID, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::S3::Bucket", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_DynamoDBTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		logicalID  string
		props      map[string]any
		wantPhysID string
		doDelete   bool
	}{
		{
			name:      "provisioned_throughput",
			logicalID: "MyTable",
			props: map[string]any{
				"TableName": "cfn-test-table",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "id", "AttributeType": "S"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "id", "KeyType": "HASH"},
				},
				"ProvisionedThroughput": map[string]any{
					"ReadCapacityUnits":  float64(5),
					"WriteCapacityUnits": float64(5),
				},
			},
			wantPhysID: "cfn-test-table",
			doDelete:   true,
		},
		{
			name:      "pay_per_request",
			logicalID: "OnDemandTable",
			props: map[string]any{
				"TableName":   "cfn-ondemand-table",
				"BillingMode": "PAY_PER_REQUEST",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "pk", "AttributeType": "S"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
				},
			},
			wantPhysID: "cfn-ondemand-table",
		},
		{
			name:      "default_name",
			logicalID: "MyTable",
			props: map[string]any{
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "id", "AttributeType": "N"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "id", "KeyType": "HASH"},
				},
			},
			wantPhysID: "MyTable",
		},
		{
			name:      "binary_attribute_type",
			logicalID: "BinaryTable",
			props: map[string]any{
				"TableName": "cfn-binary-table",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "id", "AttributeType": "B"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "id", "KeyType": "HASH"},
				},
			},
			wantPhysID: "cfn-binary-table",
		},
		{
			name:      "range_key",
			logicalID: "RangeTable",
			props: map[string]any{
				"TableName": "cfn-range-table",
				"AttributeDefinitions": []any{
					map[string]any{"AttributeName": "pk", "AttributeType": "S"},
					map[string]any{"AttributeName": "sk", "AttributeType": "S"},
				},
				"KeySchema": []any{
					map[string]any{"AttributeName": "pk", "KeyType": "HASH"},
					map[string]any{"AttributeName": "sk", "KeyType": "RANGE"},
				},
			},
			wantPhysID: "cfn-range-table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::DynamoDB::Table",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantPhysID, physID)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::DynamoDB::Table", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_SQSQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		wantNotEmpty bool
		doDelete     bool
	}{
		{
			name:         "explicit_name",
			logicalID:    "MyQueue",
			props:        map[string]any{"QueueName": "cfn-test-queue"},
			wantNotEmpty: true,
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultQueue",
			props:        map[string]any{"VisibilityTimeout": "30"},
			wantNotEmpty: true,
		},
		{
			name:         "fifo",
			logicalID:    "MyFIFOQueue",
			props:        map[string]any{"QueueName": "cfn-fifo-queue", "FifoQueue": true},
			wantContains: ".fifo",
		},
		{
			name:      "with_attributes",
			logicalID: "AttrQueue",
			props: map[string]any{
				"QueueName":              "attr-queue",
				"VisibilityTimeout":      "45",
				"MessageRetentionPeriod": "86400",
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::SQS::Queue",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)

			if tt.wantNotEmpty {
				assert.NotEmpty(t, physID)
			}

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SQS::Queue", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_SNSTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		logicalID    string
		props        map[string]any
		wantContains string
		doDelete     bool
	}{
		{
			name:         "explicit_name",
			logicalID:    "MyTopic",
			props:        map[string]any{"TopicName": "cfn-test-topic"},
			wantContains: "cfn-test-topic",
			doDelete:     true,
		},
		{
			name:         "default_name",
			logicalID:    "MyDefaultTopic",
			props:        map[string]any{},
			wantContains: "MyDefaultTopic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::SNS::Topic",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			if tt.doDelete {
				err = rc.Delete(t.Context(), "AWS::SNS::Topic", physID, tt.props)
				require.NoError(t, err)
			}
		})
	}
}

func TestResourceCreator_KinesisStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:      "basic_stream",
			logicalID: "MyStream",
			props: map[string]any{
				"Name":       "cfn-test-stream",
				"ShardCount": float64(1),
			},
			wantContains: "cfn-test-stream",
		},
		{
			name:      "default_name",
			logicalID: "MyStream2",
			props:     map[string]any{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Kinesis::Stream",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.NotEmpty(t, physID)

			if tt.wantContains != "" {
				assert.Contains(t, physID, tt.wantContains)
			}

			err = rc.Delete(t.Context(), "AWS::Kinesis::Stream", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_SNSSubscription(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create a topic first.
	topicARN, err := rc.Create(t.Context(), "MyTopic", "AWS::SNS::Topic",
		map[string]any{"TopicName": "cfn-test-topic"}, nil, nil)
	require.NoError(t, err)

	// Create subscription.
	subARN, err := rc.Create(t.Context(), "MySub", "AWS::SNS::Subscription",
		map[string]any{
			"TopicArn": topicARN,
			"Protocol": "sqs",
			"Endpoint": "https://sqs.us-east-1.amazonaws.com/000000000000/my-queue",
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, subARN)

	err = rc.Delete(t.Context(), "AWS::SNS::Subscription", subARN, nil)
	require.NoError(t, err)
}

func TestResourceCreator_EventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		props        map[string]any
		name         string
		logicalID    string
		wantContains string
	}{
		{
			name:         "custom_event_bus",
			logicalID:    "MyEventBus",
			props:        map[string]any{"Name": "cfn-custom-bus"},
			wantContains: "cfn-custom-bus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backends := newExtendedServiceBackends()
			rc := cloudformation.NewResourceCreator(backends)

			physID, err := rc.Create(
				t.Context(),
				tt.logicalID,
				"AWS::Events::EventBus",
				tt.props,
				nil,
				nil,
			)
			require.NoError(t, err)
			assert.Contains(t, physID, tt.wantContains)

			err = rc.Delete(t.Context(), "AWS::Events::EventBus", physID, nil)
			require.NoError(t, err)
		})
	}
}

func TestResourceCreator_S3BucketPolicy(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create bucket first.
	bucketName, err := rc.Create(t.Context(), "MyBucket", "AWS::S3::Bucket",
		map[string]any{"BucketName": "cfn-test-bucket-policy"}, nil, nil)
	require.NoError(t, err)

	// Apply bucket policy.
	physID, err := rc.Create(t.Context(), "MyBucketPolicy", "AWS::S3::BucketPolicy",
		map[string]any{
			"Bucket":         bucketName,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, bucketName, physID)

	err = rc.Delete(t.Context(), "AWS::S3::BucketPolicy", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_SQSQueuePolicy(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create queue first.
	queueURL, err := rc.Create(t.Context(), "MyQueue", "AWS::SQS::Queue",
		map[string]any{"QueueName": "cfn-test-queue-policy"}, nil, nil)
	require.NoError(t, err)

	// Apply queue policy.
	physID, err := rc.Create(t.Context(), "MyQueuePolicy", "AWS::SQS::QueuePolicy",
		map[string]any{
			"Queues":         []any{queueURL},
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)
	assert.NotEmpty(t, physID)

	err = rc.Delete(t.Context(), "AWS::SQS::QueuePolicy", physID, nil)
	require.NoError(t, err)
}

func TestResourceCreator_DeleteSNSSubscription_NilBackend(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends() // SNS field is set but we want to test nil case; override
	backends.SNS = nil
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "AWS::SNS::Subscription",
		"arn:aws:sns:us-east-1:000000000000:topic:sub-id", nil)
	require.NoError(t, err)
}

func TestResourceCreator_DeleteS3BucketPolicy_NilBackend(t *testing.T) {
	t.Parallel()

	backends := newServiceBackends()
	backends.S3 = nil
	rc := cloudformation.NewResourceCreator(backends)

	err := rc.Delete(t.Context(), "AWS::S3::BucketPolicy", "my-bucket", nil)
	require.NoError(t, err)
}

func TestResourceCreator_DeleteS3BucketPolicy_RealBackend(t *testing.T) {
	t.Parallel()

	backends := newExtendedServiceBackends()
	rc := cloudformation.NewResourceCreator(backends)

	// Create bucket then apply policy, then delete policy.
	bucketName, err := rc.Create(t.Context(), "DelBucket", "AWS::S3::Bucket",
		map[string]any{"BucketName": "cfn-del-bucket-pol"}, nil, nil)
	require.NoError(t, err)

	physID, err := rc.Create(t.Context(), "DelBucketPolicy", "AWS::S3::BucketPolicy",
		map[string]any{
			"Bucket":         bucketName,
			"PolicyDocument": `{"Version":"2012-10-17","Statement":[]}`,
		}, nil, nil)
	require.NoError(t, err)

	err = rc.Delete(t.Context(), "AWS::S3::BucketPolicy", physID, nil)
	require.NoError(t, err)
}
