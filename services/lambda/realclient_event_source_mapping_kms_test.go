package lambda_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_EventSourceMappingKMS drives event source mapping and
// function code KMS/S3-field fixes through the real aws-sdk-go-v2 client
// (newTestLambdaClient, shared with realclient_durable_execution_test.go)
// and asserts the observable effect of each.
func TestRealClient_EventSourceMappingKMS(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCreateEventSourceMappingKMSKeyArn, "create_event_source_mapping_kms_key_arn"},
		{testUpdateEventSourceMappingKMSKeyArn, "update_event_source_mapping_kms_key_arn"},
		{testUpdateFunctionCodeS3ObjectStorageMode, "update_function_code_s3_object_storage_mode"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

// testCreateEventSourceMappingKMSKeyArn drives
// CreateEventSourceMappingInput.KMSKeyArn (a restjson1 body field, confirmed
// against awsRestjson1_serializeOpDocumentCreateEventSourceMappingInput --
// not httpQuery/Label/Header) through the real SDK client and asserts it
// round-trips on both the Create response and a follow-up
// GetEventSourceMapping.
func testCreateEventSourceMappingKMSKeyArn(t *testing.T) {
	t.Helper()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	createFunctionForTest(t, h, "slice4-esm-create-fn")

	createOut, err := client.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:slice4-esm-create-queue"),
		FunctionName:   aws.String("slice4-esm-create-fn"),
		KMSKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/slice4-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/slice4-key", aws.ToString(createOut.KMSKeyArn))

	getOut, err := client.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/slice4-key", aws.ToString(getOut.KMSKeyArn))
}

// testUpdateEventSourceMappingKMSKeyArn drives
// UpdateEventSourceMappingInput.KMSKeyArn (a restjson1 body field, same
// confirmation as above for the Update serializer) through the real SDK
// client and asserts it round-trips on both the Update response and a
// follow-up GetEventSourceMapping.
func testUpdateEventSourceMappingKMSKeyArn(t *testing.T) {
	t.Helper()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	createFunctionForTest(t, h, "slice4-esm-update-fn")

	createOut, err := client.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn: aws.String("arn:aws:sqs:us-east-1:000000000000:slice4-esm-update-queue"),
		FunctionName:   aws.String("slice4-esm-update-fn"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(createOut.KMSKeyArn))

	updOut, err := client.UpdateEventSourceMapping(ctx, &lambdasdk.UpdateEventSourceMappingInput{
		UUID:      createOut.UUID,
		KMSKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/slice4-updated-key"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/slice4-updated-key", aws.ToString(updOut.KMSKeyArn))

	getOut, err := client.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: createOut.UUID,
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/slice4-updated-key", aws.ToString(getOut.KMSKeyArn))
}

// testUpdateFunctionCodeS3ObjectStorageMode drives
// UpdateFunctionCodeInput.S3ObjectStorageMode (a restjson1 body field,
// confirmed against awsRestjson1_serializeOpDocumentUpdateFunctionCodeInput)
// through the real SDK client. UpdateFunctionCodeOutput itself carries no
// Code/ResolvedS3Object member (confirmed against
// api_op_UpdateFunctionCode.go -- it is FunctionConfiguration-shaped, not
// GetFunctionOutput-shaped), so the observable effect is checked via a
// follow-up GetFunction: REFERENCE mode must populate Code.ResolvedS3Object
// (the real, documented signal that Lambda is referencing the caller's S3
// object in place rather than uploading its own copy); the default COPY
// mode must leave it unset.
func testUpdateFunctionCodeS3ObjectStorageMode(t *testing.T) {
	t.Helper()

	h, _ := newInMemoryHandler(t)
	client := newTestLambdaClient(t, h)
	ctx := t.Context()

	_, err := client.CreateFunction(ctx, &lambdasdk.CreateFunctionInput{
		FunctionName: aws.String("slice4-code-storage-fn"),
		PackageType:  lambdatypes.PackageTypeZip,
		Runtime:      lambdatypes.RuntimeNodejs20x,
		Handler:      aws.String("index.handler"),
		Role:         aws.String("arn:aws:iam:::role/r"),
		Code:         &lambdatypes.FunctionCode{ZipFile: []byte("dummy-zip-bytes")},
	})
	require.NoError(t, err)

	_, err = client.UpdateFunctionCode(ctx, &lambdasdk.UpdateFunctionCodeInput{
		FunctionName:        aws.String("slice4-code-storage-fn"),
		S3Bucket:            aws.String("slice4-bucket"),
		S3Key:               aws.String("slice4-key.zip"),
		S3ObjectStorageMode: lambdatypes.S3ObjectStorageModeReference,
	})
	require.NoError(t, err)

	referenceGet, err := client.GetFunction(ctx, &lambdasdk.GetFunctionInput{
		FunctionName: aws.String("slice4-code-storage-fn"),
	})
	require.NoError(t, err)
	require.NotNil(t, referenceGet.Code)
	require.NotNil(t, referenceGet.Code.ResolvedS3Object, "REFERENCE mode must surface ResolvedS3Object")
	assert.Equal(t, "slice4-bucket", aws.ToString(referenceGet.Code.ResolvedS3Object.S3Bucket))
	assert.Equal(t, "slice4-key.zip", aws.ToString(referenceGet.Code.ResolvedS3Object.S3Key))

	_, err = client.UpdateFunctionCode(ctx, &lambdasdk.UpdateFunctionCodeInput{
		FunctionName: aws.String("slice4-code-storage-fn"),
		S3Bucket:     aws.String("slice4-bucket-2"),
		S3Key:        aws.String("slice4-key-2.zip"),
	})
	require.NoError(t, err)

	copyGet, err := client.GetFunction(ctx, &lambdasdk.GetFunctionInput{
		FunctionName: aws.String("slice4-code-storage-fn"),
	})
	require.NoError(t, err)
	require.NotNil(t, copyGet.Code)
	assert.Nil(t, copyGet.Code.ResolvedS3Object, "default COPY mode must not surface ResolvedS3Object")
}
