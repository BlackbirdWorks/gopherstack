package app

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
)

const lambdaKMSRole = "arn:aws:iam::000000000000:role/r"

func lambdaKMSCreate(
	t *testing.T, lc *lambda.Client, name, keyARN string, vars map[string]string,
) (*lambda.CreateFunctionOutput, error) {
	t.Helper()

	return lc.CreateFunction(t.Context(), &lambda.CreateFunctionInput{
		FunctionName: aws.String(name), PackageType: lambdatypes.PackageTypeImage,
		Code:        &lambdatypes.FunctionCode{ImageUri: aws.String("x:latest")},
		Role:        aws.String(lambdaKMSRole),
		KMSKeyArn:   aws.String(keyARN),
		Environment: &lambdatypes.Environment{Variables: vars},
	})
}

func TestLambdaKMSEnvironmentEncryption(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	lc := lambda.NewFromConfig(fx.cfg)
	kc := kms.NewFromConfig(fx.cfg)

	key, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
	require.NoError(t, err)

	keyARN := aws.ToString(key.KeyMetadata.Arn)
	vars := map[string]string{"DB_PASSWORD": "hunter2-plaintext", "EMPTY": ""}

	created, err := lambdaKMSCreate(t, lc, "kms-fn", keyARN, vars)
	require.NoError(t, err)
	assert.Equal(t, keyARN, aws.ToString(created.KMSKeyArn))

	cfg, err := lc.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
		FunctionName: aws.String("kms-fn"),
	})
	require.NoError(t, err)
	assert.Equal(t, vars, cfg.Environment.Variables)
	assert.Equal(t, keyARN, aws.ToString(cfg.KMSKeyArn))

	bk := fx.lambdaBackend(t)
	snap := string(bk.Snapshot(t.Context()))
	assert.NotContains(t, snap, "hunter2-plaintext", "snapshot must hold ciphertext only")
	assert.Contains(t, snap, "envCiphertext")

	t.Run("restore_keeps_plaintext_readable", func(t *testing.T) {
		require.NoError(t, bk.Restore(t.Context(), []byte(snap)))

		got, gerr := lc.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
			FunctionName: aws.String("kms-fn"),
		})
		require.NoError(t, gerr)
		assert.Equal(t, vars, got.Environment.Variables)
	})

	t.Run("published_version_keeps_ciphertext", func(t *testing.T) {
		ver, verr := lc.PublishVersion(t.Context(), &lambda.PublishVersionInput{FunctionName: aws.String("kms-fn")})
		require.NoError(t, verr)

		got, gerr := lc.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
			FunctionName: aws.String("kms-fn"), Qualifier: ver.Version,
		})
		require.NoError(t, gerr)
		assert.Equal(t, vars, got.Environment.Variables)
		assert.NotContains(t, string(bk.Snapshot(t.Context())), "hunter2-plaintext")
	})
}

func TestLambdaKMSKeyValidation(t *testing.T) {
	t.Parallel()

	fx := newSFNFixture(t)
	lc := lambda.NewFromConfig(fx.cfg)
	kc := kms.NewFromConfig(fx.cfg)

	disabled, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	_, err = kc.DisableKey(t.Context(), &kms.DisableKeyInput{KeyId: disabled.KeyMetadata.KeyId})
	require.NoError(t, err)

	pending, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	_, err = kc.ScheduleKeyDeletion(t.Context(), &kms.ScheduleKeyDeletionInput{
		KeyId: pending.KeyMetadata.KeyId, PendingWindowInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	signing, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{
		KeyUsage: kmstypes.KeyUsageTypeSignVerify, KeySpec: kmstypes.KeySpecRsa2048,
	})
	require.NoError(t, err)

	tests := []struct {
		name   string
		keyARN string
	}{
		{name: "missing", keyARN: "arn:aws:kms:us-east-1:000000000000:key/00000000-0000-0000-0000-000000000000"},
		{name: "disabled", keyARN: aws.ToString(disabled.KeyMetadata.Arn)},
		{name: "pending_deletion", keyARN: aws.ToString(pending.KeyMetadata.Arn)},
		{name: "wrong_usage", keyARN: aws.ToString(signing.KeyMetadata.Arn)},
		{name: "not_an_arn", keyARN: "alias/foo"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, cerr := lambdaKMSCreate(t, lc, "bad-"+tt.name, tt.keyARN, map[string]string{"VAR_ONE": "b"})
			require.Error(t, cerr)
			assert.Equal(t, "InvalidParameterValueException", apiErrorCode(cerr))
		})
	}

	t.Run("update_with_disabled_key", func(t *testing.T) {
		t.Parallel()

		good, gerr := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
		require.NoError(t, gerr)
		_, gerr = lambdaKMSCreate(t, lc, "upd-fn", aws.ToString(good.KeyMetadata.Arn), map[string]string{"VAR_ONE": "b"})
		require.NoError(t, gerr)

		_, gerr = lc.UpdateFunctionConfiguration(t.Context(), &lambda.UpdateFunctionConfigurationInput{
			FunctionName: aws.String("upd-fn"), KMSKeyArn: disabled.KeyMetadata.Arn,
		})
		require.Error(t, gerr)
		assert.Equal(t, "InvalidParameterValueException", apiErrorCode(gerr))
	})
}

func TestLambdaKMSInvokeKeyState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		mutate   func(t *testing.T, kc *kms.Client, keyID *string)
		wantCode string
	}{
		{
			name: "disabled",
			mutate: func(t *testing.T, kc *kms.Client, keyID *string) {
				t.Helper()
				_, err := kc.DisableKey(t.Context(), &kms.DisableKeyInput{KeyId: keyID})
				require.NoError(t, err)
			},
			wantCode: "KMSDisabledException",
		},
		{
			name: "pending_deletion",
			mutate: func(t *testing.T, kc *kms.Client, keyID *string) {
				t.Helper()
				_, err := kc.ScheduleKeyDeletion(t.Context(), &kms.ScheduleKeyDeletionInput{
					KeyId: keyID, PendingWindowInDays: aws.Int32(7),
				})
				require.NoError(t, err)
			},
			wantCode: "KMSInvalidStateException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newSFNFixture(t)
			lc := lambda.NewFromConfig(fx.cfg)
			kc := kms.NewFromConfig(fx.cfg)

			key, err := kc.CreateKey(t.Context(), &kms.CreateKeyInput{})
			require.NoError(t, err)

			_, err = lambdaKMSCreate(t, lc, "inv-fn", aws.ToString(key.KeyMetadata.Arn), map[string]string{"VAR_ONE": "b"})
			require.NoError(t, err)

			tt.mutate(t, kc, key.KeyMetadata.KeyId)

			_, err = lc.Invoke(t.Context(), &lambda.InvokeInput{FunctionName: aws.String("inv-fn")})
			require.Error(t, err)
			assert.Equal(t, tt.wantCode, apiErrorCode(err))

			cfg, err := lc.GetFunctionConfiguration(t.Context(), &lambda.GetFunctionConfigurationInput{
				FunctionName: aws.String("inv-fn"),
			})
			require.NoError(t, err)
			assert.Equal(t, map[string]string{"VAR_ONE": "b"}, cfg.Environment.Variables)
		})
	}
}

func (fx *sfnFixture) lambdaBackend(t *testing.T) *lambdabackend.InMemoryBackend {
	t.Helper()

	h, ok := serviceByName(fx.services)["Lambda"].(*lambdabackend.Handler)
	require.True(t, ok)

	bk, ok := h.Backend.(*lambdabackend.InMemoryBackend)
	require.True(t, ok)

	t.Cleanup(func() { bk.Close(context.Background()) })

	return bk
}
