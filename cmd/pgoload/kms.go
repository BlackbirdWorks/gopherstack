package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
)

// ensureKMSKey creates one KMS key for the whole run and returns its ID.
func ensureKMSKey(ctx context.Context, cl *kms.Client, log *slog.Logger) (string, error) {
	out, err := cl.CreateKey(ctx, &kms.CreateKeyInput{Description: aws.String("pgoload key")})
	if err != nil {
		return "", fmt.Errorf("create key: %w", err)
	}

	log.InfoContext(ctx, "kms key ready", "keyId", aws.ToString(out.KeyMetadata.KeyId))

	return aws.ToString(out.KeyMetadata.KeyId), nil
}

// kmsWorker repeatedly runs a mix of KMS operations, staggered by workerID,
// until ctx is done.
func kmsWorker(ctx context.Context, cl *kms.Client, res *resources, workerID int, c *opCounter, log *slog.Logger) {
	ops := []opFunc{
		func(ctx context.Context, _, i int) error { return kmsEncryptDecryptOp(ctx, cl, res.kmsKeyID, i) },
		func(ctx context.Context, _, _ int) error { return kmsGenerateDataKeyOp(ctx, cl, res.kmsKeyID) },
		func(ctx context.Context, _, _ int) error { return kmsDescribeKeyOp(ctx, cl, res.kmsKeyID) },
		func(ctx context.Context, _, _ int) error { return kmsListKeysOp(ctx, cl) },
	}

	runOpLoop(ctx, workerID, ops, c, "kms", log)
}

// kmsEncryptDecryptOp encrypts a payload and immediately decrypts it back,
// exercising both wire paths without needing shared ciphertext state.
func kmsEncryptDecryptOp(ctx context.Context, cl *kms.Client, keyID string, i int) error {
	enc, err := cl.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: fmt.Appendf(nil, "pgoload-plaintext-%d", i),
	})
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}

	if _, decErr := cl.Decrypt(ctx, &kms.DecryptInput{
		KeyId:          aws.String(keyID),
		CiphertextBlob: enc.CiphertextBlob,
	}); decErr != nil {
		return fmt.Errorf("decrypt: %w", decErr)
	}

	return nil
}

func kmsGenerateDataKeyOp(ctx context.Context, cl *kms.Client, keyID string) error {
	_, err := cl.GenerateDataKey(ctx, &kms.GenerateDataKeyInput{
		KeyId:   aws.String(keyID),
		KeySpec: kmstypes.DataKeySpecAes256,
	})

	return err
}

func kmsDescribeKeyOp(ctx context.Context, cl *kms.Client, keyID string) error {
	_, err := cl.DescribeKey(ctx, &kms.DescribeKeyInput{KeyId: aws.String(keyID)})

	return err
}

func kmsListKeysOp(ctx context.Context, cl *kms.Client) error {
	_, err := cl.ListKeys(ctx, &kms.ListKeysInput{})

	return err
}
