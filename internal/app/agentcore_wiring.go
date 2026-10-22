package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	agentcoredataplane "github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
	agentcorebackend "github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

var errAgentCoreNoSecrets = errors.New("secrets manager is not wired")

const (
	maxAgentCoreObjectBytes = 64 << 20
	arnResourceFields       = 7
)

// agentCoreRefs resolves the resources AgentCore control-plane resources point at in other emulated services.
type agentCoreRefs struct {
	ecr     *ecrbackend.Handler
	lambda  *lambdabackend.Handler
	secrets *secretsmanagerbackend.Handler
	s3      *s3backend.S3Handler
}

func scopedCtx(account, region string) context.Context {
	return awsmeta.Set(context.Background(), &awsmeta.Metadata{
		Account: account, Region: region, Partition: awsmeta.DefaultPartition,
	})
}

func (r agentCoreRefs) ECRRepositoryExists(account, region, repository string) bool {
	if r.ecr == nil {
		return true
	}

	_, err := r.ecr.BackendFor(region).DescribeRepositories(scopedCtx(account, region), []string{repository})

	return err == nil
}

func (r agentCoreRefs) LambdaFunctionExists(functionARN string) bool {
	if r.lambda == nil {
		return true
	}

	parts := strings.Split(functionARN, ":")
	if len(parts) < arnResourceFields {
		return false
	}

	_, err := r.lambda.BackendFor(parts[3]).GetFunction(parts[6])

	return err == nil
}

func (r agentCoreRefs) SecretExists(secretARN string) bool {
	if r.secrets == nil {
		return true
	}

	parts := strings.Split(secretARN, ":")
	if len(parts) < arnResourceFields {
		return false
	}

	_, err := r.secrets.Backend.DescribeSecret(scopedCtx(parts[4], parts[3]),
		&secretsmanagerbackend.DescribeSecretInput{SecretID: secretARN})

	return err == nil
}

// PutSecret creates the secret or stores value as its new current version.
func (r agentCoreRefs) PutSecret(account, region, name, value string) (string, error) {
	if r.secrets == nil {
		return "", errAgentCoreNoSecrets
	}

	ctx := scopedCtx(account, region)

	existing, derr := r.secrets.Backend.DescribeSecret(ctx, &secretsmanagerbackend.DescribeSecretInput{SecretID: name})
	if derr == nil {
		out, perr := r.secrets.Backend.PutSecretValue(ctx,
			&secretsmanagerbackend.PutSecretValueInput{SecretID: existing.ARN, SecretString: value})
		if perr != nil {
			return "", fmt.Errorf("put secret value: %w", perr)
		}

		return out.ARN, nil
	}

	out, err := r.secrets.Backend.CreateSecret(
		ctx,
		&secretsmanagerbackend.CreateSecretInput{
			Name:         name,
			SecretString: value,
			Description:  "Bedrock AgentCore identity",
		},
	)
	if err != nil {
		return "", fmt.Errorf("create secret: %w", err)
	}

	return out.ARN, nil
}

// GetSecret returns the current string value of the secret.
func (r agentCoreRefs) GetSecret(secretARN string) (string, bool) {
	parts := strings.Split(secretARN, ":")
	if r.secrets == nil || len(parts) < arnResourceFields {
		return "", false
	}

	out, err := r.secrets.Backend.GetSecretValue(scopedCtx(parts[4], parts[3]),
		&secretsmanagerbackend.GetSecretValueInput{SecretID: secretARN})
	if err != nil {
		return "", false
	}

	return out.SecretString, true
}

// DeleteSecret removes the secret without a recovery window.
func (r agentCoreRefs) DeleteSecret(secretARN string) {
	parts := strings.Split(secretARN, ":")
	if r.secrets == nil || len(parts) < arnResourceFields {
		return
	}

	_, _ = r.secrets.Backend.DeleteSecret(scopedCtx(parts[4], parts[3]),
		&secretsmanagerbackend.DeleteSecretInput{SecretID: secretARN, ForceDeleteWithoutRecovery: true})
}

func (r agentCoreRefs) ReadObject(bucket, key string) ([]byte, bool) {
	if r.s3 == nil {
		return nil, false
	}

	out, err := r.s3.Backend.GetObject(context.Background(), &awss3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil || out == nil || out.Body == nil {
		return nil, false
	}

	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxAgentCoreObjectBytes))
	if err != nil {
		return nil, false
	}

	return data, true
}

// wireAgentCore lets AgentCore validate ECR images, Lambda targets, secrets and dataset objects against the emulator.
func wireAgentCore(byName map[string]service.Registerable) {
	h, ok := byName["BedrockAgentCoreControl"].(*agentcorebackend.Handler)
	if !ok {
		return
	}

	refs := agentCoreRefs{}
	refs.ecr, _ = byName["ECR"].(*ecrbackend.Handler)
	refs.lambda, _ = byName["Lambda"].(*lambdabackend.Handler)
	refs.secrets, _ = byName["SecretsManager"].(*secretsmanagerbackend.Handler)
	refs.s3, _ = byName["S3"].(*s3backend.S3Handler)

	h.Backend.SetReferences(refs)

	if dp, found := byName["BedrockAgentCore"].(*agentcoredataplane.Handler); found {
		dp.Backend.SetControl(h.Backend.DataPlane())
	}
}
