package app

import (
	"context"
	"io"
	"strings"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	agentcorebackend "github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
	ecrbackend "github.com/blackbirdworks/gopherstack/services/ecr"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

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
}
