package cloudformation

import (
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	kmsbackend "github.com/blackbirdworks/gopherstack/kms"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	secretsmanagerbackend "github.com/blackbirdworks/gopherstack/secretsmanager"
	snsbackend "github.com/blackbirdworks/gopherstack/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/sqs"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
)

// BackendsProvider is a private interface to extract service backends for resource creation.
type BackendsProvider interface {
	GetDynamoDBHandler() service.Registerable
	GetS3Handler() service.Registerable
	GetSQSHandler() service.Registerable
	GetSNSHandler() service.Registerable
	GetSSMHandler() service.Registerable
	GetKMSHandler() service.Registerable
	GetSecretsManagerHandler() service.Registerable
	GetGlobalConfig() config.GlobalConfig
}

// Provider implements service.Provider for the CloudFormation service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string { return "CloudFormation" }

// Init initializes the CloudFormation service backend and handler.
//
//nolint:ireturn,nolintlint // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	accountID := MockAccountID
	region := MockRegion

	var backends *ServiceBackends

	if bp, ok := ctx.Config.(BackendsProvider); ok {
		cfg := bp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region

		backends = &ServiceBackends{
			AccountID: accountID,
			Region:    region,
		}

		if h := bp.GetDynamoDBHandler(); h != nil {
			if ddb, ok := h.(*ddbbackend.DynamoDBHandler); ok {
				backends.DynamoDB = ddb
			}
		}
		if h := bp.GetS3Handler(); h != nil {
			if s3, ok := h.(*s3backend.S3Handler); ok {
				backends.S3 = s3
			}
		}
		if h := bp.GetSQSHandler(); h != nil {
			if sqs, ok := h.(*sqsbackend.Handler); ok {
				backends.SQS = sqs
			}
		}
		if h := bp.GetSNSHandler(); h != nil {
			if sns, ok := h.(*snsbackend.Handler); ok {
				backends.SNS = sns
			}
		}
		if h := bp.GetSSMHandler(); h != nil {
			if ssm, ok := h.(*ssmbackend.Handler); ok {
				backends.SSM = ssm
			}
		}
		if h := bp.GetKMSHandler(); h != nil {
			if kms, ok := h.(*kmsbackend.Handler); ok {
				backends.KMS = kms
			}
		}
		if h := bp.GetSecretsManagerHandler(); h != nil {
			if sm, ok := h.(*secretsmanagerbackend.Handler); ok {
				backends.SecretsManager = sm
			}
		}
	} else if cp, ok := ctx.Config.(config.Provider); ok {
		cfg := cp.GetGlobalConfig()
		accountID = cfg.AccountID
		region = cfg.Region
	}

	creator := NewResourceCreator(backends)
	backend := NewInMemoryBackendWithConfig(accountID, region, creator)
	handler := NewHandler(backend, ctx.Logger)

	return handler, nil
}
