package dashboard

import (
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/s3"
	"github.com/blackbirdworks/gopherstack/sns"
	"github.com/blackbirdworks/gopherstack/ssm"
)

// AWSSDKProvider is a private interface to extract AWS SDK clients
// from the abstract AppContext Config.
type AWSSDKProvider interface {
	GetDynamoDBClient() *ddbsdk.Client
	GetS3Client() *s3sdk.Client
	GetSSMClient() *ssmsdk.Client
	GetDynamoDBHandler() service.Registerable
	GetS3Handler() service.Registerable
	GetSSMHandler() service.Registerable
	GetSNSHandler() service.Registerable
}

// Provider implements service.Provider for the Dashboard service.
type Provider struct{}

// Name returns the logical name of the provider.
func (p *Provider) Name() string {
	return "Dashboard"
}

// Init initializes the Dashboard service.
//
//nolint:ireturn // architecturally required to return interface
func (p *Provider) Init(ctx *service.AppContext) (service.Registerable, error) {
	var ddbClient *ddbsdk.Client
	var s3Client *s3sdk.Client
	var ssmClient *ssmsdk.Client
	var ddbHandler service.Registerable
	var s3Handler service.Registerable
	var ssmHandler service.Registerable
	var snsHandler service.Registerable

	// Try to extract SDK clients and handlers if the config implements the extractor interface
	if ap, ok := ctx.Config.(AWSSDKProvider); ok {
		ddbClient = ap.GetDynamoDBClient()
		s3Client = ap.GetS3Client()
		ssmClient = ap.GetSSMClient()
		ddbHandler = ap.GetDynamoDBHandler()
		s3Handler = ap.GetS3Handler()
		ssmHandler = ap.GetSSMHandler()
		snsHandler = ap.GetSNSHandler()
	}

	// For dashboard, having the clients mapped is pretty much a requirement.
	// The dashboard logic connects via SDK back into the memory stores.

	ddb, _ := ddbHandler.(*dynamodb.DynamoDBHandler)
	s3h, _ := s3Handler.(*s3.S3Handler)
	var ssmOps *ssm.Handler
	if ssmHandler != nil {
		ssmOps, _ = ssmHandler.(*ssm.Handler)
	}
	var snsOps *sns.Handler
	if snsHandler != nil {
		snsOps, _ = snsHandler.(*sns.Handler)
	}

	handler := NewHandler(ddbClient, s3Client, ssmClient, ddb, s3h, ssmOps, snsOps, ctx.Logger)

	return handler, nil
}
