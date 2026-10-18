package app

import (
	"context"

	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appintegrationsbackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

// connectResolvers checks Lambda functions and S3 objects named by Connect configuration.
type connectResolvers struct {
	lambda lambdabackend.StorageBackend
	s3     s3backend.StorageBackend
}

func (r connectResolvers) LambdaFunctionExists(_ context.Context, functionARN string) bool {
	_, err := r.lambda.GetFunction(functionARN)

	return err == nil
}

func (r connectResolvers) BucketExists(ctx context.Context, bucket string) bool {
	_, err := r.s3.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: &bucket})

	return err == nil
}

func (r connectResolvers) ObjectExists(ctx context.Context, bucket, key string) bool {
	_, err := r.s3.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &bucket, Key: &key})

	return err == nil
}

// connectIntegrations registers Connect integration associations with AppIntegrations.
type connectIntegrations struct {
	ai *appintegrationsbackend.InMemoryBackend
}

func (c connectIntegrations) CreateEventAssociation(ctx context.Context, name, clientID string) (string, error) {
	a, err := c.ai.CreateEventIntegrationAssociation(ctx, name, clientID, nil)
	if err != nil {
		return "", err
	}

	return a.EventIntegrationAssociationID, nil
}

func (c connectIntegrations) DeleteEventAssociation(ctx context.Context, id string) error {
	return c.ai.DeleteEventIntegrationAssociation(ctx, id)
}

func (c connectIntegrations) CreateApplicationAssociation(ctx context.Context, appID, clientID string) (string, error) {
	a, err := c.ai.CreateApplicationAssociation(ctx, appID, clientID)
	if err != nil {
		return "", err
	}

	return a.AssociationID, nil
}

func (c connectIntegrations) DeleteApplicationAssociation(_ context.Context, id string) error {
	return c.ai.DeleteApplicationAssociation(id)
}

// appIntegrationsEventRules lets event integration associations own EventBridge rules.
type appIntegrationsEventRules struct{ eb ebbackend.StorageBackend }

func (e appIntegrationsEventRules) BusExists(ctx context.Context, bus string) bool {
	_, err := e.eb.DescribeEventBus(ctx, bus)

	return err == nil
}

func (e appIntegrationsEventRules) PutRule(ctx context.Context, bus, name, source string) error {
	_, err := e.eb.PutRule(ctx, ebbackend.PutRuleInput{
		Name: name, EventBusName: bus, State: "ENABLED", EventPattern: `{"source":["` + source + `"]}`,
		Description: "AppIntegrations event integration association",
	})

	return err
}

func (e appIntegrationsEventRules) DeleteRule(ctx context.Context, bus, name string) error {
	return e.eb.DeleteRuleForce(ctx, name, bus, true)
}

// wireConnectIntegrations connects Connect and AppIntegrations to Lambda, S3 and EventBridge.
func wireConnectIntegrations(byName map[string]service.Registerable) {
	ai, _ := byName["AppIntegrations"].(*appintegrationsbackend.Handler)
	cn, _ := byName["Connect"].(*connectbackend.Handler)

	if ai != nil {
		if eb, ok := byName["EventBridge"].(*ebbackend.Handler); ok {
			ai.Backend.SetEventRules(appIntegrationsEventRules{eb: eb.Backend})
		}
	}

	if cn == nil {
		return
	}

	if ai != nil {
		cn.Backend.SetIntegrations(connectIntegrations{ai: ai.Backend})
	}

	lambdaH, lok := byName["Lambda"].(*lambdabackend.Handler)
	s3H, sok := byName["S3"].(*s3backend.S3Handler)

	if lok && sok {
		cn.Backend.SetResolvers(connectResolvers{lambda: lambdaH.Backend, s3: s3H.Backend})
	}
}
