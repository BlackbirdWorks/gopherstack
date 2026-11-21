package app

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	appintegrationsbackend "github.com/blackbirdworks/gopherstack/services/appintegrations"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	rambackend "github.com/blackbirdworks/gopherstack/services/ram"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
)

// connectMailer delivers Connect outbound email through SES.
type connectMailer struct{ ses *sesbackend.InMemoryBackend }

func (m connectMailer) SendEmail(_ context.Context, msg connectbackend.EmailMessage) error {
	_, err := m.ses.SendEmail(sesbackend.SendEmailInput{
		From: msg.From, To: msg.To, Cc: msg.Cc, Subject: msg.Subject, BodyHTML: msg.HTML, BodyText: msg.Text,
	})

	return err
}

// connectShares creates the RAM resource shares behind Connect analytics data set associations.
type connectShares struct{ ram *rambackend.InMemoryBackend }

func (c connectShares) CreateShare(_ context.Context, name, targetAccountID, resourceARN string) (string, error) {
	rs, err := c.ram.CreateResourceShare(name, true, nil, []string{targetAccountID}, []string{resourceARN})
	if err != nil {
		return "", err
	}

	return rs.ARN, nil
}

func (c connectShares) DeleteShare(_ context.Context, shareARN string) error {
	return c.ram.DeleteResourceShare(shareARN)
}

// connectResolvers checks Lambda functions and S3 objects named by Connect configuration.
type connectResolvers struct {
	lambda lambdabackend.StorageBackend
	s3     s3backend.StorageBackend
	s3h    *s3backend.S3Handler
}

const connectPresignFallbackKey = "AKIAGOPHERSTACK"

func (r connectResolvers) EnsureBucket(ctx context.Context, bucket string) error {
	if r.BucketExists(ctx, bucket) {
		return nil
	}

	_, err := r.s3.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: &bucket})
	if err != nil && !r.BucketExists(ctx, bucket) {
		return err
	}

	return nil
}

func (r connectResolvers) ObjectSize(ctx context.Context, bucket, key string) (int64, bool, error) {
	out, err := r.s3.HeadObject(ctx, &awss3.HeadObjectInput{Bucket: &bucket, Key: &key})
	if errors.Is(err, s3backend.ErrNoSuchKey) {
		return 0, false, nil
	}

	if err != nil {
		return 0, false, err
	}

	return aws.ToInt64(out.ContentLength), true, nil
}

func (r connectResolvers) PutObject(ctx context.Context, bucket, key string, data []byte) error {
	_, err := r.s3.PutObject(ctx, &awss3.PutObjectInput{Bucket: &bucket, Key: &key, Body: bytes.NewReader(data)})

	return err
}

func (r connectResolvers) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := r.s3.DeleteObject(ctx, &awss3.DeleteObjectInput{Bucket: &bucket, Key: &key})

	return err
}

func (r connectResolvers) Presign(
	ctx context.Context,
	method, baseURL, bucket, key string,
	ttl time.Duration,
	at time.Time,
	bound map[string]string,
) string {
	accessKey := awsmeta.AccessKeyID(ctx)
	if accessKey == "" {
		accessKey = connectPresignFallbackKey
	}

	region := awsmeta.Region(ctx)
	if region == "" {
		region = r.s3h.DefaultRegion
	}

	target := baseURL + "/" + bucket + "/" + (&url.URL{Path: key}).EscapedPath()

	return r.s3h.PresignURL(method, target, accessKey, region, ttl, at, bound)
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

func (r connectResolvers) ReadObject(ctx context.Context, bucket, key string) ([]byte, error) {
	out, err := r.s3.GetObject(ctx, &awss3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, err
	}

	defer out.Body.Close()

	return io.ReadAll(out.Body)
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

	if sesH, ok := byName["SES"].(*sesbackend.Handler); ok {
		if bk, bkOK := sesH.Backend.(*sesbackend.InMemoryBackend); bkOK {
			cn.Backend.SetMailer(connectMailer{ses: bk})
		}
	}

	if ramH, ok := byName["RAM"].(*rambackend.Handler); ok {
		if bk, bkOK := ramH.Backend.(*rambackend.InMemoryBackend); bkOK {
			cn.Backend.SetShares(connectShares{ram: bk})
		}
	}

	lambdaH, lok := byName["Lambda"].(*lambdabackend.Handler)
	s3H, sok := byName["S3"].(*s3backend.S3Handler)

	if lok && sok {
		cn.Backend.SetResolvers(connectResolvers{lambda: lambdaH.Backend, s3: s3H.Backend, s3h: s3H})
	}
}
