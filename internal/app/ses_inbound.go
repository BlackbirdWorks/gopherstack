package app

import (
	"bytes"
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	sesbackend "github.com/blackbirdworks/gopherstack/services/ses"
)

// sesInboundS3 adapts the S3 backend to ses.InboundObjectStore.
type sesInboundS3 struct{ bk *s3backend.InMemoryBackend }

func (a sesInboundS3) PutObject(ctx context.Context, bucket, key string, body []byte) error {
	_, err := a.bk.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
		Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	})

	return err
}

// wireSESInbound connects the SES inbound SMTP listener's S3 and Lambda receipt actions to their backends.
func wireSESInbound(byName map[string]service.Registerable) {
	sesH, ok := byName["SES"].(*sesbackend.Handler)
	if !ok {
		return
	}

	sesBk, ok := sesH.Backend.(*sesbackend.InMemoryBackend)
	if !ok {
		return
	}

	if s3H, s3Ok := byName["S3"].(*s3backend.S3Handler); s3Ok {
		if bk, bkOk := s3H.Backend.(*s3backend.InMemoryBackend); bkOk {
			sesBk.SetInboundObjectStore(sesInboundS3{bk: bk})
		}
	}

	if lH, lOk := byName["Lambda"].(*lambdabackend.Handler); lOk {
		if bk, bkOk := lH.Backend.(*lambdabackend.InMemoryBackend); bkOk {
			sesBk.SetInboundLambdaInvoker(bk)
		}
	}
}
