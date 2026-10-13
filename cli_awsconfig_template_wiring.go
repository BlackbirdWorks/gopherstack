package main

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	awsconfigbackend "github.com/blackbirdworks/gopherstack/services/awsconfig"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/services/ssm"
	sfnbackend "github.com/blackbirdworks/gopherstack/services/stepfunctions"
	sfnasl "github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// awsconfigTemplateSource adapts S3 and SSM to awsconfig.TemplateSource.
type awsconfigTemplateSource struct {
	s3  sfnasl.S3Reader
	ssm ssmbackend.StorageBackend
}

func (a awsconfigTemplateSource) S3Template(ctx context.Context, bucket, key string) ([]byte, error) {
	return a.s3.GetObjectBytes(ctx, bucket, key)
}

func (a awsconfigTemplateSource) SSMTemplate(ctx context.Context, name, version string) (string, error) {
	out, err := a.ssm.GetDocument(ctx, &ssmbackend.GetDocumentInput{Name: name, DocumentVersion: version})
	if err != nil {
		return "", err
	}

	return out.Content, nil
}

// wireAWSConfigTemplates lets PutConformancePack read TemplateS3Uri and TemplateSSMDocumentDetails.
func wireAWSConfigTemplates(byName map[string]service.Registerable) {
	cfgH, ok := byName["AWSConfig"].(*awsconfigbackend.Handler)
	if !ok {
		return
	}

	s3H, ok := byName["S3"].(*s3backend.S3Handler)
	if !ok {
		return
	}

	ssmH, ok := byName["SSM"].(*ssmbackend.Handler)
	if !ok {
		return
	}

	cfgH.SetTemplateSource(awsconfigTemplateSource{s3: sfnbackend.NewS3Integration(s3H.Backend), ssm: ssmH.Backend})
}
