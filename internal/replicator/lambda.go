package replicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func (r *run) replicateLambda(ctx context.Context) {
	pager := lambda.NewListFunctionsPaginator(r.src.lambda, &lambda.ListFunctionsInput{})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			r.listFail(TypeLambda, err)

			return
		}

		for _, f := range page.Functions {
			name := aws.ToString(f.FunctionName)
			if r.match(name, aws.ToString(f.FunctionArn)) {
				r.copyFunction(ctx, name)
			}
		}
	}
}

func (r *run) copyFunction(ctx context.Context, name string) {
	got, err := r.src.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: &name})
	if err != nil {
		r.fail(TypeLambda, name, err)

		return
	}

	cfg := got.Configuration
	if cfg.PackageType == lambdatypes.PackageTypeImage {
		r.add(TypeLambda, name, ActionSkipped, imageSkipReason(got))

		return
	}

	_, err = r.dst.lambda.GetFunction(ctx, &lambda.GetFunctionInput{FunctionName: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeLambda, name, err)

		return
	}

	f := fnCopy{name: name, cfg: cfg}
	f.vpc = r.planVPC(ctx, name, cfg)

	if r.cfg.DryRun {
		r.add(TypeLambda, name, upsert(exists), "")
		r.copyExtras(ctx, f, exists)

		return
	}

	if got.Code == nil || aws.ToString(got.Code.Location) == "" {
		r.fail(TypeLambda, name, errNoCodeLocation)

		return
	}

	zip, err := r.downloadCode(ctx, aws.ToString(got.Code.Location))
	if err != nil {
		r.fail(TypeLambda, name, err)

		return
	}

	f.zip = zip

	if exists {
		err = r.updateFunction(ctx, f)
	} else {
		err = r.createFunction(ctx, f, got.Tags)
	}

	r.done(TypeLambda, name, upsert(exists), err)

	if err == nil {
		r.copyExtras(ctx, f, exists)
	}
}

func imageSkipReason(got *lambda.GetFunctionOutput) string {
	uri := ""
	if got.Code != nil {
		uri = aws.ToString(got.Code.ImageUri)
	}

	return "container image function (image " + uri + " lives in ECR; ECR replication is not available)"
}

func (r *run) downloadCode(ctx context.Context, location string) ([]byte, error) {
	rest, ok := strings.CutPrefix(location, "s3://")
	if !ok {
		return r.src.http.fetch(ctx, location, r.cfg.MaxObjectBytes)
	}

	bucket, key, _ := strings.Cut(rest, "/")

	obj, err := r.src.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return nil, fmt.Errorf("get code object: %w", err)
	}
	defer obj.Body.Close()

	body, err := io.ReadAll(io.LimitReader(obj.Body, r.cfg.MaxObjectBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read code object: %w", err)
	}

	if int64(len(body)) > r.cfg.MaxObjectBytes {
		return nil, errTooLarge
	}

	return body, nil
}

var errNoCodeLocation = errors.New("source returned no code location")

func (r *run) createFunction(ctx context.Context, f fnCopy, tags map[string]string) error {
	c, zip := f.cfg, f.zip
	in := &lambda.CreateFunctionInput{
		FunctionName: c.FunctionName, Runtime: c.Runtime, Handler: c.Handler,
		Role:        aws.String(r.rm.ARN(aws.ToString(c.Role))),
		Code:        &lambdatypes.FunctionCode{ZipFile: zip},
		Description: c.Description, Timeout: c.Timeout, MemorySize: c.MemorySize,
		Architectures: c.Architectures, Tags: tags, PackageType: lambdatypes.PackageTypeZip,
		VpcConfig: f.vpc,
	}
	if c.Environment != nil && len(c.Environment.Variables) > 0 {
		in.Environment = &lambdatypes.Environment{Variables: c.Environment.Variables}
	}

	if c.TracingConfig != nil {
		in.TracingConfig = &lambdatypes.TracingConfig{Mode: c.TracingConfig.Mode}
	}

	if c.EphemeralStorage != nil {
		in.EphemeralStorage = &lambdatypes.EphemeralStorage{Size: c.EphemeralStorage.Size}
	}

	if c.DeadLetterConfig != nil {
		in.DeadLetterConfig = &lambdatypes.DeadLetterConfig{
			TargetArn: aws.String(r.rm.ARN(aws.ToString(c.DeadLetterConfig.TargetArn))),
		}
	}

	for _, l := range c.Layers {
		in.Layers = append(in.Layers, r.rm.ARN(aws.ToString(l.Arn)))
	}

	if _, err := r.dst.lambda.CreateFunction(ctx, in); err != nil {
		return fmt.Errorf("create function: %w", err)
	}

	return nil
}

func (r *run) updateFunction(ctx context.Context, f fnCopy) error {
	c, zip := f.cfg, f.zip
	if _, err := r.dst.lambda.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{
		FunctionName: c.FunctionName, ZipFile: zip,
	}); err != nil {
		return fmt.Errorf("update code: %w", err)
	}

	in := &lambda.UpdateFunctionConfigurationInput{
		FunctionName: c.FunctionName, Runtime: c.Runtime, Handler: c.Handler,
		Role:        aws.String(r.rm.ARN(aws.ToString(c.Role))),
		Description: c.Description, Timeout: c.Timeout, MemorySize: c.MemorySize,
		VpcConfig: f.vpc,
	}
	if c.Environment != nil {
		in.Environment = &lambdatypes.Environment{Variables: c.Environment.Variables}
	}

	if _, err := r.dst.lambda.UpdateFunctionConfiguration(ctx, in); err != nil {
		return fmt.Errorf("update configuration: %w", err)
	}

	return nil
}
