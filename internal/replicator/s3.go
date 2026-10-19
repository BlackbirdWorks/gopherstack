package replicator

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (r *run) replicateS3(ctx context.Context) {
	out, err := r.src.s3.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		r.listFail(TypeS3, err)

		return
	}

	for _, b := range out.Buckets {
		name := aws.ToString(b.Name)
		if !r.match(name) {
			continue
		}

		if reg := aws.ToString(b.BucketRegion); reg != "" && reg != r.src.region {
			r.add(TypeS3, name, ActionSkipped, "bucket is in region "+reg)

			continue
		}

		r.copyBucket(ctx, name)
	}
}

func (r *run) copyBucket(ctx context.Context, name string) {
	_, err := r.dst.s3.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: &name})

	exists, err := existsErr(err)
	if err != nil {
		r.fail(TypeS3, name, err)

		return
	}

	if r.cfg.DryRun {
		r.add(TypeS3, name, upsert(exists), "")

		return
	}

	if !exists {
		in := &s3.CreateBucketInput{Bucket: &name}
		if r.dst.region != defaultRegion {
			in.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
				LocationConstraint: s3types.BucketLocationConstraint(r.dst.region),
			}
		}

		if _, err = r.dst.s3.CreateBucket(ctx, in); err != nil {
			r.fail(TypeS3, name, err)

			return
		}
	}

	detail, err := r.copyBucketConfig(ctx, name)
	if err != nil {
		r.fail(TypeS3, name, err)

		return
	}

	r.add(TypeS3, name, upsert(exists), detail)
}

func (r *run) copyBucketConfig(ctx context.Context, name string) (string, error) {
	steps := []func(context.Context, string) error{r.copyVersioning, r.copyTags}
	for _, step := range steps {
		if err := step(ctx, name); err != nil {
			return "", err
		}
	}

	detail := ""

	if r.cfg.IncludeData {
		copied, skipped, err := r.copyObjects(ctx, name)
		if err != nil {
			return "", err
		}

		detail = fmt.Sprintf("%d objects copied, %d skipped", copied, skipped)
	}

	for _, step := range []func(context.Context, string) error{r.copyLifecycle, r.copyBucketPolicy} {
		if err := step(ctx, name); err != nil {
			return "", err
		}
	}

	return detail, nil
}

func (r *run) copyVersioning(ctx context.Context, name string) error {
	v, err := r.src.s3.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{Bucket: &name})
	if err != nil {
		return fmt.Errorf("get versioning: %w", err)
	}

	if v.Status == "" {
		return nil
	}

	_, err = r.dst.s3.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
		Bucket:                  &name,
		VersioningConfiguration: &s3types.VersioningConfiguration{Status: v.Status},
	})
	if err != nil {
		return fmt.Errorf("put versioning: %w", err)
	}

	return nil
}

func (r *run) copyTags(ctx context.Context, name string) error {
	t, err := r.src.s3.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{Bucket: &name})
	if err != nil {
		if isNotFound(err) {
			return nil
		}

		return fmt.Errorf("get tags: %w", err)
	}

	if len(t.TagSet) == 0 {
		return nil
	}

	_, err = r.dst.s3.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
		Bucket: &name, Tagging: &s3types.Tagging{TagSet: t.TagSet},
	})
	if err != nil {
		return fmt.Errorf("put tags: %w", err)
	}

	return nil
}

func (r *run) copyLifecycle(ctx context.Context, name string) error {
	l, err := r.src.s3.GetBucketLifecycleConfiguration(ctx, &s3.GetBucketLifecycleConfigurationInput{Bucket: &name})
	if err != nil {
		if isNotFound(err) {
			return nil
		}

		return fmt.Errorf("get lifecycle: %w", err)
	}

	if len(l.Rules) == 0 {
		return nil
	}

	_, err = r.dst.s3.PutBucketLifecycleConfiguration(ctx, &s3.PutBucketLifecycleConfigurationInput{
		Bucket: &name, LifecycleConfiguration: &s3types.BucketLifecycleConfiguration{Rules: l.Rules},
	})
	if err != nil {
		return fmt.Errorf("put lifecycle: %w", err)
	}

	return nil
}

func (r *run) copyBucketPolicy(ctx context.Context, name string) error {
	p, err := r.src.s3.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{Bucket: &name})
	if err != nil {
		if isNotFound(err) {
			return nil
		}

		return fmt.Errorf("get policy: %w", err)
	}

	_, err = r.dst.s3.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
		Bucket: &name, Policy: aws.String(r.rm.Text(aws.ToString(p.Policy))),
	})
	if err != nil {
		return fmt.Errorf("put policy: %w", err)
	}

	return nil
}

func (r *run) copyObjects(ctx context.Context, bucket string) (int, int, error) {
	var copied, skipped int

	pager := s3.NewListObjectsV2Paginator(r.src.s3, &s3.ListObjectsV2Input{Bucket: &bucket})
	for pager.HasMorePages() {
		page, perr := pager.NextPage(ctx)
		if perr != nil {
			return copied, skipped, fmt.Errorf("list objects: %w", perr)
		}

		for _, o := range page.Contents {
			if aws.ToInt64(o.Size) > r.cfg.MaxObjectBytes {
				skipped++

				continue
			}

			if err := r.copyObject(ctx, bucket, aws.ToString(o.Key)); err != nil {
				return copied, skipped, err
			}

			copied++
		}
	}

	return copied, skipped, nil
}

func (r *run) copyObject(ctx context.Context, bucket, key string) error {
	obj, err := r.src.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: &bucket, Key: &key})
	if err != nil {
		return fmt.Errorf("get %s: %w", key, err)
	}
	defer obj.Body.Close()

	body, err := io.ReadAll(io.LimitReader(obj.Body, r.cfg.MaxObjectBytes+1))
	if err != nil {
		return fmt.Errorf("read %s: %w", key, err)
	}

	_, err = r.dst.s3.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket, Key: &key, Body: bytes.NewReader(body),
		ContentType: obj.ContentType, ContentEncoding: obj.ContentEncoding,
		CacheControl: obj.CacheControl, ContentDisposition: obj.ContentDisposition,
		Metadata: obj.Metadata,
	})
	if err != nil {
		return fmt.Errorf("put %s: %w", key, err)
	}

	return nil
}
