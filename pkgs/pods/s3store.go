package pods

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const s3Timeout = 5 * time.Minute

// S3Config configures an S3-compatible remote. Endpoint may point at real AWS S3
// (leave empty), another S3-compatible server, or Gopherstack itself.
type S3Config struct {
	Bucket    string
	Prefix    string
	Endpoint  string
	Region    string
	AccessKey string
	SecretKey string
	PathStyle bool
}

// S3Store stores blobs as S3 objects under Prefix.
type S3Store struct {
	client *s3.Client
	bucket string
	prefix string
}

// NewS3Store builds an S3-backed BlobStore. Without explicit keys the default
// AWS credential chain is used.
func NewS3Store(ctx context.Context, cfg S3Config) (*S3Store, error) {
	if cfg.Bucket == "" {
		return nil, ErrBucketRequired
	}

	region := cfg.Region
	if region == "" {
		region = "us-east-1"
	}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(region),
		awsconfig.WithHTTPClient(httpclient.New(s3Timeout)),
	}

	if cfg.AccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("pods: load aws config: %w", err)
	}

	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}

		o.UsePathStyle = cfg.PathStyle || cfg.Endpoint != ""
	})

	return &S3Store{client: client, bucket: cfg.Bucket, prefix: strings.Trim(cfg.Prefix, "/")}, nil
}

func (s *S3Store) key(k string) string {
	if s.prefix == "" {
		return k
	}

	return s.prefix + "/" + k
}

// Put uploads data.
func (s *S3Store) Put(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return fmt.Errorf("pods: s3 put: %w", err)
	}

	return nil
}

// Get downloads a blob.
func (s *S3Store) Get(ctx context.Context, key string) ([]byte, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		if _, ok := errors.AsType[*types.NoSuchKey](err); ok {
			return nil, ErrNotFound
		}

		return nil, fmt.Errorf("pods: s3 get: %w", err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("pods: s3 read: %w", err)
	}

	return data, nil
}

// List returns keys (relative to Prefix) that start with prefix.
func (s *S3Store) List(ctx context.Context, prefix string) ([]string, error) {
	full := s.key(prefix)
	strip := ""

	if s.prefix != "" {
		strip = s.prefix + "/"
	}

	var keys []string

	pager := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(full),
	})

	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("pods: s3 list: %w", err)
		}

		for _, o := range page.Contents {
			keys = append(keys, strings.TrimPrefix(aws.ToString(o.Key), strip))
		}
	}

	sort.Strings(keys)

	return keys, nil
}

// Delete removes an object.
func (s *S3Store) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return fmt.Errorf("pods: s3 delete: %w", err)
	}

	return nil
}
