package kafkaconnect

import (
	"context"
	"crypto/md5" //nolint:gosec // MSK Connect's fileMd5 is defined as an MD5 digest, not a security control
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const s3BucketARNPrefix = ":::"

// siblingServices is the slice of *CLI used to read plugin archives from emulated S3;
// resolved lazily because handlers are wired only after every provider initialises.
type siblingServices interface {
	GetS3Handler() service.Registerable
}

// SetAppConfig records the service.AppContext.Config for lazy sibling lookup.
func (b *InMemoryBackend) SetAppConfig(cfg any) {
	b.mu.Lock("SetAppConfig")
	defer b.mu.Unlock()

	b.appConfig = cfg
}

func (b *InMemoryBackend) appConfigValue() any {
	b.mu.RLock("appConfigValue")
	defer b.mu.RUnlock()

	return b.appConfig
}

func (b *InMemoryBackend) s3Backend() (s3backend.StorageBackend, bool) {
	s, ok := b.appConfigValue().(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// pluginObject is what reading a custom plugin's S3 object yielded.
type pluginObject struct {
	md5     string
	size    int64
	missing bool
}

// readPluginObject reads the plugin archive from emulated S3; ok is false when S3
// is not wired, so callers keep the location as opaque input.
func (b *InMemoryBackend) readPluginObject(bucketArn, fileKey, version string) (pluginObject, bool) {
	backend, ok := b.s3Backend()
	if !ok {
		return pluginObject{}, false
	}

	_, bucket, found := strings.Cut(bucketArn, s3BucketARNPrefix)
	if !found || bucket == "" {
		return pluginObject{missing: true}, true
	}

	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fileKey)}
	if version != "" {
		in.VersionId = aws.String(version)
	}

	out, err := backend.GetObject(context.Background(), in)
	if err != nil || out.Body == nil {
		return pluginObject{missing: true}, true
	}

	defer out.Body.Close()

	sum := md5.New() //nolint:gosec // see import
	n, err := io.Copy(sum, out.Body)

	if err != nil {
		return pluginObject{missing: true}, true
	}

	return pluginObject{md5: hex.EncodeToString(sum.Sum(nil)), size: n}, true
}

// readPluginBytes returns the plugin archive stored in emulated S3.
func (b *InMemoryBackend) readPluginBytes(bucketArn, fileKey, version string) ([]byte, error) {
	backend, ok := b.s3Backend()
	if !ok {
		return nil, errS3Unavailable
	}

	_, bucket, found := strings.Cut(bucketArn, s3BucketARNPrefix)
	if !found || bucket == "" {
		return nil, errPluginLocation
	}

	in := &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(fileKey)}
	if version != "" {
		in.VersionId = aws.String(version)
	}

	out, err := backend.GetObject(context.Background(), in)
	if err != nil {
		return nil, fmt.Errorf("get plugin object: %w", err)
	}

	defer out.Body.Close()

	return io.ReadAll(io.LimitReader(out.Body, maxPluginBytes)) //nolint:wrapcheck // read errors are self-describing
}

const maxPluginBytes = 512 << 20

var (
	errS3Unavailable  = errors.New("emulated S3 is not wired")
	errPluginLocation = errors.New("plugin bucket ARN is malformed")
)
