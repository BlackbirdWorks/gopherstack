package translate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const maxS3DocumentBytes = 64 << 20

// siblingServices is matched structurally against *CLI; handlers are wired after every provider initialises.
type siblingServices interface {
	GetS3Handler() service.Registerable
	GetKMSHandler() service.Registerable
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

// s3Backend returns the emulator's S3 backend; callers hold b.mu or not as they please.
func (b *InMemoryBackend) s3Backend() (s3backend.StorageBackend, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// kmsKeyUsable reports (exists, known); known is false when KMS is not wired.
func (b *InMemoryBackend) kmsKeyUsable(keyID string) (bool, bool) {
	s, ok := b.appConfig.(siblingServices)
	if !ok {
		return false, false
	}

	h, ok := s.GetKMSHandler().(*kmsbackend.Handler)
	if !ok || h == nil || h.Backend == nil {
		return false, false
	}

	out, err := h.Backend.DescribeKey(context.Background(), &kmsbackend.DescribeKeyInput{KeyID: keyID})

	return err == nil && out != nil, true
}

// validateEncryptionKey checks EncryptionKey.Type against the SDK enum (KMS) and the key against KMS.
func (b *InMemoryBackend) validateEncryptionKey(key *EncryptionKey) error {
	if key == nil {
		return nil
	}

	if key.Type != "KMS" {
		return fmt.Errorf("%w: EncryptionKey.Type must be KMS", ErrInvalidParameter)
	}

	if key.ID == "" {
		return fmt.Errorf("%w: EncryptionKey.Id is required", ErrInvalidParameter)
	}

	if usable, known := b.kmsKeyUsable(key.ID); known && !usable {
		return fmt.Errorf("%w: KMS key %q was not found", ErrInvalidParameter, key.ID)
	}

	return nil
}

// splitS3URI splits s3://bucket/key into bucket and key.
func splitS3URI(uri string) (string, string, bool) {
	rest, ok := strings.CutPrefix(uri, "s3://")
	if !ok {
		return "", "", false
	}

	bucket, key, _ := strings.Cut(rest, "/")

	return bucket, key, bucket != ""
}

// readS3Object reads s3://bucket/key from the emulated S3; ok is false when unreadable.
func (b *InMemoryBackend) readS3Object(uri string) ([]byte, bool) {
	backend, ok := b.s3Backend()
	if !ok {
		return nil, false
	}

	bucket, key, ok := splitS3URI(uri)
	if !ok || key == "" {
		return nil, false
	}

	out, err := backend.GetObject(context.Background(), &awss3.GetObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key),
	})
	if err != nil || out == nil || out.Body == nil {
		return nil, false
	}

	defer out.Body.Close()

	data, err := io.ReadAll(io.LimitReader(out.Body, maxS3DocumentBytes))

	return data, err == nil
}

// s3Document is an input object of a translation job.
type s3Document struct {
	key  string
	data []byte
}

// listS3Documents reads every object under the s3:// prefix.
func (b *InMemoryBackend) listS3Documents(prefixURI string) ([]s3Document, bool) {
	backend, ok := b.s3Backend()
	if !ok {
		return nil, false
	}

	bucket, prefix, ok := splitS3URI(prefixURI)
	if !ok {
		return nil, false
	}

	list, err := backend.ListObjectsV2(context.Background(), &awss3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(prefix),
	})
	if err != nil || list == nil {
		return nil, false
	}

	var docs []s3Document

	for _, obj := range list.Contents {
		key := aws.ToString(obj.Key)
		if strings.HasSuffix(key, "/") {
			continue
		}

		if data, readable := b.readS3Object("s3://" + bucket + "/" + key); readable {
			docs = append(docs, s3Document{key: key, data: data})
		}
	}

	return docs, true
}

// writeS3Object stores data at s3://bucket/key.
func (b *InMemoryBackend) writeS3Object(bucket, key string, data []byte) error {
	backend, ok := b.s3Backend()
	if !ok {
		return errS3Unavailable
	}

	_, err := backend.PutObject(context.Background(), &awss3.PutObjectInput{
		Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(data),
	})

	return err
}

var errS3Unavailable = errors.New("S3 is not available")

func outputKey(prefix, accountID, jobID, targetLang, inputKey string) string {
	return path.Join(prefix, accountID+"-TranslateText-"+jobID, targetLang+"."+path.Base(inputKey))
}
