package transfer

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	s3svc "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	efsDirPerm  os.FileMode = 0o755
	efsFilePerm os.FileMode = 0o644
	efsFSPrefix             = "fs-"
	domainEFS               = "EFS"
)

// EFSStorage resolves the host directory backing an EFS file system.
type EFSStorage interface {
	FileSystemHostDir(region, fsID string) (string, error)
}

// efsStore maps the ObjectStore view (bucket = file system ID, key = path) onto an EFS file system's host directory.
type efsStore struct {
	efs    EFSStorage
	region string
}

func (e *efsStore) root(fsID string) (*os.Root, error) {
	if !strings.HasPrefix(fsID, efsFSPrefix) {
		return nil, s3svc.ErrNoSuchBucket
	}

	dir, err := e.efs.FileSystemHostDir(e.region, fsID)
	if err != nil {
		return nil, s3svc.ErrNoSuchBucket
	}

	return os.OpenRoot(dir)
}

func efsRel(key string) string {
	key = strings.TrimSuffix(path.Clean("/"+key), "/")

	return strings.TrimPrefix(key, "/")
}

func efsErr(err error) error {
	if errors.Is(err, fs.ErrNotExist) {
		return s3svc.ErrNoSuchKey
	}

	return err
}

func (e *efsStore) ListBuckets(context.Context, *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return &s3.ListBucketsOutput{}, nil
}

func (e *efsStore) HeadBucket(_ context.Context, in *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}

	_ = r.Close()

	return &s3.HeadBucketOutput{}, nil
}

func (e *efsStore) PutObject(_ context.Context, in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	key := aws.ToString(in.Key)
	rel := efsRel(key)

	if rel == "" {
		return &s3.PutObjectOutput{}, nil
	}

	if strings.HasSuffix(key, "/") {
		return &s3.PutObjectOutput{}, efsErr(r.MkdirAll(rel, efsDirPerm))
	}

	if dir := path.Dir(rel); dir != "." {
		if err = r.MkdirAll(dir, efsDirPerm); err != nil {
			return nil, err
		}
	}

	data, err := io.ReadAll(in.Body)
	if err != nil {
		return nil, err
	}

	return &s3.PutObjectOutput{}, efsErr(writeRootFile(r, rel, data))
}

func writeRootFile(r *os.Root, rel string, data []byte) error {
	f, err := r.OpenFile(rel, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, efsFilePerm)
	if err != nil {
		return err
	}

	if _, err = f.Write(data); err != nil {
		_ = f.Close()

		return err
	}

	return f.Close()
}

func (e *efsStore) GetObject(_ context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	rel := efsRel(aws.ToString(in.Key))

	info, err := r.Stat(rel)
	if err != nil || info.IsDir() {
		return nil, s3svc.ErrNoSuchKey
	}

	f, err := r.Open(rel)
	if err != nil {
		return nil, efsErr(err)
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return &s3.GetObjectOutput{
		Body: io.NopCloser(bytes.NewReader(data)), ContentLength: aws.Int64(int64(len(data))),
		LastModified: aws.Time(info.ModTime()),
	}, nil
}

func (e *efsStore) HeadObject(_ context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	info, err := r.Stat(efsRel(aws.ToString(in.Key)))
	if err != nil || info.IsDir() {
		return nil, s3svc.ErrNoSuchKey
	}

	return &s3.HeadObjectOutput{ContentLength: aws.Int64(info.Size()), LastModified: aws.Time(info.ModTime())}, nil
}

func (e *efsStore) DeleteObject(_ context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	rel := efsRel(aws.ToString(in.Key))
	if rel == "" {
		return &s3.DeleteObjectOutput{}, nil
	}

	if err = r.Remove(rel); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}

	return &s3.DeleteObjectOutput{}, nil
}

func (e *efsStore) ListObjectsV2(_ context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	r, err := e.root(aws.ToString(in.Bucket))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	prefix := aws.ToString(in.Prefix)
	dirRel := efsRel(prefix)

	if dirRel == "" {
		dirRel = "."
	}

	out := &s3.ListObjectsV2Output{}

	entries, rerr := fs.ReadDir(r.FS(), dirRel)
	if rerr == nil && len(entries) == 0 && dirRel != "." {
		out.Contents = append(out.Contents, s3types.Object{Key: aws.String(prefix), Size: aws.Int64(0)})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, ent := range entries {
		key := path.Join(dirRel, ent.Name())
		if dirRel == "." {
			key = ent.Name()
		}

		if ent.IsDir() {
			out.CommonPrefixes = append(out.CommonPrefixes, s3types.CommonPrefix{Prefix: aws.String(key + "/")})

			continue
		}

		info, ierr := ent.Info()
		if ierr != nil {
			continue
		}

		out.Contents = append(out.Contents, s3types.Object{
			Key: aws.String(key), Size: aws.Int64(info.Size()), LastModified: aws.Time(info.ModTime()),
		})
	}

	return out, nil
}
