package transfer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/sftp"

	s3svc "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	sftpListPageSize = 1000
	sftpRmdirProbe   = 2
	sftpDirMode      = os.ModeDir | 0o755
	sftpFileMode     = os.FileMode(0o644)
)

// ObjectStore is the S3 surface SFTP file operations map onto; the emulator's S3 backend satisfies it.
type ObjectStore interface {
	ListBuckets(ctx context.Context, in *s3.ListBucketsInput) (*s3.ListBucketsOutput, error)
	HeadBucket(ctx context.Context, in *s3.HeadBucketInput) (*s3.HeadBucketOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	GetObject(ctx context.Context, in *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error)
}

type sftpFS struct {
	ctx     context.Context
	store   ObjectStore
	sess    *sftpSession
	authz   *sftpAuthz
	setStat bool
}

// loc is a resolved S3 location; virtual entries are synthesized LOGICAL ancestors of mappings.
type loc struct {
	bucket   string
	key      string
	children []string
	virtual  bool
}

func (f *sftpFS) startDir() string { return f.sess.Home }

func splitBucketKey(p string) (string, string) {
	p = strings.TrimPrefix(path.Clean("/"+p), "/")
	bucket, key, _ := strings.Cut(p, "/")

	return bucket, key
}

func (f *sftpFS) resolve(p string) (loc, error) {
	p = path.Clean("/" + p)
	if !f.sess.Logical {
		b, k := splitBucketKey(p)

		return loc{bucket: b, key: k}, nil
	}

	best := -1

	var target string

	for _, m := range f.sess.Mappings {
		e := path.Clean("/" + m.Entry)
		if (p == e || e == "/" || strings.HasPrefix(p, e+"/")) && len(e) > best {
			best = len(e)
			target = path.Join(m.Target, strings.TrimPrefix(strings.TrimPrefix(p, e), "/"))
		}
	}

	if best >= 0 {
		b, k := splitBucketKey(target)

		return loc{bucket: b, key: k}, nil
	}

	seen := map[string]bool{}

	var kids []string

	for _, m := range f.sess.Mappings {
		e := path.Clean("/" + m.Entry)
		rest, ok := strings.CutPrefix(e, strings.TrimSuffix(p, "/")+"/")

		if !ok {
			continue
		}

		name, _, _ := strings.Cut(rest, "/")
		if !seen[name] {
			seen[name] = true
			kids = append(kids, name)
		}
	}

	if len(kids) == 0 {
		return loc{}, os.ErrPermission
	}

	sort.Strings(kids)

	return loc{virtual: true, children: kids}, nil
}

type sftpInfo struct {
	mod  time.Time
	name string
	size int64
	mode os.FileMode
}

func (i *sftpInfo) Name() string       { return i.name }
func (i *sftpInfo) Size() int64        { return i.size }
func (i *sftpInfo) Mode() os.FileMode  { return i.mode }
func (i *sftpInfo) ModTime() time.Time { return i.mod }
func (i *sftpInfo) IsDir() bool        { return i.mode.IsDir() }
func (i *sftpInfo) Sys() any           { return nil }

func dirInfo(name string, mod time.Time) *sftpInfo {
	return &sftpInfo{name: name, mode: sftpDirMode, mod: mod}
}

func mapS3Err(err error) error {
	if errors.Is(err, s3svc.ErrNoSuchKey) || errors.Is(err, s3svc.ErrNoSuchBucket) {
		return os.ErrNotExist
	}

	return err
}

func baseName(p string) string {
	if b := path.Base(path.Clean("/" + p)); b != "." {
		return b
	}

	return "/"
}

func (f *sftpFS) stat(p string) (*sftpInfo, error) {
	l, err := f.resolve(p)
	if err != nil {
		return nil, err
	}

	name := baseName(p)

	switch {
	case l.virtual || (l.bucket == "" && l.key == ""):
		return dirInfo(name, time.Time{}), nil
	case l.key == "":
		if _, herr := f.store.HeadBucket(f.ctx, &s3.HeadBucketInput{Bucket: aws.String(l.bucket)}); herr != nil {
			return nil, mapS3Err(herr)
		}

		return dirInfo(name, time.Time{}), nil
	}

	out, err := f.store.HeadObject(f.ctx, &s3.HeadObjectInput{Bucket: aws.String(l.bucket), Key: aws.String(l.key)})
	if err == nil {
		return &sftpInfo{
			name: name,
			size: aws.ToInt64(out.ContentLength),
			mode: sftpFileMode,
			mod:  aws.ToTime(out.LastModified),
		}, nil
	}

	if !errors.Is(err, s3svc.ErrNoSuchKey) {
		return nil, mapS3Err(err)
	}

	if f.prefixExists(l.bucket, l.key) {
		return dirInfo(name, time.Time{}), nil
	}

	return nil, os.ErrNotExist
}

func (f *sftpFS) prefixExists(bucket, key string) bool {
	out, err := f.store.ListObjectsV2(f.ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket), Prefix: aws.String(key + "/"), MaxKeys: aws.Int32(1),
	})

	return err == nil && (len(out.Contents) > 0 || len(out.CommonPrefixes) > 0)
}

// Fileread implements sftp.FileReader.
func (f *sftpFS) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	l, err := f.resolve(r.Filepath)
	if err != nil {
		return nil, err
	}

	if l.key == "" {
		return nil, os.ErrInvalid
	}

	if err = f.authz.check(opRead, l.bucket, l.key); err != nil {
		return nil, err
	}

	out, err := f.store.GetObject(f.ctx, &s3.GetObjectInput{Bucket: aws.String(l.bucket), Key: aws.String(l.key)})
	if err != nil {
		return nil, mapS3Err(err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("read object: %w", err)
	}

	return bytes.NewReader(data), nil
}

type sftpWriter struct {
	f      *sftpFS
	bucket string
	key    string
	buf    []byte
}

const sftpMaxObjectSize = 5 << 30

// WriteAt buffers the write; the object is stored when the handle closes.
func (w *sftpWriter) WriteAt(p []byte, off int64) (int, error) {
	if off < 0 || off > sftpMaxObjectSize-int64(len(p)) {
		return 0, os.ErrInvalid
	}

	if end := int(off) + len(p); end > len(w.buf) {
		w.buf = append(w.buf, make([]byte, end-len(w.buf))...)
	}

	copy(w.buf[off:], p)

	return len(p), nil
}

// Close stores the buffered bytes as the S3 object.
func (w *sftpWriter) Close() error {
	_, err := w.f.store.PutObject(w.f.ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.bucket), Key: aws.String(w.key),
		Body: bytes.NewReader(w.buf), ContentLength: aws.Int64(int64(len(w.buf))),
	})

	return err
}

// Filewrite implements sftp.FileWriter.
func (f *sftpFS) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	l, err := f.resolve(r.Filepath)
	if err != nil {
		return nil, err
	}

	if l.virtual || l.key == "" {
		return nil, os.ErrPermission
	}

	if err = f.authz.check(opWrite, l.bucket, l.key); err != nil {
		return nil, err
	}

	if _, herr := f.store.HeadBucket(f.ctx, &s3.HeadBucketInput{Bucket: aws.String(l.bucket)}); herr != nil {
		return nil, mapS3Err(herr)
	}

	w := &sftpWriter{f: f, bucket: l.bucket, key: l.key}

	if info, serr := f.stat(r.Filepath); serr == nil {
		if info.IsDir() {
			return nil, os.ErrInvalid
		}

		if !r.Pflags().Trunc {
			if err = f.preload(w, r, info.Size()); err != nil {
				return nil, err
			}
		}
	}

	return w, nil
}

func (f *sftpFS) preload(w *sftpWriter, r *sftp.Request, size int64) error {
	rd, err := f.Fileread(r)
	if err != nil {
		return err
	}

	w.buf = make([]byte, size)
	if _, err = rd.ReadAt(w.buf, 0); err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return nil
}

// Filecmd implements sftp.FileCmder.
func (f *sftpFS) Filecmd(r *sftp.Request) error {
	switch r.Method {
	case "Setstat":
		if f.setStat {
			return nil
		}

		return sftp.ErrSSHFxOpUnsupported
	case "Mkdir":
		return f.mkdir(r.Filepath)
	case "Remove":
		return f.remove(r.Filepath)
	case "Rmdir":
		return f.rmdir(r.Filepath)
	case "Rename":
		return f.rename(r.Filepath, r.Target)
	default:
		return sftp.ErrSSHFxOpUnsupported
	}
}

func (f *sftpFS) fileLoc(p string, op sftpOp) (loc, error) {
	l, err := f.resolve(p)
	if err != nil {
		return l, err
	}

	if l.virtual || l.key == "" {
		return l, os.ErrPermission
	}

	return l, f.authz.check(op, l.bucket, l.key)
}

func (f *sftpFS) mkdir(p string) error {
	l, err := f.fileLoc(p, opMkdir)
	if err != nil {
		return err
	}

	if _, serr := f.stat(p); serr == nil {
		return os.ErrExist
	}

	_, err = f.store.PutObject(f.ctx, &s3.PutObjectInput{
		Bucket: aws.String(l.bucket), Key: aws.String(l.key + "/"),
		Body: bytes.NewReader(nil), ContentLength: aws.Int64(0),
	})

	return mapS3Err(err)
}

func (f *sftpFS) remove(p string) error {
	l, err := f.fileLoc(p, opDelete)
	if err != nil {
		return err
	}

	info, err := f.stat(p)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return os.ErrInvalid
	}

	_, err = f.store.DeleteObject(f.ctx, &s3.DeleteObjectInput{Bucket: aws.String(l.bucket), Key: aws.String(l.key)})

	return mapS3Err(err)
}

func (f *sftpFS) rmdir(p string) error {
	l, err := f.fileLoc(p, opDelete)
	if err != nil {
		return err
	}

	info, err := f.stat(p)
	if err != nil {
		return err
	}

	if !info.IsDir() {
		return os.ErrInvalid
	}

	marker := l.key + "/"

	out, err := f.store.ListObjectsV2(f.ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(l.bucket), Prefix: aws.String(marker), MaxKeys: aws.Int32(sftpRmdirProbe),
	})
	if err != nil {
		return mapS3Err(err)
	}

	for _, o := range out.Contents {
		if aws.ToString(o.Key) != marker {
			return sftp.ErrSSHFxFailure
		}
	}

	_, err = f.store.DeleteObject(f.ctx, &s3.DeleteObjectInput{Bucket: aws.String(l.bucket), Key: aws.String(marker)})

	return mapS3Err(err)
}

// rename moves a file (get, put, delete); S3 has no directory rename.
func (f *sftpFS) rename(from, to string) error {
	src, err := f.fileLoc(from, opRead)
	if err != nil {
		return err
	}

	if err = f.authz.check(opDelete, src.bucket, src.key); err != nil {
		return err
	}

	dst, err := f.fileLoc(to, opWrite)
	if err != nil {
		return err
	}

	info, err := f.stat(from)
	if err != nil {
		return err
	}

	if info.IsDir() {
		return sftp.ErrSSHFxOpUnsupported
	}

	out, err := f.store.GetObject(f.ctx, &s3.GetObjectInput{Bucket: aws.String(src.bucket), Key: aws.String(src.key)})
	if err != nil {
		return mapS3Err(err)
	}
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return fmt.Errorf("read object: %w", err)
	}

	if _, err = f.store.PutObject(f.ctx, &s3.PutObjectInput{
		Bucket: aws.String(dst.bucket), Key: aws.String(dst.key),
		Body: bytes.NewReader(data), ContentLength: aws.Int64(int64(len(data))),
	}); err != nil {
		return mapS3Err(err)
	}

	if src.bucket == dst.bucket && src.key == dst.key {
		return nil
	}

	_, err = f.store.DeleteObject(
		f.ctx,
		&s3.DeleteObjectInput{Bucket: aws.String(src.bucket), Key: aws.String(src.key)},
	)

	return mapS3Err(err)
}

type sftpLister []os.FileInfo

// ListAt implements sftp.ListerAt.
func (l sftpLister) ListAt(dst []os.FileInfo, off int64) (int, error) {
	if off >= int64(len(l)) {
		return 0, io.EOF
	}

	n := copy(dst, l[off:])
	if int(off)+n >= len(l) {
		return n, io.EOF
	}

	return n, nil
}

// Filelist implements sftp.FileLister.
func (f *sftpFS) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	switch r.Method {
	case "Stat":
		info, err := f.stat(r.Filepath)
		if err != nil {
			return nil, err
		}

		return sftpLister{info}, nil
	case "List":
		return f.list(r.Filepath)
	default:
		return nil, sftp.ErrSSHFxOpUnsupported
	}
}

func (f *sftpFS) list(p string) (sftp.ListerAt, error) {
	info, err := f.stat(p)
	if err != nil {
		return nil, err
	}

	if !info.IsDir() {
		return nil, os.ErrInvalid
	}

	l, err := f.resolve(p)
	if err != nil {
		return nil, err
	}

	if !l.virtual {
		if err = f.authz.check(opList, l.bucket, l.key); err != nil {
			return nil, err
		}
	}

	var out []os.FileInfo

	switch {
	case l.virtual:
		for _, k := range l.children {
			out = append(out, dirInfo(k, time.Time{}))
		}
	case l.bucket == "":
		bk, lerr := f.store.ListBuckets(f.ctx, &s3.ListBucketsInput{})
		if lerr != nil {
			return nil, lerr
		}

		for _, b := range bk.Buckets {
			out = append(out, dirInfo(aws.ToString(b.Name), aws.ToTime(b.CreationDate)))
		}
	default:
		if out, err = f.listObjects(l); err != nil {
			return nil, err
		}
	}

	return sftpLister(out), nil
}

func (f *sftpFS) listObjects(l loc) ([]os.FileInfo, error) {
	prefix := ""
	if l.key != "" {
		prefix = l.key + "/"
	}

	var (
		out   []os.FileInfo
		token *string
	)

	for {
		page, err := f.store.ListObjectsV2(f.ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(l.bucket), Prefix: aws.String(prefix), Delimiter: aws.String("/"),
			MaxKeys: aws.Int32(sftpListPageSize), ContinuationToken: token,
		})
		if err != nil {
			return nil, mapS3Err(err)
		}

		for _, cp := range page.CommonPrefixes {
			out = append(out, dirInfo(path.Base(strings.TrimSuffix(aws.ToString(cp.Prefix), "/")), time.Time{}))
		}

		for _, o := range page.Contents {
			k := aws.ToString(o.Key)
			if k == prefix {
				continue
			}

			out = append(out, &sftpInfo{
				name: path.Base(k), size: aws.ToInt64(o.Size), mode: sftpFileMode, mod: aws.ToTime(o.LastModified),
			})
		}

		if !aws.ToBool(page.IsTruncated) || page.NextContinuationToken == nil {
			break
		}

		token = page.NextContinuationToken
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })

	return out, nil
}
