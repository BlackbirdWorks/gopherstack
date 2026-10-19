package codegurureviewer

import (
	"archive/zip"
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
	codecommitbackend "github.com/blackbirdworks/gopherstack/services/codecommit"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const (
	maxArchiveBytes = 256 << 20
	maxArchiveFiles = 5000
	configFileName  = "aws-codeguru-reviewer.yml"
	bucketPrefix    = "codeguru-reviewer-"
)

// siblingServices is matched structurally against *CLI; handlers are wired after every provider initialises.
type siblingServices interface {
	GetCodeCommitHandler() service.Registerable
	GetS3Handler() service.Registerable
}

func (b *InMemoryBackend) siblings() (siblingServices, bool) {
	b.mu.RLock("siblings")
	defer b.mu.RUnlock()

	s, ok := b.appConfig.(siblingServices)

	return s, ok
}

func (b *InMemoryBackend) codeCommit(region string) (*codecommitbackend.InMemoryBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetCodeCommitHandler().(*codecommitbackend.Handler)
	if !ok || h == nil {
		return nil, false
	}

	rh := h.RegionHandler(region)
	if rh == nil || rh.Backend == nil {
		return nil, false
	}

	return rh.Backend, true
}

func (b *InMemoryBackend) s3() (s3backend.StorageBackend, bool) {
	s, ok := b.siblings()
	if !ok {
		return nil, false
	}

	h, ok := s.GetS3Handler().(*s3backend.S3Handler)
	if !ok || h == nil || h.Backend == nil {
		return nil, false
	}

	return h.Backend, true
}

// repositoryExists reports (exists, known); known is false when CodeCommit is not wired.
func (b *InMemoryBackend) repositoryExists(region, name string) (bool, bool) {
	cc, ok := b.codeCommit(region)
	if !ok {
		return false, false
	}

	_, err := cc.GetRepository(name)

	return err == nil, true
}

// bucketExists reports (exists, known); known is false when S3 is not wired.
func (b *InMemoryBackend) bucketExists(ctx context.Context, name string) (bool, bool) {
	be, ok := b.s3()
	if !ok {
		return false, false
	}

	_, err := be.HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: aws.String(name)})

	return err == nil, true
}

var (
	errUnreachable      = errors.New("source code is not reachable")
	errRepoMissing      = errors.New("CodeCommit repository does not exist")
	errBranchMissing    = errors.New("branch does not exist")
	errBranchUnreadable = errors.New("cannot read branch")
	errNoSourceKey      = errors.New("SourceCodeArtifactsObjectKey is required for S3 repositories")
	errArchiveMissing   = errors.New("source archive does not exist")
	errArchiveUnread    = errors.New("cannot read source archive")
	errNotZip           = errors.New("the source archive is not a valid .zip file")
	errArchiveTooLarge  = errors.New("the source archive is too large to analyze")
)

// fetchSources reads the files a review analyzes; the error text becomes the review's StateReason.
func (b *InMemoryBackend) fetchSources(
	ctx context.Context, region string, a *associationRecord, src *SourceCodeType,
) ([]sourceFile, error) {
	switch a.ProviderType {
	case providerCodeCommit:
		return b.codeCommitFiles(region, a.Name, src.RepositoryHead.BranchName)
	case providerS3:
		return b.s3Files(ctx, src.S3BucketRepository)
	default:
		return nil, fmt.Errorf("%w: %s repositories have no emulated source host", errUnreachable, a.ProviderType)
	}
}

func (b *InMemoryBackend) codeCommitFiles(region, repo, branch string) ([]sourceFile, error) {
	cc, ok := b.codeCommit(region)
	if !ok {
		return nil, fmt.Errorf("%w: CodeCommit is not available", errUnreachable)
	}

	r, err := cc.GetRepository(repo)
	if err != nil {
		return nil, fmt.Errorf("%w: %q", errRepoMissing, repo)
	}

	if branch == "" {
		branch = r.DefaultBranch
	}

	if _, err = cc.GetBranch(repo, branch); err != nil {
		return nil, fmt.Errorf("%w: %q in repository %q", errBranchMissing, branch, repo)
	}

	files, err := cc.GetFolderFiles(repo, branch, "")
	if err != nil {
		return nil, fmt.Errorf("%w: %q of repository %q", errBranchUnreadable, branch, repo)
	}

	out := make([]sourceFile, 0, len(files))

	for _, f := range files {
		content := f.FileContent
		if content == nil {
			if content, err = cc.GetBlob(repo, f.BlobID); err != nil {
				continue
			}
		}

		out = append(out, sourceFile{Path: f.FilePath, Content: content})
	}

	return out, nil
}

func (b *InMemoryBackend) s3Files(ctx context.Context, repo *S3BucketRepository) ([]sourceFile, error) {
	if repo == nil || repo.Details == nil || repo.Details.CodeArtifacts == nil ||
		repo.Details.CodeArtifacts.SourceCodeArtifactsObjectKey == "" {
		return nil, errNoSourceKey
	}

	be, ok := b.s3()
	if !ok {
		return nil, fmt.Errorf("%w: S3 is not available", errUnreachable)
	}

	bucket, key := repo.Details.BucketName, repo.Details.CodeArtifacts.SourceCodeArtifactsObjectKey

	obj, err := be.GetObject(ctx, &awss3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
	if err != nil || obj == nil || obj.Body == nil {
		return nil, fmt.Errorf("%w: s3://%s/%s", errArchiveMissing, bucket, key)
	}

	defer obj.Body.Close()

	data, err := io.ReadAll(io.LimitReader(obj.Body, maxArchiveBytes))
	if err != nil {
		return nil, fmt.Errorf("%w: s3://%s/%s", errArchiveUnread, bucket, key)
	}

	return unzip(data)
}

func unzip(data []byte) ([]sourceFile, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, errNotZip
	}

	var (
		out   []sourceFile
		total uint64
	)

	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}

		if len(out) >= maxArchiveFiles || total+f.UncompressedSize64 > maxArchiveBytes {
			return nil, errArchiveTooLarge
		}

		rc, openErr := f.Open()
		if openErr != nil {
			continue
		}

		content, readErr := io.ReadAll(io.LimitReader(rc, maxFileBytes+1))
		_ = rc.Close()

		if readErr != nil {
			continue
		}

		total += uint64(len(content))
		out = append(out, sourceFile{Path: strings.TrimPrefix(path.Clean(f.Name), "/"), Content: content})
	}

	return out, nil
}

func hasConfigFile(files []sourceFile) bool {
	for _, f := range files {
		if f.Path == configFileName {
			return true
		}
	}

	return false
}
