package codepipeline

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
)

const artifactRefSeparator = "::"

// zipFiles builds a zip artifact from name->content.
func zipFiles(files map[string][]byte) ([]byte, error) {
	names := make([]string, 0, len(files))
	for n := range files {
		names = append(names, n)
	}

	sort.Strings(names)

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, n := range names {
		w, err := zw.Create(n)
		if err != nil {
			return nil, err
		}

		if _, err = w.Write(files[n]); err != nil {
			return nil, err
		}
	}

	if err := zw.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// zipEntry returns one file from a zip artifact.
func zipEntry(data []byte, name string) ([]byte, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, errArtifactNotZip
	}

	for _, f := range zr.File {
		if f.Name != strings.TrimPrefix(name, "/") {
			continue
		}

		rc, openErr := f.Open()
		if openErr != nil {
			return nil, openErr
		}

		defer rc.Close()

		return io.ReadAll(io.LimitReader(rc, maxExtractedBytes))
	}

	return nil, fmt.Errorf("%w: file %q not found in artifact", ErrValidation, name)
}

// artifactFile reads file from the named input artifact of action.
func (b *InMemoryBackend) artifactFile(
	ctx context.Context, p *Pipeline, execID string, action Action, artifact, file string,
) ([]byte, error) {
	if b.artifactS3 == nil {
		return nil, fmt.Errorf("%w: no artifact store is available", ErrValidation)
	}

	found := false

	for _, in := range action.InputArtifacts {
		if in.Name == artifact {
			found = true

			break
		}
	}

	if !found {
		return nil, fmt.Errorf("%w: %q is not an input artifact of action %q", ErrValidation, artifact, action.Name)
	}

	data, err := b.getArtifact(ctx, p, action, execID, artifact)
	if err != nil {
		return nil, err
	}

	return zipEntry(data, file)
}

// artifactRef reads an "ArtifactName::path/to/file" reference.
func (b *InMemoryBackend) artifactRef(
	ctx context.Context, p *Pipeline, execID string, action Action, ref string,
) ([]byte, error) {
	artifact, file, ok := strings.Cut(ref, artifactRefSeparator)
	if !ok || artifact == "" || file == "" {
		return nil, fmt.Errorf("%w: %q is not of the form ArtifactName::FileName", ErrValidation, ref)
	}

	return b.artifactFile(ctx, p, execID, action, artifact, file)
}

// putOutputFiles writes files as every output artifact of action.
func (b *InMemoryBackend) putOutputFiles(
	ctx context.Context, p *Pipeline, execID string, action Action, files map[string][]byte,
) error {
	if len(action.OutputArtifacts) == 0 {
		return nil
	}

	if b.artifactS3 == nil {
		return fmt.Errorf("%w: no artifact store is available", ErrValidation)
	}

	data, err := zipFiles(files)
	if err != nil {
		return err
	}

	for _, out := range action.OutputArtifacts {
		if err = b.putArtifact(ctx, p, action, execID, out.Name, data); err != nil {
			return err
		}
	}

	return nil
}
