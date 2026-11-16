package codebuild

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
)

const (
	maxReportExportBytes = 256 << 20
	packagingZIP         = "ZIP"
)

type reportExport struct {
	cfg       ReportExportConfig
	reportArn string
	groupName string
}

// exportReport copies the raw report files of a build into the report group's S3 destination
// (docs.aws.amazon.com/codebuild/latest/userguide/report-group-export-settings.html); the key layout is
// <path>/<report id>/<file> or <path>/<report id>.zip.
func (r *buildRunner) exportReport(base string, files []string, exp reportExport) {
	d := exp.cfg.S3Destination
	if exp.cfg.ExportConfigType != "S3" || d == nil || d.Bucket == "" {
		return
	}

	if r.svc.Store == nil {
		r.log.write(r.ctx, marker("Report export to %s skipped: S3 is not available", d.Bucket))

		return
	}

	objs, err := readReportFiles(base, files, maxReportExportBytes)
	if err != nil {
		r.log.write(r.ctx, marker("Report export failed: %v", err))

		return
	}

	id := exp.reportArn[strings.LastIndex(exp.reportArn, ":")+1:]
	prefix := strings.Trim(path.Join(d.Path, id), "/")

	if d.Packaging == packagingZIP {
		data, zerr := zipObjects(objs)
		if zerr != nil {
			r.log.write(r.ctx, marker("Report export failed: %v", zerr))

			return
		}

		objs = map[string][]byte{"": data}
		prefix += ".zip"
	}

	for name, data := range objs {
		key := prefix
		if name != "" {
			key = prefix + "/" + name
		}

		if perr := r.svc.Store.PutObjectBytes(r.ctx, d.Bucket, key, data); perr != nil {
			r.log.write(r.ctx, marker("Report export to s3://%s/%s failed: %v", d.Bucket, key, perr))
		}
	}
}

// readReportFiles reads files through os.Root so a symlink cannot leave the base directory.
func readReportFiles(base string, files []string, limit int) (map[string][]byte, error) {
	root, err := os.OpenRoot(base)
	if err != nil {
		return nil, fmt.Errorf("open report base: %w", err)
	}
	defer root.Close()

	out := make(map[string][]byte, len(files))
	total := 0

	for _, f := range files {
		fh, oerr := root.Open(f)
		if oerr != nil {
			return nil, fmt.Errorf("read %s: %w", f, oerr)
		}

		data, rerr := io.ReadAll(io.LimitReader(fh, int64(limit-total)+1))
		_ = fh.Close()

		if rerr != nil {
			return nil, fmt.Errorf("read %s: %w", f, rerr)
		}

		total += len(data)
		if total > limit {
			return nil, fmt.Errorf("%w: report files exceed %d bytes", errUnsafePath, limit)
		}

		out[f] = data
	}

	return out, nil
}

func zipObjects(objs map[string][]byte) ([]byte, error) {
	names := make([]string, 0, len(objs))
	for n := range objs {
		names = append(names, n)
	}

	sort.Strings(names)

	var buf bytes.Buffer

	zw := zip.NewWriter(&buf)

	for _, n := range names {
		w, err := zw.Create(n)
		if err != nil {
			return nil, fmt.Errorf("zip %s: %w", n, err)
		}

		if _, err = w.Write(objs[n]); err != nil {
			return nil, fmt.Errorf("zip %s: %w", n, err)
		}
	}

	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("zip close: %w", err)
	}

	return buf.Bytes(), nil
}
