package gcpartifactregistry

import (
	"archive/tar"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
	"github.com/ulikunitz/xz"
)

const (
	arMagic     = "!<arch>\n"
	arHeaderLen = 60
	maxControl  = 1 << 20
	arAlign     = 2
)

var errNotDeb = errors.New("not a Debian binary package")

// debControl is a parsed binary package: the raw control stanza and the fields the index needs.
type debControl struct {
	fields  map[string]string
	Package string
	Version string
	Arch    string
	order   []string
}

func decompress(name string, r io.Reader) (io.Reader, error) {
	switch {
	case strings.HasSuffix(name, ".gz"):
		return gzip.NewReader(r)
	case strings.HasSuffix(name, ".xz"):
		return xz.NewReader(r)
	case strings.HasSuffix(name, ".bz2"):
		return bzip2.NewReader(r), nil
	case strings.HasSuffix(name, ".zst"):
		zr, err := zstd.NewReader(r)
		if err != nil {
			return nil, fmt.Errorf("zstd: %w", err)
		}

		return zr, nil
	case strings.HasSuffix(name, ".tar"):
		return r, nil
	default:
		return nil, fmt.Errorf("%w: unsupported control archive %s", errNotDeb, name)
	}
}

// arMember returns the content of the first member of an ar archive whose name has the prefix.
func arMember(data []byte, prefix string) (string, []byte, error) {
	if !bytes.HasPrefix(data, []byte(arMagic)) {
		return "", nil, errNotDeb
	}

	pos := len(arMagic)

	for pos+arHeaderLen <= len(data) {
		hdr := data[pos : pos+arHeaderLen]
		name := strings.TrimRight(strings.TrimSpace(string(hdr[:16])), "/")

		size, err := strconv.Atoi(strings.TrimSpace(string(hdr[48:58])))
		if err != nil || size < 0 || pos+arHeaderLen+size > len(data) {
			return "", nil, errNotDeb
		}

		body := data[pos+arHeaderLen : pos+arHeaderLen+size]
		if strings.HasPrefix(name, prefix) {
			return name, body, nil
		}

		pos += arHeaderLen + size + size%arAlign
	}

	return "", nil, fmt.Errorf("%w: no %s member", errNotDeb, prefix)
}

func readControlFile(name string, archive []byte) ([]byte, error) {
	r, err := decompress(name, bytes.NewReader(archive))
	if err != nil {
		return nil, err
	}

	tr := tar.NewReader(r)

	for {
		h, nextErr := tr.Next()
		if nextErr != nil {
			return nil, fmt.Errorf("%w: no control file", errNotDeb)
		}

		if path.Clean(h.Name) == "control" {
			return io.ReadAll(io.LimitReader(tr, maxControl))
		}
	}
}

// parseDeb reads the control file of a .deb.
func parseDeb(data []byte) (debControl, []byte, error) {
	name, archive, err := arMember(data, "control.tar")
	if err != nil {
		return debControl{}, nil, err
	}

	raw, err := readControlFile(name, archive)
	if err != nil {
		return debControl{}, nil, err
	}

	c := parseControl(raw)
	if c.Package == "" || c.Version == "" || c.Arch == "" {
		return debControl{}, nil, fmt.Errorf("%w: control lacks Package, Version or Architecture", errNotDeb)
	}

	return c, bytes.TrimRight(raw, "\n"), nil
}

func parseControl(raw []byte) debControl {
	c := debControl{fields: map[string]string{}}
	key := ""

	for line := range strings.SplitSeq(string(raw), "\n") {
		switch {
		case line == "":
		case line[0] == ' ' || line[0] == '\t':
			if key != "" {
				c.fields[key] += "\n" + line
			}
		default:
			k, v, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}

			key = k
			c.fields[k] = strings.TrimSpace(v)
			c.order = append(c.order, k)
		}
	}

	c.Package, c.Version, c.Arch = c.fields["Package"], c.fields["Version"], c.fields["Architecture"]

	return c
}
