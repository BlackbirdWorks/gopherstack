package azurearm

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"maps"
	"os"
	"regexp"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	flexDumpMaxBytes  = 256 << 20
	flexDumpDirMode   = 0o700
	flexDumpFileMode  = 0o600
	flexDumpIDBytes   = 12
	flexInstanceBytes = 4
	flexDumpSuffix    = ".dump"
)

// flexDumpNameRE is the only shape of file name the store creates: <kind>-<instance>-<24 hex>.dump.
var flexDumpNameRE = regexp.MustCompile(`^[a-z]{1,16}-[0-9a-f]{8}-[0-9a-f]{24}\.dump$`)

var flexInstanceRE = regexp.MustCompile(`^[0-9a-f]{8}$`)

// newFlexInstanceID names one provider's dumps so another emulator sharing the directory never sweeps them.
func newFlexInstanceID() string {
	b := make([]byte, flexInstanceBytes)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func validFlexDumpName(name string) bool { return flexDumpNameRE.MatchString(name) }

var (
	errFlexDumpTooLarge = errors.New("azurearm: the database dump exceeds the size limit")
	errFlexDumpCorrupt  = errors.New("azurearm: the stored database dump does not match its recorded checksum")
	errFlexDumpName     = errors.New("azurearm: invalid dump file name")
)

// flexDumpRef locates one stored dump and the checksum it must still have when it is restored.
type flexDumpRef struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
}

// flexDumpStore keeps database dumps as files under one directory, confined with os.Root. With no configured
// directory it creates a temporary one on first use and removes it on close.
type flexDumpStore struct {
	mu   *lockmetrics.RWMutex
	root *os.Root
	live map[string]bool
	dir  string
	max  int64
	tmp  bool
}

func newFlexDumpStore(dir string, maxBytes int64) *flexDumpStore {
	if maxBytes <= 0 {
		maxBytes = flexDumpMaxBytes
	}

	return &flexDumpStore{
		mu: lockmetrics.New("azurearm.flexdb.dumps"), dir: dir, max: maxBytes, live: map[string]bool{},
	}
}

func (s *flexDumpStore) open() (*os.Root, error) {
	s.mu.Lock("open")
	defer s.mu.Unlock()

	if s.root != nil {
		return s.root, nil
	}

	dir := s.dir
	if dir == "" {
		d, err := os.MkdirTemp("", "gopherstack-azflex-")
		if err != nil {
			return nil, fmt.Errorf("dump directory: %w", err)
		}

		dir, s.tmp, s.dir = d, true, d
	} else if err := os.MkdirAll(dir, flexDumpDirMode); err != nil {
		return nil, fmt.Errorf("dump directory: %w", err)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("dump directory: %w", err)
	}

	s.root = root

	return root, nil
}

type capWriter struct {
	w    io.Writer
	left int64
}

func (c *capWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > c.left {
		return 0, errFlexDumpTooLarge
	}

	n, err := c.w.Write(p)
	c.left -= int64(n)

	return n, err
}

// write creates a new dump file named with prefix and fills it; the size cap is enforced while fill streams. The
// file stays pinned against sweep until the caller calls release.
func (s *flexDumpStore) write(prefix string, fill func(io.Writer) error) (flexDumpRef, error) {
	root, err := s.open()
	if err != nil {
		return flexDumpRef{}, err
	}

	id := make([]byte, flexDumpIDBytes)
	_, _ = rand.Read(id)
	name := prefix + "-" + hex.EncodeToString(id) + flexDumpSuffix

	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, flexDumpFileMode)
	if err != nil {
		return flexDumpRef{}, fmt.Errorf("dump file: %w", err)
	}

	s.setLive(name, true)

	sum := sha256.New()
	cw := &capWriter{w: io.MultiWriter(f, sum), left: s.max}

	err = fill(cw)

	if cerr := f.Close(); err == nil {
		err = cerr
	}

	if err != nil {
		_ = root.Remove(name)
		s.setLive(name, false)

		return flexDumpRef{}, err
	}

	return flexDumpRef{File: name, SHA256: hex.EncodeToString(sum.Sum(nil)), Bytes: s.max - cw.left}, nil
}

// release unpins a file returned by write once it is recorded in a backup or no longer needed.
func (s *flexDumpStore) release(name string) { s.setLive(name, false) }

func (s *flexDumpStore) setLive(name string, on bool) {
	s.mu.Lock("setLive")
	defer s.mu.Unlock()

	if on {
		s.live[name] = true
	} else {
		delete(s.live, name)
	}
}

// verifiedDump streams a stored dump and reports, after the consumer is done, whether what it read still matched.
type verifiedDump struct {
	f    *os.File
	sum  hash.Hash
	want string
	n    int64
}

func (v *verifiedDump) Read(p []byte) (int, error) {
	n, err := v.f.Read(p)
	_, _ = v.sum.Write(p[:n])
	v.n += int64(n)

	return n, err
}

func (v *verifiedDump) Close() error { return v.f.Close() }

// Verify reports whether every byte read hashed to the recorded checksum.
func (v *verifiedDump) Verify() error {
	if hex.EncodeToString(v.sum.Sum(nil)) != v.want {
		return errFlexDumpCorrupt
	}

	return nil
}

func hashFile(f *os.File) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("dump file: %w", err)
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// read returns the dump after checking its checksum; the caller checks Verify again once the data was consumed.
func (s *flexDumpStore) read(ref flexDumpRef) (*verifiedDump, error) {
	if !validFlexDumpName(ref.File) {
		return nil, errFlexDumpName
	}

	root, err := s.open()
	if err != nil {
		return nil, err
	}

	f, err := root.Open(ref.File)
	if err != nil {
		return nil, fmt.Errorf("dump file: %w", err)
	}

	sum, err := hashFile(f)
	if err != nil {
		_ = f.Close()

		return nil, err
	}

	if sum != ref.SHA256 {
		_ = f.Close()

		return nil, errFlexDumpCorrupt
	}

	if _, err = f.Seek(0, io.SeekStart); err != nil {
		_ = f.Close()

		return nil, fmt.Errorf("dump file: %w", err)
	}

	return &verifiedDump{f: f, sum: sha256.New(), want: ref.SHA256}, nil
}

func (s *flexDumpStore) remove(name string) {
	if !validFlexDumpName(name) {
		return
	}

	if root, err := s.open(); err == nil {
		_ = root.Remove(name)
	}
}

// sweep removes the store's own files named with prefix (which includes the owning instance) that keep does not
// list; files of another instance sharing the directory have another prefix and are left alone.
func (s *flexDumpStore) sweep(prefix string, keep map[string]bool) {
	root, err := s.open()
	if err != nil {
		return
	}

	entries, err := fs.ReadDir(root.FS(), ".")
	if err != nil {
		return
	}

	s.mu.RLock("sweep")
	live := maps.Clone(s.live)
	s.mu.RUnlock()

	for _, e := range entries {
		n := e.Name()
		if validFlexDumpName(n) && strings.HasPrefix(n, prefix+"-") && !keep[n] && !live[n] {
			_ = root.Remove(n)
		}
	}
}

func (s *flexDumpStore) close() {
	s.mu.Lock("close")
	defer s.mu.Unlock()

	if s.root != nil {
		_ = s.root.Close()
		s.root = nil
	}

	if s.tmp {
		_ = os.RemoveAll(s.dir)
		s.dir, s.tmp = "", false
	}
}

func (s *flexDumpStore) exists(name string) bool {
	if !validFlexDumpName(name) {
		return false
	}

	root, err := s.open()
	if err != nil {
		return false
	}

	_, err = root.Stat(name)

	return err == nil
}

func isFlexDumpCorrupt(err error) bool  { return errors.Is(err, errFlexDumpCorrupt) }
func isFlexDumpTooLarge(err error) bool { return errors.Is(err, errFlexDumpTooLarge) }
