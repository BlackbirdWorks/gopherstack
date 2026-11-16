package bedrockagentcore

import (
	"archive/zip"
	"bytes"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	codeMount        = "/var/task"
	maxCodeZipped    = 250 << 20
	maxCodeUnzipped  = 750 << 20
	maxCodeFiles     = 100000
	execMask         = 0o111
	runtimeNode22    = "NODE_22"
	interpPython     = "python3"
	runtimePythonPre = "PYTHON_"
)

var pythonRuntime = regexp.MustCompile(`^PYTHON_3_[0-9]{1,2}$`)

// codeImage maps a managed language runtime to the image that provides it.
func codeImage(runtime string) (string, error) {
	if runtime == runtimeNode22 {
		return "node:22-slim", nil
	}

	if pythonRuntime.MatchString(runtime) {
		return "python:" + strings.ReplaceAll(strings.TrimPrefix(runtime, runtimePythonPre), "_", ".") + "-slim", nil
	}

	return "", invalid("runtime %q is not a managed language runtime", runtime)
}

// codeCommand turns an entryPoint such as ["opentelemetry-instrument", "main.py"] into the process to run: the
// last element is the program file and any elements before it wrap the language interpreter.
func codeCommand(runtime string, entry []string) []string {
	if len(entry) == 0 {
		return nil
	}

	interp := interpPython
	if runtime == runtimeNode22 {
		interp = "node"
	}

	out := append([]string{}, entry[:len(entry)-1]...)

	return append(out, interp, entry[len(entry)-1])
}

func stageCode(code *CodeInfo) (string, error) {
	if code.Load == nil {
		return "", internal("the code package of the runtime is unavailable")
	}

	data, err := code.Load()
	if err != nil {
		return "", internal("reading the code package failed: %v", err)
	}

	dir, err := os.MkdirTemp("", "agentcore-code-*")
	if err != nil {
		return "", internal("staging the code package failed")
	}

	if err = extractZip(data, dir); err != nil {
		removeAll(dir)

		return "", err
	}

	return dir, nil
}

func applyCode(spec *container.Spec, code *CodeInfo, dir string) {
	image, _ := codeImage(code.Runtime)
	spec.Image = image
	spec.Cmd = codeCommand(code.Runtime, code.EntryPoint)
	spec.WorkingDir = codeMount
	spec.User = strconv.Itoa(os.Getuid()) + ":" + strconv.Itoa(os.Getgid())
	spec.Mounts = append(spec.Mounts, dir+":"+codeMount+":ro")
}

// extractZip unpacks a code package into dir within the documented 250 MB zipped and 750 MB unzipped limits;
// entries that would leave dir are rejected.
func extractZip(data []byte, dir string) error {
	if len(data) > maxCodeZipped {
		return invalid("the code package exceeds %d bytes", maxCodeZipped)
	}

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return invalid("the code package is not a valid zip archive: %v", err)
	}

	if len(zr.File) > maxCodeFiles {
		return invalid("the code package holds more than %d files", maxCodeFiles)
	}

	root, err := os.OpenRoot(dir)
	if err != nil {
		return internal("staging the code package failed")
	}

	defer root.Close()

	var total int64

	for _, f := range zr.File {
		n, werr := writeEntry(root, f, maxCodeUnzipped-total)
		if werr != nil {
			return werr
		}

		total += n
	}

	return nil
}

func writeEntry(root *os.Root, f *zip.File, budget int64) (int64, error) {
	name := path.Clean(strings.TrimPrefix(f.Name, "/"))
	if !fs.ValidPath(name) || name == "." {
		return 0, invalid("the code package entry %q is not a relative path inside the package", f.Name)
	}

	local := filepath.FromSlash(name)

	if f.FileInfo().IsDir() {
		if err := root.MkdirAll(local, dirMode); err != nil {
			return 0, internal("staging the code package failed")
		}

		return 0, nil
	}

	if !f.Mode().IsRegular() {
		return 0, nil
	}

	if dirPart := filepath.Dir(local); dirPart != "." {
		if err := root.MkdirAll(dirPart, dirMode); err != nil {
			return 0, internal("staging the code package failed")
		}
	}

	mode := fs.FileMode(fileMode)
	if f.Mode()&execMask != 0 {
		mode = dirMode
	}

	return copyEntry(root, f, local, mode, budget)
}

func copyEntry(root *os.Root, f *zip.File, local string, mode fs.FileMode, budget int64) (int64, error) {
	src, err := f.Open()
	if err != nil {
		return 0, invalid("the code package entry %q is unreadable", f.Name)
	}

	defer src.Close()

	dst, err := root.OpenFile(local, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, mode)
	if err != nil {
		return 0, internal("staging the code package failed")
	}

	defer dst.Close()

	n, err := io.Copy(dst, io.LimitReader(src, budget+1))
	if err != nil {
		return 0, internal("staging the code package failed")
	}

	if n > budget {
		return 0, invalid("the code package exceeds %d bytes unzipped", maxCodeUnzipped)
	}

	return n, nil
}
