package guards_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestMain chdirs to the module root; guard tests walk services/, pkgs/ and docs/ by relative path.
func TestMain(m *testing.M) {
	dir, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			break
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			fmt.Fprintln(os.Stderr, "go.mod not found above test working directory")
			os.Exit(1)
		}

		dir = parent
	}

	if err = os.Chdir(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	os.Exit(m.Run())
}
