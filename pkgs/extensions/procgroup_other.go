//go:build !unix

package extensions

import (
	"os/exec"
	"syscall"
)

// Windows has no process groups here: only the direct child is signalled.
func setProcessGroup(*exec.Cmd) {}

func signalGroup(cmd *exec.Cmd, _ syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}

	return cmd.Process.Kill()
}
