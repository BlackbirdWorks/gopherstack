//go:build unix

package extensions

import (
	"os/exec"
	"syscall"
)

// setProcessGroup puts the child in its own group so wrappers (sh -c, go run) die with it.
func setProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

func signalGroup(cmd *exec.Cmd, sig syscall.Signal) error {
	if cmd.Process == nil {
		return nil
	}

	return syscall.Kill(-cmd.Process.Pid, sig)
}
