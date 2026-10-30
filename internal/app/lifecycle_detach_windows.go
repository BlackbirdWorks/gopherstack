//go:build windows

package app

import (
	"os/exec"
	"syscall"
)

const winCreateNewProcessGroup = 0x200

func detachProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: winCreateNewProcessGroup}
}
