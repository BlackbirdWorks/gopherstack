//go:build !windows

package app

import (
	"fmt"
	"os"
	"syscall"
)

func processAlive(pid int) bool {
	p, err := os.FindProcess(pid)
	if err != nil {
		return false
	}

	return p.Signal(syscall.Signal(0)) == nil
}

func terminateProcess(pid int, force bool) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process %d: %w", pid, err)
	}

	sig := os.Interrupt
	if force {
		sig = os.Kill
	}

	if err = p.Signal(sig); err != nil {
		return fmt.Errorf("signal process %d: %w", pid, err)
	}

	return nil
}
