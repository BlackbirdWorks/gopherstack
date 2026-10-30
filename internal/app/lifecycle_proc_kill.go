package app

import "fmt"

// procKiller is the host-process control surface; Windows has no POSIX signals.
type procKiller interface {
	CtrlBreak(pid int) error
	Kill(pid int) error
}

// terminateWith asks for a graceful stop and falls back to Kill when none can be delivered
// (no shared console); a forced stop always kills.
func terminateWith(k procKiller, pid int, force bool) error {
	if !force {
		if k.CtrlBreak(pid) == nil {
			return nil
		}
	}

	if err := k.Kill(pid); err != nil {
		return fmt.Errorf("kill process %d: %w", pid, err)
	}

	return nil
}
