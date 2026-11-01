//go:build windows

package app

import (
	"fmt"
	"math"
	"os"
	"syscall"
)

const (
	winProcessQueryLimited = 0x1000
	winStillActive         = 259
	winCtrlBreakEvent      = 1
)

var procGenerateCtrlEvent = syscall.NewLazyDLL("kernel32.dll").NewProc("GenerateConsoleCtrlEvent")

type winKiller struct{}

func (winKiller) CtrlBreak(pid int) error {
	r, _, err := procGenerateCtrlEvent.Call(uintptr(winCtrlBreakEvent), uintptr(pid))
	if r == 0 {
		return fmt.Errorf("ctrl-break pid %d: %w", pid, err)
	}

	return nil
}

func (winKiller) Kill(pid int) error {
	p, err := os.FindProcess(pid)
	if err != nil {
		return fmt.Errorf("find process %d: %w", pid, err)
	}

	return p.Kill()
}

func processAlive(pid int) bool {
	if pid <= 0 || pid > math.MaxInt32 {
		return false
	}

	h, err := syscall.OpenProcess(winProcessQueryLimited, false, uint32(pid))
	if err != nil {
		return false
	}
	defer syscall.CloseHandle(h)

	var code uint32
	if syscall.GetExitCodeProcess(h, &code) != nil {
		return false
	}

	return code == winStillActive
}

func terminateProcess(pid int, force bool) error { return terminateWith(winKiller{}, pid, force) }
