//go:build windows

package app

import "os/exec"

func detachProcess(*exec.Cmd) {}
