package gcpsql

import (
	"context"
	"math"
	"strconv"
	"strings"
)

const maxFlagValueLen = 1024

func flagByName(version, name string) (FlagInfo, bool) {
	for _, f := range flagCatalog() {
		if f.Name == name && contains(f.AppliesTo, version) {
			return f, true
		}
	}

	return FlagInfo{}, false
}

// validateFlags checks settings.databaseFlags against the flags that apply to version: the name must be known,
// appear once, and carry a value of the flag's type within its range.
func validateFlags(version string, settings map[string]any) error {
	seen := map[string]bool{}

	for _, f := range flagsOf(settings) {
		info, ok := flagByName(version, f.Name)
		if !ok {
			return errInvalid("Unsupported flag %q for %s; see flags.list for the supported flags.", f.Name, version)
		}

		if seen[f.Name] {
			return errInvalid("Flag %q is set more than once.", f.Name)
		}

		seen[f.Name] = true

		if err := checkFlagValue(info, f.Value); err != nil {
			return err
		}
	}

	return nil
}

func checkFlagValue(info FlagInfo, v string) error {
	bad := func(why string) error { return errInvalid("Invalid value %q for flag %q: %s.", v, info.Name, why) }

	if v == "" {
		return bad("a value is required")
	}

	if len(v) > maxFlagValueLen || strings.ContainsRune(v, 0) {
		return bad("not a valid string value")
	}

	switch info.Type {
	case "BOOLEAN":
		if v != "on" && v != "off" {
			return bad("boolean flags take on or off")
		}
	case "INTEGER":
		return checkIntFlag(info, v, bad)
	case "FLOAT":
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return bad("not a number")
		}

		return checkFloatRange(info, f, bad)
	case "STRING":
		if len(info.AllowedStringValues) > 0 && !contains(info.AllowedStringValues, v) {
			return bad("allowed values are " + strings.Join(info.AllowedStringValues, ", "))
		}
	}

	return nil
}

func checkIntFlag(info FlagInfo, v string, bad func(string) error) error {
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return bad("not an integer")
	}

	if lo, perr := strconv.ParseInt(info.MinValue, 10, 64); perr == nil && n < lo {
		return bad("minimum is " + info.MinValue)
	}

	if hi, perr := strconv.ParseInt(info.MaxValue, 10, 64); perr == nil && n > hi {
		return bad("maximum is " + info.MaxValue)
	}

	return nil
}

func checkFloatRange(info FlagInfo, f float64, bad func(string) error) error {
	if lo, err := strconv.ParseFloat(info.MinValue, 64); err == nil && f < lo {
		return bad("minimum is " + info.MinValue)
	}

	if hi, err := strconv.ParseFloat(info.MaxValue, 64); err == nil && f > hi {
		return bad("maximum is " + info.MaxValue)
	}

	return nil
}

// flagsNeedRestart reports whether any flag that differs between old and next takes effect only at a restart.
func flagsNeedRestart(version string, old, next []Flag) bool {
	before := map[string]string{}
	for _, f := range old {
		before[f.Name] = f.Value
	}

	after := map[string]string{}
	for _, f := range next {
		after[f.Name] = f.Value
	}

	for name, v := range after {
		if prev, had := before[name]; !had || prev != v {
			if info, ok := flagByName(version, name); !ok || info.RequiresRestart {
				return true
			}
		}
	}

	for name := range before {
		if _, kept := after[name]; !kept {
			if info, ok := flagByName(version, name); !ok || info.RequiresRestart {
				return true
			}
		}
	}

	return false
}

// FlagReloader is implemented by engines that can apply flags needing no restart to a running server.
type FlagReloader interface {
	// ReloadFlags persists next in place of old and applies it without a restart.
	ReloadFlags(ctx context.Context, old, next []Flag) error
}

// applyFlags applies a changed flag list: a restart when a changed flag needs one or the engine cannot reload.
func (b *InMemoryBackend) applyFlags(ctx context.Context, rec *instRec, old, next []Flag) error {
	eng := b.engineOf(rec)
	if eng == nil {
		return nil
	}

	if rl, ok := eng.(FlagReloader); ok && !flagsNeedRestart(rec.Inst.DatabaseVersion, old, next) {
		return rl.ReloadFlags(ctx, old, next)
	}

	return eng.Restart(ctx, next)
}
