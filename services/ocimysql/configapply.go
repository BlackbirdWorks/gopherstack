package ocimysql

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	errReadOnlyVar   = "read only variable"
	errUnknownVar    = "unknown system variable"
	maxVariableValue = 1024
)

var errBadVariable = errors.New("ocimysql: invalid configuration variable")

var variableValueRe = regexp.MustCompile(`^[A-Za-z0-9_,=.:+/ -]*$`)

// sysvarName maps an SDK ConfigurationVariables field to the MySQL system variable it sets.
func sysvarName(field string) string {
	var sb strings.Builder

	for i, r := range field {
		if unicode.IsUpper(r) && i > 0 {
			sb.WriteByte('_')
		}

		sb.WriteRune(unicode.ToLower(r))
	}

	return sb.String()
}

func sqlValue(v any) (string, bool) {
	switch x := v.(type) {
	case bool:
		if x {
			return "ON", true
		}

		return "OFF", true
	case int:
		return strconv.Itoa(x), true
	case int64:
		return strconv.FormatInt(x, 10), true
	case float64:
		return strconv.FormatInt(int64(x), 10), true
	case string:
		if len(x) > maxVariableValue || !variableValueRe.MatchString(x) {
			return "", false
		}

		return "'" + x + "'", true
	}

	return "", false
}

func sameValue(a, b any) bool {
	na, ok1 := numeric(a)
	nb, ok2 := numeric(b)

	if ok1 || ok2 {
		return ok1 && ok2 && na == nb
	}

	return reflect.DeepEqual(a, b)
}

func numeric(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		return int64(x), true
	}

	return 0, false
}

// configOverrides lists the variables a configuration sets beyond its shape's defaults. The defaults describe the
// OCI shape's resources and are not applied to the container, which is sized by its host.
func (b *InMemoryBackend) configOverrides(configID string) map[string]any {
	c, ok := b.lookupConfig(configID)
	if !ok || c.Type == configDefault {
		return nil
	}

	sh, ok := shapeByName(c.Shape)
	if !ok {
		return nil
	}

	base := defaultVariables(sh)
	out := map[string]any{}

	for k, v := range c.Variables {
		if bv, has := base[k]; !has || !sameValue(bv, v) {
			out[k] = v
		}
	}

	return out
}

// applyVariables sets vars on a running server. A variable the server only reads at start is persisted for the next
// start and reported through restart; one this MySQL build lacks is logged and skipped.
func applyVariables(ctx context.Context, q Querier, vars map[string]any) (bool, error) {
	names := make([]string, 0, len(vars))
	for k := range vars {
		names = append(names, k)
	}

	slices.Sort(names)

	restart := false

	for _, k := range names {
		val, ok := sqlValue(vars[k])
		if !ok {
			return false, fmt.Errorf("%w: %s has an unsupported value", errBadVariable, k)
		}

		stmt := "SET PERSIST " + sysvarName(k) + " = " + val
		_, err := q.Query(ctx, "", stmt)

		if err != nil && strings.Contains(strings.ToLower(err.Error()), errReadOnlyVar) {
			restart = true
			_, err = q.Query(ctx, "", strings.Replace(stmt, "SET PERSIST", "SET PERSIST_ONLY", 1))
		}

		if err != nil && strings.Contains(strings.ToLower(err.Error()), "non persistent") {
			_, err = q.Query(ctx, "", strings.Replace(stmt, "SET PERSIST", "SET GLOBAL", 1))
		}

		switch {
		case err == nil:
		case strings.Contains(strings.ToLower(err.Error()), errUnknownVar):
			logger.Load(ctx).WarnContext(ctx, "ocimysql: configuration variable skipped, unknown to this MySQL server",
				"variable", sysvarName(k))
		default:
			return false, fmt.Errorf("%w: %s: %w", errBadVariable, k, err)
		}
	}

	return restart, nil
}

// configureInstance applies a configuration's variables to a server and restarts it when it must.
func configureInstance(ctx context.Context, inst Instance, vars map[string]any, reset bool) error {
	q, ok := inst.(Querier)
	if !ok {
		return nil
	}

	if reset {
		if _, err := q.Query(ctx, "", "RESET PERSIST"); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	restart, err := applyVariables(ctx, q, vars)
	if err != nil {
		return err
	}

	if !restart && !reset {
		return nil
	}

	if err = inst.Stop(ctx); err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	return inst.Start(ctx) //nolint:wrapcheck // engine errors carry the cause
}

// reapplyConfig moves a running server from one configuration to another.
func (b *InMemoryBackend) reapplyConfig(ctx context.Context, id string) error {
	b.mu.RLock("reapplyConfig")
	vars, cfgID := b.currentVariables(id)
	b.mu.RUnlock()

	inst := b.instance(id)
	if inst == nil {
		return errNoServer
	}

	if err := configureInstance(ctx, inst, vars, true); err != nil {
		return err
	}

	b.mu.Lock("reapplyConfig")
	defer b.mu.Unlock()

	if s, ok := b.systems[id]; ok {
		s.AppliedConfig = cfgID
	} else if r, found := b.replicas[id]; found {
		r.AppliedConfig = cfgID
	}

	return nil
}

// syncConfig applies the configuration a stopped server missed.
func (b *InMemoryBackend) syncConfig(ctx context.Context, id string) error {
	b.mu.Lock("syncConfig")
	vars, want := b.currentVariables(id)
	have := ""

	s, isSystem := b.systems[id]
	r := b.replicas[id]

	switch {
	case isSystem:
		have = s.AppliedConfig
	case r != nil:
		have = r.AppliedConfig
	}

	if have == want || (len(vars) == 0 && len(b.configOverrides(have)) == 0) {
		switch {
		case isSystem:
			s.AppliedConfig = want
		case r != nil:
			r.AppliedConfig = want
		}

		b.mu.Unlock()

		return nil
	}

	b.mu.Unlock()

	return b.reapplyConfig(ctx, id)
}

// currentVariables resolves the overrides of the configuration of a DB system or replica; the caller holds the lock.
func (b *InMemoryBackend) currentVariables(id string) (map[string]any, string) {
	if s, ok := b.systems[id]; ok {
		return b.configOverrides(s.ConfigID), s.ConfigID
	}

	if r, ok := b.replicas[id]; ok {
		cfg := r.config(b.systems[r.DBSystemID])

		return b.configOverrides(cfg), cfg
	}

	return nil, ""
}
