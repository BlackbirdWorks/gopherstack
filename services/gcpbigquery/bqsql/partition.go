package bqsql

import (
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	keyField     = "field"
	keyType      = "type"
	keyTimePart  = "timePartitioning"
	argsPair     = 2
	argsMaxRange = 3
)

const partitionHintText = "PARTITION BY expression must be DATE(<timestamp_column>), DATE(<datetime_column>), " +
	"DATETIME_TRUNC(<datetime_column>, DAY/HOUR/MONTH/YEAR), a DATE column, " +
	"TIMESTAMP_TRUNC(<timestamp_column>, DAY/HOUR/MONTH/YEAR), " +
	"TIMESTAMP_TRUNC(_PARTITIONTIME, DAY/HOUR/MONTH/YEAR), DATE_TRUNC(<date_column>, MONTH/YEAR), or " +
	"RANGE_BUCKET(<int64_column>, GENERATE_ARRAY(<int64_value>, <int64_value>[, <int64_value>]))"

func partitionColumn(n *node, fields []Field) (*Field, bool) {
	if n == nil || n.kind != nPath || len(n.parts) != 1 {
		return nil, false
	}

	for i := range fields {
		if strs.Equal(fields[i].Name, n.parts[0]) {
			return &fields[i], true
		}
	}

	return nil, false
}

func kindIs(f *Field, kinds ...Kind) bool {
	k, _ := KindOf(f.Type)

	for _, want := range kinds {
		if k == want && !strings.EqualFold(f.Mode, ModeRepeated) {
			return true
		}
	}

	return false
}

func truncUnit(n *node, allowed ...string) (string, bool) {
	if n == nil || n.kind != nPath || len(n.parts) != 1 {
		return "", false
	}

	for _, a := range allowed {
		if strs.Equal(a, n.parts[0]) {
			return a, true
		}
	}

	return "", false
}

// partitionSpec maps a PARTITION BY expression onto the REST timePartitioning or rangePartitioning block.
func partitionSpec(n *node, fields []Field) (string, map[string]any, error) {
	bad := invalid("%s", partitionHintText)

	switch {
	case n == nil:
		return "", nil, nil
	case n.kind == nPath && len(n.parts) == 1 && strs.Equal(n.parts[0], "_PARTITIONDATE"):
		return keyTimePart, map[string]any{keyType: litDay}, nil
	case n.kind == nPath:
		f, ok := partitionColumn(n, fields)
		if !ok || !kindIs(f, KindDate) {
			return "", nil, bad
		}

		return keyTimePart, map[string]any{keyType: litDay, keyField: f.Name}, nil
	case n.kind != nCall:
		return "", nil, bad
	}

	return callPartition(n, fields, bad)
}

func callPartition(n *node, fields []Field, bad error) (string, map[string]any, error) {
	switch n.name {
	case "DATE":
		if f, ok := partitionColumn(first(n.args), fields); ok && len(n.args) == 1 &&
			kindIs(f, KindTimestamp, KindDatetime) {
			return keyTimePart, map[string]any{keyType: litDay, keyField: f.Name}, nil
		}
	case "DATE_TRUNC":
		return truncPartition(n, fields, KindDate, bad, "MONTH", "YEAR")
	case "DATETIME_TRUNC":
		return truncPartition(n, fields, KindDatetime, bad, "DAY", "HOUR", "MONTH", "YEAR")
	case "TIMESTAMP_TRUNC":
		if len(n.args) == argsPair && isPartitionTime(n.args[0]) {
			if u, ok := truncUnit(n.args[1], "DAY", "HOUR", "MONTH", "YEAR"); ok {
				return keyTimePart, map[string]any{keyType: u}, nil
			}
		}

		return truncPartition(n, fields, KindTimestamp, bad, "DAY", "HOUR", "MONTH", "YEAR")
	case "RANGE_BUCKET":
		return rangePartition(n, fields, bad)
	}

	return "", nil, bad
}

func first(args []*node) *node {
	if len(args) == 0 {
		return nil
	}

	return args[0]
}

func truncPartition(
	n *node, fields []Field, kind Kind, bad error, units ...string,
) (string, map[string]any, error) {
	if len(n.args) != argsPair {
		return "", nil, bad
	}

	f, ok := partitionColumn(n.args[0], fields)
	u, uok := truncUnit(n.args[1], units...)

	if !ok || !uok || !kindIs(f, kind) {
		return "", nil, bad
	}

	return keyTimePart, map[string]any{keyType: strings.ToUpper(u), keyField: f.Name}, nil
}

func rangePartition(n *node, fields []Field, bad error) (string, map[string]any, error) {
	if len(n.args) != argsPair || n.args[1].kind != nCall || n.args[1].name != "GENERATE_ARRAY" {
		return "", nil, bad
	}

	f, ok := partitionColumn(n.args[0], fields)
	if !ok || !kindIs(f, KindInt) {
		return "", nil, bad
	}

	gen := n.args[1].args
	if len(gen) < argsPair || len(gen) > argsMaxRange {
		return "", nil, bad
	}

	vals := make([]string, 0, len(gen))

	for _, g := range gen {
		v, lok := intLiteral(g)
		if !lok {
			return "", nil, bad
		}

		vals = append(vals, strconv.FormatInt(v, 10))
	}

	interval := "1"
	if len(vals) == argsMaxRange {
		interval = vals[2]
	}

	return "rangePartitioning", map[string]any{
		keyField: f.Name, "range": map[string]any{"start": vals[0], "end": vals[1], "interval": interval},
	}, nil
}

func intLiteral(n *node) (int64, bool) {
	sign := int64(1)

	if n.kind == nUnary && n.name == "-" && len(n.args) == 1 {
		sign, n = -1, n.args[0]
	}

	if n.kind != nInt {
		return 0, false
	}

	v, err := strconv.ParseInt(n.str, 10, 64)

	return sign * v, err == nil
}

func isPartitionTime(n *node) bool {
	return n.kind == nPath && strs.Equal(n.parts[len(n.parts)-1], "_PARTITIONTIME")
}
