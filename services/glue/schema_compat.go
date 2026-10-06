package glue

import (
	"errors"
	"fmt"
	"strings"
)

// Modes: https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html. BACKWARD: new reads old;
// FORWARD: old reads new; FULL: both; _ALL checks every prior version, not just the latest.

var errSchemaIncompatible = errors.New("incompatible schema")

func schemaCompatErr(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errSchemaIncompatible, fmt.Sprintf(format, args...))
}

// checkSchemaCompatibility returns nil when newDef may be registered after
// prior (oldest first) under the given compatibility mode.
func checkSchemaCompatibility(mode, dataFormat, newDef string, prior []string) error {
	mode = strings.ToUpper(mode)
	if len(prior) == 0 || mode == "" || mode == "NONE" {
		return nil
	}

	var targets []string

	switch mode {
	case "BACKWARD", "FORWARD", "FULL":
		targets = prior[len(prior)-1:]
	case "BACKWARD_ALL", "FORWARD_ALL", "FULL_ALL":
		targets = prior
	default:
		return nil
	}

	backward := strings.HasPrefix(mode, "BACKWARD") || strings.HasPrefix(mode, "FULL")
	forward := strings.HasPrefix(mode, "FORWARD") || strings.HasPrefix(mode, "FULL")

	for _, old := range targets {
		if backward {
			if err := schemaCanRead(dataFormat, newDef, old); err != nil {
				return fmt.Errorf("%s compatibility check failed: %w", mode, err)
			}
		}

		if forward {
			if err := schemaCanRead(dataFormat, old, newDef); err != nil {
				return fmt.Errorf("%s compatibility check failed: %w", mode, err)
			}
		}
	}

	return nil
}

// schemaCanRead reports whether data written with writerDef can be read by a
// consumer using readerDef.
func schemaCanRead(dataFormat, readerDef, writerDef string) error {
	switch strings.ToUpper(dataFormat) {
	case "AVRO":
		return avroCanRead(readerDef, writerDef)
	case "JSON":
		return jsonSchemaCanRead(readerDef, writerDef)
	case "PROTOBUF":
		return protoCanRead(readerDef, writerDef)
	default:
		return nil
	}
}
