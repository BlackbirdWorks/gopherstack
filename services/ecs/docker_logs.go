package ecs

import (
	"context"
	"errors"
	"fmt"
	"maps"

	mobycontainer "github.com/moby/moby/api/types/container"
)

var errInvalidLogOption = errors.New("invalid log option")

const (
	dockerLogJSONFile = "json-file"
	logDriverNone     = "none"

	logOptsExtra = 2

	logOptMode          = "mode"
	logOptMaxBufferSize = "max-buffer-size"

	logModeBlocking    = "blocking"
	logModeNonBlocking = "non-blocking"

	defaultLogMode          = logModeNonBlocking
	defaultLogMaxBufferSize = "10m"
)

// buildLogConfig maps a container's logConfiguration onto a Docker log driver. awslogs containers log through
// json-file so the runner can read their output and forward it to CloudWatch Logs; every other driver is passed
// through with its options and secretOptions. mode and max-buffer-size default as documented.
func buildLogConfig(lc *LogConfiguration, secretOpts map[string]string) (mobycontainer.LogConfig, error) {
	if lc == nil || lc.LogDriver == "" {
		return mobycontainer.LogConfig{}, nil
	}

	opts := make(map[string]string, len(lc.Options)+len(secretOpts)+logOptsExtra)
	maps.Copy(opts, lc.Options)
	maps.Copy(opts, secretOpts)

	driver := lc.LogDriver

	if driver == logDriverAwslogs {
		driver = dockerLogJSONFile
		opts = pickLogOptions(opts, logOptMode, logOptMaxBufferSize)
	}

	if driver == logDriverNone {
		return mobycontainer.LogConfig{Type: driver}, nil
	}

	switch opts[logOptMode] {
	case "":
		opts[logOptMode] = defaultLogMode
	case logModeBlocking, logModeNonBlocking:
	default:
		return mobycontainer.LogConfig{}, fmt.Errorf("%w: logConfiguration option %q must be %s or %s, got %q",
			errInvalidLogOption, logOptMode, logModeBlocking, logModeNonBlocking, opts[logOptMode])
	}

	if opts[logOptMode] == logModeNonBlocking && opts[logOptMaxBufferSize] == "" {
		opts[logOptMaxBufferSize] = defaultLogMaxBufferSize
	}

	return mobycontainer.LogConfig{Type: driver, Config: opts}, nil
}

func pickLogOptions(opts map[string]string, keys ...string) map[string]string {
	out := make(map[string]string, len(keys))

	for _, k := range keys {
		if v, ok := opts[k]; ok {
			out[k] = v
		}
	}

	return out
}

// resolveLogSecrets resolves each container's logConfiguration.secretOptions to option values.
func resolveLogSecrets(
	ctx context.Context,
	lookup func(ctx context.Context, valueFrom string) (string, error),
	defs []ContainerDefinition,
) (map[string]map[string]string, error) {
	out := make(map[string]map[string]string, len(defs))

	for _, cd := range defs {
		if cd.LogConfiguration == nil {
			continue
		}

		for _, so := range cd.LogConfiguration.SecretOptions {
			if lookup == nil {
				return nil, secretLaunchError(sourceASM, so.ValueFrom, errSecretsUnavailable)
			}

			val, err := lookup(ctx, so.ValueFrom)
			if err != nil {
				return nil, err
			}

			if out[cd.Name] == nil {
				out[cd.Name] = map[string]string{}
			}

			out[cd.Name][so.Name] = val
		}
	}

	return out, nil
}
