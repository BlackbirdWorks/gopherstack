package mwaa

import (
	"fmt"
	"slices"
)

const (
	cliDelete = "delete"
	cliExport = "export"
	cliGet    = "get"
	cliImport = "import"
	cliList   = "list"
	cliCreate = "create"
)

// mwaaCLICommands is the Airflow CLI surface MWAA exposes; a nil subcommand set means the command takes none.
var mwaaCLICommands = map[string][]string{ //nolint:gochecknoglobals // immutable allowlist
	"cheat-sheet": nil,
	"info":        nil,
	"version":     nil,
	"sync-perm":   nil,
	"connections": {"add", cliDelete, cliExport, cliGet, cliImport, cliList},
	"dags": {
		"backfill", cliDelete, cliList, "list-jobs", "list-runs", "next-execution", "pause", "report", "show",
		"state", "test", "trigger", "unpause",
	},
	"jobs":    {"check"},
	"plugins": nil,
	"pools":   {cliDelete, cliExport, cliGet, cliImport, cliList, "set"},
	"providers": {
		"auth", "behaviours", cliGet, "hooks", "lazy-loaded", "links", cliList, "logging", "secrets", "widgets",
	},
	"roles": {"add-perms", cliCreate, "del-perms", cliDelete, cliExport, cliImport, cliList},
	"tasks": {
		"clear", "failed-deps", cliList, "render", "run", "state", "states-for-dag-run", "test",
	},
	"users":     {"add-role", cliCreate, cliDelete, cliExport, cliImport, cliList, "remove-role"},
	"variables": {cliDelete, cliExport, cliGet, cliImport, cliList, "set"},
}

// checkCLICommand rejects commands MWAA's /aws_mwaa/cli endpoint does not support.
func checkCLICommand(args []string) error {
	subs, ok := mwaaCLICommands[args[0]]
	if !ok {
		return fmt.Errorf("%w: airflow command %q is not supported by Amazon MWAA", ErrInvalidParameter, args[0])
	}

	if subs == nil {
		return nil
	}

	if len(args) < 2 || !slices.Contains(subs, args[1]) {
		return fmt.Errorf("%w: airflow command \"%s %s\" is not supported by Amazon MWAA",
			ErrInvalidParameter, args[0], argAt(args, 1))
	}

	return nil
}

func argAt(args []string, i int) string {
	if i < len(args) {
		return args[i]
	}

	return ""
}
