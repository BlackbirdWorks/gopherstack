package azurefunctions

import (
	"fmt"
	"slices"
	"strings"
)

const (
	imageRepo = "mcr.microsoft.com/azure-functions/"

	workerNode     = "node"
	workerPython   = "python"
	workerDotnetIs = "dotnet-isolated"
	workerDotnet   = "dotnet"
	workerJava     = "java"
	workerPS       = "powershell"
	workerCustom   = "custom"

	settingWorker      = "FUNCTIONS_WORKER_RUNTIME"
	settingExtVersion  = "FUNCTIONS_EXTENSION_VERSION"
	settingNodeVersion = "WEBSITE_NODE_DEFAULT_VERSION"
	settingRunFromPkg  = "WEBSITE_RUN_FROM_PACKAGE"
	dockerPrefix       = "DOCKER|"
)

// runtimeFamily is one worker runtime and the image tags the official Functions v4 images publish for it.
type runtimeFamily struct {
	tagFmt   string
	repo     string
	def      string
	versions []string
}

func runtimeFamilies() map[string]runtimeFamily {
	return map[string]runtimeFamily{
		workerNode: {
			repo: "node", tagFmt: "4-node%s", def: "20", versions: []string{"18", "20", "22", "24"},
		},
		workerPython: {
			repo: "python", tagFmt: "4-python%s", def: "3.11",
			versions: []string{"3.9", "3.10", "3.11", "3.12", "3.13"},
		},
		workerDotnetIs: {
			repo:     "dotnet-isolated",
			tagFmt:   "4-dotnet-isolated%s",
			def:      "8.0",
			versions: []string{"8.0", "9.0", "10.0"},
		},
		workerDotnet: {repo: "dotnet", tagFmt: "%s", def: "4", versions: []string{"4"}},
		workerJava: {
			repo: "java", tagFmt: "4-java%s", def: "17", versions: []string{"11", "17", "21", "25"},
		},
		workerPS: {
			repo: "powershell", tagFmt: "4-powershell%s", def: "7.4", versions: []string{"7.2", "7.4"},
		},
		workerCustom: {repo: "base", tagFmt: "%s", def: "4", versions: []string{"4"}},
	}
}

// runtimeChoice is the worker runtime and image a function app runs in.
type runtimeChoice struct {
	worker string
	image  string
	custom bool
}

func workerFromLinuxFx(fx string) string {
	head, _, _ := strings.Cut(strings.ToLower(fx), "|")

	switch head {
	case "node", "python", "java", "powershell", "dotnet-isolated", "dotnet":
		return head
	case "custom":
		return workerCustom
	}

	return ""
}

func normalizeVersion(v string) string {
	v = strings.TrimPrefix(strings.TrimSpace(v), "~")
	v, _, _ = strings.Cut(v, "-")

	return v
}

// chooseRuntime picks the worker and image from the app's settings and linuxFxVersion.
func chooseRuntime(settings map[string]string, linuxFx string) (runtimeChoice, error) {
	if after, ok := strings.CutPrefix(linuxFx, dockerPrefix); ok && after != "" {
		return runtimeChoice{worker: strings.ToLower(settings[settingWorker]), image: after, custom: true}, nil
	}

	if ext := settings[settingExtVersion]; ext != "" && ext != "~4" && ext != "4" {
		return runtimeChoice{}, fmt.Errorf(
			"%w: %s=%q (only Functions v4 is supported)",
			errUnsupportedRT,
			settingExtVersion,
			ext,
		)
	}

	worker := strings.ToLower(settings[settingWorker])
	if worker == "" {
		worker = workerFromLinuxFx(linuxFx)
	}

	fam, ok := runtimeFamilies()[worker]
	if !ok {
		return runtimeChoice{}, fmt.Errorf(
			"%w: set %s to one of node, python, dotnet-isolated, dotnet, java, powershell, custom",
			errUnsupportedRT,
			settingWorker,
		)
	}

	version := fam.def
	if _, v, found := strings.Cut(linuxFx, "|"); found && worker != workerCustom && normalizeVersion(v) != "" {
		version = normalizeVersion(v)
	} else if worker == workerNode && settings[settingNodeVersion] != "" {
		version = normalizeVersion(settings[settingNodeVersion])
	}

	if !slices.Contains(fam.versions, version) {
		return runtimeChoice{}, fmt.Errorf("%w: %s %q; supported: %s", errUnsupportedRT, worker, version,
			strings.Join(fam.versions, ", "))
	}

	return runtimeChoice{worker: worker, image: imageRepo + fam.repo + ":" + fmt.Sprintf(fam.tagFmt, version)}, nil
}
