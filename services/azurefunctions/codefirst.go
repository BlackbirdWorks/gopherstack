package azurefunctions

import (
	"archive/zip"
	"bytes"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"
)

const maxScanBytes = 8 << 20

// Node v4 (app.<trigger>(...)) and Python v2 (@app.<trigger>(...)) registrations of non-HTTP triggers; the
// names are the registration methods of @azure/functions v4 and azure.functions FunctionApp.
var codeFirstTriggerRE = regexp.MustCompile(
	`\b\w+\.(timer|storageQueue|storageBlob|serviceBusQueue|serviceBusTopic|eventHub|cosmosDB|eventGrid|` +
		`timer_trigger|queue_trigger|blob_trigger|service_bus_queue_trigger|service_bus_topic_trigger|` +
		`event_hub_message_trigger|cosmos_db_trigger|event_grid_trigger)\s*\(`)

// compiledTriggerMarkers are attribute type names that appear in .NET assemblies built for the isolated worker.
func compiledTriggerMarkers() [][]byte {
	return [][]byte{
		[]byte("TimerTriggerAttribute"), []byte("QueueTriggerAttribute"), []byte("BlobTriggerAttribute"),
		[]byte("ServiceBusTriggerAttribute"), []byte("EventHubTriggerAttribute"),
		[]byte("CosmosDBTriggerAttribute"), []byte("EventGridTriggerAttribute"),
	}
}

func isDependencyPath(name string) bool {
	for seg := range strings.SplitSeq(name, "/") {
		switch seg {
		case "node_modules", ".python_packages", "site-packages", "__pycache__":
			return true
		}
	}

	return false
}

func fileHasCodeFirstTrigger(f *zip.File) bool {
	ext := strings.ToLower(path.Ext(f.Name))
	if isDependencyPath(f.Name) || f.UncompressedSize64 > maxScanBytes {
		return false
	}

	switch ext {
	case ".js", ".mjs", ".cjs", ".py", ".dll":
	default:
		return false
	}

	rc, err := f.Open()
	if err != nil {
		return false
	}
	defer rc.Close()

	raw, err := io.ReadAll(io.LimitReader(rc, maxScanBytes))
	if err != nil {
		return false
	}

	if ext == ".dll" {
		for _, m := range compiledTriggerMarkers() {
			if bytes.Contains(raw, m) {
				return true
			}
		}

		return false
	}

	return codeFirstTriggerRE.Match(raw)
}

// hasCodeFirstTrigger reports whether application code registers a non-HTTP trigger (Node v4, Python v2 or a
// compiled .NET isolated app); such functions are not in any function.json.
func hasCodeFirstTrigger(zipData []byte) bool {
	zr, err := zip.NewReader(bytes.NewReader(zipData), int64(len(zipData)))
	if err != nil {
		return false
	}

	return slices.ContainsFunc(zr.File, fileHasCodeFirstTrigger)
}
