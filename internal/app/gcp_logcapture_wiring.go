package app

import (
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	gcpfunctionsbackend "github.com/blackbirdworks/gopherstack/services/gcpfunctions"
	gcploggingbackend "github.com/blackbirdworks/gopherstack/services/gcplogging"
	gcprunbackend "github.com/blackbirdworks/gopherstack/services/gcprun"
)

// wireGCPLogCapture sends Cloud Run and Cloud Functions container output and request logs to Cloud Logging.
func wireGCPLogCapture(byName map[string]service.Registerable) {
	logH, ok := byName["GCPLogging"].(*gcploggingbackend.Handler)
	if !ok {
		return
	}

	if run, isRun := byName["GCPRun"].(*gcprunbackend.Handler); isRun {
		run.Backend.SetLogSink(logH.Backend)
	}

	if fns, isFn := byName["GCPFunctions"].(*gcpfunctionsbackend.Handler); isFn {
		fns.Backend.SetLogSink(logH.Backend)
	}
}
