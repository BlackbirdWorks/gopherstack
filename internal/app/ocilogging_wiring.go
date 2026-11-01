package app

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ociidentitybackend "github.com/blackbirdworks/gopherstack/services/ociidentity"
	ocilogging "github.com/blackbirdworks/gopherstack/services/ocilogging"
	ocimonitoring "github.com/blackbirdworks/gopherstack/services/ocimonitoring"
	ocinotificationsbackend "github.com/blackbirdworks/gopherstack/services/ocinotifications"
)

const (
	notificationsEndpoint = "http://localhost"
	messageTypeJSON       = "JSON"
	functionsLogService   = "functions"
	functionsLogCategory  = "invoke"
	functionsEventType    = "com.oraclecloud.functions.invoke"
)

// GetOCILoggingSettings returns OCI Logging settings (ocilogging.ConfigProvider).
func (c *CLI) GetOCILoggingSettings() ocilogging.Settings { return c.OCILogging }

// GetOCIMonitoringSettings returns OCI Monitoring settings (ocimonitoring.ConfigProvider).
func (c *CLI) GetOCIMonitoringSettings() ocimonitoring.Settings { return c.OCIMonitoring }

// wireOCILoggingMonitoring links OCI Logging and Monitoring to the other OCI services: API keys from OCI
// Identity, function invocation logs into SERVICE logs, and alarm notifications into OCI Notifications.
func wireOCILoggingMonitoring(byName map[string]service.Registerable) {
	logH, _ := byName["OCILogging"].(*ocilogging.Handler)
	monH, _ := byName["OCIMonitoring"].(*ocimonitoring.Handler)

	if identityH, ok := byName["OCIIdentity"].(*ociidentitybackend.Handler); ok {
		if logH != nil {
			logH.Backend.Auth.Link(identityH.Backend.Auth)
		}

		if monH != nil {
			monH.Backend.Auth.Link(identityH.Backend.Auth)
		}
	}

	if fnH, ok := byName["OCIFunctions"].(*ocifunctionsbackend.Handler); ok && logH != nil {
		fnH.Backend.SetInvokeObserver(functionLogObserver(logH.Backend))
	}

	if notifyH, ok := byName["OCINotifications"].(*ocinotificationsbackend.Handler); ok && monH != nil {
		monH.Backend.SetPublisher(alarmPublisher(notifyH.Backend))
		monH.Backend.SetTopicChecker(func(id string) bool {
			_, err := notifyH.Backend.GetTopic(id, notificationsEndpoint)

			return err == nil
		})
	}
}

// functionLogObserver writes each invocation to the SERVICE log enabled for the function's application.
func functionLogObserver(b *ocilogging.InMemoryBackend) ocifunctionsbackend.InvokeObserver {
	return func(ev ocifunctionsbackend.InvokeEvent) {
		data := map[string]any{
			keyMessageField: "function invocation finished", "applicationId": ev.AppID, "applicationName": ev.AppName,
			"functionId": ev.FunctionID, "functionName": ev.FunctionName, "callId": ev.CallID,
			lifecycleCmdStatus: ev.Status, "durationMs": ev.Duration.Milliseconds(),
		}
		if ev.ErrorCode != "" {
			data["errorCode"], data["errorMessage"] = ev.ErrorCode, ev.ErrorMessage
		}

		b.EmitServiceLog(functionsLogService, ev.AppID, functionsLogCategory, ocilogging.ServiceEvent{
			Time: ev.Time, Type: functionsEventType, Subject: ev.FunctionID, Data: data,
		})
	}
}

// alarmPublisher publishes alarm notifications to OCI Notifications topics.
func alarmPublisher(b *ocinotificationsbackend.InMemoryBackend) ocimonitoring.Publisher {
	return func(_ context.Context, topicID, title, body string, asJSON bool) error {
		msgType := ""
		if asJSON {
			msgType = messageTypeJSON
		}

		_, err := b.PublishMessage(topicID, msgType, ocinotificationsbackend.PublishIn{Title: &title, Body: &body})

		return err
	}
}
