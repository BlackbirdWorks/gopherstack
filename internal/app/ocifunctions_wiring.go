package app

import (
	"context"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ociartifacts"
	ocicorebackend "github.com/blackbirdworks/gopherstack/services/ocicore"
	ocifunctionsbackend "github.com/blackbirdworks/gopherstack/services/ocifunctions"
	ocilogging "github.com/blackbirdworks/gopherstack/services/ocilogging"
	ocinotificationsbackend "github.com/blackbirdworks/gopherstack/services/ocinotifications"
	ociqueuebackend "github.com/blackbirdworks/gopherstack/services/ociqueue"
	ocistreaming "github.com/blackbirdworks/gopherstack/services/ocistreaming"
	ocivaultbackend "github.com/blackbirdworks/gopherstack/services/ocivault"
)

const (
	functionsLogLineType = "com.oraclecloud.functions.log"
	lineFormatJSON       = "JSON"
)

// wireOCIFunctionsLinks resolves application networking against OCI Core, function images and image policy
// signatures against OCI Artifacts and OCI Vault, detached results into Streaming, Queue and Notifications, and
// function output into OCI Logging.
func wireOCIFunctionsLinks(byName map[string]service.Registerable) {
	fnH, ok := byName["OCIFunctions"].(*ocifunctionsbackend.Handler)
	if !ok {
		return
	}

	if coreH, found := byName["OCICore"].(*ocicorebackend.Handler); found {
		fnH.Backend.SetNetwork(ocifunctionsbackend.Network{
			Subnet: func(id string) (string, bool) {
				ref, live := coreH.Backend.LookupSubnet(id)

				return ref.VcnID, live
			},
			NSG: coreH.Backend.LookupNSG,
		})
	}

	if artH, found := byName["OCIArtifacts"].(*ociartifacts.Handler); found {
		vaultH, _ := byName["OCIVault"].(*ocivaultbackend.Handler)
		fnH.Backend.SetImages(functionImages(artH.Backend, vaultH))
	}

	fnH.Backend.SetDestinations(functionDestinations(byName))

	if logH, found := byName["OCILogging"].(*ocilogging.Handler); found {
		fnH.Backend.SetLogSink(functionOutputSink(logH.Backend))
	}
}

func functionImages(art *ociartifacts.InMemoryBackend, vault *ocivaultbackend.Handler) ocifunctionsbackend.Images {
	return ocifunctionsbackend.Images{
		Resolve: func(ref string) (ocifunctionsbackend.ImageInfo, bool, bool) {
			img, found, owned := art.ResolveImage(ref)

			return ocifunctionsbackend.ImageInfo{ID: img.ID, Digest: img.Digest}, found, owned
		},
		Verified: func(img ocifunctionsbackend.ImageInfo, keyIDs []string) bool {
			if vault == nil {
				return false
			}

			for _, s := range art.ImageSignatures(img.ID) {
				if !slices.Contains(keyIDs, s.KeyID) {
					continue
				}

				res, err := vault.Backend.Verify("", ocivaultbackend.VerifyIn{
					KeyID: s.KeyID, KeyVersionID: s.KeyVersionID, Signature: s.Signature, Message: s.Message,
					Algorithm: s.Algorithm,
				})
				if err == nil && res.IsSignatureValid {
					return true
				}
			}

			return false
		},
	}
}

func functionDestinations(byName map[string]service.Registerable) ocifunctionsbackend.Destinations {
	streamH, _ := byName["OCIStreaming"].(*ocistreaming.Handler)
	queueH, _ := byName["OCIQueue"].(*ociqueuebackend.Handler)
	notifyH, _ := byName["OCINotifications"].(*ocinotificationsbackend.Handler)

	var d ocifunctionsbackend.Destinations

	if streamH != nil {
		d.Stream = func(_ context.Context, id string, v []byte) error { return streamH.Backend.AppendMessage(id, nil, v) }
	}

	if queueH != nil {
		d.Queue = func(_ context.Context, id, channel, content string) error {
			return queueH.Backend.AppendMessage(id, channel, content)
		}
	}

	if notifyH != nil {
		d.Notify = func(ctx context.Context, id, title, body string) error {
			_, err := notifyH.Backend.PublishMessage(
				ctx,
				id,
				"",
				ocinotificationsbackend.PublishIn{Title: &title, Body: &body},
			)

			return err
		}
	}

	d.Exists = func(kind, id string) bool {
		switch kind {
		case "STREAM":
			return streamH == nil || streamH.Backend.StreamActive(id)
		case "QUEUE":
			return queueH == nil || queueH.Backend.QueueActive(id)
		case "NOTIFICATION":
			if notifyH == nil {
				return true
			}

			_, err := notifyH.Backend.GetTopic(id, notificationsEndpoint)

			return err == nil
		}

		return true
	}

	return d
}

// functionOutputSink writes each stdout or stderr line to the SERVICE log enabled for the function's application.
func functionOutputSink(b *ocilogging.InMemoryBackend) func(ocifunctionsbackend.LogLine) int {
	return func(l ocifunctionsbackend.LogLine) int {
		data := map[string]any{keyMessageField: l.Text}
		if l.LineFormat == lineFormatJSON {
			data["stream"], data["applicationId"], data["functionId"], data["callId"] = l.Stream, l.AppID, l.FunctionID, l.CallID
		}

		return b.EmitServiceLog(functionsLogService, l.AppID, functionsLogCategory, ocilogging.ServiceEvent{
			Time: l.Time, Type: functionsLogLineType, Subject: l.FunctionID, Data: data,
		})
	}
}
