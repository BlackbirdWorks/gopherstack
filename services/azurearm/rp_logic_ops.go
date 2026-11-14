package azurearm

import (
	"encoding/json/v2"
	"sort"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

type logicRef3 struct {
	sub string
	rg  string
	wf  string
}

// read runs f on the workflow under the read lock.
func (p *LogicProvider) read(r logicRef3, f func(w *logicWorkflow) (any, error)) (any, error) {
	p.mu.RLock("read")
	defer p.mu.RUnlock()

	w, err := p.findWorkflow(r.sub, r.rg, r.wf)
	if err != nil {
		return nil, err
	}

	return f(w)
}

// write runs f on the workflow under the write lock.
func (p *LogicProvider) write(r logicRef3, f func(w *logicWorkflow) (any, error)) (any, error) {
	p.mu.Lock("write")
	defer p.mu.Unlock()

	w, err := p.findWorkflow(r.sub, r.rg, r.wf)
	if err != nil {
		return nil, err
	}

	return f(w)
}

// setState enables or disables a workflow.
func (p *LogicProvider) setState(r logicRef3, state string) error {
	_, err := p.write(r, func(w *logicWorkflow) (any, error) {
		w.State, w.Changed = state, p.clock.Now().UTC()
		p.armWorkflowLocked(w)

		return nil, nil //nolint:nilnil // void result
	})

	return err
}

func (p *LogicProvider) regenerateKey(r logicRef3, keyType string) error {
	var secondary bool

	switch strings.ToLower(keyType) {
	case logicKeyPrimary:
	case "secondary":
		secondary = true
	default:
		return logicBadRequest(
			"InvalidRequestContent",
			"The key type '%s' is not valid; use Primary or Secondary.",
			keyType,
		)
	}

	_, err := p.write(r, func(w *logicWorkflow) (any, error) {
		if secondary {
			w.KeySecondary = randomLogicToken()
		} else {
			w.KeyPrimary = randomLogicToken()
		}

		return nil, nil //nolint:nilnil // void result
	})

	return err
}

// requestTriggerName returns the workflow's request trigger ("manual" preferred).
func (w *logicWorkflow) requestTriggerName() (string, bool) {
	defs, _ := objectOf(w.Definition[logicSegTriggers])
	names := make([]string, 0, len(defs))

	for n, d := range defs {
		if typeOf(d) == logicTrigRequest || typeOf(d) == logicTrigManual {
			names = append(names, n)
		}
	}

	if len(names) == 0 {
		return "", false
	}

	sort.Strings(names)

	for _, n := range names {
		if n == logicTrigManual {
			return n, true
		}
	}

	return names[0], true
}

func (p *LogicProvider) callbackFor(r logicRef3, trigger, base string, body map[string]any) (any, error) {
	keyType := stringOf(body["keyType"])
	if !validCallbackKeyType(keyType) {
		return nil, logicBadRequest("InvalidRequestContent", "The key type '%s' is not valid.", keyType)
	}

	var notAfter *time.Time

	if s := stringOf(body["notAfter"]); s != "" {
		ts, err := parseLogicTime(s)
		if err != nil {
			return nil, logicBadRequest("InvalidRequestContent", "The notAfter value '%s' is not a valid timestamp.", s)
		}

		notAfter = &ts.t
	}

	return p.read(r, func(w *logicWorkflow) (any, error) {
		name := trigger

		if name == "" {
			n, ok := w.requestTriggerName()
			if !ok {
				return nil, logicBadRequest("TriggerNotFound", "The workflow '%s' has no request trigger.", w.Name)
			}

			name = n
		}

		def := w.triggerDef(name)
		if t := typeOf(def); t != logicTrigRequest && t != logicTrigManual {
			return nil, logicBadRequest("InvalidTriggerType",
				"The trigger '%s' is not a request trigger and has no callback URL.", name)
		}

		if _, ok := w.Triggers[name]; !ok {
			return nil, ErrLogicTriggerNotFound
		}

		return w.callbackURL(base, name, keyType, notAfter), nil
	})
}

func (p *LogicProvider) triggerSchema(r logicRef3, trigger string) (any, error) {
	return p.read(r, func(w *logicWorkflow) (any, error) {
		if _, ok := w.Triggers[trigger]; !ok {
			return nil, ErrLogicTriggerNotFound
		}

		def := w.triggerDef(trigger)
		if t := typeOf(def); t != logicTrigRequest && t != logicTrigManual {
			return nil, logicBadRequest("InvalidTriggerType", "The trigger '%s' does not have a JSON schema.", trigger)
		}

		in, _ := objectOf(def["inputs"])

		schema := in["schema"]
		if schema == nil {
			schema = map[string]any{}
		}

		raw, err := json.Marshal(schema, jsonx.V1())
		if err != nil {
			return nil, err
		}

		return map[string]any{"title": trigger, logicKContent: string(raw)}, nil
	})
}

// setTriggerState copies the enabled/disabled state of the source trigger onto the target trigger.
func (p *LogicProvider) setTriggerState(r logicRef3, trigger string, body map[string]any) error {
	src, _ := objectOf(body["source"])
	srcID := stringOf(src["id"])

	if srcID == "" {
		return logicBadRequest("InvalidRequestContent", "The setState source id is required.")
	}

	id, err := ParseGenericResourcePath("/" + strings.TrimPrefix(srcID, "/"))
	if err != nil || len(id.Names) != 2 || !strings.EqualFold(id.Types[1], logicSegTriggers) {
		return logicBadRequest(
			"InvalidRequestContent",
			"The setState source id '%s' is not a workflow trigger id.",
			srcID,
		)
	}

	p.mu.Lock("setTriggerState")
	defer p.mu.Unlock()

	srcWf, err := p.findWorkflow(id.SubscriptionID, id.ResourceGroup, id.Names[0])
	if err != nil {
		return err
	}

	srcTrig, ok := srcWf.Triggers[id.Names[1]]
	if !ok {
		return ErrLogicTriggerNotFound
	}

	w, err := p.findWorkflow(r.sub, r.rg, r.wf)
	if err != nil {
		return err
	}

	ts, ok := w.Triggers[trigger]
	if !ok {
		return ErrLogicTriggerNotFound
	}

	ts.State, ts.Changed = srcTrig.State, p.clock.Now().UTC()
	p.armWorkflowLocked(w)

	return nil
}

func sortedTriggerNames(w *logicWorkflow) []string {
	names := make([]string, 0, len(w.Triggers))
	for n := range w.Triggers {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}

func (p *LogicProvider) validateWorkflow(name string, body map[string]any) error {
	if err := validateLogicWorkflowName(name); err != nil {
		return err
	}

	in, err := parseLogicPut(body, false)
	if err != nil {
		return err
	}

	return validateLogicDefinition(in.definition)
}
