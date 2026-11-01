package cosmosdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// docOp is a document write with the triggers requested for it.
type docOp struct {
	body    map[string]any
	kind    string
	pk      string
	id      string
	ifMatch string
	pre     []string
	post    []string
}

// docOpResult is the outcome of a triggered document write.
type docOpResult struct {
	body    any
	logs    []string
	info    DocumentInfo
	hasBody bool
	created bool
	deleted bool
}

// runTriggered runs a create/upsert/replace/delete with its pre- and post-triggers in one transaction.
func (b *InMemoryBackend) runTriggered(dbID, collID string, op docOp) (docOpResult, error) {
	b.mu.Lock("runTriggered")
	defer b.mu.Unlock()

	tx, err := b.beginTx(dbID, collID)
	if err != nil {
		return docOpResult{}, err
	}

	res, err := tx.runDocOp(op)
	if err != nil {
		tx.rollback()
	}

	return res, err
}

func (tx *containerTx) runDocOp(op docOp) (docOpResult, error) {
	scope, err := tx.opScope(op)
	if err != nil {
		return docOpResult{}, err
	}

	trigOp := tx.triggerOperation(op, scope)

	pre, err := tx.resolveTriggers(op.pre, triggerPre, trigOp)
	if err != nil {
		return docOpResult{}, err
	}

	post, err := tx.resolveTriggers(op.post, triggerPost, trigOp)
	if err != nil {
		return docOpResult{}, err
	}

	vm, stop := tx.b.newSandbox()
	defer stop()

	env := newScriptEnv(vm, tx, scope)

	body, err := env.runPreTriggers(pre, op.body)
	if err != nil {
		return docOpResult{}, err
	}

	res, err := tx.applyDocOp(op, scope, body)
	if err != nil {
		return docOpResult{}, err
	}

	err = env.runPostTriggers(post, &res)
	res.logs = env.logs

	return res, err
}

// opScope is the partition the operation's triggers are confined to.
func (tx *containerTx) opScope(op docOp) (string, error) {
	if op.kind == batchCreate || op.kind == batchUpsert {
		return partitionKeyForBody(op.body, tx.c.paths())
	}

	return op.pk, nil
}

// triggerOperation names the operation for trigger matching; an upsert is a Create or a Replace by existence.
func (tx *containerTx) triggerOperation(op docOp, scope string) string {
	switch op.kind {
	case batchCreate:
		return trigOpCreate
	case batchReplace, batchPatch:
		return trigOpReplace
	case batchDelete:
		return trigOpDelete
	}

	id, _ := op.body["id"].(string)
	if _, exists := tx.c.lookup(documentCompositeKey{PartitionKeyJSON: scope, ID: id}, tx.now); exists {
		return trigOpReplace
	}

	return trigOpCreate
}

func triggerMatches(triggerOp, op string) bool {
	return triggerOp == trigOpAll || triggerOp == op || (triggerOp == trigOpUpdate && op == trigOpReplace)
}

func (tx *containerTx) resolveTriggers(names []string, typ, op string) ([]*storedScript, error) {
	out := make([]*storedScript, 0, len(names))

	for _, name := range names {
		s, ok := tx.c.scriptMap(kindTrigger, false)[name]
		if !ok {
			return nil, fmt.Errorf("%w: trigger %q does not exist", ErrInvalidTrigger, name)
		}

		if s.TriggerType != typ || !triggerMatches(s.TriggerOperation, op) {
			return nil, fmt.Errorf("%w: trigger %q is a %s %s trigger, not a %s trigger for %s",
				ErrInvalidTrigger, name, s.TriggerType, s.TriggerOperation, typ, op)
		}

		out = append(out, s)
	}

	return out, nil
}

func (tx *containerTx) applyDocOp(op docOp, scope string, body map[string]any) (docOpResult, error) {
	var (
		res docOpResult
		err error
	)

	switch op.kind {
	case batchCreate, batchUpsert:
		var existed bool

		res.info, existed, err = tx.createDoc(scope, body, op.kind == batchUpsert)
		res.created = !existed
	case batchReplace:
		res.info, err = tx.replaceDoc(op.pk, op.id, body, op.ifMatch)
	case batchPatch:
		res.info, err = tx.patchWithBody(op, body)
	case batchDelete:
		res.deleted = true
		err = tx.deleteDoc(op.pk, op.id, op.ifMatch)
	}

	return res, err
}

// runPreTriggers threads the request body through each pre-trigger; a trigger may replace it.
func (e *scriptEnv) runPreTriggers(pre []*storedScript, body map[string]any) (map[string]any, error) {
	if len(pre) == 0 || body == nil {
		return body, e.runAll(pre)
	}

	e.reqObj = toJS(e.vm, body)

	if err := e.runAll(pre); err != nil {
		return nil, err
	}

	out, ok := jsonValue(e.reqObj).(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%w: a pre-trigger must leave the request body a JSON object", ErrScriptFailed)
	}

	return out, nil
}

func (e *scriptEnv) runAll(triggers []*storedScript) error {
	for _, s := range triggers {
		if err := e.runTrigger(s); err != nil {
			return err
		}
	}

	return nil
}

// runPostTriggers runs post-triggers against the stored result; a trigger may replace the response body.
func (e *scriptEnv) runPostTriggers(post []*storedScript, res *docOpResult) error {
	if len(post) == 0 {
		return nil
	}

	if !res.deleted {
		e.respObj = toJS(e.vm, documentAsMap(res.info))
	}

	if err := e.runAll(post); err != nil {
		return err
	}

	if e.respSet {
		res.body, res.hasBody = jsonValue(e.respObj), true
	}

	return nil
}

// triggerNames splits a comma-separated trigger-include header into names.
func triggerNames(header string) []string {
	var out []string

	for n := range strings.SplitSeq(header, ",") {
		if n = strings.TrimSpace(n); n != "" {
			out = append(out, n)
		}
	}

	return out
}

// patchWithBody applies a patch whose request body (the operations, after any pre-trigger edit) is body.
func (tx *containerTx) patchWithBody(op docOp, body map[string]any) (DocumentInfo, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return DocumentInfo{}, invalidPatchf("the patch body is not valid JSON")
	}

	req, err := parsePatchRequest(raw)
	if err != nil {
		return DocumentInfo{}, err
	}

	return tx.patchDoc(op.pk, op.id, req, op.ifMatch)
}
