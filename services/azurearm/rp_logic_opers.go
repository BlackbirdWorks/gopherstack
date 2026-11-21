package azurearm

import (
	"strings"
	"time"

	"github.com/google/uuid"
)

const logicMaxOperations = 1000

// logicOperation is a run operation: the id the service hands out for an action taken on a run (a resubmit or a
// cancel), readable under workflows/{w}/runs/{r}/operations/{id}.
type logicOperation struct {
	Created time.Time `json:"created"`
	ID      string    `json:"id"`
	Run     string    `json:"run"`
	Kind    string    `json:"kind"`
}

func (p *LogicProvider) recordOperationLocked(w *logicWorkflow, run, kind string) string {
	op := &logicOperation{ID: uuid.NewString(), Run: run, Kind: kind, Created: p.clock.Now().UTC()}
	w.Operations = append(w.Operations, op)

	if len(w.Operations) > logicMaxOperations {
		w.Operations = w.Operations[len(w.Operations)-logicMaxOperations:]
	}

	return op.ID
}

func (w *logicWorkflow) operationLocation(base, run, op string) string {
	return base + w.armID() + "/runs/" + run + "/operations/" + op + "?" + logicKAPIVersion + "=" +
		logicAPIVersions()[0]
}

func (h *Handler) logicGetOperation(q logicReq) error {
	out, err := q.lp.read(q.ref, func(w *logicWorkflow) (any, error) {
		r, ok := findRun(w, q.rest[1])
		if !ok {
			return nil, ErrLogicRunNotFound
		}

		for _, op := range w.Operations {
			if op.Run == r.Name && strings.EqualFold(op.ID, q.rest[3]) {
				return w.runBody(r, q.base), nil
			}
		}

		return nil, ErrLogicOperationMissing
	})

	return h.logicReply(q, out, err)
}
