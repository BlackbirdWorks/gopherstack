package cloudfront

import (
	"github.com/dop251/goja"
)

// originState records the origin helper calls made by a running function.
type originState struct {
	vm        *goja.Runtime
	cat       *originCatalog
	dir       *originDirective
	eventType string
}

func (s *originState) fail(err error) {
	panic(s.vm.NewGoError(err))
}

func (s *originState) paramsArg(v goja.Value, required bool) *originParams {
	if v == nil || goja.IsUndefined(v) || goja.IsNull(v) {
		if required {
			s.fail(fnError("params must be an object"))
		}

		return nil
	}

	m, ok := v.Export().(map[string]any)
	if !ok {
		s.fail(fnError("params must be an object"))
	}

	p, err := parseOriginParams(m)
	if err != nil {
		s.fail(err)
	}

	return p
}

func (s *originState) guard() {
	if s.eventType != "" && s.eventType != eventViewerRequest {
		s.fail(fnError("origin modification is only available in viewer-request functions"))
	}
}

func (s *originState) update(call goja.FunctionCall) goja.Value {
	s.guard()

	p := s.paramsArg(call.Argument(0), true)

	d := s.current()
	d.Params = p

	return goja.Undefined()
}

func (s *originState) selectByID(call goja.FunctionCall) goja.Value {
	s.guard()

	id, ok := call.Argument(0).Export().(string)
	if !ok || id == "" {
		s.fail(fnError("originId must be a non-empty string"))
	}

	if !s.cat.has(id) {
		s.fail(fnError("origin " + id + " does not exist in the distribution"))
	}

	p := s.paramsArg(call.Argument(1), false)
	s.dir = &originDirective{Selected: id, Params: p}

	return goja.Undefined()
}

func (s *originState) createGroup(call goja.FunctionCall) goja.Value {
	s.guard()

	m, ok := call.Argument(0).Export().(map[string]any)
	if !ok {
		s.fail(fnError("params must be an object"))
	}

	g, err := parseGroupSpec(m, s.cat)
	if err != nil {
		s.fail(err)
	}

	s.dir = &originDirective{Group: g}

	return goja.Undefined()
}

func (s *originState) current() *originDirective {
	if s.dir == nil || s.dir.Group != nil {
		s.dir = &originDirective{}
	}

	return s.dir
}
