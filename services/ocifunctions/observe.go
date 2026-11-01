package ocifunctions

import "time"

// InvokeEvent describes one finished function invocation.
type InvokeEvent struct {
	Time         time.Time
	AppID        string
	AppName      string
	FunctionID   string
	FunctionName string
	CallID       string
	ErrorCode    string
	ErrorMessage string
	Duration     time.Duration
	Status       int
}

// InvokeObserver receives every finished invocation, successful or not.
type InvokeObserver func(InvokeEvent)

// SetInvokeObserver registers the observer; nil removes it.
func (b *InMemoryBackend) SetInvokeObserver(fn InvokeObserver) {
	if fn == nil {
		b.observer.Store(nil)

		return
	}

	b.observer.Store(&fn)
}

func (b *InMemoryBackend) observe(p execPlan, start time.Time, callID string, status int, aerr *APIError) {
	fn := b.observer.Load()
	if fn == nil {
		return
	}

	now := b.nowUTC()
	ev := InvokeEvent{
		Time: now, AppID: p.appID, AppName: p.appName, FunctionID: p.fnID, FunctionName: p.fnName,
		CallID: callID, Status: status, Duration: now.Sub(start),
	}

	if aerr != nil {
		ev.Status, ev.ErrorCode, ev.ErrorMessage = aerr.Status, aerr.Code, aerr.Message
	}

	(*fn)(ev)
}
