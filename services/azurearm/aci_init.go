package azurearm

import (
	"fmt"
)

func initAsContainer(ic aciInitWire) aciContainerWire {
	return aciContainerWire{Name: ic.Name, Properties: aciContainerPropsWire{
		Image: ic.Properties.Image, Command: ic.Properties.Command,
		EnvironmentVariables: ic.Properties.EnvironmentVariables, VolumeMounts: ic.Properties.VolumeMounts,
	}}
}

// initThenApps runs the init containers one after another to completion, then starts the app containers. A
// terminal init failure is handed to awaitStart through the app containers' start error.
func (e *ACIEngine) initThenApps(run *aciRun, cfg aciConfig) {
	for i, c := range run.inits {
		if err := e.runInit(run, cfg, initAsContainer(cfg.Props.InitContainers[i]), c); err != nil {
			e.mu.Lock("initThenApps")

			for _, app := range run.conts {
				app.startErr = fmt.Errorf("init container %s: %w", c.name, err)
				close(app.ready)
			}
			e.mu.Unlock()

			return
		}
	}

	e.startApps(run, cfg)
}

// runInit runs one init container to a zero exit. A non-zero exit is retried with the platform back-off unless
// the group's restart policy is Never (Learn, init containers: Always and OnFailure map to OnFailure).
func (e *ACIEngine) runInit(run *aciRun, cfg aciConfig, wire aciContainerWire, c *aciCont) error {
	for {
		id, err := e.launch(run, cfg, wire, c)
		if err != nil {
			e.mu.Lock("runInit")
			c.cur = ACIState{State: aciStateWaiting, Detail: err.Error()}
			e.record(&c.events, aciEventWarning, aciStateFailed, err.Error())
			e.mu.Unlock()

			return err
		}

		e.mu.Lock("runInit")
		c.id = id
		now := e.now().UTC()
		c.cur = ACIState{State: aciStateRunning, StartTime: &now}
		e.mu.Unlock()

		code, werr := run.rt.Wait(run.ctx, id)
		if werr != nil {
			e.noteWaitFailure(run, c, werr)

			return fmt.Errorf("wait: %w", werr)
		}

		if e.afterInitExit(run, cfg, c, code) {
			return nil
		}

		if cfg.Props.RestartPolicy == aciRestartNever {
			return fmt.Errorf("%w with exit code %d", errACIInitFailed, code)
		}

		e.removeContainer(run, c, id)

		if !e.sleep(run.ctx, e.backoffFor(c)) {
			return run.ctx.Err()
		}
	}
}

// afterInitExit records the exit and reports success; on failure under a restarting policy it also arms the
// restart bookkeeping and releases awaitStart.
func (e *ACIEngine) afterInitExit(run *aciRun, cfg aciConfig, c *aciCont, code int) bool {
	now := e.now().UTC()

	e.mu.Lock("afterInitExit")
	defer e.mu.Unlock()

	done := ACIState{State: aciStateTerminated, FinishTime: &now, ExitCode: &code, Detail: cmpStateCompleted}
	if c.cur.StartTime != nil {
		done.StartTime = c.cur.StartTime
	}

	if code == 0 {
		c.cur = done

		return true
	}

	done.Detail = "Error"
	c.cur = done

	if cfg.Props.RestartPolicy == aciRestartNever {
		return false
	}

	c.restarts++
	p := done
	c.prev = &p
	c.cur = ACIState{State: aciStateWaiting, Detail: "CrashLoopBackOff: Back-off restarting failed container"}
	e.record(&c.events, aciEventWarning, "BackOff", "back-off restarting init container "+c.name)
	run.retryOnce.Do(func() { close(run.initRetry) })

	return false
}
