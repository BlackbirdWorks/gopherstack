// Command gopherstack-desktop runs the Gopherstack server in-process and
// shows its dashboard in a native webview window.
package main

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/internal/app"
	"github.com/wailsapp/wails/v3/pkg/application"
	"github.com/wailsapp/wails/v3/pkg/events"
)

//go:embed loading
var loading embed.FS

var errServerExited = errors.New("server stopped unexpectedly")

const (
	windowWidth  = 1360
	windowHeight = 860
	shutdownWait = 10 * time.Second
)

type desktop struct {
	app      *application.App
	window   *application.WebviewWindow
	cancel   func()
	domReady chan struct{}
	errs     chan error
	stopped  chan struct{}
	args     []string
	port     int
	domOnce  sync.Once
}

func (d *desktop) url() string {
	return "http://127.0.0.1:" + strconv.Itoa(d.port) + "/dashboard"
}

func (d *desktop) startup() {
	srvCtx, cancel := context.WithCancel(context.Background())
	d.cancel = cancel

	go func() {
		d.errs <- app.RunServer(srvCtx, d.args)
		close(d.stopped)
	}()

	go func() {
		healthErr := make(chan error, 1)

		go func() { healthErr <- waitHealthy(srvCtx, d.port) }()

		select {
		case err := <-d.errs:
			if err == nil {
				err = errServerExited
			}

			d.fail(fmt.Errorf("server exited: %w", err))
		case err := <-healthErr:
			if err != nil {
				d.fail(err)

				return
			}

			select {
			case <-d.domReady:
			case <-srvCtx.Done():
				return
			}

			d.window.SetURL(d.url())
		}
	}()
}

func (d *desktop) fail(err error) {
	d.app.Dialog.Error().
		SetTitle("Gopherstack").
		SetMessage("Failed to start the Gopherstack server: " + err.Error()).
		Show()
	d.app.Quit()
}

func (d *desktop) shutdown() {
	if d.cancel == nil {
		return
	}

	d.cancel()

	select {
	case <-d.stopped:
	case <-time.After(shutdownWait):
	}
}

func (d *desktop) menu() *application.Menu {
	m := d.app.NewMenu()

	file := m.AddSubmenu("Gopherstack")
	file.Add("Reload").SetAccelerator("CmdOrCtrl+R").OnClick(func(*application.Context) {
		d.window.Reload()
	})
	file.Add("Open in Browser").SetAccelerator("CmdOrCtrl+B").OnClick(func(*application.Context) {
		_ = d.app.Browser.OpenURL(d.url())
	})
	file.AddSeparator()
	file.Add("Quit").SetAccelerator("CmdOrCtrl+Q").OnClick(func(*application.Context) {
		d.app.Quit()
	})

	return m
}

func main() {
	port, args, err := resolvePort(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}

	d := &desktop{
		port:     port,
		args:     args,
		domReady: make(chan struct{}),
		errs:     make(chan error, 1),
		stopped:  make(chan struct{}),
	}

	d.app = application.New(application.Options{
		Name:        "Gopherstack",
		Description: "AWS emulator with a native dashboard window",
		Assets:      application.AssetOptions{Handler: application.BundledAssetFileServer(loadingFS())},
		OnShutdown:  d.shutdown,
	})

	d.app.Menu.Set(d.menu())

	d.window = d.app.Window.NewWithOptions(application.WebviewWindowOptions{
		Title:              "Gopherstack",
		Width:              windowWidth,
		Height:             windowHeight,
		UseApplicationMenu: true,
	})
	d.window.OnWindowEvent(events.Common.WindowRuntimeReady, func(*application.WindowEvent) {
		d.domOnce.Do(func() { close(d.domReady) })
	})

	d.app.Event.OnApplicationEvent(events.Common.ApplicationStarted, func(*application.ApplicationEvent) {
		d.startup()
	})

	if err = d.app.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
