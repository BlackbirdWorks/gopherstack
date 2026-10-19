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
	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/menu"
	"github.com/wailsapp/wails/v2/pkg/menu/keys"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/runtime"
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
	ctx      context.Context
	cancel   context.CancelFunc
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

func (d *desktop) startup(ctx context.Context) {
	d.ctx = ctx

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

			runtime.WindowExecJS(ctx, `window.location.replace(`+strconv.Quote(d.url())+`)`)
		}
	}()
}

func (d *desktop) fail(err error) {
	_, _ = runtime.MessageDialog(d.ctx, runtime.MessageDialogOptions{
		Type:    runtime.ErrorDialog,
		Title:   "Gopherstack",
		Message: "Failed to start the Gopherstack server: " + err.Error(),
	})
	runtime.Quit(d.ctx)
}

func (d *desktop) shutdown(context.Context) {
	if d.cancel == nil {
		return
	}

	d.cancel()

	select {
	case <-d.stopped:
	case <-time.After(shutdownWait):
	}
}

func (d *desktop) menu() *menu.Menu {
	m := menu.NewMenu()

	file := m.AddSubmenu("Gopherstack")
	file.AddText("Reload", keys.CmdOrCtrl("r"), func(*menu.CallbackData) {
		runtime.WindowReloadApp(d.ctx)
	})
	file.AddText("Open in Browser", keys.CmdOrCtrl("b"), func(*menu.CallbackData) {
		runtime.BrowserOpenURL(d.ctx, d.url())
	})
	file.AddSeparator()
	file.AddText("Quit", keys.CmdOrCtrl("q"), func(*menu.CallbackData) {
		runtime.Quit(d.ctx)
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

	err = wails.Run(&options.App{
		Title:       "Gopherstack",
		Width:       windowWidth,
		Height:      windowHeight,
		AssetServer: &assetserver.Options{Assets: loadingFS()},
		Menu:        d.menu(),
		OnStartup:   d.startup,
		OnDomReady:  func(context.Context) { d.domOnce.Do(func() { close(d.domReady) }) },
		OnShutdown:  d.shutdown,
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		os.Exit(1)
	}
}
