package extensions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

const helperEnv = "GOPHERSTACK_EXT_TEST_HELPER"

// TestMain doubles as the extension binary: with helperEnv set the test binary acts as one.
func TestMain(m *testing.M) {
	if os.Getenv(helperEnv) != "" {
		runHelper()

		return
	}

	testleak.VerifyTestMain(m)
}

func appendLog(line string) {
	p := os.Getenv("HELPER_LOG")
	if p == "" {
		return
	}

	f, err := os.OpenFile(p, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}

	defer f.Close()

	fmt.Fprintln(f, line)
}

func runHelper() {
	port := os.Getenv("GOPHERSTACK_EXTENSION_PORT")
	mux := http.NewServeMux()
	mux.HandleFunc("/hello", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "hello from %s %s", os.Getenv("GOPHERSTACK_EXTENSION_NAME"), r.URL.RawQuery)
	})
	mux.HandleFunc("/health", func(http.ResponseWriter, *http.Request) {})
	mux.HandleFunc("/hooks/", func(_ http.ResponseWriter, r *http.Request) {
		appendLog(r.URL.Path)
	})

	l, err := net.Listen("tcp", "127.0.0.1:"+port)
	if err != nil {
		os.Exit(2)
	}

	go func() { _ = http.Serve(l, mux) }()

	_, _ = os.Stdout.WriteString("token is " + os.Getenv("GOPHERSTACK_EXTENSION_TOKEN") + "\n")

	reg := os.Getenv("HELPER_REG")
	req, _ := http.NewRequest(
		http.MethodPost,
		os.Getenv("GOPHERSTACK_CONTROL_URL")+"/_gopherstack/extensions/register",
		bytes.NewBufferString(reg),
	)
	req.Header.Set("Authorization", "Bearer "+os.Getenv("GOPHERSTACK_EXTENSION_TOKEN"))

	resp, err := (&http.Client{Transport: httpclient.NewTransport()}).Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		os.Exit(3)
	}

	_ = resp.Body.Close()

	if os.Getenv("HELPER_CRASH") != "" {
		os.Exit(4)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM, os.Interrupt)
	<-sig
	appendLog("sigterm")
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}

	return string(b)
}
