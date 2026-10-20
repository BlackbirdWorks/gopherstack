package jsruntime_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsruntime"
)

func newSandbox(t *testing.T, opts jsruntime.Options) *jsruntime.Sandbox {
	t.Helper()

	sb := jsruntime.New(opts)
	t.Cleanup(sb.Close)

	return sb
}

func TestSandbox_Run(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		src      string
		wantMsg  string
		wantKind jsruntime.Kind
		wantOK   bool
	}{
		{name: "arithmetic", src: `1 + 1`, wantOK: true},
		{
			name:     "require_absent",
			src:      `require('fs')`,
			wantKind: jsruntime.KindException,
			wantMsg:  "require is not defined",
		},
		{
			name:     "process_absent",
			src:      `process.env`,
			wantKind: jsruntime.KindException,
			wantMsg:  "process is not defined",
		},
		{
			name:     "module_absent",
			src:      `module.exports`,
			wantKind: jsruntime.KindException,
			wantMsg:  "module is not defined",
		},
		{
			name:     "fetch_absent",
			src:      `fetch('http://x')`,
			wantKind: jsruntime.KindException,
			wantMsg:  "fetch is not defined",
		},
		{
			name:     "xhr_absent",
			src:      `new XMLHttpRequest()`,
			wantKind: jsruntime.KindException,
			wantMsg:  "XMLHttpRequest is not defined",
		},
		{name: "infinite_loop", src: `while (true) {}`, wantKind: jsruntime.KindTimeout},
		{name: "recursion_bounded", src: `function f() { return f(); } f()`, wantKind: jsruntime.KindException},
		{name: "throw", src: `throw new Error('boom')`, wantKind: jsruntime.KindException, wantMsg: "boom"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sb := newSandbox(t, jsruntime.Options{Timeout: 100 * time.Millisecond})
			_, err := sb.Run(tt.src)

			if tt.wantOK {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)

			kind, msg := jsruntime.Classify(err)
			assert.Equal(t, tt.wantKind, kind)
			assert.Contains(t, msg, tt.wantMsg)
		})
	}
}

func TestSandbox_Console(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []string
		max  int
	}{
		{
			name: "levels",
			src:  `console.log('a'); console.info('b'); console.warn('c'); console.error('d'); console.debug('e')`,
			want: []string{"a", "b", "c", "d", "e"},
		},
		{name: "objects_as_json", src: `console.log('x', {a: [1, 2]}, 3)`, want: []string{`x {"a":[1,2]} 3`}},
		{
			name: "capped",
			src:  `for (var i = 0; i < 10; i++) { console.log(i); }`,
			max:  3,
			want: []string{"0", "1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sb := newSandbox(t, jsruntime.Options{Console: true, MaxLogLines: tt.max})
			_, err := sb.Run(tt.src)
			require.NoError(t, err)
			assert.Equal(t, tt.want, sb.Logs())
		})
	}

	t.Run("absent_by_default", func(t *testing.T) {
		t.Parallel()

		sb := newSandbox(t, jsruntime.Options{})
		_, err := sb.Run(`console.log(1)`)
		require.Error(t, err)
	})
}

func TestSandbox_JSON(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t, jsruntime.Options{})

	v, err := sb.ToJS(map[string]any{"a": []any{1, "x"}})
	require.NoError(t, err)

	got, err := sb.FromJS(v)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"a": []any{float64(1), "x"}}, got)

	_, err = sb.ToJS(make(chan int))
	require.ErrorIs(t, err, jsruntime.ErrNotJSON)

	undef, err := sb.Run(`undefined`)
	require.NoError(t, err)

	nilVal, err := sb.FromJS(undef)
	require.NoError(t, err)
	assert.Nil(t, nilVal)
}

func TestSandbox_Call(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t, jsruntime.Options{})
	_, err := sb.Run(`function add(a, b) { return a + b; }`)
	require.NoError(t, err)

	arg, err := sb.ToJS(2)
	require.NoError(t, err)

	out, err := sb.Call("add", arg, arg)
	require.NoError(t, err)
	assert.EqualValues(t, 4, out.ToInteger())

	_, err = sb.Call("missing")
	require.ErrorIs(t, err, jsruntime.ErrNoFunction)
}

func TestRewriteModule(t *testing.T) {
	t.Parallel()

	mods := map[string]string{"m": "__m"}

	tests := []struct {
		name    string
		code    string
		want    string
		wantErr bool
	}{
		{name: "named", code: `import { a, b as c } from 'm';`, want: `var {a,b: c} = __m;`},
		{name: "namespace", code: `import * as ns from 'm';`, want: `var ns = __m;`},
		{name: "default", code: `import d from 'm';`, want: `var d = __m;`},
		{name: "export_fn", code: `export function f() {}`, want: `function f() {}`},
		{name: "export_list", code: `const x = 1; export { x };`, want: `const x = 1; `},
		{name: "unknown_module", code: `import x from 'fs';`, wantErr: true},
		{name: "default_export", code: `export default 1;`, wantErr: true},
		{name: "bad_form", code: `import a, { b } from 'm';`, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := jsruntime.RewriteModule(tt.code, mods)
			if tt.wantErr {
				require.ErrorIs(t, err, jsruntime.ErrModule)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
