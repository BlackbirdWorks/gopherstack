<script module lang="ts">
	// The set of services this dashboard can genuinely report a live
	// connection for today. Every AWS API call gopherstack handles — for
	// every service, not just these five — passes through the same global
	// HTTP middleware (`logger.APIConsoleMiddleware`, wired once in
	// `cli.go` via `e.Use(...)`) that feeds the Connect-RPC `StreamConsole`
	// server-stream already consumed by the dashboard's home page and
	// `/console` (see `dashboard/connect_server.go`'s `StreamConsole` and
	// `ui/src/routes/console/+page.svelte`). That stream is real, and any
	// of these five services' traffic would appear on it — so reporting
	// "connected" for one of them is an honest statement about an actually
	// observable capability, not a fabricated signal.
	//
	// This list is intentionally scoped to the pages this batch actually
	// wired up (see url-state.svelte.ts's page batch), not every service
	// StreamConsole could technically cover — grey is the honest default
	// for everything else until a page actually reads the resulting state.
	// Extending this to another page is a one-line addition here.
	const LIVE_SERVICES = ['s3', 'dynamodb', 'lambda', 'ec2', 'cloudwatchlogs'] as const;

	export type LiveDotService = (typeof LIVE_SERVICES)[number];

	export function hasLiveSupport(service: string | undefined): service is LiveDotService {
		return service !== undefined && (LIVE_SERVICES as readonly string[]).includes(service);
	}
</script>

<script lang="ts">
	import { dashboardClient } from '$lib/api/connect-client';

	type Props = {
		/**
		 * The service the current page is viewing, e.g. `'s3'`. Omitted (or
		 * not one of `LIVE_SERVICES`) renders grey — live updates not
		 * implemented for this service. This is the honest default: a page
		 * that hasn't opted in never shows a green dot that means nothing.
		 */
		service?: string;
	};

	let { service }: Props = $props();

	type ConnState = 'connecting' | 'live' | 'error';
	let conn = $state<ConnState>('connecting');

	// Mirrors the connection lifecycle already used by the console and
	// dashboard home pages (`isConnected` in routes/console/+page.svelte,
	// routes/+page.svelte): `conn` starts 'connecting', flips to 'live' the
	// moment the server stream call resolves (the server has genuinely
	// accepted a live connection — CapturedRequest frames for THIS page's
	// service, if any occur, would arrive over this exact channel with no
	// further filtering needed), and flips to 'error' if the stream ever
	// rejects or drops for a reason other than this effect tearing itself
	// down on unmount/service change.
	$effect(() => {
		if (!hasLiveSupport(service)) {
			conn = 'connecting';
			return;
		}

		const controller = new AbortController();
		let torndown = false;
		conn = 'connecting';

		(async () => {
			try {
				const stream = await dashboardClient.streamConsole(
					{},
					{ signal: controller.signal }
				);
				if (torndown) return;
				conn = 'live';
				for await (const response of stream) {
					if (torndown) return;
					if (response) conn = 'live';
				}
			} catch (err: unknown) {
				if (torndown) return;
				const e = err as Error;
				if (e?.name !== 'AbortError') {
					conn = 'error';
				}
			}
		})();

		return () => {
			torndown = true;
			controller.abort();
		};
	});

	const color = $derived(
		hasLiveSupport(service) ? (conn === 'error' ? 'red' : conn === 'live' ? 'green' : 'grey') : 'grey'
	);

	const DOT_CLASSES: Record<'grey' | 'red' | 'green', string> = {
		grey: 'bg-gray-300 dark:bg-gray-600',
		red: 'bg-red-500',
		green: 'bg-emerald-500'
	};

	const LABELS: Record<'grey' | 'red' | 'green', string> = {
		grey: 'Live updates not implemented for this service',
		red: 'Live updates disconnected',
		green: 'Live updates connected'
	};
</script>

<!--
	aria-hidden: a status dot living inside a page's <h1> (this component is
	used both standalone in bespoke page headers and via PageHeader.svelte)
	must not contribute to that heading's accessible name — several page
	test suites assert `getByRole('heading', { name: <exact title> })`, and
	an aria-label here would get folded into that computed name. `title`
	still gives mouse users a tooltip; screen-reader exposure of live-status
	is a known gap, not attempted here.
-->
<span
	class="inline-block w-2.5 h-2.5 rounded-full flex-shrink-0 {DOT_CLASSES[color]} {color === 'green'
		? 'animate-pulse'
		: ''}"
	title={LABELS[color]}
	aria-hidden="true"
	data-testid="live-dot"
	data-state={color}
></span>
