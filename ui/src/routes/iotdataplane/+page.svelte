<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getIoTDataPlaneClient } from '$lib/aws-client';
	import {
		PublishCommand,
		GetThingShadowCommand,
		UpdateThingShadowCommand,
		DeleteThingShadowCommand,
		ListNamedShadowsForThingCommand,
		GetRetainedMessageCommand,
		ListRetainedMessagesCommand,
		DeleteConnectionCommand
	} from '@aws-sdk/client-iot-data-plane';
	import { toast } from 'svelte-sonner';
	import {
		Radio,
		Send,
		Eye,
		Trash2,
		RefreshCw,
		Layers,
		MessageSquare,
		Wifi,
		Copy,
		Search,
		Plus,
		Clock,
		ChevronRight
	} from 'lucide-svelte';

	const client = regionalClient(getIoTDataPlaneClient);

	type Tab = 'publish' | 'shadows' | 'retained' | 'connections';
	let activeTab = $state<Tab>('publish');

	// Shared operation history log
	type HistoryEntry = { op: string; detail: string; ts: number; ok: boolean };
	let history = $state<HistoryEntry[]>([]);

	function logOp(op: string, detail: string, ok: boolean) {
		history = [{ op, detail, ts: Date.now(), ok }, ...history.slice(0, 49)];
	}

	function relativeTime(ts: number): string {
		const diff = Math.floor((Date.now() - ts) / 1000);
		if (diff < 60) return `${diff}s ago`;
		if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
		return `${Math.floor(diff / 3600)}h ago`;
	}

	async function copyText(text: string, label = 'Copied') {
		await navigator.clipboard.writeText(text);
		toast.success(label);
	}

	function tryPrettyJson(raw: string): string {
		try {
			return JSON.stringify(JSON.parse(raw), null, 2);
		} catch {
			return raw;
		}
	}

	function formatJson(raw: string): string {
		return tryPrettyJson(raw);
	}

	// ---- Publish tab ----
	let pubTopic = $state('my/device/telemetry');
	let pubPayload = $state('{"temperature": 22.5, "humidity": 60}');
	let pubQos = $state(0);
	let pubRetain = $state(false);
	let pubBusy = $state(false);
	let pubResult = $state<string | null>(null);
	let prettyPubPayload = $state(true);

	const pubFormatted = $derived(prettyPubPayload ? tryPrettyJson(pubPayload) : pubPayload);

	async function doPublish() {
		if (!pubTopic.trim()) { toast.error('Topic is required'); return; }
		pubBusy = true;
		pubResult = null;
		try {
			await client().send(new PublishCommand({
				topic: pubTopic.trim(),
				payload: new TextEncoder().encode(pubPayload),
				qos: pubQos,
				retain: pubRetain
			}));
			pubResult = `Published at ${new Date().toLocaleTimeString()}`;
			toast.success('Published');
			logOp('Publish', pubTopic.trim(), true);
		} catch (e) {
			pubResult = `Error: ${e}`;
			toast.error(`Publish failed: ${e}`);
			logOp('Publish', `${pubTopic}: ${e}`, false);
		} finally {
			pubBusy = false;
		}
	}

	function applyTemplate(payload: string) {
		pubPayload = payload;
	}

	const pubTemplates = [
		{ label: 'Telemetry', payload: '{"temperature": 22.5, "humidity": 60, "ts": 0}' },
		{ label: 'Alert', payload: '{"level": "WARN", "code": "TEMP_HIGH", "value": 85}' },
		{ label: 'Status', payload: '{"online": true, "battery": 92, "rssi": -65}' }
	];

	// ---- Shadow tab ----
	let shadowThing = $state('my-device');
	let shadowName = $state('');
	let shadowPayload = $state('{"state":{"desired":{"power":"on","brightness":80}}}');
	let shadowBusy = $state(false);
	let shadowResult = $state<string | null>(null);
	let shadowList = $state<string[]>([]);
	let thingsList = $state<string[]>([]);
	let prettyResult = $state(true);
	let showThingsBrowser = $state(false);

	async function getShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client().send(new GetThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = prettyResult ? tryPrettyJson(text) : text;
			logOp('GetShadow', shadowThing.trim(), true);
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Get shadow failed: ${e}`);
			logOp('GetShadow', `${shadowThing}: ${e}`, false);
		} finally {
			shadowBusy = false;
		}
	}

	async function updateShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client().send(new UpdateThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined,
				payload: new TextEncoder().encode(shadowPayload)
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = prettyResult ? tryPrettyJson(text) : text;
			toast.success('Shadow updated');
			logOp('UpdateShadow', shadowThing.trim(), true);
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Update shadow failed: ${e}`);
			logOp('UpdateShadow', `${shadowThing}: ${e}`, false);
		} finally {
			shadowBusy = false;
		}
	}

	async function deleteShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client().send(new DeleteThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = prettyResult ? tryPrettyJson(text) : text;
			toast.success('Shadow deleted');
			logOp('DeleteShadow', shadowThing.trim(), true);
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Delete shadow failed: ${e}`);
			logOp('DeleteShadow', `${shadowThing}: ${e}`, false);
		} finally {
			shadowBusy = false;
		}
	}

	async function listShadows() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client().send(new ListNamedShadowsForThingCommand({
				thingName: shadowThing.trim()
			}));
			shadowList = res.results ?? [];
			shadowResult = shadowList.length > 0
				? `Named shadows: ${shadowList.join(', ')}`
				: 'No named shadows found';
			logOp('ListShadows', shadowThing.trim(), true);
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`List shadows failed: ${e}`);
			logOp('ListShadows', `${shadowThing}: ${e}`, false);
		} finally {
			shadowBusy = false;
		}
	}

	async function loadThingsBrowser() {
		try {
			const res = await fetch('/api/things/shadow/ListThingsWithShadows');
			if (res.ok) {
				const data = await res.json();
				thingsList = data.things ?? [];
			}
		} catch (_) {}
		showThingsBrowser = true;
	}

	function selectThing(thing: string) {
		shadowThing = thing;
		showThingsBrowser = false;
	}

	function formatShadowPayload() {
		shadowPayload = formatJson(shadowPayload);
	}

	function parseShadowSections(result: string): { desired?: object; reported?: object; delta?: object } | null {
		try {
			const parsed = JSON.parse(result);
			if (parsed?.state) {
				return {
					desired: parsed.state.desired,
					reported: parsed.state.reported,
					delta: parsed.state.delta
				};
			}
		} catch (_) {}
		return null;
	}

	const shadowSections = $derived(shadowResult ? parseShadowSections(shadowResult) : null);

	// ---- Retained Messages tab ----
	type RetainedSummary = { topic: string; payloadSize?: number; qos?: number; lastModifiedTime?: number };
	let retainedMessages = $state<RetainedSummary[]>([]);
	let retainedBusy = $state(false);
	let retainedDetail = $state<string | null>(null);
	let retainedFilter = $state('');
	let retainedRowBusy = $state<string | null>(null);

	const filteredRetained = $derived(
		retainedMessages.filter((m) => m.topic.toLowerCase().includes(retainedFilter.toLowerCase()))
	);

	async function listRetained() {
		retainedBusy = true; retainedDetail = null;
		try {
			const res = await client().send(new ListRetainedMessagesCommand({}));
			retainedMessages = (res.retainedTopics ?? []).map((t) => ({
				topic: t.topic ?? '',
				payloadSize: t.payloadSize ? Number(t.payloadSize) : undefined,
				qos: t.qos,
				lastModifiedTime: t.lastModifiedTime ? Number(t.lastModifiedTime) : undefined
			}));
			logOp('ListRetained', `${retainedMessages.length} messages`, true);
		} catch (e) {
			toast.error(`List retained failed: ${e}`);
			logOp('ListRetained', `${e}`, false);
		} finally {
			retainedBusy = false;
		}
	}

	async function getRetained(topic: string) {
		retainedRowBusy = topic;
		retainedDetail = null;
		try {
			const res = await client().send(new GetRetainedMessageCommand({ topic }));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '';
			const pretty = tryPrettyJson(text);
			retainedDetail = `Topic: ${res.topic}\nQoS: ${res.qos}\nPayload:\n${pretty}`;
			logOp('GetRetained', topic, true);
		} catch (e) {
			retainedDetail = `Error: ${e}`;
			toast.error(`Get retained failed: ${e}`);
			logOp('GetRetained', `${topic}: ${e}`, false);
		} finally {
			retainedRowBusy = null;
		}
	}

	async function clearRetained(topic: string) {
		retainedRowBusy = topic;
		try {
			// Publish empty payload with retain=true to clear the retained message.
			await client().send(new PublishCommand({
				topic,
				payload: new Uint8Array(0),
				retain: true
			}));
			toast.success(`Cleared retained: ${topic}`);
			logOp('ClearRetained', topic, true);
			await listRetained();
		} catch (e) {
			toast.error(`Clear retained failed: ${e}`);
			logOp('ClearRetained', `${topic}: ${e}`, false);
		} finally {
			retainedRowBusy = null;
		}
	}

	function qosBadgeClass(qos?: number): string {
		return qos === 1
			? 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300'
			: 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
	}

	// ---- Connections tab ----
	type ConnEntry = { clientId: string; sourceIp: string; connectedAt: number };
	let connections = $state<ConnEntry[]>([]);
	let connBusy = $state(false);
	let connClientId = $state('');
	let connRowBusy = $state<string | null>(null);

	async function listConnections() {
		connBusy = true;
		try {
			const res = await fetch('/connections');
			if (res.ok) {
				const data = await res.json();
				connections = data.connections ?? [];
			}
		} catch (e) {
			toast.error(`List connections failed: ${e}`);
		} finally {
			connBusy = false;
		}
	}

	async function registerConnection() {
		if (!connClientId.trim()) { toast.error('Client ID required'); return; }
		connRowBusy = connClientId.trim();
		try {
			const res = await fetch(`/connections/${encodeURIComponent(connClientId.trim())}`, { method: 'POST' });
			if (res.ok) {
				toast.success(`Registered: ${connClientId.trim()}`);
				logOp('RegisterConnection', connClientId.trim(), true);
				connClientId = '';
				await listConnections();
			} else {
				const body = await res.json();
				toast.error(`Register failed: ${body.error ?? res.status}`);
				logOp('RegisterConnection', `${connClientId}: ${body.error}`, false);
			}
		} catch (e) {
			toast.error(`Register failed: ${e}`);
			logOp('RegisterConnection', `${connClientId}: ${e}`, false);
		} finally {
			connRowBusy = null;
		}
	}

	async function deleteConnection(clientId: string) {
		connRowBusy = clientId;
		try {
			await client().send(new DeleteConnectionCommand({ clientId }));
			toast.success(`Disconnected: ${clientId}`);
			logOp('DeleteConnection', clientId, true);
			await listConnections();
		} catch (e) {
			toast.error(`Delete connection failed: ${e}`);
			logOp('DeleteConnection', `${clientId}: ${e}`, false);
		} finally {
			connRowBusy = null;
		}
	}

	function handleKeydown(e: KeyboardEvent, action: () => void) {
		if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
			e.preventDefault();
			action();
		}
	}

	onRegionChange(() => {
		listRetained();
		listConnections();
	});

	const tabs: { id: Tab; label: string }[] = [
		{ id: 'publish', label: 'Publish' },
		{ id: 'shadows', label: 'Thing Shadows' },
		{ id: 'retained', label: 'Retained Messages' },
		{ id: 'connections', label: 'Connections' }
	];
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Wifi class="h-8 w-8 text-teal-600" />
			<div>
				<h1 class="text-2xl font-bold">IoT Data Plane</h1>
				<p class="text-sm text-muted-foreground">Publish messages, manage shadows and retained messages</p>
			</div>
		</div>
		<div class="flex items-center gap-2 text-xs text-muted-foreground">
			<Clock class="h-3.5 w-3.5" />
			{history.length} operations logged
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each tabs as tab}
			<button
				onclick={() => (activeTab = tab.id)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id
					? 'border-primary text-primary'
					: 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
				{#if tab.id === 'retained' && retainedMessages.length > 0}
					<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{retainedMessages.length}</span>
				{/if}
				{#if tab.id === 'connections' && connections.length > 0}
					<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{connections.length}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- Publish Tab -->
	{#if activeTab === 'publish'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center gap-2">
				<Send class="h-5 w-5 text-teal-600" />
				<h2 class="font-semibold">Publish Message</h2>
				<span class="ml-auto text-xs text-muted-foreground">Ctrl+Enter to publish</span>
			</div>

			<!-- Topic input -->
			<div>
				<label for="pub-topic" class="block text-sm font-medium mb-1">Topic *</label>
				<input
					id="pub-topic"
					type="text"
					bind:value={pubTopic}
					placeholder="my/device/telemetry"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>

			<!-- Payload with templates -->
			<div>
				<div class="flex items-center justify-between mb-1">
					<label for="pub-payload" class="text-sm font-medium">Payload</label>
					<div class="flex items-center gap-2">
						<span class="text-xs text-muted-foreground">Templates:</span>
						{#each pubTemplates as tmpl}
							<button
								onclick={() => applyTemplate(tmpl.payload)}
								class="rounded border px-2 py-0.5 text-xs hover:bg-accent"
							>{tmpl.label}</button>
						{/each}
					</div>
				</div>
				<textarea
					id="pub-payload"
					bind:value={pubPayload}
					rows={5}
					placeholder="json payload"
					onkeydown={(e) => handleKeydown(e, doPublish)}
					class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				></textarea>
			</div>

			<!-- QoS + Retain -->
			<div class="flex flex-wrap gap-4 items-center">
				<div>
					<label for="pub-qos" class="block text-sm font-medium mb-1">QoS</label>
					<select
						id="pub-qos"
						bind:value={pubQos}
						class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value={0}>0 — At most once</option>
						<option value={1}>1 — At least once</option>
					</select>
				</div>
				<label class="flex items-center gap-2 pt-5 text-sm">
					<input type="checkbox" bind:checked={pubRetain} class="h-4 w-4 rounded" />
					Retain message
				</label>
			</div>

			<!-- Actions -->
			<div class="flex items-center gap-2">
				<button
					onclick={doPublish}
					disabled={pubBusy}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{#if pubBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Send class="h-4 w-4" />{/if}
					Publish
				</button>
				{#if pubResult}
					<button onclick={() => (pubResult = null)} class="text-xs text-muted-foreground hover:text-foreground">Clear</button>
				{/if}
			</div>

			{#if pubResult}
				<div class="flex items-start gap-2">
					<pre class="flex-1 rounded-md bg-muted p-3 text-xs">{pubResult}</pre>
					<button onclick={() => copyText(pubResult ?? '', 'Result copied')} class="mt-1 rounded p-1 hover:bg-accent">
						<Copy class="h-4 w-4 text-muted-foreground" />
					</button>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Shadows Tab -->
	{#if activeTab === 'shadows'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center gap-2">
				<Layers class="h-5 w-5 text-teal-600" />
				<h2 class="font-semibold">Thing Shadow Operations</h2>
				<button
					onclick={loadThingsBrowser}
					class="ml-auto flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent"
				>
					<ChevronRight class="h-3 w-3" />
					Browse Things
				</button>
			</div>

			<!-- Things browser overlay -->
			{#if showThingsBrowser}
				<div class="rounded-lg border bg-muted/30 p-3 space-y-2">
					<div class="flex items-center justify-between">
						<span class="text-sm font-medium">Things with Shadows</span>
						<button onclick={() => (showThingsBrowser = false)} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
					</div>
					{#if thingsList.length === 0}
						<p class="text-xs text-muted-foreground">No things with shadows found</p>
					{:else}
						<div class="flex flex-wrap gap-2">
							{#each thingsList as thing}
								<button
									onclick={() => selectThing(thing)}
									class="rounded bg-background border px-2 py-1 text-xs hover:bg-accent font-mono"
								>{thing}</button>
							{/each}
						</div>
					{/if}
				</div>
			{/if}

			<!-- Inputs -->
			<div class="grid gap-4 sm:grid-cols-2">
				<div>
					<label for="shadow-thing" class="block text-sm font-medium mb-1">Thing Name *</label>
					<input
						id="shadow-thing"
						type="text"
						bind:value={shadowThing}
						placeholder="my-device"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="shadow-name" class="block text-sm font-medium mb-1">Shadow Name (blank = classic)</label>
					<input
						id="shadow-name"
						type="text"
						bind:value={shadowName}
						placeholder="optional-named-shadow"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="sm:col-span-2">
					<div class="flex items-center justify-between mb-1">
						<label for="shadow-payload" class="text-sm font-medium">Payload (for Update)</label>
						<button onclick={formatShadowPayload} class="text-xs text-muted-foreground hover:text-foreground rounded border px-2 py-0.5">Format JSON</button>
					</div>
					<textarea
						id="shadow-payload"
						bind:value={shadowPayload}
						rows={4}
						placeholder="shadow document json"
						onkeydown={(e) => handleKeydown(e, updateShadow)}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					></textarea>
				</div>
			</div>

			<!-- Action buttons -->
			<div class="flex flex-wrap gap-2">
				<button
					onclick={getShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					{#if shadowBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Eye class="h-4 w-4" />{/if}
					Get
				</button>
				<button
					onclick={updateShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					Update
				</button>
				<button
					onclick={deleteShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border border-red-300 px-3 py-2 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
				>
					<Trash2 class="h-4 w-4" />
					Delete
				</button>
				<button
					onclick={listShadows}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					List Named
				</button>
				<div class="ml-auto flex items-center gap-2">
					<label class="flex items-center gap-1.5 text-xs text-muted-foreground">
						<input type="checkbox" bind:checked={prettyResult} class="h-3.5 w-3.5 rounded" />
						Pretty-print
					</label>
					{#if shadowResult}
						<button onclick={() => (shadowResult = null)} class="text-xs text-muted-foreground hover:text-foreground">Clear</button>
					{/if}
				</div>
			</div>

			<!-- Named shadow quick-select -->
			{#if shadowList.length > 0}
				<div class="flex flex-wrap gap-2">
					{#each shadowList as name}
						<button
							onclick={() => { shadowName = name; getShadow(); }}
							class="rounded bg-muted px-2 py-1 text-xs hover:bg-accent font-mono"
						>{name}</button>
					{/each}
				</div>
			{/if}

			<!-- Result + copy -->
			{#if shadowResult}
				<div class="space-y-2">
					<div class="flex items-start gap-2">
						<pre class="flex-1 rounded-md bg-muted p-3 text-xs overflow-auto max-h-64">{shadowResult}</pre>
						<button onclick={() => copyText(shadowResult ?? '', 'Shadow copied')} class="mt-1 rounded p-1 hover:bg-accent">
							<Copy class="h-4 w-4 text-muted-foreground" />
						</button>
					</div>

					<!-- State sections breakdown -->
					{#if shadowSections}
						<div class="grid gap-2 sm:grid-cols-3 text-xs">
							{#each [['Desired', shadowSections.desired], ['Reported', shadowSections.reported], ['Delta', shadowSections.delta]] as [label, section]}
								{#if section}
									<div class="rounded border p-2">
										<div class="font-medium mb-1 text-muted-foreground">{label}</div>
										<pre class="overflow-auto max-h-32">{JSON.stringify(section, null, 2)}</pre>
									</div>
								{/if}
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	<!-- Retained Messages Tab -->
	{#if activeTab === 'retained'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<MessageSquare class="h-5 w-5 text-teal-600" />
					<h2 class="font-semibold">Retained Messages</h2>
				</div>
				<button
					onclick={listRetained}
					disabled={retainedBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					{#if retainedBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<RefreshCw class="h-4 w-4" />{/if}
					Refresh
				</button>
			</div>

			<!-- Filter -->
			<div class="relative">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Filter by topic..."
					bind:value={retainedFilter}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>

			{#if filteredRetained.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<MessageSquare class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">{retainedMessages.length === 0 ? 'No retained messages' : 'No matches'}</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Topic</th>
								<th class="px-4 py-3 text-left font-medium">QoS</th>
								<th class="px-4 py-3 text-left font-medium">Size</th>
								<th class="px-4 py-3 text-left font-medium">Modified</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each filteredRetained as msg}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{msg.topic}</td>
									<td class="px-4 py-3">
										<span class="rounded-full px-2 py-0.5 text-xs font-medium {qosBadgeClass(msg.qos)}">
											QoS {msg.qos ?? 0}
										</span>
									</td>
									<td class="px-4 py-3 text-muted-foreground text-xs">{msg.payloadSize ?? 0}B</td>
									<td class="px-4 py-3 text-xs text-muted-foreground" title={msg.lastModifiedTime ? new Date(msg.lastModifiedTime).toLocaleString() : ''}>
										{msg.lastModifiedTime ? relativeTime(msg.lastModifiedTime) : '—'}
									</td>
									<td class="px-4 py-3">
										<div class="flex justify-end gap-1">
											<button
												onclick={() => getRetained(msg.topic)}
												disabled={retainedRowBusy === msg.topic}
												class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950 disabled:opacity-50"
												title="View payload"
											>
												{#if retainedRowBusy === msg.topic}
													<RefreshCw class="h-4 w-4 animate-spin" />
												{:else}
													<Eye class="h-4 w-4" />
												{/if}
											</button>
											<button
												onclick={() => clearRetained(msg.topic)}
												disabled={retainedRowBusy === msg.topic}
												class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
												title="Clear retained message"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}

			{#if retainedDetail}
				<div class="flex items-start gap-2">
					<pre class="flex-1 rounded-md bg-muted p-3 text-xs overflow-auto max-h-48">{retainedDetail}</pre>
					<button onclick={() => copyText(retainedDetail ?? '', 'Payload copied')} class="mt-1 rounded p-1 hover:bg-accent">
						<Copy class="h-4 w-4 text-muted-foreground" />
					</button>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Connections Tab -->
	{#if activeTab === 'connections'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<Radio class="h-5 w-5 text-teal-600" />
					<h2 class="font-semibold">MQTT Connections</h2>
				</div>
				<button
					onclick={listConnections}
					disabled={connBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					{#if connBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<RefreshCw class="h-4 w-4" />{/if}
					Refresh
				</button>
			</div>

			<!-- Register new connection -->
			<div class="rounded-md border bg-muted/30 p-4 space-y-3">
				<p class="text-sm font-medium">Register Test Connection</p>
				<div class="flex gap-2">
					<input
						type="text"
						bind:value={connClientId}
						placeholder="my-device-client-id"
						onkeydown={(e) => { if (e.key === 'Enter') registerConnection(); }}
						class="flex-1 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<button
						onclick={registerConnection}
						disabled={!connClientId.trim() || connRowBusy !== null}
						class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
					>
						<Plus class="h-4 w-4" />
						Register
					</button>
				</div>
			</div>

			<!-- Connection list -->
			{#if connections.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<Radio class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">No connections registered</p>
					<p class="text-xs">Register a test connection above or connect via MQTT</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Client ID</th>
								<th class="px-4 py-3 text-left font-medium">Source IP</th>
								<th class="px-4 py-3 text-left font-medium">Connected</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each connections as conn}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">
										<div class="flex items-center gap-1">
											{conn.clientId}
											<button onclick={() => copyText(conn.clientId, 'Client ID copied')} class="rounded p-0.5 hover:bg-accent opacity-50 hover:opacity-100">
												<Copy class="h-3 w-3" />
											</button>
										</div>
									</td>
									<td class="px-4 py-3 text-xs text-muted-foreground">{conn.sourceIp || '—'}</td>
									<td class="px-4 py-3 text-xs text-muted-foreground" title={new Date(conn.connectedAt * 1000).toLocaleString()}>
										{relativeTime(conn.connectedAt * 1000)}
									</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={() => deleteConnection(conn.clientId)}
											disabled={connRowBusy === conn.clientId}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
											title="Delete connection"
										>
											{#if connRowBusy === conn.clientId}
												<RefreshCw class="h-4 w-4 animate-spin" />
											{:else}
												<Trash2 class="h-4 w-4" />
											{/if}
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Operation History (always visible at bottom) -->
	{#if history.length > 0}
		<details class="rounded-lg border p-4">
			<summary class="cursor-pointer text-sm font-medium text-muted-foreground hover:text-foreground">
				Operation History ({history.length})
			</summary>
			<div class="mt-3 space-y-1 max-h-48 overflow-auto">
				{#each history as entry}
					<div class="flex items-center gap-2 text-xs {entry.ok ? 'text-foreground' : 'text-red-600 dark:text-red-400'}">
						<span class="w-2 h-2 rounded-full flex-shrink-0 {entry.ok ? 'bg-green-500' : 'bg-red-500'}"></span>
						<span class="font-medium w-28 flex-shrink-0">{entry.op}</span>
						<span class="flex-1 truncate font-mono">{entry.detail}</span>
						<span class="text-muted-foreground flex-shrink-0">{relativeTime(entry.ts)}</span>
					</div>
				{/each}
			</div>
		</details>
	{/if}
</div>
