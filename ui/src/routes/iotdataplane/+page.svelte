<script lang="ts">
	import { onMount } from 'svelte';
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
	import { Radio, Send, Eye, Trash2, RefreshCw, Layers, MessageSquare, Wifi } from 'lucide-svelte';

	const client = getIoTDataPlaneClient();

	type Tab = 'publish' | 'shadows' | 'retained' | 'connections';
	let activeTab = $state<Tab>('publish');

	// --- Publish ---
	let pubTopic = $state('my/topic');
	let pubPayload = $state('{"hello":"world"}');
	let pubQos = $state(0);
	let pubRetain = $state(false);
	let pubBusy = $state(false);
	let pubResult = $state<string | null>(null);

	async function doPublish() {
		if (!pubTopic.trim()) { toast.error('Topic is required'); return; }
		pubBusy = true;
		pubResult = null;
		try {
			await client.send(new PublishCommand({
				topic: pubTopic.trim(),
				payload: new TextEncoder().encode(pubPayload),
				qos: pubQos,
				retain: pubRetain
			}));
			pubResult = 'Published successfully';
			toast.success(pubResult);
		} catch (e) {
			pubResult = `Error: ${e}`;
			toast.error(pubResult);
		} finally {
			pubBusy = false;
		}
	}

	// --- Shadows ---
	let shadowThing = $state('my-device');
	let shadowName = $state('');
	let shadowPayload = $state('{"state":{"desired":{"key":"value"}}}');
	let shadowBusy = $state(false);
	let shadowResult = $state<string | null>(null);
	let shadowList = $state<string[]>([]);

	async function getShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client.send(new GetThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = JSON.stringify(JSON.parse(text), null, 2);
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Get shadow failed: ${e}`);
		} finally {
			shadowBusy = false;
		}
	}

	async function updateShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client.send(new UpdateThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined,
				payload: new TextEncoder().encode(shadowPayload)
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = JSON.stringify(JSON.parse(text), null, 2);
			toast.success('Shadow updated');
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Update shadow failed: ${e}`);
		} finally {
			shadowBusy = false;
		}
	}

	async function deleteShadow() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client.send(new DeleteThingShadowCommand({
				thingName: shadowThing.trim(),
				shadowName: shadowName.trim() || undefined
			}));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '{}';
			shadowResult = JSON.stringify(JSON.parse(text), null, 2);
			toast.success('Shadow deleted');
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`Delete shadow failed: ${e}`);
		} finally {
			shadowBusy = false;
		}
	}

	async function listShadows() {
		if (!shadowThing.trim()) { toast.error('Thing name required'); return; }
		shadowBusy = true; shadowResult = null;
		try {
			const res = await client.send(new ListNamedShadowsForThingCommand({
				thingName: shadowThing.trim()
			}));
			shadowList = res.results ?? [];
			shadowResult = shadowList.length > 0
				? `Named shadows: ${shadowList.join(', ')}`
				: 'No named shadows';
		} catch (e) {
			shadowResult = `Error: ${e}`;
			toast.error(`List shadows failed: ${e}`);
		} finally {
			shadowBusy = false;
		}
	}

	// --- Retained Messages ---
	type RetainedSummary = { topic: string; payloadSize?: number; qos?: number; lastModifiedTime?: number };
	let retainedMessages = $state<RetainedSummary[]>([]);
	let retainedBusy = $state(false);
	let retainedTopic = $state('');
	let retainedDetail = $state<string | null>(null);

	async function listRetained() {
		retainedBusy = true; retainedDetail = null;
		try {
			const res = await client.send(new ListRetainedMessagesCommand({}));
			retainedMessages = (res.retainedTopics ?? []).map((t) => ({
				topic: t.topic ?? '',
				payloadSize: t.payloadSize ? Number(t.payloadSize) : undefined,
				qos: t.qos,
				lastModifiedTime: t.lastModifiedTime ? Number(t.lastModifiedTime) : undefined
			}));
		} catch (e) {
			toast.error(`List retained failed: ${e}`);
		} finally {
			retainedBusy = false;
		}
	}

	async function getRetained(topic: string) {
		retainedBusy = true; retainedDetail = null;
		retainedTopic = topic;
		try {
			const res = await client.send(new GetRetainedMessageCommand({ topic }));
			const text = res.payload ? new TextDecoder().decode(res.payload) : '';
			retainedDetail = `Topic: ${res.topic}\nQoS: ${res.qos}\nPayload: ${text}`;
		} catch (e) {
			retainedDetail = `Error: ${e}`;
			toast.error(`Get retained failed: ${e}`);
		} finally {
			retainedBusy = false;
		}
	}

	onMount(() => listRetained());

	// --- Connections ---
	let connClientId = $state('');
	let connBusy = $state(false);
	let connResult = $state<string | null>(null);

	async function deleteConnection() {
		if (!connClientId.trim()) { toast.error('Client ID required'); return; }
		connBusy = true; connResult = null;
		try {
			await client.send(new DeleteConnectionCommand({ clientId: connClientId.trim() }));
			connResult = `Connection "${connClientId.trim()}" deleted`;
			toast.success(connResult);
			connClientId = '';
		} catch (e) {
			connResult = `Error: ${e}`;
			toast.error(`Delete connection failed: ${e}`);
		} finally {
			connBusy = false;
		}
	}

	const tabs: { id: Tab; label: string }[] = [
		{ id: 'publish', label: 'Publish' },
		{ id: 'shadows', label: 'Thing Shadows' },
		{ id: 'retained', label: 'Retained Messages' },
		{ id: 'connections', label: 'Connections' }
	];
</script>

<div class="space-y-6">
	<div class="flex items-center gap-3">
		<Wifi class="h-8 w-8 text-teal-600" />
		<div>
			<h1 class="text-2xl font-bold">IoT Data Plane</h1>
			<p class="text-sm text-muted-foreground">Publish messages and manage thing shadows</p>
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
			</button>
		{/each}
	</div>

	<!-- Publish Tab -->
	{#if activeTab === 'publish'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center gap-2">
				<Send class="h-5 w-5 text-teal-600" />
				<h2 class="font-semibold">Publish Message</h2>
			</div>
			<div class="grid gap-4 sm:grid-cols-2">
				<div class="sm:col-span-2">
					<label for="pub-topic" class="block text-sm font-medium mb-1">Topic *</label>
					<input
						id="pub-topic"
						type="text"
						bind:value={pubTopic}
						placeholder="my/device/topic"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="sm:col-span-2">
					<label for="pub-payload" class="block text-sm font-medium mb-1">Payload</label>
					<textarea
						id="pub-payload"
						bind:value={pubPayload}
						rows={4}
						placeholder="json payload"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					></textarea>
				</div>
				<div>
					<label for="pub-qos" class="block text-sm font-medium mb-1">QoS</label>
					<select
						id="pub-qos"
						bind:value={pubQos}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value={0}>0 — At most once</option>
						<option value={1}>1 — At least once</option>
					</select>
				</div>
				<div class="flex items-center gap-2 pt-6">
					<input id="pub-retain" type="checkbox" bind:checked={pubRetain} class="h-4 w-4" />
					<label for="pub-retain" class="text-sm font-medium">Retain message</label>
				</div>
			</div>
			<button
				onclick={doPublish}
				disabled={pubBusy}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
			>
				{#if pubBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Send class="h-4 w-4" />{/if}
				Publish
			</button>
			{#if pubResult}
				<pre class="rounded-md bg-muted p-3 text-xs">{pubResult}</pre>
			{/if}
		</div>
	{/if}

	<!-- Shadows Tab -->
	{#if activeTab === 'shadows'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center gap-2">
				<Layers class="h-5 w-5 text-teal-600" />
				<h2 class="font-semibold">Thing Shadow Operations</h2>
			</div>
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
					<label for="shadow-name" class="block text-sm font-medium mb-1">Shadow Name (leave blank for classic)</label>
					<input
						id="shadow-name"
						type="text"
						bind:value={shadowName}
						placeholder="optional-named-shadow"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="sm:col-span-2">
					<label for="shadow-payload" class="block text-sm font-medium mb-1">Payload (for Update)</label>
					<textarea
						id="shadow-payload"
						bind:value={shadowPayload}
						rows={4}
						placeholder="shadow document json"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					></textarea>
				</div>
			</div>
			<div class="flex flex-wrap gap-2">
				<button
					onclick={getShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					{#if shadowBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Eye class="h-4 w-4" />{/if}
					Get Shadow
				</button>
				<button
					onclick={updateShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					Update Shadow
				</button>
				<button
					onclick={deleteShadow}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border border-red-300 px-3 py-2 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
				>
					<Trash2 class="h-4 w-4" />
					Delete Shadow
				</button>
				<button
					onclick={listShadows}
					disabled={shadowBusy}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					List Named Shadows
				</button>
			</div>
			{#if shadowResult}
				<pre class="rounded-md bg-muted p-3 text-xs overflow-auto max-h-64">{shadowResult}</pre>
			{/if}
			{#if shadowList.length > 0}
				<div class="flex flex-wrap gap-2">
					{#each shadowList as name}
						<button
							onclick={() => { shadowName = name; getShadow(); }}
							class="rounded bg-muted px-2 py-1 text-xs hover:bg-accent"
						>
							{name}
						</button>
					{/each}
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

			{#if retainedMessages.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<MessageSquare class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">No retained messages</p>
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
							{#each retainedMessages as msg}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{msg.topic}</td>
									<td class="px-4 py-3">{msg.qos ?? 0}</td>
									<td class="px-4 py-3 text-muted-foreground">{msg.payloadSize ?? 0}B</td>
									<td class="px-4 py-3 text-xs text-muted-foreground">
										{msg.lastModifiedTime
											? new Date(msg.lastModifiedTime).toLocaleString()
											: '—'}
									</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={() => getRetained(msg.topic)}
											class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
											title="View payload"
										>
											<Eye class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}

			{#if retainedDetail}
				<pre class="rounded-md bg-muted p-3 text-xs overflow-auto max-h-48">{retainedDetail}</pre>
			{/if}
		</div>
	{/if}

	<!-- Connections Tab -->
	{#if activeTab === 'connections'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center gap-2">
				<Radio class="h-5 w-5 text-teal-600" />
				<h2 class="font-semibold">Delete MQTT Connection</h2>
			</div>
			<p class="text-sm text-muted-foreground">
				Force-disconnect a client by ID. Equivalent to AWS IoT DeleteConnection.
			</p>
			<div>
				<label for="conn-client-id" class="block text-sm font-medium mb-1">Client ID *</label>
				<input
					id="conn-client-id"
					type="text"
					bind:value={connClientId}
					placeholder="my-device-client-id"
					class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={deleteConnection}
				disabled={connBusy || !connClientId.trim()}
				class="flex items-center gap-2 rounded-md border border-red-300 px-3 py-2 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50"
			>
				{#if connBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Trash2 class="h-4 w-4" />{/if}
				Delete Connection
			</button>
			{#if connResult}
				<pre class="rounded-md bg-muted p-3 text-xs">{connResult}</pre>
			{/if}
		</div>
	{/if}
</div>
