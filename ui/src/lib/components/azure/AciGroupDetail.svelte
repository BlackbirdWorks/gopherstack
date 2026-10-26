<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { ACI_DEFAULT_TAIL, containerLogs, type AciContainer, type AciEvent, type AciGroup, type AciState } from '#lib/azureaci.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { subscription: string; group: AciGroup };
	let { subscription, group }: Props = $props();

	let tails = $state<Record<string, number>>({});
	let logs = $state<Record<string, string>>({});
	let logErrors = $state<Record<string, string>>({});
	let logBusy = $state<Record<string, boolean>>({});

	async function fetchLogs(c: AciContainer) {
		logBusy[c.name] = true;
		try {
			logs[c.name] = await containerLogs(subscription, group, c.name, tails[c.name] ?? ACI_DEFAULT_TAIL);
			logErrors[c.name] = '';
		} catch (e) {
			logErrors[c.name] = describeGcpError(e);
		} finally {
			logBusy[c.name] = false;
		}
	}

	const cell = 'px-2 py-1';
	const stateText = (s: AciState | null) =>
		s ? [s.state, s.detail, s.exitCode === null ? '' : `exit code ${s.exitCode}`, s.started ? `started ${s.started}` : '', s.finished ? `finished ${s.finished}` : ''].filter(Boolean).join(' · ') : '—';
</script>

{#snippet events(list: AciEvent[], testId: string)}
	{#if list.length === 0}
		<p class="text-gray-500 dark:text-gray-400">No events</p>
	{:else}
		<table class="w-full text-xs" data-testid={testId}>
			<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Type</th><th class={cell}>Reason</th><th class={cell}>Count</th><th class={cell}>Last seen</th><th class={cell}>Message</th></tr></thead>
			<tbody>
				{#each list as ev, i (i)}
					<tr data-testid="{testId}-row"><td class={cell}>{ev.type}</td><td class={cell}>{ev.name}</td><td class={cell}>{ev.count}</td><td class="{cell} whitespace-nowrap">{ev.last}</td><td class="{cell} break-all">{ev.message}</td></tr>
				{/each}
			</tbody>
		</table>
	{/if}
{/snippet}

<div class="space-y-4 text-xs" data-testid="aci-detail">
	<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
		<dt class="text-gray-500 dark:text-gray-400">Resource group</dt><dd class="font-mono">{group.resourceGroup}</dd>
		<dt class="text-gray-500 dark:text-gray-400">OS / location</dt><dd>{group.osType} · {group.location}</dd>
		<dt class="text-gray-500 dark:text-gray-400">IP address</dt><dd class="font-mono" data-testid="aci-ip">{group.ip ? `${group.ip} (${group.ipType})` : 'none'}</dd>
		<dt class="text-gray-500 dark:text-gray-400">FQDN</dt><dd class="font-mono">{group.fqdn || '—'}</dd>
		<dt class="text-gray-500 dark:text-gray-400">Ports</dt><dd>{group.ports.join(', ') || '—'}</dd>
		<dt class="text-gray-500 dark:text-gray-400">Image registries</dt><dd data-testid="aci-registries">{group.registries.join(', ') || '—'} <span class="text-gray-400">(passwords are never shown)</span></dd>
		<dt class="text-gray-500 dark:text-gray-400">Tags</dt><dd>{Object.entries(group.tags).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}</dd>
	</dl>

	{#each group.containers as c (c.name)}
		<section class="rounded-md border border-gray-200 dark:border-gray-700 p-3 space-y-3" data-testid="aci-container">
			<div class="flex flex-wrap items-baseline gap-x-4">
				<h3 class="text-sm font-semibold font-mono">{c.name}</h3>
				<span class="font-mono">{c.image}</span>
				<span data-testid="aci-restarts">restarts: {c.restartCount}</span>
				<span>{c.cpu} CPU · {c.memoryGb} GB</span>
			</div>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1">
				<dt class="text-gray-500 dark:text-gray-400">Current state</dt><dd data-testid="aci-current">{stateText(c.current)}</dd>
				<dt class="text-gray-500 dark:text-gray-400">Previous state</dt><dd data-testid="aci-previous">{stateText(c.previous)}</dd>
				<dt class="text-gray-500 dark:text-gray-400">Command</dt><dd class="font-mono break-all">{c.command.join(' ') || '—'}</dd>
				<dt class="text-gray-500 dark:text-gray-400">Ports</dt><dd>{c.ports.join(', ') || '—'}</dd>
			</dl>
			<div>
				<h4 class="font-medium mb-1">Environment variables</h4>
				{#if c.env.length === 0}
					<p class="text-gray-500 dark:text-gray-400">None</p>
				{:else}
					<table class="w-full" data-testid="aci-env">
						<tbody>
							{#each c.env as v (v.name)}
								<tr data-testid="aci-env-row"><td class="{cell} font-mono w-1/3">{v.name}</td><td class="{cell} font-mono break-all">{#if v.secure}<span class="italic text-gray-500" data-testid="aci-env-secure">secure value (hidden)</span>{:else}{v.value}{/if}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</div>
			<div>
				<h4 class="font-medium mb-1">Events</h4>
				{@render events(c.events, 'aci-events')}
			</div>
			<div class="space-y-2">
				<div class="flex flex-wrap items-center gap-2">
					<h4 class="font-medium">Logs</h4>
					<label class="flex items-center gap-1 text-gray-500 dark:text-gray-400" for="aci-tail-{c.name}">Tail
						<input id="aci-tail-{c.name}" type="number" min="1" max="10000" class="{GCP_INPUT} !w-20 !py-1 text-xs" value={tails[c.name] ?? ACI_DEFAULT_TAIL} oninput={(e) => (tails[c.name] = Number(e.currentTarget.value) || ACI_DEFAULT_TAIL)} data-testid="aci-tail" />
					</label>
					<button class={GCP_MINI} onclick={() => fetchLogs(c)} disabled={logBusy[c.name]} data-testid="aci-logs-refresh">
						<RefreshCw class="inline h-3 w-3 {logBusy[c.name] ? 'animate-spin' : ''}" /> {c.name in logs ? 'Refresh' : 'Load logs'}
					</button>
				</div>
				{#if logErrors[c.name]}
					<p class="text-red-600" role="alert" data-testid="aci-logs-error">{logErrors[c.name]}</p>
				{:else if c.name in logs}
					<pre class="max-h-72 overflow-auto whitespace-pre-wrap break-all rounded-md bg-slate-900 p-2 font-mono text-slate-100" data-testid="aci-logs">{logs[c.name] || '(no output)'}</pre>
				{/if}
			</div>
		</section>
	{/each}

	<div>
		<h4 class="font-medium mb-1">Container group events</h4>
		{@render events(group.events, 'aci-group-events')}
	</div>
</div>
