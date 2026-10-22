<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { ScrollText, Send } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, GCP_INPUT, GCP_GHOST, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { readStoredProject, validateProjectId, writeStoredProject } from '#lib/gcpproject.ts';
	import {
		LOGGING_PROJECT_KEY,
		SEVERITIES,
		listEntries,
		listSinks,
		severityTone,
		validateLogId,
		writeEntry,
		type LogEntry,
		type LogSink
	} from '#lib/gcplogging.ts';

	const TABS = [
		{ id: 'entries', label: 'Log entries' },
		{ id: 'sinks', label: 'Sinks' }
	];

	let project = $state(readStoredProject(LOGGING_PROJECT_KEY));
	let projectInput = $state(readStoredProject(LOGGING_PROJECT_KEY));
	let tab = $state('entries');
	let filter = $state('');
	let entries = $state<LogEntry[]>([]);
	let sinks = $state<LogSink[]>([]);
	let loading = $state(true);
	let loadError = $state('');
	let queryError = $state('');
	let wLog = $state('gopherstack-test');
	let wSeverity = $state('INFO');
	let wText = $state('hello from the dashboard');

	const projectError = $derived(validateProjectId(projectInput.trim()));
	const writeError = $derived(validateLogId(wLog) || (wText ? '' : 'Message is required'));

	async function load() {
		loading = true;
		loadError = '';
		try {
			[entries, sinks] = await Promise.all([listEntries(project, filter.trim()), listSinks(project)]);
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Cloud Logging: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function runQuery() {
		queryError = '';
		try {
			entries = await listEntries(project, filter.trim());
		} catch (e) {
			queryError = describeGcpError(e);
		}
	}

	async function applyProject(e: Event) {
		e.preventDefault();
		if (projectError) return;
		project = projectInput.trim();
		writeStoredProject(LOGGING_PROJECT_KEY, project);
		await load();
	}

	async function write(e: Event) {
		e.preventDefault();
		if (writeError) return;
		if (await attempt(() => writeEntry(project, wLog, wSeverity, wText), 'Log entry written', 'Failed to write entry')) await load();
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="logging-dashboard">
	<PageHeader icon={ScrollText} title="Cloud Logging" description="Google Cloud Logging: log entries and sinks" color="green" onRefresh={load} />

	<form class="flex flex-wrap items-end gap-2" onsubmit={applyProject}>
		<label class="text-sm">Project<input class={GCP_INPUT} bind:value={projectInput} data-testid="logging-project" /></label>
		<button type="submit" class={GCP_GHOST} disabled={Boolean(projectError)}>Apply</button>
		{#if projectError}<span class="text-sm text-red-600">{projectError}</span>{/if}
	</form>

	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="green" />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="logging-error">
			<p class="font-medium">Could not reach Cloud Logging</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if tab === 'entries'}
		<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => { e.preventDefault(); void runQuery(); }}>
			<label class="grow text-sm">Filter
				<input class="{GCP_INPUT} font-mono" bind:value={filter} placeholder={'severity>=ERROR AND textPayload:"timeout"'} data-testid="logging-filter" />
			</label>
			<button type="submit" class={GCP_PRIMARY} data-testid="logging-run">Run query</button>
		</form>

		{#if queryError}<p class="text-sm text-red-600" role="alert" data-testid="logging-query-error">{queryError}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="logging-entries">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Time</th><th class="px-4 py-2">Severity</th><th class="px-4 py-2">Log</th><th class="px-4 py-2">Message</th></tr></thead>
				<tbody>
					{#each entries as en (en.insertId + en.timestamp + en.logName)}
						<tr class="border-t border-gray-100 align-top dark:border-gray-800" data-testid="logging-entry">
							<td class="whitespace-nowrap px-4 py-2 font-mono text-xs">{en.timestamp}</td>
							<td class="px-4 py-2"><span class="rounded px-2 py-0.5 text-xs {severityTone(en.severity)}">{en.severity}</span></td>
							<td class="px-4 py-2">{en.logName}</td>
							<td class="px-4 py-2 font-mono break-all">{en.message}</td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="4">No log entries match.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		<form class="flex flex-wrap items-end gap-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={write} data-testid="logging-write-form">
			<label class="text-sm">Log ID<input class={GCP_INPUT} bind:value={wLog} /></label>
			<label class="text-sm">Severity
				<select class={GCP_INPUT} bind:value={wSeverity}>{#each SEVERITIES as s (s)}<option value={s}>{s}</option>{/each}</select>
			</label>
			<label class="grow text-sm">Message<input class={GCP_INPUT} bind:value={wText} data-testid="logging-message" /></label>
			<button type="submit" class={GCP_PRIMARY} disabled={Boolean(writeError)} data-testid="logging-write"><Send class="mr-1 inline h-4 w-4" />Write entry</button>
		</form>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="logging-sinks">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Destination</th><th class="px-4 py-2">Filter</th><th class="px-4 py-2">State</th></tr></thead>
				<tbody>
					{#each sinks as s (s.name)}
						<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2">{s.name}</td><td class="px-4 py-2 font-mono break-all">{s.destination}</td><td class="px-4 py-2 font-mono">{s.filter || '(all)'}</td><td class="px-4 py-2">{s.disabled ? 'Disabled' : 'Enabled'}</td></tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="4">No sinks.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
