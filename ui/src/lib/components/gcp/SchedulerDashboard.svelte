<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { AlarmClock, Plus, RefreshCw, Pause, Play, Trash2, Zap, Pencil } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { rowsToMap, validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import {
		DEFAULT_TIME_ZONE,
		HTTP_METHODS,
		countdown,
		createJob,
		deleteJob,
		describeLastAttempt,
		jobAction,
		lastAttemptOk,
		listJobs,
		readStoredSchedulerLocation,
		readStoredSchedulerProject,
		updateSchedule,
		validateHeaderKey,
		validateJobId,
		validateSchedule,
		validateTimeZone,
		validateTopic,
		validateUrl,
		writeStoredSchedulerScope,
		type HttpMethod,
		type JobAction,
		type JobInfo,
		type TargetKind
	} from '#lib/gcpscheduler.ts';

	const REFRESH_MS = 3000;
	const TICK_MS = 1000;

	let project = $state(readStoredSchedulerProject());
	let location = $state(readStoredSchedulerLocation());
	let projectInput = $state(readStoredSchedulerProject());
	let locationInput = $state(readStoredSchedulerLocation());
	let jobs = $state<JobInfo[]>([]);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');
	let autoRefresh = $state(false);
	let expanded = $state<string | null>(null);
	let now = $state(Date.now());

	let jobModal = $state<Modal | null>(null);
	let fId = $state('');
	let fDescription = $state('');
	let fSchedule = $state('* * * * *');
	let fZone = $state(DEFAULT_TIME_ZONE);
	let fKind = $state<TargetKind>('http');
	let fUrl = $state('');
	let fMethod = $state<HttpMethod>('POST');
	let fHeaders = $state<MetaRow[]>([]);
	let fBody = $state('');
	let fTopic = $state('');
	let fData = $state('');
	let jobTouched = $state(false);
	const idError = $derived(validateJobId(fId.trim()));
	const scheduleError = $derived(validateSchedule(fSchedule));
	const zoneError = $derived(validateTimeZone(fZone));
	const urlError = $derived(fKind === 'http' ? validateUrl(fUrl.trim()) : null);
	const topicError = $derived(fKind === 'pubsub' ? validateTopic(fTopic.trim(), project) : null);
	const bodyError = $derived(fKind === 'http' && fBody && ['GET', 'HEAD', 'OPTIONS'].includes(fMethod) ? `${fMethod} requests cannot have a body` : null);

	let editModal = $state<Modal | null>(null);
	let editing = $state<JobInfo | null>(null);
	let eSchedule = $state('');
	let eZone = $state('');
	const eScheduleError = $derived(validateSchedule(eSchedule));
	const eZoneError = $derived(validateTimeZone(eZone));

	const describe = describeGcpError;
	const scope = $derived({ project, location });
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const mini = 'rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const cell = 'px-4 py-3';

	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');
	const stateClass = (st: string) =>
		st === 'ENABLED' ? 'text-green-700 dark:text-green-400' : st === 'PAUSED' ? 'text-amber-700 dark:text-amber-400' : 'text-red-700 dark:text-red-400';

	async function load() {
		try {
			jobs = (await listJobs(scope)).toSorted((a, b) => a.id.localeCompare(b.id));
			loadError = '';
		} catch (e) {
			loadError = describe(e);
			if (!autoRefresh) toast.error(`Failed to load jobs: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function run(label: string, action: () => Promise<void>, success: string, failure: string): Promise<boolean> {
		busy = label;
		try {
			await action();
			if (success) toast.success(success);
			return true;
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
			return false;
		} finally {
			busy = '';
		}
	}

	function openJob() {
		fId = '';
		fDescription = '';
		fSchedule = '* * * * *';
		fZone = DEFAULT_TIME_ZONE;
		fKind = 'http';
		fUrl = '';
		fMethod = 'POST';
		fHeaders = [];
		fBody = '';
		fTopic = '';
		fData = '';
		jobTouched = false;
		jobModal?.open();
	}

	async function submitJob(event: Event) {
		event.preventDefault();
		jobTouched = true;
		if (idError || scheduleError || zoneError || urlError || topicError || bodyError) return;
		const bad = validateRows(fHeaders, validateHeaderKey);
		if (bad) return void toast.error(`Invalid ${fKind === 'http' ? 'header' : 'attribute'} ${bad}`);
		const id = fId.trim();
		const ok = await run('create', async () => {
			await createJob(scope, { id, description: fDescription, schedule: fSchedule, timeZone: fZone, kind: fKind, url: fUrl, method: fMethod, headers: rowsToMap(fHeaders), body: fBody, topic: fTopic, data: fData });
		}, `Created job ${id}`, `Failed to create ${id}`);
		if (ok) {
			jobModal?.close('created');
			await load();
		}
	}

	async function act(j: JobInfo, action: JobAction) {
		const done = { pause: 'Paused', resume: 'Resumed', run: 'Triggered' }[action];
		const ok = await run(`${action}-${j.id}`, async () => { await jobAction(scope, j.id, action); }, `${done} ${j.id}`, `Failed to ${action} ${j.id}`);
		if (ok) await load();
	}

	async function remove(j: JobInfo) {
		if (!(await confirmDestructive({ title: 'Delete job', message: `Delete job "${j.id}"?` }))) return;
		const ok = await run(`del-${j.id}`, async () => { await deleteJob(scope, j.id); }, `Deleted job ${j.id}`, `Failed to delete ${j.id}`);
		if (ok) await load();
	}

	function openEdit(j: JobInfo) {
		editing = j;
		eSchedule = j.schedule;
		eZone = j.timeZone;
		editModal?.open();
	}

	async function submitEdit(event: Event) {
		event.preventDefault();
		const j = editing;
		if (!j || eScheduleError || eZoneError) return;
		const ok = await run('edit', async () => { await updateSchedule(scope, j.id, eSchedule, eZone); }, `Updated schedule of ${j.id}`, `Failed to update ${j.id}`);
		if (ok) {
			editModal?.close('saved');
			await load();
		}
	}

	function applyScope(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		const l = locationInput.trim();
		if (!p || !l) return;
		project = p;
		location = l;
		writeStoredSchedulerScope(p, l);
		jobs = [];
		loading = true;
		void load();
	}

	$effect(() => {
		if (!autoRefresh) return;
		const timer = setInterval(() => void load(), REFRESH_MS);
		return () => clearInterval(timer);
	});

	$effect(() => {
		const timer = setInterval(() => (now = Date.now()), TICK_MS);
		return () => clearInterval(timer);
	});

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="scheduler-dashboard">
	<PageHeader icon={AlarmClock} title="Cloud Scheduler" description="Google Cloud Scheduler: cron jobs with HTTP and Pub/Sub targets" color="blue" onRefresh={load}>
		{#snippet actions()}
			<label class="flex items-center gap-2 text-sm text-gray-600 dark:text-gray-300">
				<input type="checkbox" bind:checked={autoRefresh} data-testid="scheduler-auto-refresh" /> Auto-refresh
			</label>
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={openJob} disabled={loading || Boolean(loadError)} data-testid="scheduler-new-job">
				<Plus class="h-4 w-4" /> New job
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyScope}>
		<label for="scheduler-project">Project</label>
		<input id="scheduler-project" class="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="scheduler-project" />
		<label for="scheduler-location">Location</label>
		<input id="scheduler-location" class="w-32 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={locationInput} data-testid="scheduler-location" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || !locationInput.trim() || (projectInput.trim() === project && locationInput.trim() === location)}>Switch</button>
		<span>Jobs are listed per project and location.</span>
	</form>

	{#if loadError && jobs.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="scheduler-error">
			<p class="font-medium">Could not reach Cloud Scheduler</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if jobs.length === 0}
		<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="scheduler-no-jobs">No jobs in {location}</p>
	{:else}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="scheduler-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Job</th><th class={cell}>Schedule</th><th class={cell}>Next run</th><th class={cell}>State</th><th class={cell}>Target</th><th class={cell}>Last attempt</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each jobs as j (j.name)}
						{@const ok = lastAttemptOk(j)}
						<tr data-testid="scheduler-job-row">
							<td class="{cell} font-mono text-xs max-w-[10rem]">
								<button class="block max-w-full truncate text-left underline decoration-dotted" title={j.id} onclick={() => (expanded = expanded === j.name ? null : j.name)} aria-expanded={expanded === j.name} data-testid="scheduler-job-toggle">{j.id}</button>
							</td>
							<td class="{cell} text-xs whitespace-nowrap"><span class="font-mono" data-testid="scheduler-schedule">{j.schedule}</span><br /><span class="text-gray-500 dark:text-gray-400">{j.timeZone}</span></td>
							<td class="{cell} text-xs whitespace-nowrap" title={when(j.scheduleTime)} data-testid="scheduler-next">{j.state === 'ENABLED' ? countdown(j.scheduleTime, now) : '—'}</td>
							<td class="{cell} text-xs font-medium {stateClass(j.state)}" data-testid="scheduler-state">{j.state}</td>
							<td class="{cell} text-xs break-all min-w-[12rem]"><span class="font-semibold">{j.targetType}{j.method ? ` · ${j.method}` : ''}</span><br />{j.target}</td>
							<td class="{cell} text-xs {ok === true ? 'text-green-700 dark:text-green-400' : ok === false ? 'text-red-700 dark:text-red-400' : 'text-gray-500 dark:text-gray-400'}" data-testid="scheduler-last-attempt">{describeLastAttempt(j)}{#if j.lastAttemptTime}<br /><span class="text-gray-500 dark:text-gray-400">{when(j.lastAttemptTime)}</span>{/if}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={mini} disabled={busy === `run-${j.id}`} onclick={() => act(j, 'run')} data-testid="scheduler-run"><Zap class="inline h-3.5 w-3.5" /> Run now</button>
								{#if j.state === 'PAUSED'}
									<button class={mini} disabled={busy === `resume-${j.id}`} onclick={() => act(j, 'resume')} data-testid="scheduler-resume"><Play class="inline h-3.5 w-3.5" /> Resume</button>
								{:else}
									<button class={mini} disabled={busy === `pause-${j.id}`} onclick={() => act(j, 'pause')} data-testid="scheduler-pause"><Pause class="inline h-3.5 w-3.5" /> Pause</button>
								{/if}
								<button class={mini} onclick={() => openEdit(j)} data-testid="scheduler-edit" aria-label="Edit schedule of {j.id}"><Pencil class="inline h-3.5 w-3.5" /></button>
								<button class="{mini} text-red-600" disabled={busy === `del-${j.id}`} onclick={() => remove(j)} data-testid="scheduler-delete" aria-label="Delete job {j.id}"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
						{#if expanded === j.name}
							<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="scheduler-job-detail">
								<td colspan="7" class="px-4 py-3 text-xs space-y-1">
									<p class="font-mono break-all">{j.name}</p>
									<p><span class={dt}>Description</span> {j.description || '—'} · <span class={dt}>Next run</span> {when(j.scheduleTime)}</p>
									<p><span class={dt}>{j.targetType === 'Pub/Sub' ? 'Attributes' : 'Headers'}</span> {Object.entries(j.headers).map(([hk, hv]) => `${hk}: ${hv}`).join(', ') || '—'}</p>
									<p class={dt}>{j.targetType === 'Pub/Sub' ? 'Data' : 'Body'}</p>
									<pre class="whitespace-pre-wrap break-all rounded-md bg-white dark:bg-slate-900 p-2 font-mono">{j.body === null ? '(binary)' : j.body || '(empty)'}</pre>
								</td>
							</tr>
						{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

<Modal bind:this={jobModal} title="New job" titleId="scheduler-job-title">
	{#snippet children()}
		<form id="scheduler-job-form" class="space-y-3" onsubmit={submitJob} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Job name</span>
				<input class={input} bind:value={fId} placeholder="my-job" autocomplete="off" aria-label="Job name" />
				{#if jobTouched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Description (optional)</span>
				<input class={input} bind:value={fDescription} aria-label="Description" />
			</label>
			<div class="grid grid-cols-2 gap-3">
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Schedule (cron)</span>
					<input class="{input} font-mono" bind:value={fSchedule} placeholder="*/5 * * * *" autocomplete="off" aria-label="Schedule" />
					{#if jobTouched && scheduleError}<span class="block text-xs text-red-600 dark:text-red-400">{scheduleError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Time zone</span>
					<input class={input} bind:value={fZone} placeholder="Etc/UTC" autocomplete="off" aria-label="Time zone" />
					{#if jobTouched && zoneError}<span class="block text-xs text-red-600 dark:text-red-400">{zoneError}</span>{/if}
				</label>
			</div>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Target type</span>
				<select class={input} bind:value={fKind} aria-label="Target type">
					<option value="http">HTTP</option>
					<option value="pubsub">Pub/Sub</option>
				</select>
			</label>
			{#if fKind === 'http'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">URL</span>
					<input class={input} bind:value={fUrl} placeholder="http://localhost:9000/hook" autocomplete="off" aria-label="URL" />
					{#if jobTouched && urlError}<span class="block text-xs text-red-600 dark:text-red-400">{urlError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">HTTP method</span>
					<select class={input} bind:value={fMethod} aria-label="HTTP method">
						{#each HTTP_METHODS as m (m)}<option value={m}>{m}</option>{/each}
					</select>
				</label>
				<KeyValueEditor bind:rows={fHeaders} label="Headers" testId="scheduler-job-headers" validateKey={validateHeaderKey} hideSave />
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Body (optional)</span>
					<textarea class="{input} font-mono" rows="3" bind:value={fBody} aria-label="Body"></textarea>
					{#if jobTouched && bodyError}<span class="block text-xs text-red-600 dark:text-red-400">{bodyError}</span>{/if}
				</label>
			{:else}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Topic</span>
					<input class={input} bind:value={fTopic} placeholder="my-topic or projects/{project}/topics/my-topic" autocomplete="off" aria-label="Topic" />
					{#if jobTouched && topicError}<span class="block text-xs text-red-600 dark:text-red-400">{topicError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Message data (optional)</span>
					<textarea class="{input} font-mono" rows="3" bind:value={fData} aria-label="Message data"></textarea>
				</label>
				<KeyValueEditor bind:rows={fHeaders} label="Attributes" testId="scheduler-job-attributes" validateKey={validateHeaderKey} hideSave />
			{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">Created in {project} / {location}.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => jobModal?.close()}>Cancel</button>
		<button type="submit" form="scheduler-job-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create'} data-testid="scheduler-job-submit">{busy === 'create' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit schedule" titleId="scheduler-edit-title">
	{#snippet children()}
		<form id="scheduler-edit-form" class="space-y-3" onsubmit={submitEdit} novalidate>
			<p class="font-mono text-xs text-gray-500 dark:text-gray-400">{editing?.id}</p>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Schedule (cron)</span>
				<input class="{input} font-mono" bind:value={eSchedule} autocomplete="off" aria-label="Cron expression" />
				{#if eScheduleError}<span class="block text-xs text-red-600 dark:text-red-400">{eScheduleError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Time zone</span>
				<input class={input} bind:value={eZone} autocomplete="off" aria-label="Zone name" />
				{#if eZoneError}<span class="block text-xs text-red-600 dark:text-red-400">{eZoneError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => editModal?.close()}>Cancel</button>
		<button type="submit" form="scheduler-edit-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'edit' || Boolean(eScheduleError) || Boolean(eZoneError)} data-testid="scheduler-edit-submit">{busy === 'edit' ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
