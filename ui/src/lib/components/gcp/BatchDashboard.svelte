<script lang="ts">
	import { onMount } from 'svelte';
	import { Layers, Plus, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import BatchJobDetail from '#lib/components/gcp/BatchJobDetail.svelte';
	import { attempt, confirmedAttempt, GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		BATCH_ALL_LOCATIONS,
		BATCH_LOCATIONS,
		cancelJob,
		canCancel,
		createJob,
		deleteJob,
		formatDuration,
		isSettled,
		listJobs,
		listProjects,
		readStoredProject,
		statePill,
		validEnvName,
		validJobId,
		writeStoredProject,
		type BatchJob
	} from '#lib/gcpbatch.ts';

	const POLL_MS = 2000;

	let projects = $state<string[]>([]);
	let project = $state('');
	let location = $state(BATCH_ALL_LOCATIONS);
	let jobs = $state<BatchJob[]>([]);
	let selected = $state('');
	let loading = $state(true);
	let error = $state('');
	let creating = $state(false);
	let newProject = $state('');
	let newLocation = $state(BATCH_LOCATIONS[0]);
	let id = $state('');
	let image = $state('alpine:3');
	let commands = $state('echo\nhello from batch');
	let taskCount = $state(1);
	let parallelism = $state(1);
	let env = $state<{ name: string; value: string }[]>([]);

	const idOk = $derived(validJobId(id));
	const envOk = $derived(env.every((e) => !e.name.trim() || validEnvName(e.name.trim())));
	const formOk = $derived(Boolean(newProject.trim()) && idOk && Boolean(image.trim()) && envOk && taskCount >= 1 && parallelism >= 0);
	const selectedJob = $derived(jobs.find((j) => j.name === selected));

	async function boot() {
		try {
			projects = await listProjects();
			const s = readStoredProject();
			project = projects.includes(s) ? s : (projects[0] ?? '');
			newProject = project || 'gopherstack-local';
			error = '';
			await load();
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function load() {
		if (!project) return;
		try {
			jobs = await listJobs(project, location);
			error = '';
			if (selected && !jobs.some((j) => j.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function pickProject(p: string) {
		project = p;
		writeStoredProject(p);
		selected = '';
		await load();
	}

	async function pickLocation(l: string) {
		location = l;
		selected = '';
		await load();
	}

	async function submit() {
		if (!formOk) return;
		const ok = await attempt(
			() => createJob(newProject.trim(), newLocation, { id, image, commands, taskCount, parallelism, env }),
			`Created job ${id}`,
			`Failed to create ${id}`
		);
		if (!ok) return;
		creating = false;
		if (!projects.includes(newProject.trim())) projects = [...projects, newProject.trim()].toSorted();
		project = newProject.trim();
		writeStoredProject(project);
		location = newLocation;
		selected = `projects/${project}/locations/${newLocation}/jobs/${id}`;
		id = '';
		await load();
	}

	async function cancel(j: BatchJob) {
		const ok = await confirmedAttempt({ title: 'Cancel job', message: `Cancel job "${j.id}"? Running tasks are stopped.`, confirmLabel: 'Cancel job' }, () => cancelJob(j.name), `Cancelling ${j.id}`, `Failed to cancel ${j.id}`);
		if (ok) await load();
	}

	async function remove(j: BatchJob) {
		const ok = await confirmedAttempt({ title: 'Delete job', message: `Delete job "${j.id}" and its tasks?`, confirmLabel: 'Delete' }, () => deleteJob(j.name), `Deleting ${j.id}`, `Failed to delete ${j.id}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';

	onMount(() => {
		void boot();
		const timer = setInterval(() => {
			if (project && jobs.some((j) => !isSettled(j))) void load();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-6 pb-20" data-testid="batch-dashboard">
	<PageHeader icon={Layers} title="Cloud Batch" description="Jobs, task groups and tasks" color="blue" onRefresh={load} />

	{#if error && !project}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="batch-error">
			<p class="font-medium">Could not load Cloud Batch</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={boot}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
			{#if project}
				<label for="batch-project">Project</label>
				<select id="batch-project" class="{GCP_INPUT} !w-56 !py-1 font-mono text-xs" value={project} onchange={(e) => pickProject(e.currentTarget.value)} data-testid="batch-project">
					{#each projects as p (p)}<option value={p}>{p}</option>{/each}
				</select>
				<label for="batch-location">Location</label>
				<select id="batch-location" class="{GCP_INPUT} !w-48 !py-1 font-mono text-xs" value={location} onchange={(e) => pickLocation(e.currentTarget.value)} data-testid="batch-location">
					<option value={BATCH_ALL_LOCATIONS}>All locations</option>
					{#each BATCH_LOCATIONS as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			{/if}
			<button class="{GCP_PRIMARY} ml-auto flex items-center gap-1 !py-1" onclick={() => (creating = !creating)} data-testid="batch-create-toggle"><Plus class="h-4 w-4" /> Create job</button>
		</div>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="batch-scope-error">{error}</p>{/if}

		{#if creating}
			<form class="space-y-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void submit(); }} data-testid="batch-create-form">
				<div class="grid gap-3 sm:grid-cols-3">
					<label>Project<input class="{GCP_INPUT} font-mono" bind:value={newProject} data-testid="batch-new-project" /></label>
					<label>Location
						<select class="{GCP_INPUT} font-mono" bind:value={newLocation} data-testid="batch-new-location">
							{#each BATCH_LOCATIONS as l (l)}<option value={l}>{l}</option>{/each}
						</select>
					</label>
					<label>Job ID<input class="{GCP_INPUT} font-mono" bind:value={id} placeholder="my-job" data-testid="batch-new-id" />
						{#if id && !idOk}<span class="text-xs text-red-600">Lowercase letters, digits and dashes, starting with a letter.</span>{/if}
					</label>
				</div>
				<label class="block">Container image<input class="{GCP_INPUT} font-mono" bind:value={image} data-testid="batch-new-image" /></label>
				<label class="block">Commands (one argument per line)<textarea class="{GCP_INPUT} font-mono" rows="3" bind:value={commands} data-testid="batch-new-commands"></textarea></label>
				<div class="grid gap-3 sm:grid-cols-2">
					<label>Task count<input class={GCP_INPUT} type="number" min="1" max="1000" bind:value={taskCount} data-testid="batch-new-taskcount" /></label>
					<label>Parallelism<input class={GCP_INPUT} type="number" min="0" bind:value={parallelism} data-testid="batch-new-parallelism" /></label>
				</div>
				<div class="space-y-2">
					<p>Environment variables <span class="text-xs text-gray-500">(plain values only; secrets are not set from this page)</span></p>
					{#each env as row, i (i)}
						<div class="flex gap-2" data-testid="batch-new-env">
							<input class="{GCP_INPUT} font-mono" placeholder="NAME" bind:value={row.name} aria-label="Variable name" />
							<input class="{GCP_INPUT} font-mono" placeholder="value" bind:value={row.value} aria-label="Variable value" />
							<button type="button" class={GCP_MINI} onclick={() => (env = env.filter((_, k) => k !== i))}>Remove</button>
						</div>
					{/each}
					{#if !envOk}<p class="text-xs text-red-600">Variable names use letters, digits and underscores.</p>{/if}
					<button type="button" class={GCP_MINI} onclick={() => (env = [...env, { name: '', value: '' }])} data-testid="batch-new-env-add">Add variable</button>
				</div>
				<div class="flex gap-2">
					<button type="submit" class={GCP_PRIMARY} disabled={!formOk} data-testid="batch-new-submit">Create</button>
					<button type="button" class={GCP_GHOST} onclick={() => (creating = false)}>Close</button>
				</div>
			</form>
		{/if}

		{#if !project}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="batch-empty">No Batch jobs yet. Create one above or with the Google Cloud Batch client.</p>
		{:else if jobs.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="batch-jobs-empty">No jobs in this scope.</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="batch-job-table">
					<thead class={thead}><tr><th class={cell}>Job</th><th class={cell}>State</th><th class={cell}>Tasks</th><th class={cell}>Created</th><th class={cell}>Run duration</th><th class={cell}>Labels</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each jobs as j (j.name)}
							<tr data-testid="batch-job" class={selected === j.name ? 'bg-blue-50/50 dark:bg-blue-950/20' : ''}>
								<td class="{cell} font-mono text-xs"><button class="text-blue-600 underline dark:text-blue-400" onclick={() => (selected = selected === j.name ? '' : j.name)} data-testid="batch-job-open">{j.id}</button><div class="text-gray-400">{j.location}</div></td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {statePill(j.state)}" data-testid="batch-job-state">{j.state || '—'}</span></td>
								<td class="{cell} text-xs" data-testid="batch-job-tasks">{j.taskCount}</td>
								<td class="{cell} text-xs">{j.createTime || '—'}</td>
								<td class="{cell} text-xs" data-testid="batch-job-duration">{formatDuration(j.runDuration)}</td>
								<td class="{cell} font-mono text-xs break-all">{Object.entries(j.labels).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}</td>
								<td class="{cell} text-right whitespace-nowrap space-x-1">
									<button class={GCP_MINI} onclick={() => cancel(j)} disabled={!canCancel(j)} data-testid="batch-job-cancel">Cancel</button>
									<button class={GCP_MINI} onclick={() => remove(j)} disabled={j.state === 'DELETION_IN_PROGRESS'} data-testid="batch-job-delete">Delete</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		{#if selectedJob}
			<BatchJobDetail job={selectedJob} />
		{/if}
	{/if}
</div>
