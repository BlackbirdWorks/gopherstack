<script lang="ts">
	// AWS Glue DataBrew has five listable families: Datasets, Projects,
	// Recipes, Jobs, Schedules.
	//
	// Recipe deletion is the one genuine subtlety here: there is no
	// DeleteRecipe operation in the real API at all (confirmed against
	// botocore's databrew service-2.json -- the only DELETE route under
	// /recipes is DELETE /recipes/{name}/recipeVersion/{recipeVersion}).
	// Real clients delete a recipe by deleting each of its versions via
	// DeleteRecipeVersion or BatchDeleteRecipeVersion, including the
	// LATEST_WORKING draft. services/databrew/PARITY.md documents this
	// exact bug class: a fabricated "DeleteRecipe" op was previously
	// advertised and has since been removed from GetSupportedOperations
	// (the handler route stays wired only as internal test scaffolding, not
	// reachable via GetSupportedOperations, so this UI does not call it).
	// This page's recipe delete control opens a version list
	// (ListRecipeVersions) and deletes through DeleteRecipeVersion /
	// BatchDeleteRecipeVersion, matching the real API's shape.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDataBrewClient } from '$lib/aws-client';
	import {
		ListDatasetsCommand,
		ListJobsCommand,
		ListProjectsCommand,
		ListRecipesCommand,
		ListSchedulesCommand,
		ListRecipeVersionsCommand,
		DescribeDatasetCommand,
		DescribeProjectCommand,
		DescribeRecipeCommand,
		DescribeJobCommand,
		DescribeScheduleCommand,
		CreateDatasetCommand,
		CreateProjectCommand,
		CreateRecipeCommand,
		CreateRecipeJobCommand,
		CreateScheduleCommand,
		UpdateDatasetCommand,
		UpdateProjectCommand,
		UpdateScheduleCommand,
		PublishRecipeCommand,
		DeleteDatasetCommand,
		DeleteProjectCommand,
		DeleteJobCommand,
		DeleteScheduleCommand,
		DeleteRecipeVersionCommand,
		BatchDeleteRecipeVersionCommand,
		StartJobRunCommand,
		type Dataset,
		type Job,
		type Project,
		type Recipe,
		type Schedule,
		type DescribeDatasetCommandOutput,
		type DescribeProjectCommandOutput,
		type DescribeJobCommandOutput,
		type DescribeScheduleCommandOutput
	} from '@aws-sdk/client-databrew';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Brush, Plus, Trash2, Eye, Pencil, UploadCloud, Play } from 'lucide-svelte';

	const client = regionalClient(getDataBrewClient);

	type TabId = 'datasets' | 'projects' | 'recipes' | 'jobs' | 'schedules';

	const tabs: TabDef[] = [
		{ id: 'datasets', label: 'Datasets' },
		{ id: 'projects', label: 'Projects' },
		{ id: 'recipes', label: 'Recipes' },
		{ id: 'jobs', label: 'Jobs' },
		{ id: 'schedules', label: 'Schedules' }
	];

	const INPUT_FORMATS = ['CSV', 'EXCEL', 'JSON', 'ORC', 'PARQUET'];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	let activeTab = $state<TabId>('datasets');
	let searchQuery = $state('');

	let datasets = $state<Dataset[]>([]);
	let projects = $state<Project[]>([]);
	let recipes = $state<Recipe[]>([]);
	let jobs = $state<Job[]>([]);
	let schedules = $state<Schedule[]>([]);

	async function fetchDatasets(): Promise<void> {
		const resp = await client().send(new ListDatasetsCommand({}));
		datasets = resp.Datasets ?? [];
	}
	async function fetchProjects(): Promise<void> {
		const resp = await client().send(new ListProjectsCommand({}));
		projects = resp.Projects ?? [];
	}
	async function fetchRecipes(): Promise<void> {
		const resp = await client().send(new ListRecipesCommand({}));
		recipes = resp.Recipes ?? [];
	}
	async function fetchJobs(): Promise<void> {
		const resp = await client().send(new ListJobsCommand({}));
		jobs = resp.Jobs ?? [];
	}
	async function fetchSchedules(): Promise<void> {
		const resp = await client().send(new ListSchedulesCommand({}));
		schedules = resp.Schedules ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		datasets: () => fetchDatasets().catch(rethrowDescribed),
		projects: () => fetchProjects().catch(rethrowDescribed),
		recipes: () => fetchRecipes().catch(rethrowDescribed),
		jobs: () => fetchJobs().catch(rethrowDescribed),
		schedules: () => fetchSchedules().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// DataBrew resource names are unique per-account-per-region, so any
	// selected-resource state must be cleared on region change.
	onRegionChange(() => {
		detailModal?.close();
		versionsModal?.close();
		detailKind = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredDatasets = $derived(datasets.filter((d) => matches(searchQuery.toLowerCase(), d.Name, d.Format)));
	const filteredProjects = $derived(projects.filter((p) => matches(searchQuery.toLowerCase(), p.Name, p.DatasetName, p.RecipeName)));
	const filteredRecipes = $derived(recipes.filter((r) => matches(searchQuery.toLowerCase(), r.Name, r.RecipeVersion)));
	const filteredJobs = $derived(jobs.filter((j) => matches(searchQuery.toLowerCase(), j.Name, j.Type, j.DatasetName)));
	const filteredSchedules = $derived(schedules.filter((s) => matches(searchQuery.toLowerCase(), s.Name, s.CronExpression)));

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Dataset create ---

	let createDatasetModal = $state<Modal | null>(null);
	let creatingDataset = $state(false);
	let createDatasetError = $state<string | null>(null);
	let newDatasetName = $state('');
	let newDatasetFormat = $state('CSV');
	let newDatasetBucket = $state('');
	let newDatasetKey = $state('');

	function openCreateDatasetModal(): void {
		createDatasetError = null;
		newDatasetName = '';
		newDatasetFormat = 'CSV';
		newDatasetBucket = '';
		newDatasetKey = '';
		createDatasetModal?.open();
	}

	async function submitCreateDataset(): Promise<void> {
		if (!newDatasetName || !newDatasetBucket) {
			createDatasetError = 'Name and S3 bucket are required.';
			return;
		}
		creatingDataset = true;
		createDatasetError = null;
		try {
			await client().send(
				new CreateDatasetCommand({
					Name: newDatasetName,
					Format: newDatasetFormat as never,
					Input: { S3InputDefinition: { Bucket: newDatasetBucket, Key: newDatasetKey || undefined } }
				})
			);
			toast.success('Dataset created');
			createDatasetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			createDatasetError = msg;
			toast.error(msg);
		} finally {
			creatingDataset = false;
		}
	}

	async function deleteDataset(d: Dataset): Promise<void> {
		if (!d.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dataset',
			message: `Delete dataset "${d.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDatasetCommand({ Name: d.Name }));
			toast.success('Dataset deleted');
			await tabLoader.refresh('datasets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Project create ---

	let createProjectModal = $state<Modal | null>(null);
	let creatingProject = $state(false);
	let createProjectError = $state<string | null>(null);
	let newProjectName = $state('');
	let newProjectDatasetName = $state('');
	let newProjectRecipeName = $state('');
	let newProjectRoleArn = $state('');

	function openCreateProjectModal(): void {
		createProjectError = null;
		newProjectName = '';
		newProjectDatasetName = '';
		newProjectRecipeName = '';
		newProjectRoleArn = '';
		createProjectModal?.open();
	}

	async function submitCreateProject(): Promise<void> {
		if (!newProjectName || !newProjectDatasetName || !newProjectRecipeName || !newProjectRoleArn) {
			createProjectError = 'Name, dataset, recipe, and role ARN are all required.';
			return;
		}
		creatingProject = true;
		createProjectError = null;
		try {
			await client().send(
				new CreateProjectCommand({
					Name: newProjectName,
					DatasetName: newProjectDatasetName,
					RecipeName: newProjectRecipeName,
					RoleArn: newProjectRoleArn
				})
			);
			toast.success('Project created');
			createProjectModal?.close();
			await tabLoader.refresh('projects');
		} catch (e) {
			const msg = describeError(e);
			createProjectError = msg;
			toast.error(msg);
		} finally {
			creatingProject = false;
		}
	}

	async function deleteProject(p: Project): Promise<void> {
		if (!p.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete project',
			message: `Delete project "${p.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteProjectCommand({ Name: p.Name }));
			toast.success('Project deleted');
			await tabLoader.refresh('projects');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Recipe create ---

	let createRecipeModal = $state<Modal | null>(null);
	let creatingRecipe = $state(false);
	let createRecipeError = $state<string | null>(null);
	let newRecipeName = $state('');
	let newRecipeDescription = $state('');
	let newRecipeOperation = $state('');
	let newRecipeParametersJson = $state('{}');

	function openCreateRecipeModal(): void {
		createRecipeError = null;
		newRecipeName = '';
		newRecipeDescription = '';
		newRecipeOperation = '';
		newRecipeParametersJson = '{}';
		createRecipeModal?.open();
	}

	async function submitCreateRecipe(): Promise<void> {
		if (!newRecipeName || !newRecipeOperation) {
			createRecipeError = 'Name and a first step operation are required.';
			return;
		}
		let parameters: Record<string, string>;
		try {
			parameters = JSON.parse(newRecipeParametersJson || '{}');
		} catch {
			createRecipeError = 'Step parameters must be valid JSON.';
			return;
		}
		creatingRecipe = true;
		createRecipeError = null;
		try {
			await client().send(
				new CreateRecipeCommand({
					Name: newRecipeName,
					Description: newRecipeDescription || undefined,
					Steps: [{ Action: { Operation: newRecipeOperation, Parameters: parameters } }]
				})
			);
			toast.success('Recipe created (as LATEST_WORKING draft)');
			createRecipeModal?.close();
			await tabLoader.refresh('recipes');
		} catch (e) {
			const msg = describeError(e);
			createRecipeError = msg;
			toast.error(msg);
		} finally {
			creatingRecipe = false;
		}
	}

	async function publishRecipe(r: Recipe): Promise<void> {
		if (!r.Name) return;
		try {
			await client().send(new PublishRecipeCommand({ Name: r.Name }));
			toast.success(`Published a new version of "${r.Name}"`);
			await tabLoader.refresh('recipes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Recipe versions (the real "delete a recipe" flow) ---

	let versionsModal = $state<Modal | null>(null);
	let versionsRecipeName = $state('');
	let versionsLoading = $state(false);
	let versionsError = $state<string | null>(null);
	let recipeVersions = $state<Recipe[]>([]);

	async function openVersions(r: Recipe): Promise<void> {
		if (!r.Name) return;
		versionsRecipeName = r.Name;
		versionsError = null;
		recipeVersions = [];
		versionsModal?.open();
		await refreshVersions();
	}

	async function refreshVersions(): Promise<void> {
		if (!versionsRecipeName) return;
		versionsLoading = true;
		try {
			const resp = await client().send(new ListRecipeVersionsCommand({ Name: versionsRecipeName }));
			recipeVersions = resp.Recipes ?? [];
		} catch (e) {
			versionsError = describeError(e);
		} finally {
			versionsLoading = false;
		}
	}

	async function deleteOneVersion(version: string): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Delete recipe version',
			message: `Delete version "${version}" of "${versionsRecipeName}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteRecipeVersionCommand({ Name: versionsRecipeName, RecipeVersion: version }));
			toast.success(`Deleted version ${version}`);
			await refreshVersions();
			await tabLoader.refresh('recipes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteAllVersions(): Promise<void> {
		const allVersions = recipeVersions.map((r) => r.RecipeVersion).filter((v): v is string => !!v);
		if (allVersions.length === 0) return;
		const confirmed = await confirmDestructive({
			title: 'Delete entire recipe',
			message: `Delete all ${allVersions.length} version(s) of "${versionsRecipeName}", including the working draft? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new BatchDeleteRecipeVersionCommand({ Name: versionsRecipeName, RecipeVersions: [...allVersions, 'LATEST_WORKING'] })
			);
			toast.success(`Deleted recipe "${versionsRecipeName}"`);
			versionsModal?.close();
			await tabLoader.refresh('recipes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Job create (recipe job -- the more common of the two job types) ---

	let createJobModal = $state<Modal | null>(null);
	let creatingJob = $state(false);
	let createJobError = $state<string | null>(null);
	let newJobName = $state('');
	let newJobDatasetName = $state('');
	let newJobRecipeName = $state('');
	let newJobRoleArn = $state('');
	let newJobOutputBucket = $state('');
	let newJobOutputKey = $state('');

	function openCreateJobModal(): void {
		createJobError = null;
		newJobName = '';
		newJobDatasetName = '';
		newJobRecipeName = '';
		newJobRoleArn = '';
		newJobOutputBucket = '';
		newJobOutputKey = '';
		createJobModal?.open();
	}

	async function submitCreateJob(): Promise<void> {
		if (!newJobName || !newJobDatasetName || !newJobRecipeName || !newJobRoleArn || !newJobOutputBucket) {
			createJobError = 'Name, dataset, recipe, role ARN, and output bucket are all required.';
			return;
		}
		creatingJob = true;
		createJobError = null;
		try {
			await client().send(
				new CreateRecipeJobCommand({
					Name: newJobName,
					DatasetName: newJobDatasetName,
					RecipeReference: { Name: newJobRecipeName, RecipeVersion: 'LATEST_PUBLISHED' },
					RoleArn: newJobRoleArn,
					Outputs: [{ Location: { Bucket: newJobOutputBucket, Key: newJobOutputKey || undefined } }]
				})
			);
			toast.success('Recipe job created');
			createJobModal?.close();
			await tabLoader.refresh('jobs');
		} catch (e) {
			const msg = describeError(e);
			createJobError = msg;
			toast.error(msg);
		} finally {
			creatingJob = false;
		}
	}

	async function deleteJob(j: Job): Promise<void> {
		if (!j.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete job',
			message: `Delete job "${j.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteJobCommand({ Name: j.Name }));
			toast.success('Job deleted');
			await tabLoader.refresh('jobs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function runJob(j: Job): Promise<void> {
		if (!j.Name) return;
		try {
			await client().send(new StartJobRunCommand({ Name: j.Name }));
			toast.success(`Started a run of "${j.Name}"`);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Schedule create ---

	let createScheduleModal = $state<Modal | null>(null);
	let creatingSchedule = $state(false);
	let createScheduleError = $state<string | null>(null);
	let newScheduleName = $state('');
	let newScheduleCron = $state('');
	let newScheduleJobNames = $state('');

	function openCreateScheduleModal(): void {
		createScheduleError = null;
		newScheduleName = '';
		newScheduleCron = '';
		newScheduleJobNames = '';
		createScheduleModal?.open();
	}

	async function submitCreateSchedule(): Promise<void> {
		if (!newScheduleName || !newScheduleCron) {
			createScheduleError = 'Name and cron expression are required.';
			return;
		}
		creatingSchedule = true;
		createScheduleError = null;
		try {
			const jobNames = newScheduleJobNames.split(',').map((s) => s.trim()).filter(Boolean);
			await client().send(
				new CreateScheduleCommand({
					Name: newScheduleName,
					CronExpression: newScheduleCron,
					JobNames: jobNames.length > 0 ? jobNames : undefined
				})
			);
			toast.success('Schedule created');
			createScheduleModal?.close();
			await tabLoader.refresh('schedules');
		} catch (e) {
			const msg = describeError(e);
			createScheduleError = msg;
			toast.error(msg);
		} finally {
			creatingSchedule = false;
		}
	}

	async function deleteSchedule(s: Schedule): Promise<void> {
		if (!s.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete schedule',
			message: `Delete schedule "${s.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteScheduleCommand({ Name: s.Name }));
			toast.success('Schedule deleted');
			await tabLoader.refresh('schedules');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail (per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'dataset' | 'project' | 'recipe' | 'job' | 'schedule' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedDataset = $state<DescribeDatasetCommandOutput | null>(null);
	let viewedProject = $state<DescribeProjectCommandOutput | null>(null);
	let viewedRecipe = $state<Recipe | null>(null);
	let viewedJob = $state<DescribeJobCommandOutput | null>(null);
	let viewedSchedule = $state<DescribeScheduleCommandOutput | null>(null);

	async function openDatasetDetail(d: Dataset): Promise<void> {
		detailKind = 'dataset';
		viewedDataset = null;
		detailError = null;
		detailModal?.open();
		if (!d.Name) return;
		detailLoading = true;
		try {
			viewedDataset = await client().send(new DescribeDatasetCommand({ Name: d.Name }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openProjectDetail(p: Project): Promise<void> {
		detailKind = 'project';
		viewedProject = null;
		detailError = null;
		detailModal?.open();
		if (!p.Name) return;
		detailLoading = true;
		try {
			viewedProject = await client().send(new DescribeProjectCommand({ Name: p.Name }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openRecipeDetail(r: Recipe): Promise<void> {
		detailKind = 'recipe';
		viewedRecipe = null;
		detailError = null;
		detailModal?.open();
		if (!r.Name) return;
		detailLoading = true;
		try {
			viewedRecipe = await client().send(new DescribeRecipeCommand({ Name: r.Name }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openJobDetail(j: Job): Promise<void> {
		detailKind = 'job';
		viewedJob = null;
		detailError = null;
		detailModal?.open();
		if (!j.Name) return;
		detailLoading = true;
		try {
			viewedJob = await client().send(new DescribeJobCommand({ Name: j.Name }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openScheduleDetail(s: Schedule): Promise<void> {
		detailKind = 'schedule';
		viewedSchedule = null;
		detailError = null;
		detailModal?.open();
		if (!s.Name) return;
		detailLoading = true;
		try {
			viewedSchedule = await client().send(new DescribeScheduleCommand({ Name: s.Name }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	// --- Edit (Dataset format, Project role, Schedule cron/jobs) ---

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editDatasetName = $state('');
	let editDatasetFormat = $state('CSV');
	let editDatasetBucket = $state('');
	let editDatasetKey = $state('');
	let editProjectName = $state('');
	let editProjectRoleArn = $state('');
	let editScheduleName = $state('');
	let editScheduleCron = $state('');
	let editScheduleJobNames = $state('');

	function openEditDataset(): void {
		editError = null;
		editDatasetName = viewedDataset?.Name ?? '';
		editDatasetFormat = viewedDataset?.Format ?? 'CSV';
		editDatasetBucket = viewedDataset?.Input?.S3InputDefinition?.Bucket ?? '';
		editDatasetKey = viewedDataset?.Input?.S3InputDefinition?.Key ?? '';
		editModal?.open();
	}

	async function submitEditDataset(): Promise<void> {
		editing = true;
		editError = null;
		try {
			await client().send(
				new UpdateDatasetCommand({
					Name: editDatasetName,
					Format: editDatasetFormat as never,
					Input: { S3InputDefinition: { Bucket: editDatasetBucket, Key: editDatasetKey || undefined } }
				})
			);
			toast.success('Dataset updated');
			editModal?.close();
			await tabLoader.refresh('datasets');
			await openDatasetDetail({ Name: editDatasetName } as Dataset);
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}

	function openEditProject(): void {
		editError = null;
		editProjectName = viewedProject?.Name ?? '';
		editProjectRoleArn = viewedProject?.RoleArn ?? '';
		editModal?.open();
	}

	async function submitEditProject(): Promise<void> {
		editing = true;
		editError = null;
		try {
			await client().send(new UpdateProjectCommand({ Name: editProjectName, RoleArn: editProjectRoleArn }));
			toast.success('Project updated');
			editModal?.close();
			await tabLoader.refresh('projects');
			await openProjectDetail({ Name: editProjectName } as Project);
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}

	function openEditSchedule(): void {
		editError = null;
		editScheduleName = viewedSchedule?.Name ?? '';
		editScheduleCron = viewedSchedule?.CronExpression ?? '';
		editScheduleJobNames = (viewedSchedule?.JobNames ?? []).join(', ');
		editModal?.open();
	}

	async function submitEditSchedule(): Promise<void> {
		editing = true;
		editError = null;
		try {
			const jobNames = editScheduleJobNames.split(',').map((s) => s.trim()).filter(Boolean);
			await client().send(
				new UpdateScheduleCommand({
					Name: editScheduleName,
					CronExpression: editScheduleCron,
					JobNames: jobNames.length > 0 ? jobNames : undefined
				})
			);
			toast.success('Schedule updated');
			editModal?.close();
			await tabLoader.refresh('schedules');
			await openScheduleDetail({ Name: editScheduleName } as Schedule);
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={Brush} title="AWS Glue DataBrew" description="Visual data preparation" onRefresh={handleRefresh} color="amber">
		{#snippet actions()}
			{#if activeTab === 'datasets'}
				<button onclick={openCreateDatasetModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Create dataset
				</button>
			{:else if activeTab === 'projects'}
				<button onclick={openCreateProjectModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Create project
				</button>
			{:else if activeTab === 'recipes'}
				<button onclick={openCreateRecipeModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Create recipe
				</button>
			{:else if activeTab === 'jobs'}
				<button onclick={openCreateJobModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Create recipe job
				</button>
			{:else if activeTab === 'schedules'}
				<button onclick={openCreateScheduleModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm">
					<Plus class="w-4 h-4" /> Create schedule
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="amber" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'datasets'}
				{#snippet datasetActionsCell(d: Dataset)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDatasetDetail(d)} title="View" aria-label="View dataset {d.Name}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDataset(d)} title="Delete" aria-label="Delete dataset {d.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const datasetColumns = defineColumns<Dataset>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Format', label: 'Format' },
					{ key: 'actions', label: '', render: datasetActionsCell }
				])}
				<DataTable rows={filteredDatasets} rowKey={(d) => d.Name ?? ''} columns={datasetColumns} loading={tabLoader.isLoading('datasets')} emptyMessage="No datasets found" />
			{:else if activeTab === 'projects'}
				{#snippet projectActionsCell(p: Project)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openProjectDetail(p)} title="View" aria-label="View project {p.Name}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteProject(p)} title="Delete" aria-label="Delete project {p.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const projectColumns = defineColumns<Project>([
					{ key: 'Name', label: 'Name' },
					{ key: 'DatasetName', label: 'Dataset' },
					{ key: 'RecipeName', label: 'Recipe' },
					{ key: 'actions', label: '', render: projectActionsCell }
				])}
				<DataTable rows={filteredProjects} rowKey={(p) => p.Name ?? ''} columns={projectColumns} loading={tabLoader.isLoading('projects')} emptyMessage="No projects found" />
			{:else if activeTab === 'recipes'}
				{#snippet recipeActionsCell(r: Recipe)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openRecipeDetail(r)} title="View" aria-label="View recipe {r.Name}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => publishRecipe(r)} title="Publish" aria-label="Publish recipe {r.Name}" class="text-gray-400 hover:text-green-500"><UploadCloud class="w-4 h-4" /></button>
						<button onclick={() => openVersions(r)} title="Delete (manage versions)" aria-label="Manage versions of recipe {r.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const recipeColumns = defineColumns<Recipe>([
					{ key: 'Name', label: 'Name' },
					{ key: 'RecipeVersion', label: 'Version' },
					{ key: 'actions', label: '', render: recipeActionsCell }
				])}
				<DataTable rows={filteredRecipes} rowKey={(r) => `${r.Name ?? ''}:${r.RecipeVersion ?? ''}`} columns={recipeColumns} loading={tabLoader.isLoading('recipes')} emptyMessage="No recipes found" />
			{:else if activeTab === 'jobs'}
				{#snippet jobActionsCell(j: Job)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => runJob(j)} title="Run now" aria-label="Run job {j.Name}" class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button>
						<button onclick={() => openJobDetail(j)} title="View" aria-label="View job {j.Name}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteJob(j)} title="Delete" aria-label="Delete job {j.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const jobColumns = defineColumns<Job>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Type', label: 'Type' },
					{ key: 'DatasetName', label: 'Dataset' },
					{ key: 'actions', label: '', render: jobActionsCell }
				])}
				<DataTable rows={filteredJobs} rowKey={(j) => j.Name ?? ''} columns={jobColumns} loading={tabLoader.isLoading('jobs')} emptyMessage="No jobs found" />
			{:else if activeTab === 'schedules'}
				{#snippet scheduleActionsCell(s: Schedule)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openScheduleDetail(s)} title="View" aria-label="View schedule {s.Name}" class="text-gray-400 hover:text-amber-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteSchedule(s)} title="Delete" aria-label="Delete schedule {s.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const scheduleColumns = defineColumns<Schedule>([
					{ key: 'Name', label: 'Name' },
					{ key: 'CronExpression', label: 'Cron' },
					{ key: 'actions', label: '', render: scheduleActionsCell }
				])}
				<DataTable rows={filteredSchedules} rowKey={(s) => s.Name ?? ''} columns={scheduleColumns} loading={tabLoader.isLoading('schedules')} emptyMessage="No schedules found" />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createDatasetModal} title="Create Dataset">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="db-dataset-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="db-dataset-name" bind:value={newDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-dataset-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
				<select id="db-dataset-format" bind:value={newDatasetFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each INPUT_FORMATS as f (f)}<option value={f}>{f}</option>{/each}
				</select>
			</div>
			<div>
				<label for="db-dataset-bucket" class="text-sm text-slate-600 dark:text-slate-300">S3 bucket</label>
				<input id="db-dataset-bucket" bind:value={newDatasetBucket} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-dataset-key" class="text-sm text-slate-600 dark:text-slate-300">S3 key (optional)</label>
				<input id="db-dataset-key" bind:value={newDatasetKey} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createDatasetError}<p class="text-sm text-red-600 dark:text-red-400">{createDatasetError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDatasetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDataset} disabled={creatingDataset} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{creatingDataset ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createProjectModal} title="Create Project">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="db-project-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="db-project-name" bind:value={newProjectName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-project-dataset" class="text-sm text-slate-600 dark:text-slate-300">Dataset</label>
				<select id="db-project-dataset" bind:value={newProjectDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset…</option>
					{#each datasets as d (d.Name)}<option value={d.Name}>{d.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="db-project-recipe" class="text-sm text-slate-600 dark:text-slate-300">Recipe</label>
				<select id="db-project-recipe" bind:value={newProjectRecipeName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a recipe…</option>
					{#each recipes as r (r.Name)}<option value={r.Name}>{r.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="db-project-role" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
				<input id="db-project-role" bind:value={newProjectRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createProjectError}<p class="text-sm text-red-600 dark:text-red-400">{createProjectError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createProjectModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateProject} disabled={creatingProject} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{creatingProject ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createRecipeModal} title="Create Recipe">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="db-recipe-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="db-recipe-name" bind:value={newRecipeName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-recipe-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="db-recipe-description" bind:value={newRecipeDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-recipe-operation" class="text-sm text-slate-600 dark:text-slate-300">First step: operation</label>
				<input id="db-recipe-operation" bind:value={newRecipeOperation} placeholder="UPPER_CASE" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-recipe-params" class="text-sm text-slate-600 dark:text-slate-300">First step: parameters (JSON)</label>
				<textarea id="db-recipe-params" bind:value={newRecipeParametersJson} rows={3} class="mt-1 w-full font-mono text-xs px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Creates the recipe as its LATEST_WORKING draft. Use Publish to cut a numbered version.</p>
			{#if createRecipeError}<p class="text-sm text-red-600 dark:text-red-400">{createRecipeError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRecipeModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateRecipe} disabled={creatingRecipe} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{creatingRecipe ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createJobModal} title="Create Recipe Job">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="db-job-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="db-job-name" bind:value={newJobName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-job-dataset" class="text-sm text-slate-600 dark:text-slate-300">Dataset</label>
				<select id="db-job-dataset" bind:value={newJobDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset…</option>
					{#each datasets as d (d.Name)}<option value={d.Name}>{d.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="db-job-recipe" class="text-sm text-slate-600 dark:text-slate-300">Recipe (uses LATEST_PUBLISHED)</label>
				<select id="db-job-recipe" bind:value={newJobRecipeName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a recipe…</option>
					{#each recipes as r (r.Name)}<option value={r.Name}>{r.Name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="db-job-role" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
				<input id="db-job-role" bind:value={newJobRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-job-outbucket" class="text-sm text-slate-600 dark:text-slate-300">Output S3 bucket</label>
				<input id="db-job-outbucket" bind:value={newJobOutputBucket} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-job-outkey" class="text-sm text-slate-600 dark:text-slate-300">Output S3 key (optional)</label>
				<input id="db-job-outkey" bind:value={newJobOutputKey} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Profile jobs (CreateProfileJob) are also implemented by the backend but not exposed by this create form.</p>
			{#if createJobError}<p class="text-sm text-red-600 dark:text-red-400">{createJobError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createJobModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateJob} disabled={creatingJob} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{creatingJob ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createScheduleModal} title="Create Schedule">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="db-schedule-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="db-schedule-name" bind:value={newScheduleName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-schedule-cron" class="text-sm text-slate-600 dark:text-slate-300">Cron expression</label>
				<input id="db-schedule-cron" bind:value={newScheduleCron} placeholder="cron(0 12 * * ? *)" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="db-schedule-jobs" class="text-sm text-slate-600 dark:text-slate-300">Job names (comma-separated, optional)</label>
				<input id="db-schedule-jobs" bind:value={newScheduleJobNames} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createScheduleError}<p class="text-sm text-red-600 dark:text-red-400">{createScheduleError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createScheduleModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSchedule} disabled={creatingSchedule} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{creatingSchedule ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={versionsModal} title="Recipe Versions -- {versionsRecipeName}">
	{#snippet children()}
		{#if versionsLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if versionsError}
			<p class="text-sm text-red-600 dark:text-red-400">{versionsError}</p>
		{:else if recipeVersions.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No published versions.</p>
		{:else}
			<ul class="space-y-2">
				{#each recipeVersions as v (v.RecipeVersion)}
					<li class="flex items-center justify-between gap-2 rounded-lg bg-gray-50 dark:bg-slate-700/50 px-3 py-2">
						<span class="text-sm text-slate-900 dark:text-white">{v.RecipeVersion}</span>
						<button onclick={() => v.RecipeVersion && deleteOneVersion(v.RecipeVersion)} aria-label="Delete version {v.RecipeVersion}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</li>
				{/each}
			</ul>
		{/if}
		<p class="mt-3 text-xs text-slate-500 dark:text-slate-400">
			There is no DeleteRecipe operation -- deleting a recipe means deleting every version, including
			LATEST_WORKING, via DeleteRecipeVersion/BatchDeleteRecipeVersion.
		</p>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => versionsModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		<button type="button" onclick={deleteAllVersions} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete entire recipe</button>
	{/snippet}
</Modal>

<Modal
	bind:this={detailModal}
	title={detailKind === 'dataset' ? 'Dataset' : detailKind === 'project' ? 'Project' : detailKind === 'recipe' ? 'Recipe' : detailKind === 'job' ? 'Job' : 'Schedule'}
>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'dataset' && viewedDataset}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Format</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.Format ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">S3 location</dt><dd class="break-all text-slate-900 dark:text-white">s3://{viewedDataset.Input?.S3InputDefinition?.Bucket ?? '—'}/{viewedDataset.Input?.S3InputDefinition?.Key ?? ''}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedDataset.ResourceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDataset.CreateDate)}</dd></div>
			</dl>
		{:else if detailKind === 'project' && viewedProject}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedProject.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Dataset</dt><dd class="text-slate-900 dark:text-white">{viewedProject.DatasetName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Recipe</dt><dd class="text-slate-900 dark:text-white">{viewedProject.RecipeName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProject.RoleArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProject.ResourceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedProject.CreateDate)}</dd></div>
			</dl>
		{:else if detailKind === 'recipe' && viewedRecipe}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedRecipe.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Version</dt><dd class="text-slate-900 dark:text-white">{viewedRecipe.RecipeVersion ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedRecipe.Description ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Steps</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedRecipe.Steps ?? [], null, 2)}</pre>
					</dd>
				</div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedRecipe.CreateDate)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Published</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedRecipe.PublishedDate)}</dd></div>
			</dl>
		{:else if detailKind === 'job' && viewedJob}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedJob.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedJob.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Dataset</dt><dd class="text-slate-900 dark:text-white">{viewedJob.DatasetName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Recipe</dt><dd class="text-slate-900 dark:text-white">{viewedJob.RecipeReference?.Name ?? '—'} ({viewedJob.RecipeReference?.RecipeVersion ?? '—'})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedJob.RoleArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedJob.ResourceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedJob.CreateDate)}</dd></div>
			</dl>
		{:else if detailKind === 'schedule' && viewedSchedule}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedSchedule.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Cron</dt><dd class="text-slate-900 dark:text-white">{viewedSchedule.CronExpression ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Job names</dt><dd class="text-slate-900 dark:text-white">{(viewedSchedule.JobNames ?? []).join(', ') || 'None'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedSchedule.ResourceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSchedule.CreateDate)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'dataset' && viewedDataset}
			<button type="button" onclick={openEditDataset} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'project' && viewedProject}
			<button type="button" onclick={openEditProject} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'schedule' && viewedSchedule}
			<button type="button" onclick={openEditSchedule} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={editModal} title={detailKind === 'dataset' ? 'Edit Dataset' : detailKind === 'project' ? 'Edit Project' : 'Edit Schedule'}>
	{#snippet children()}
		{#if detailKind === 'dataset'}
			<div class="space-y-3">
				<div>
					<label for="db-edit-dataset-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
					<select id="db-edit-dataset-format" bind:value={editDatasetFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each INPUT_FORMATS as f (f)}<option value={f}>{f}</option>{/each}
					</select>
				</div>
				<div>
					<label for="db-edit-dataset-bucket" class="text-sm text-slate-600 dark:text-slate-300">S3 bucket</label>
					<input id="db-edit-dataset-bucket" bind:value={editDatasetBucket} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="db-edit-dataset-key" class="text-sm text-slate-600 dark:text-slate-300">S3 key</label>
					<input id="db-edit-dataset-key" bind:value={editDatasetKey} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{:else if detailKind === 'project'}
			<div class="space-y-3">
				<div>
					<label for="db-edit-project-role" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
					<input id="db-edit-project-role" bind:value={editProjectRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<p class="text-xs text-slate-500 dark:text-slate-400">UpdateProject only supports Name/RoleArn/Sample -- a project's dataset is fixed at creation in the real API.</p>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{:else if detailKind === 'schedule'}
			<div class="space-y-3">
				<div>
					<label for="db-edit-schedule-cron" class="text-sm text-slate-600 dark:text-slate-300">Cron expression</label>
					<input id="db-edit-schedule-cron" bind:value={editScheduleCron} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="db-edit-schedule-jobs" class="text-sm text-slate-600 dark:text-slate-300">Job names (comma-separated)</label>
					<input id="db-edit-schedule-jobs" bind:value={editScheduleJobNames} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if editError}<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		{#if detailKind === 'dataset'}
			<button type="button" onclick={submitEditDataset} disabled={editing} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{:else if detailKind === 'project'}
			<button type="button" onclick={submitEditProject} disabled={editing} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{:else if detailKind === 'schedule'}
			<button type="button" onclick={submitEditSchedule} disabled={editing} class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
		{/if}
	{/snippet}
</Modal>
