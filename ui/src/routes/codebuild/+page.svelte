<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeBuildClient } from '$lib/aws-client';
	import {
		ListProjectsCommand,
		BatchGetProjectsCommand,
		ListBuildsForProjectCommand,
		BatchGetBuildsCommand,
		CreateProjectCommand,
		DeleteProjectCommand,
		type Project,
		type Build,
		SourceType,
		ArtifactsType,
		EnvironmentType,
		ComputeType
	} from '@aws-sdk/client-codebuild';
	import { toast } from 'svelte-sonner';
	import { 
		Cpu, Search, RefreshCw, Plus, Trash2, 
		Activity, Info, Box, Clock, ShieldCheck,
		ChevronRight, ListFilter, Globe, 
		Code, Server, Terminal, Workflow,
		Play, CheckCircle2, XCircle, AlertCircle,
		Timer, BarChart3, Database, Layers,
		ArrowRight, ExternalLink, Shield, History
	} from 'lucide-svelte';

	const codebuild = getCodeBuildClient();

	// State
	let loading = $state(false);
	let searchQuery = $state('');
	let projects = $state<Project[]>([]);
	let selectedProject = $state<Project | null>(null);
	let builds = $state<Build[]>([]);
	let loadingDetails = $state(false);

	// Modal State
	let showCreateModal = $state(false);
	let newProjectName = $state('');
	let creating = $state(false);

	// Derived
	const filteredProjects = $derived(
		projects.filter(p => p.name?.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Actions
	async function loadProjects() {
		loading = true;
		try {
			const listRes = await codebuild.send(new ListProjectsCommand({}));
			const names = listRes.projects ?? [];
			
			if (names.length > 0) {
				const batchRes = await codebuild.send(new BatchGetProjectsCommand({ names }));
				projects = batchRes.projects ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load projects: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectProject(project: Project) {
		selectedProject = project;
		builds = [];
		loadingDetails = true;
		try {
			const listRes = await codebuild.send(new ListBuildsForProjectCommand({ projectName: project.name }));
			const ids = listRes.ids ?? [];

			if (ids.length > 0) {
				const batchRes = await codebuild.send(new BatchGetBuildsCommand({ ids }));
				builds = batchRes.builds ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load builds: ${(err as Error).message}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function createProject() {
		if (!newProjectName.trim()) return;
		creating = true;
		try {
			await codebuild.send(new CreateProjectCommand({
				name: newProjectName.trim(),
				serviceRole: 'arn:aws:iam::000000000000:role/codebuild-role',
				source: { type: SourceType.NO_SOURCE },
				artifacts: { type: ArtifactsType.NO_ARTIFACTS },
				environment: {
					type: EnvironmentType.LINUX_CONTAINER,
					image: 'aws/codebuild/standard:5.0',
					computeType: ComputeType.BUILD_GENERAL1_SMALL
				}
			}));
			toast.success(`Project "${newProjectName}" created`);
			showCreateModal = false;
			newProjectName = '';
			await loadProjects();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteProject(name: string | undefined) {
		if (!name || !confirm(`Delete project "${name}"? All build history will be lost.`)) return;
		try {
			await codebuild.send(new DeleteProjectCommand({ name }));
			toast.success(`Project deleted`);
			if (selectedProject?.name === name) selectedProject = null;
			await loadProjects();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function getBuildStatusIcon(status: string | undefined) {
		if (status === 'SUCCEEDED') return CheckCircle2;
		if (status === 'FAILED') return XCircle;
		if (status === 'IN_PROGRESS') return RefreshCw;
		return AlertCircle;
	}

	function getBuildStatusColor(status: string | undefined): string {
		if (status === 'SUCCEEDED') return 'text-emerald-500';
		if (status === 'FAILED') return 'text-rose-500';
		if (status === 'IN_PROGRESS') return 'text-amber-500 animate-spin';
		return 'text-slate-400';
	}

	onMount(() => {
		loadProjects();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-blue-600/20 rounded-xl shadow-inner">
				<Cpu class="w-8 h-8 text-blue-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-blue-600 to-indigo-600 dark:from-blue-400 dark:to-indigo-400 bg-clip-text text-transparent italic tracking-tight">CodeBuild Operations</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">On-demand compilation, testing, and artifact generation in ephemeral container environments.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button 
				onclick={loadProjects}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh data"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button 
				onclick={() => showCreateModal = true}
				class="flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white rounded-xl font-black shadow-lg shadow-blue-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				Deploy Build Project
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Project List -->
		<div class="lg:col-span-3 space-y-4">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-4 bg-white/20 dark:bg-slate-900/10 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative w-full">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input 
							type="text" 
							bind:value={searchQuery}
							placeholder="Search projects..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-blue-500 outline-none transition-all italic font-bold"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !projects.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse"><div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div></div>
						{/each}
					{:else}
						{#each filteredProjects as project}
							<div 
								role="button"
								tabindex="0"
								onclick={() => selectProject(project)}
								onkeydown={(e) => e.key === 'Enter' && selectProject(project)}
								class="p-4 flex items-center justify-between hover:bg-blue-500/5 dark:hover:bg-blue-500/10 cursor-pointer transition-all {selectedProject?.arn === project.arn ? 'bg-blue-500/10 border-l-4 border-blue-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3">
									<Box class="w-4 h-4 text-blue-600" />
									<div>
										<div class="font-black text-slate-900 dark:text-white uppercase tracking-tighter italic text-xs truncate max-w-[150px]">{project.name}</div>
										<div class="text-[8px] text-slate-400 font-mono tracking-tighter truncate opacity-60 italic">{project.environment?.computeType}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300" />
							</div>
						{/each}

						{#if !projects.length}
							<div class="p-12 text-center text-slate-400 text-sm italic font-bold">No build projects found.</div>
						{/if}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-9 space-y-6">
			{#if selectedProject}
				<div class="grid grid-cols-1 lg:grid-cols-2 gap-6 items-start">
					<!-- Project Config -->
					<div class="space-y-6">
						<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden animate-in fade-in slide-in-from-right-4 duration-300">
							<div class="p-8 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-blue-500/5 to-indigo-500/5 flex justify-between items-start">
								<div>
									<h2 class="text-3xl font-black text-slate-900 dark:text-white mb-2 uppercase tracking-tighter italic leading-none">{selectedProject.name}</h2>
									<div class="flex items-center gap-3 mt-4">
										<div class="px-2 py-0.5 rounded-lg bg-blue-500/10 text-blue-600 text-[9px] font-black uppercase tracking-widest border border-blue-500/20">
											{selectedProject.environment?.type}
										</div>
										<div class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
											IDLE
										</div>
									</div>
								</div>
								<button 
									onclick={() => deleteProject(selectedProject?.name)}
									class="p-2.5 bg-slate-900 dark:bg-black text-rose-500 hover:bg-rose-500/10 rounded-2xl transition-all border border-rose-500/20 shadow-xl"
									title="Explode Project"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>

							<div class="p-8 space-y-8">
								<!-- Info Grid -->
								<div class="grid grid-cols-2 gap-4">
									<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/info">
										<div class="flex items-center gap-2 mb-2">
											<Server class="w-3.5 h-3.5 text-blue-500" />
											<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Provisioning</span>
										</div>
										<div class="text-[11px] font-black text-slate-800 dark:text-white uppercase truncate">{selectedProject.environment?.image || 'Standard v5.0'}</div>
										<div class="text-[8px] text-slate-500 uppercase font-bold tracking-tighter mt-1 italic">{selectedProject.environment?.computeType}</div>
									</div>
									<div class="p-6 bg-white/60 dark:bg-slate-900/60 rounded-[2rem] border border-slate-100 dark:border-slate-700/50 shadow-sm group/info">
										<div class="flex items-center gap-2 mb-2">
											<Workflow class="w-3.5 h-3.5 text-indigo-500" />
											<span class="text-[9px] font-black text-slate-400 uppercase tracking-widest italic">Source Origin</span>
										</div>
										<div class="text-[11px] font-black text-slate-800 dark:text-white uppercase">{selectedProject.source?.type}</div>
										<div class="text-[8px] text-slate-500 uppercase font-bold tracking-tighter mt-1 italic text-rose-500 truncate">{selectedProject.source?.location || 'DIRECT_UPLOAD'}</div>
									</div>
								</div>

								<!-- ARN -->
								<div class="pt-4">
									<div class="text-[9px] font-black text-slate-400 uppercase tracking-widest mb-2 italic px-1">Resource Blueprint (ARN)</div>
									<div class="p-4 bg-slate-900 dark:bg-black rounded-3xl border border-slate-800 shadow-inner group/arn">
										<div class="font-mono text-[10px] text-blue-400 break-all select-all flex items-start gap-3 lowercase italic opacity-80 group-hover:opacity-100 transition-opacity">
											<ArrowRight class="w-3.5 h-3.5 mt-0.5 shrink-0 text-slate-600 group-hover:text-blue-500 transition-colors" />
											{selectedProject.arn}
										</div>
									</div>
								</div>
							</div>
						</div>
					</div>

					<!-- Build History -->
					<div class="space-y-6">
						<div class="bg-slate-900 dark:bg-black rounded-[2.5rem] shadow-2xl border border-slate-800 overflow-hidden h-[600px] flex flex-col animate-in slide-in-from-right-8 duration-500">
							<div class="p-6 border-b border-white/5 bg-white/5 flex items-center justify-between">
								<div class="flex items-center gap-3">
									<History class="w-5 h-5 text-blue-500" />
									<div>
										<h4 class="text-xs font-black text-white uppercase tracking-widest italic leading-none">Job Execution History</h4>
										<span class="text-[8px] font-bold text-slate-500 uppercase">Ephemeral Provisioning Ledger</span>
									</div>
								</div>
								<div class="px-2 py-0.5 rounded-lg bg-white/5 border border-white/10 text-[9px] text-slate-500 font-mono italic">
									{builds.length} JOBS
								</div>
							</div>

							<div class="flex-1 overflow-y-auto p-8 space-y-4">
								{#if loadingDetails}
									<div class="flex flex-col items-center justify-center h-full opacity-30">
										<RefreshCw class="w-10 h-10 text-blue-500 animate-spin mb-4" />
										<span class="text-[9px] uppercase font-black text-slate-500 tracking-[0.2em]">Traversing History...</span>
									</div>
								{:else if builds.length}
									{#each builds as build}
										{@const StatusIcon = getBuildStatusIcon(build.buildStatus)}
										<div class="p-5 bg-white/5 rounded-3xl border border-white/10 hover:border-blue-500/30 transition-all group/job">
											<div class="flex justify-between items-start mb-4">
												<div>
													<div class="text-[9px] font-black text-slate-400 uppercase italic tracking-tighter mb-1 font-mono">{build.id?.split(':').pop()}</div>
													<div class="flex items-center gap-2">
														<StatusIcon class="w-3.5 h-3.5 {getBuildStatusColor(build.buildStatus)}" />
														<span class="text-[10px] font-black text-white uppercase tracking-widest">{build.buildStatus}</span>
													</div>
												</div>
												<div class="text-right">
													<div class="text-[8px] font-black text-slate-600 uppercase">Duration</div>
													<span class="text-[10px] font-black text-slate-400 tabular-nums italic">
														{Math.round(((build.endTime?.getTime() || 0) - (build.startTime?.getTime() || 0)) / 1000)}s
													</span>
												</div>
											</div>
											<div class="flex items-center justify-between pt-4 border-t border-white/5">
												<div class="flex items-center gap-2">
													<Timer class="w-3 h-3 text-slate-600" />
													<span class="text-[9px] font-bold text-slate-500 tabular-nums">{build.startTime?.toLocaleString()}</span>
												</div>
												<ExternalLink class="w-3 h-3 text-slate-700 opacity-0 group-hover/job:opacity-100 transition-opacity" />
											</div>
										</div>
									{/each}
								{:else}
									<div class="flex flex-col items-center justify-center h-full opacity-20 p-12 text-center">
										<Terminal class="w-16 h-16 text-slate-600 mb-6" />
										<h4 class="text-xs font-black text-slate-500 uppercase tracking-[0.2em] mb-2">No Executions</h4>
										<p class="text-[10px] text-slate-600 italic">This project hasn't processed any build requests yet. Trigger a manual execution or push source code to begin.</p>
									</div>
								{/if}
							</div>
						</div>

						<!-- Metric Overview -->
						<div class="p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-[2.5rem] shadow-xl flex items-center justify-between group/metric overflow-hidden relative">
							<div class="absolute inset-0 bg-gradient-to-r from-blue-500/5 to-transparent pointer-events-none"></div>
							<div class="flex items-center gap-4 relative z-10">
								<div class="p-3 bg-blue-500/10 rounded-2xl group-hover/metric:scale-110 transition-transform">
									<BarChart3 class="w-6 h-6 text-blue-600" />
								</div>
								<div>
									<div class="text-[9px] font-black text-slate-500 uppercase tracking-widest mb-1 italic leading-none">Success Throughput</div>
									<div class="text-lg font-black text-slate-900 dark:text-white uppercase tracking-tighter">OPTIMIZED</div>
								</div>
							</div>
							<div class="flex items-center gap-2 relative z-10">
								<div class="w-1 h-8 rounded-full bg-emerald-500/20 group-hover:bg-emerald-500/50 transition-colors h-1"></div>
								<div class="w-1 h-5 rounded-full bg-emerald-500/20 group-hover:bg-emerald-500/50 transition-colors h-2"></div>
								<div class="w-1 h-10 rounded-full bg-emerald-500/20 group-hover:bg-emerald-500/50 transition-colors h-3"></div>
							</div>
						</div>
					</div>
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-[3rem] p-32 text-center flex flex-col items-center gap-6">
					<div class="p-8 bg-slate-50 dark:bg-slate-800 rounded-[2.5rem]">
						<Cpu class="w-16 h-16 text-slate-200 dark:text-slate-700" />
					</div>
					<h3 class="text-2xl font-black text-slate-900 dark:text-white uppercase tracking-tighter italic">Ephemeral Provisioner</h3>
					<p class="text-slate-500 dark:text-slate-400 text-sm max-w-sm italic tracking-tight font-medium lowercase">Coordinate isolated builds, manage containerized environment blueprints, and monitor ephemeral execution lineages through high-resolution throughput analysis.</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div role="none" onclick={() => showCreateModal = false} onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)} class="absolute inset-0 bg-slate-900/60 backdrop-blur-sm"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-[2.5rem] shadow-2xl border border-blue-500/20 overflow-hidden animate-in zoom-in-95">
			<div class="p-8">
				<h3 class="text-2xl font-black text-slate-900 dark:text-white mb-6 uppercase tracking-tighter italic leading-none">Assemble Build Blueprint</h3>
				
				<form onsubmit={(e) => { e.preventDefault(); createProject(); }} class="space-y-6">
					<div>
						<label for="pName" class="block text-[10px] font-black text-slate-500 uppercase tracking-widest mb-2 px-1 italic leading-none">Project Identifier</label>
						<input 
							id="pName"
							type="text" 
							bind:value={newProjectName}
							placeholder="e.g. monolith-release-pipeline"
							class="w-full px-5 py-4 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-[1.5rem] outline-none focus:ring-2 focus:ring-blue-500 transition-all font-mono text-xs italic"
							required
						/>
					</div>

					<div class="p-5 bg-blue-500/5 rounded-2xl border border-blue-500/10">
						<div class="flex items-center gap-2 mb-2">
							<Shield class="w-3.5 h-3.5 text-blue-500" />
							<span class="text-[10px] font-black text-blue-600 uppercase tracking-widest leading-none">Security Baseline</span>
						</div>
						<p class="text-[9px] text-blue-800 dark:text-blue-400 leading-relaxed font-bold uppercase tracking-tight italic">
							Standard v5.0 container blueprint will be provisioned. Computational resources are isolated per execution context.
						</p>
					</div>

					<div class="flex gap-4 pt-4">
						<button type="button" onclick={() => showCreateModal = false} class="flex-1 px-4 py-4 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-2xl font-black uppercase text-[10px] tracking-widest transition-all">Abort</button>
						<button type="submit" disabled={creating} class="flex-1 px-4 py-4 bg-blue-600 text-white rounded-2xl font-black uppercase text-[10px] tracking-widest shadow-lg shadow-blue-600/20 active:scale-95 disabled:opacity-50 transition-all">
							{creating ? 'Assembling...' : 'Deploy Blueprint'}
						</button>
					</div>
				</form>
			</div>
		</div>
	</div>
{/if}

<style>
	/* Custom scrollbar */
	::-webkit-scrollbar {
		width: 6px;
	}
	::-webkit-scrollbar-track {
		background: transparent;
	}
	::-webkit-scrollbar-thumb {
		background: rgba(148, 163, 184, 0.1);
		border-radius: 10px;
	}
	::-webkit-scrollbar-thumb:hover {
		background: rgba(148, 163, 184, 0.2);
	}
</style>
