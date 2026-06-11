<script lang="ts">
	import { onMount } from 'svelte';
	import { getPollyClient } from '$lib/aws-client';
	import {
		DescribeVoicesCommand,
		ListLexiconsCommand,
		ListSpeechSynthesisTasksCommand,
		SynthesizeSpeechCommand,
		GetLexiconCommand,
		PutLexiconCommand,
		DeleteLexiconCommand,
			type VoiceId,
		type Voice,
		type LexiconDescription,
		type SynthesisTask,
		type PollyClient
	} from '@aws-sdk/client-polly';
	import { toast } from 'svelte-sonner';
	import { Mic, RefreshCw, Search, BookOpen, Activity, Play, Plus, Pencil, Trash2, X } from 'lucide-svelte';

	let pollyClient: PollyClient | undefined;
	function polly(): PollyClient {
		return (pollyClient ??= getPollyClient());
	}

	// Lexicons applied to the synthesize demo ("test pronunciation").
	let selectedLexicons = $state<string[]>([]);
	function toggleSynthLexicon(name: string) {
		selectedLexicons = selectedLexicons.includes(name)
			? selectedLexicons.filter((n) => n !== name)
			: [...selectedLexicons, name];
	}

	let loading = $state(false);
	let activeTab = $state<'voices' | 'lexicons' | 'tasks'>('voices');
	let searchQuery = $state('');
	let languageFilter = $state('en-US');
	let voices = $state<Voice[]>([]);
	let lexicons = $state<LexiconDescription[]>([]);
	let tasks = $state<SynthesisTask[]>([]);

	let textToSynthesize = $state('Hello from Amazon Polly! This is a text-to-speech demonstration.');
	let selectedVoiceId = $state<VoiceId>('Joanna');
	let outputFormat = $state<'mp3' | 'ogg_vorbis' | 'pcm'>('mp3');
	let synthesizing = $state(false);

	const formatMime: Record<string, string> = {
		mp3: 'audio/mpeg',
		ogg_vorbis: 'audio/ogg',
		pcm: 'audio/wave'
	};

	const filteredVoices = $derived(voices.filter((v) => (v.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredLexicons = $derived(lexicons.filter((l) => (l.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const voiceLanguages = $derived([...new Set(voices.map((v) => v.LanguageCode ?? '').filter(Boolean))].toSorted());
	const filteredByLanguage = $derived(languageFilter ? filteredVoices.filter((v) => v.LanguageCode === languageFilter) : filteredVoices);

	async function loadData() {
		loading = true;
		try {
			const [voiceResp, lexResp, taskResp] = await Promise.all([
				polly().send(new DescribeVoicesCommand({})),
				polly().send(new ListLexiconsCommand({})),
				polly().send(new ListSpeechSynthesisTasksCommand({}))
			]);
			voices = voiceResp.Voices ?? [];
			lexicons = lexResp.Lexicons ?? [];
			tasks = taskResp.SynthesisTasks ?? [];
		} catch (e) {
			toast.error('Failed to load Polly data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function pcmToWav(pcm: Uint8Array, sampleRate: number): ArrayBuffer {
		const numChannels = 1;
		const bitsPerSample = 16;
		const blockAlign = (numChannels * bitsPerSample) / 8;
		const byteRate = sampleRate * blockAlign;
		const buffer = new ArrayBuffer(44 + pcm.length);
		const view = new DataView(buffer);
		const writeStr = (offset: number, s: string) => {
			for (let i = 0; i < s.length; i++) view.setUint8(offset + i, s.codePointAt(i) ?? 0);
		};
		writeStr(0, 'RIFF');
		view.setUint32(4, 36 + pcm.length, true);
		writeStr(8, 'WAVE');
		writeStr(12, 'fmt ');
		view.setUint32(16, 16, true);
		view.setUint16(20, 1, true);
		view.setUint16(22, numChannels, true);
		view.setUint32(24, sampleRate, true);
		view.setUint32(28, byteRate, true);
		view.setUint16(32, blockAlign, true);
		view.setUint16(34, bitsPerSample, true);
		writeStr(36, 'data');
		view.setUint32(40, pcm.length, true);
		new Uint8Array(buffer, 44).set(pcm);
		return buffer;
	}

	async function synthesizeSpeech() {
		if (!textToSynthesize.trim()) {
			toast.error('Please enter text to synthesize');
			return;
		}
		synthesizing = true;
		try {
			const resp = await polly().send(new SynthesizeSpeechCommand({
				Text: textToSynthesize,
				VoiceId: selectedVoiceId,
				OutputFormat: outputFormat,
				// Apply selected pronunciation lexicons (test pronunciation).
				LexiconNames: selectedLexicons.length > 0 ? selectedLexicons : undefined,
				// PCM stream is 16-bit signed little-endian; default sample rate 16000 for pcm.
				SampleRate: outputFormat === 'pcm' ? '16000' : undefined
			}));
			if (resp.AudioStream) {
				const chunks: Uint8Array[] = [];
				const reader = (resp.AudioStream as ReadableStream<Uint8Array>).getReader?.();
				if (reader) {
					while (true) {
						const { done, value } = await reader.read();
						if (done) break;
						if (value) chunks.push(value);
					}
					let parts = chunks as unknown as BlobPart[];
					if (outputFormat === 'pcm') {
						// Wrap raw PCM in a WAV container so the browser can play it.
						const total = chunks.reduce((n, c) => n + c.length, 0);
						const pcm = new Uint8Array(total);
						let off = 0;
						for (const c of chunks) { pcm.set(c, off); off += c.length; }
						parts = [pcmToWav(pcm, 16000)];
					}
					const blob = new Blob(parts, { type: formatMime[outputFormat] });
					const url = URL.createObjectURL(blob);
					const audio = new Audio(url);
					audio.play();
					toast.success(`Playing ${outputFormat.toUpperCase()} speech`);
				}
			}
		} catch (e) {
			toast.error('Failed to synthesize speech: ' + String(e));
		} finally {
			synthesizing = false;
		}
	}

	// ── Lexicon editor (PutLexicon / GetLexicon / DeleteLexicon) ────────────
	const SAMPLE_LEXICON = `<?xml version="1.0" encoding="UTF-8"?>
<lexicon version="1.0"
      xmlns="http://www.w3.org/2005/01/pronunciation-lexicon"
      xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
      xsi:schemaLocation="http://www.w3.org/2005/01/pronunciation-lexicon
        http://www.w3.org/TR/2007/CR-pronunciation-lexicon-20071212/pls.xsd"
      alphabet="ipa" xml:lang="en-US">
  <lexeme>
    <grapheme>W3C</grapheme>
    <alias>World Wide Web Consortium</alias>
  </lexeme>
</lexicon>`;

	let showLexiconModal = $state(false);
	let lexiconModalMode = $state<'create' | 'edit'>('create');
	let lexiconName = $state('');
	let lexiconContent = $state('');
	let savingLexicon = $state(false);
	let loadingLexicon = $state(false);
	let deletingLexicon = $state<string | null>(null);

	function openCreateLexicon() {
		lexiconModalMode = 'create';
		lexiconName = '';
		lexiconContent = SAMPLE_LEXICON;
		showLexiconModal = true;
	}

	async function openEditLexicon(name: string) {
		lexiconModalMode = 'edit';
		lexiconName = name;
		lexiconContent = '';
		loadingLexicon = true;
		showLexiconModal = true;
		try {
			const resp = await polly().send(new GetLexiconCommand({ Name: name }));
			lexiconContent = resp.Lexicon?.Content ?? '';
		} catch (e) {
			toast.error('Failed to load lexicon: ' + String(e));
		} finally {
			loadingLexicon = false;
		}
	}

	async function saveLexicon() {
		if (!lexiconName.trim()) {
			toast.error('Lexicon name is required');
			return;
		}
		if (!lexiconContent.trim()) {
			toast.error('Lexicon content is required');
			return;
		}
		savingLexicon = true;
		try {
			await polly().send(new PutLexiconCommand({ Name: lexiconName.trim(), Content: lexiconContent }));
			toast.success(`Lexicon "${lexiconName.trim()}" saved`);
			showLexiconModal = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to save lexicon: ' + String(e));
		} finally {
			savingLexicon = false;
		}
	}

	async function deleteLexicon(name: string) {
		deletingLexicon = name;
		try {
			await polly().send(new DeleteLexiconCommand({ Name: name }));
			toast.success(`Lexicon "${name}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete lexicon: ' + String(e));
		} finally {
			deletingLexicon = null;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Mic class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Polly</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Turn text into lifelike speech</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg"><Mic class="w-5 h-5 text-teal-600 dark:text-teal-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{voices.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Voices</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><BookOpen class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{lexicons.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Lexicons</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{tasks.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Synthesis Tasks</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white flex items-center gap-2">
			<Play class="w-5 h-5 text-teal-500" /> Text-to-Speech Demo
		</h2>
		<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
			<div>
				<label for="polly-voice" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Voice</label>
				<select id="polly-voice" bind:value={selectedVoiceId} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm">
					{#each ['Joanna', 'Matthew', 'Ivy', 'Kendra', 'Kevin', 'Kimberly', 'Salli', 'Joey', 'Justin', 'Nicole', 'Russell', 'Amy', 'Brian', 'Emma'] as voice}
						<option value={voice}>{voice}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="polly-format" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Output Format</label>
				<select id="polly-format" bind:value={outputFormat} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm">
					<option value="mp3">MP3</option>
					<option value="ogg_vorbis">Ogg Vorbis</option>
					<option value="pcm">PCM (16-bit, 16 kHz)</option>
				</select>
			</div>
			<div class="sm:col-span-2">
				<label for="polly-text" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Text</label>
				<textarea id="polly-text" bind:value={textToSynthesize} rows={2} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm resize-none"></textarea>
			</div>
			{#if lexicons.length > 0}
				<div class="sm:col-span-2">
					<span class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Apply Lexicons (test pronunciation)</span>
					<div class="flex flex-wrap gap-2">
						{#each lexicons as lex}
							<button type="button" onclick={() => lex.Name && toggleSynthLexicon(lex.Name)}
								class="px-2.5 py-1 text-xs rounded-full border {selectedLexicons.includes(lex.Name ?? '') ? 'bg-teal-600 text-white border-teal-600' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 border-gray-200 dark:border-slate-600'}">
								{lex.Name}
							</button>
						{/each}
					</div>
				</div>
			{/if}
		</div>
		<button onclick={synthesizeSpeech} disabled={synthesizing} class="flex items-center gap-2 px-4 py-2 bg-teal-500 hover:bg-teal-600 disabled:opacity-50 text-white rounded-lg text-sm font-medium">
			<Play class="w-4 h-4" /> {synthesizing ? 'Synthesizing...' : 'Synthesize & Play'}
		</button>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between items-start sm:items-center">
			<div class="flex gap-2">
				{#each [['voices', 'Voices'], ['lexicons', 'Lexicons'], ['tasks', 'Tasks']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-teal-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex gap-2">
				{#if activeTab === 'voices'}
					<select bind:value={languageFilter} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="">All Languages</option>
						{#each voiceLanguages as lang}
							<option value={lang}>{lang}</option>
						{/each}
					</select>
				{/if}
				{#if activeTab === 'lexicons'}
					<button onclick={openCreateLexicon}
						class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
						<Plus class="w-4 h-4" />
						New Lexicon
					</button>
				{/if}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
				</div>
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'voices'}
				{#if filteredByLanguage.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No voices found</div>
				{:else}
					<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-2">
						{#each filteredByLanguage as voice}
							<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center justify-between mb-1">
									<p class="font-medium text-gray-900 dark:text-white">{voice.Name}</p>
									<span class="text-xs px-1.5 py-0.5 rounded bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-400">{voice.Gender}</span>
								</div>
								<p class="text-xs text-gray-500 dark:text-gray-400">{voice.LanguageName} · {voice.LanguageCode}</p>
								{#if voice.SupportedEngines && voice.SupportedEngines.length > 0}
									<div class="flex gap-1 mt-1">
										{#each voice.SupportedEngines as engine}
											<span class="text-xs px-1 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400">{engine}</span>
										{/each}
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'lexicons'}
				{#if filteredLexicons.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No lexicons found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredLexicons as lexicon}
							<div class="flex items-center justify-between gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<BookOpen class="w-5 h-5 text-blue-500 flex-shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{lexicon.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{lexicon.Attributes?.Alphabet ?? ''} · {lexicon.Attributes?.LanguageCode ?? ''} · {lexicon.Attributes?.LexemesCount ?? 0} lexemes</p>
									</div>
								</div>
								<div class="flex items-center gap-1.5 flex-shrink-0">
									<button onclick={() => lexicon.Name && openEditLexicon(lexicon.Name)}
										class="p-1.5 text-gray-400 hover:text-teal-600 dark:hover:text-teal-400 transition-colors"
										title="View / edit lexicon" aria-label="View / edit lexicon">
										<Pencil class="w-4 h-4" />
									</button>
									<button onclick={() => lexicon.Name && deleteLexicon(lexicon.Name)}
										disabled={deletingLexicon === lexicon.Name}
										class="p-1.5 text-gray-400 hover:text-red-600 dark:hover:text-red-400 transition-colors disabled:opacity-50"
										title="Delete lexicon" aria-label="Delete lexicon">
										{#if deletingLexicon === lexicon.Name}
											<div class="w-4 h-4 animate-spin rounded-full border-b-2 border-red-500"></div>
										{:else}
											<Trash2 class="w-4 h-4" />
										{/if}
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'tasks'}
				{#if tasks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No synthesis tasks found</div>
				{:else}
					<div class="space-y-2">
						{#each tasks as task}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="text-sm text-gray-900 dark:text-white">{task.TaskId}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{task.TaskStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Lexicon Editor Modal -->
{#if showLexiconModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showLexiconModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showLexiconModal = false)} role="dialog" aria-modal="true">
<div class="relative w-full max-w-2xl" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-800">
<div class="flex items-center justify-between p-4 border-b border-slate-200 dark:border-slate-700">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">{lexiconModalMode === 'create' ? 'New Lexicon' : `Edit Lexicon: ${lexiconName}`}</h3>
<button aria-label="Close" onclick={() => { showLexiconModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-700 dark:hover:text-white"><X class="w-4 h-4" /></button>
</div>
<div class="p-4 space-y-4">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); saveLexicon(); }}>
<div>
<label for="lexicon-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Name</label>
<input type="text" id="lexicon-name" bind:value={lexiconName} disabled={lexiconModalMode === 'edit'} placeholder="myLexicon" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-teal-500 focus:border-teal-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white disabled:opacity-60" />
</div>
<div>
<label for="lexicon-content" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">PLS Content <span class="text-slate-400 font-normal">(Pronunciation Lexicon Specification XML)</span></label>
{#if loadingLexicon}
<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading lexicon...</div>
{:else}
<textarea id="lexicon-content" bind:value={lexiconContent} rows="14" spellcheck="false" class="bg-slate-50 border border-slate-300 text-slate-900 text-xs font-mono rounded-lg focus:ring-teal-500 focus:border-teal-500 block w-full p-2.5 dark:bg-slate-900 dark:border-slate-600 dark:text-slate-200"></textarea>
{/if}
</div>
<div class="flex gap-3 justify-end pt-2">
<button type="button" onclick={() => { showLexiconModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="submit" disabled={savingLexicon || loadingLexicon} class="text-white bg-teal-600 hover:bg-teal-700 focus:ring-4 focus:ring-teal-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{savingLexicon ? 'Saving...' : 'Save Lexicon'}
</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}
