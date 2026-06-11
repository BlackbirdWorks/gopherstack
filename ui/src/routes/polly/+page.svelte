<script lang="ts">
	import { onMount } from 'svelte';
	import { getPollyClient } from '$lib/aws-client';
	import {
		DescribeVoicesCommand,
		ListLexiconsCommand,
		ListSpeechSynthesisTasksCommand,
		SynthesizeSpeechCommand,
			type VoiceId,
		type Voice,
		type LexiconDescription,
		type SynthesisTask
	} from '@aws-sdk/client-polly';
	import { toast } from 'svelte-sonner';
	import { Mic, RefreshCw, Search, BookOpen, Activity, Play } from 'lucide-svelte';

	const polly = getPollyClient();

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
				polly.send(new DescribeVoicesCommand({})),
				polly.send(new ListLexiconsCommand({})),
				polly.send(new ListSpeechSynthesisTasksCommand({}))
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
			const resp = await polly.send(new SynthesizeSpeechCommand({
				Text: textToSynthesize,
				VoiceId: selectedVoiceId,
				OutputFormat: outputFormat,
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
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<BookOpen class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{lexicon.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{lexicon.Attributes?.LexiconArn}</p>
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
