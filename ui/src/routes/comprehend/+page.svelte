<script lang="ts">
	// Comprehend mixes three real shapes that don't fit one template:
	//   - real-time detection calls (Detect*) -- request/response, no state
	//   - 5 resource families (DocumentClassifier/EntityRecognizer/Endpoint/
	//     Flywheel/Dataset) -- CRUD-ish, but NOT uniform: Dataset has no
	//     Delete (datasets are immutable once created -- confirmed against
	//     the SDK's command list, no DeleteDataset), and only Endpoint/
	//     Flywheel have Update. DocumentClassifier/EntityRecognizer have no
	//     Update at all (a new version is a new Create with the same name).
	//   - 9 async job families sharing Start/Describe/List, but only 7 of the
	//     9 have a real Stop (DocumentClassificationJob and TopicsDetectionJob
	//     cannot be cancelled once started -- see services/comprehend/
	//     handler_jobs.go's jobSpec.noStop, which this UI mirrors instead of
	//     rendering a Stop button that would 400).
	// The 9 job families share one "Jobs" tab with a family selector (like
	// App Mesh's mesh selector) rather than 9 near-identical tabs.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getComprehendClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListDocumentClassifiersCommand,
		CreateDocumentClassifierCommand,
		DeleteDocumentClassifierCommand,
		ListEntityRecognizersCommand,
		CreateEntityRecognizerCommand,
		DeleteEntityRecognizerCommand,
		ListEndpointsCommand,
		CreateEndpointCommand,
		DescribeEndpointCommand,
		UpdateEndpointCommand,
		DeleteEndpointCommand,
		ListFlywheelsCommand,
		CreateFlywheelCommand,
		DescribeFlywheelCommand,
		UpdateFlywheelCommand,
		DeleteFlywheelCommand,
		StartFlywheelIterationCommand,
		ListFlywheelIterationHistoryCommand,
		ListDatasetsCommand,
		CreateDatasetCommand,
		DescribeDatasetCommand,
		DetectSentimentCommand,
		DetectEntitiesCommand,
		DetectKeyPhrasesCommand,
		DetectDominantLanguageCommand,
		DetectPiiEntitiesCommand,
		DetectSyntaxCommand,
		DetectToxicContentCommand,
		StartDocumentClassificationJobCommand,
		DescribeDocumentClassificationJobCommand,
		ListDocumentClassificationJobsCommand,
		StartEntitiesDetectionJobCommand,
		DescribeEntitiesDetectionJobCommand,
		ListEntitiesDetectionJobsCommand,
		StopEntitiesDetectionJobCommand,
		StartKeyPhrasesDetectionJobCommand,
		DescribeKeyPhrasesDetectionJobCommand,
		ListKeyPhrasesDetectionJobsCommand,
		StopKeyPhrasesDetectionJobCommand,
		StartSentimentDetectionJobCommand,
		DescribeSentimentDetectionJobCommand,
		ListSentimentDetectionJobsCommand,
		StopSentimentDetectionJobCommand,
		StartPiiEntitiesDetectionJobCommand,
		DescribePiiEntitiesDetectionJobCommand,
		ListPiiEntitiesDetectionJobsCommand,
		StopPiiEntitiesDetectionJobCommand,
		StartTopicsDetectionJobCommand,
		DescribeTopicsDetectionJobCommand,
		ListTopicsDetectionJobsCommand,
		StartTargetedSentimentDetectionJobCommand,
		DescribeTargetedSentimentDetectionJobCommand,
		ListTargetedSentimentDetectionJobsCommand,
		StopTargetedSentimentDetectionJobCommand,
		StartDominantLanguageDetectionJobCommand,
		DescribeDominantLanguageDetectionJobCommand,
		ListDominantLanguageDetectionJobsCommand,
		StopDominantLanguageDetectionJobCommand,
		StartEventsDetectionJobCommand,
		DescribeEventsDetectionJobCommand,
		ListEventsDetectionJobsCommand,
		StopEventsDetectionJobCommand,
		LanguageCode,
		PiiEntitiesDetectionMode,
		ModelType,
		DatasetType,
		type DocumentClassifierProperties,
		type EntityRecognizerProperties,
		type EndpointProperties,
		type FlywheelSummary,
		type FlywheelProperties,
		type FlywheelIterationProperties,
		type DatasetProperties,
		type SentimentType,
		type SentimentScore,
		type Entity,
		type KeyPhrase,
		type PiiEntity,
		type SyntaxToken,
		type ToxicContent
	} from '@aws-sdk/client-comprehend';
	import { toast } from 'svelte-sonner';
	import {
		MessageSquare,
		FileText,
		Tag,
		Activity,
		Play,
		ChevronDown,
		ChevronRight,
		GitCompare,
		Plus,
		Trash2,
		Eye,
		Pencil,
		RotateCw
	} from 'lucide-svelte';

	const client = regionalClient(getComprehendClient);

	type TabId = 'detection' | 'classifiers' | 'recognizers' | 'endpoints' | 'flywheels' | 'datasets' | 'jobs';

	const tabs: TabDef[] = [
		{ id: 'detection', label: 'Detection' },
		{ id: 'classifiers', label: 'Classifiers' },
		{ id: 'recognizers', label: 'Entity Recognizers' },
		{ id: 'endpoints', label: 'Endpoints' },
		{ id: 'flywheels', label: 'Flywheels' },
		{ id: 'datasets', label: 'Datasets' },
		{ id: 'jobs', label: 'Jobs' }
	];

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

	function parseJson(json: string): Record<string, unknown> {
		const trimmed = json.trim();
		return trimmed ? JSON.parse(trimmed) : {};
	}

	let activeTab = $state<TabId>('detection');
	let searchQuery = $state('');

	// --- Classifiers ---
	let classifiers = $state<DocumentClassifierProperties[]>([]);
	async function fetchClassifiers(): Promise<void> {
		const resp = await client().send(new ListDocumentClassifiersCommand({}));
		classifiers = resp.DocumentClassifierPropertiesList ?? [];
	}

	// --- Entity Recognizers ---
	let recognizers = $state<EntityRecognizerProperties[]>([]);
	async function fetchRecognizers(): Promise<void> {
		const resp = await client().send(new ListEntityRecognizersCommand({}));
		recognizers = resp.EntityRecognizerPropertiesList ?? [];
	}

	// --- Endpoints ---
	let endpoints = $state<EndpointProperties[]>([]);
	async function fetchEndpoints(): Promise<void> {
		const resp = await client().send(new ListEndpointsCommand({}));
		endpoints = resp.EndpointPropertiesList ?? [];
	}

	// --- Flywheels ---
	let flywheels = $state<FlywheelSummary[]>([]);
	async function fetchFlywheels(): Promise<void> {
		const resp = await client().send(new ListFlywheelsCommand({}));
		flywheels = resp.FlywheelSummaryList ?? [];
	}

	// --- Datasets ---
	let datasets = $state<DatasetProperties[]>([]);
	async function fetchDatasets(): Promise<void> {
		const resp = await client().send(new ListDatasetsCommand({}));
		datasets = resp.DatasetPropertiesList ?? [];
	}

	// --- Jobs (9 families sharing one tab) ---
	type JobFamily = {
		id: string;
		label: string;
		hasLanguageCode: boolean;
		hasDocumentClassifierArn: boolean;
		hasEntityRecognizerArn: boolean;
		hasFlywheelArn: boolean;
		hasPiiMode: boolean;
		hasNumberOfTopics: boolean;
		hasTargetEventTypes: boolean;
		noStop: boolean;
		objectField: string;
		listField: string;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		startCmd: new (input: any) => any;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		describeCmd: new (input: any) => any;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		listCmd: new (input: any) => any;
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		stopCmd?: new (input: any) => any;
	};

	const jobFamilies: JobFamily[] = [
		{
			id: 'DocumentClassificationJob',
			label: 'Document Classification',
			hasLanguageCode: false,
			hasDocumentClassifierArn: true,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: true,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: true,
			objectField: 'DocumentClassificationJobProperties',
			listField: 'DocumentClassificationJobPropertiesList',
			startCmd: StartDocumentClassificationJobCommand,
			describeCmd: DescribeDocumentClassificationJobCommand,
			listCmd: ListDocumentClassificationJobsCommand
		},
		{
			id: 'EntitiesDetectionJob',
			label: 'Entities Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: true,
			hasFlywheelArn: true,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'EntitiesDetectionJobProperties',
			listField: 'EntitiesDetectionJobPropertiesList',
			startCmd: StartEntitiesDetectionJobCommand,
			describeCmd: DescribeEntitiesDetectionJobCommand,
			listCmd: ListEntitiesDetectionJobsCommand,
			stopCmd: StopEntitiesDetectionJobCommand
		},
		{
			id: 'KeyPhrasesDetectionJob',
			label: 'Key Phrases Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'KeyPhrasesDetectionJobProperties',
			listField: 'KeyPhrasesDetectionJobPropertiesList',
			startCmd: StartKeyPhrasesDetectionJobCommand,
			describeCmd: DescribeKeyPhrasesDetectionJobCommand,
			listCmd: ListKeyPhrasesDetectionJobsCommand,
			stopCmd: StopKeyPhrasesDetectionJobCommand
		},
		{
			id: 'SentimentDetectionJob',
			label: 'Sentiment Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'SentimentDetectionJobProperties',
			listField: 'SentimentDetectionJobPropertiesList',
			startCmd: StartSentimentDetectionJobCommand,
			describeCmd: DescribeSentimentDetectionJobCommand,
			listCmd: ListSentimentDetectionJobsCommand,
			stopCmd: StopSentimentDetectionJobCommand
		},
		{
			id: 'PiiEntitiesDetectionJob',
			label: 'PII Entities Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: true,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'PiiEntitiesDetectionJobProperties',
			listField: 'PiiEntitiesDetectionJobPropertiesList',
			startCmd: StartPiiEntitiesDetectionJobCommand,
			describeCmd: DescribePiiEntitiesDetectionJobCommand,
			listCmd: ListPiiEntitiesDetectionJobsCommand,
			stopCmd: StopPiiEntitiesDetectionJobCommand
		},
		{
			id: 'TopicsDetectionJob',
			label: 'Topics Detection',
			hasLanguageCode: false,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: true,
			hasTargetEventTypes: false,
			noStop: true,
			objectField: 'TopicsDetectionJobProperties',
			listField: 'TopicsDetectionJobPropertiesList',
			startCmd: StartTopicsDetectionJobCommand,
			describeCmd: DescribeTopicsDetectionJobCommand,
			listCmd: ListTopicsDetectionJobsCommand
		},
		{
			id: 'TargetedSentimentDetectionJob',
			label: 'Targeted Sentiment Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'TargetedSentimentDetectionJobProperties',
			listField: 'TargetedSentimentDetectionJobPropertiesList',
			startCmd: StartTargetedSentimentDetectionJobCommand,
			describeCmd: DescribeTargetedSentimentDetectionJobCommand,
			listCmd: ListTargetedSentimentDetectionJobsCommand,
			stopCmd: StopTargetedSentimentDetectionJobCommand
		},
		{
			id: 'DominantLanguageDetectionJob',
			label: 'Dominant Language Detection',
			hasLanguageCode: false,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: false,
			noStop: false,
			objectField: 'DominantLanguageDetectionJobProperties',
			listField: 'DominantLanguageDetectionJobPropertiesList',
			startCmd: StartDominantLanguageDetectionJobCommand,
			describeCmd: DescribeDominantLanguageDetectionJobCommand,
			listCmd: ListDominantLanguageDetectionJobsCommand,
			stopCmd: StopDominantLanguageDetectionJobCommand
		},
		{
			id: 'EventsDetectionJob',
			label: 'Events Detection',
			hasLanguageCode: true,
			hasDocumentClassifierArn: false,
			hasEntityRecognizerArn: false,
			hasFlywheelArn: false,
			hasPiiMode: false,
			hasNumberOfTopics: false,
			hasTargetEventTypes: true,
			noStop: false,
			objectField: 'EventsDetectionJobProperties',
			listField: 'EventsDetectionJobPropertiesList',
			startCmd: StartEventsDetectionJobCommand,
			describeCmd: DescribeEventsDetectionJobCommand,
			listCmd: ListEventsDetectionJobsCommand,
			stopCmd: StopEventsDetectionJobCommand
		}
	];

	let selectedJobFamilyId = $state(jobFamilies[0].id);
	const selectedJobFamily = $derived(jobFamilies.find((f) => f.id === selectedJobFamilyId) ?? jobFamilies[0]);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let jobs = $state<any[]>([]);

	async function fetchJobs(): Promise<void> {
		const fam = untrack(() => selectedJobFamily);
		const resp = await client().send(new fam.listCmd({}));
		jobs = (resp as unknown as Record<string, unknown>)[fam.listField] as unknown[] ?? [];
	}

	function changeJobFamily(): void {
		tabLoader.refresh('jobs');
	}

	const tabLoader = createTabLoader<TabId>({
		detection: () => Promise.resolve(),
		classifiers: () => fetchClassifiers().catch(rethrowDescribed),
		recognizers: () => fetchRecognizers().catch(rethrowDescribed),
		endpoints: () => fetchEndpoints().catch(rethrowDescribed),
		flywheels: () => fetchFlywheels().catch(rethrowDescribed),
		datasets: () => fetchDatasets().catch(rethrowDescribed),
		jobs: () => fetchJobs().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		viewedEndpoint = null;
		viewedFlywheel = null;
		viewedDataset = null;
		viewedJob = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	function matches(q: string, ...parts: (string | undefined)[]): boolean {
		if (!q) return true;
		return parts.some((p) => (p ?? '').toLowerCase().includes(q.toLowerCase()));
	}

	const filteredClassifiers = $derived(classifiers.filter((c) => matches(searchQuery, c.DocumentClassifierArn)));
	const filteredRecognizers = $derived(recognizers.filter((r) => matches(searchQuery, r.EntityRecognizerArn)));
	const filteredEndpoints = $derived(endpoints.filter((e) => matches(searchQuery, e.EndpointArn)));
	const filteredFlywheels = $derived(flywheels.filter((f) => matches(searchQuery, f.FlywheelArn)));
	const filteredDatasets = $derived(datasets.filter((d) => matches(searchQuery, d.DatasetName)));
	const filteredJobs = $derived(jobs.filter((j) => matches(searchQuery, j.JobId, j.JobName)));

	// Training-metrics expansion + model-version comparison (classifiers).
	let expandedClassifier = $state<string | null>(null);
	let expandedRecognizer = $state<string | null>(null);
	let compareSelection = $state<string[]>([]);
	let showCompare = $state(false);

	function classifierMetrics(c: DocumentClassifierProperties) {
		const m = c.ClassifierMetadata?.EvaluationMetrics;
		return [
			['Accuracy', m?.Accuracy],
			['Precision', m?.Precision],
			['Recall', m?.Recall],
			['F1 Score', m?.F1Score],
			['Micro F1', m?.MicroF1Score],
			['Hamming Loss', m?.HammingLoss]
		] as [string, number | undefined][];
	}

	function toggleCompare(arn: string) {
		compareSelection = compareSelection.includes(arn) ? compareSelection.filter((a) => a !== arn) : [...compareSelection, arn];
	}

	const compareRows = $derived(classifiers.filter((c) => compareSelection.includes(c.DocumentClassifierArn ?? '')));

	// --- Inference tester ---
	let testText = $state('');
	let testLang = $state('en');
	let testOp = $state<'sentiment' | 'entities' | 'keyphrases' | 'language' | 'pii' | 'syntax' | 'toxicity'>('sentiment');
	let inferring = $state(false);
	let sentimentResult = $state<{ sentiment?: SentimentType; score?: SentimentScore } | null>(null);
	let entitiesResult = $state<Entity[] | null>(null);
	let keyPhrasesResult = $state<KeyPhrase[] | null>(null);
	let languageResult = $state<{ LanguageCode?: string; Score?: number }[] | null>(null);
	let piiResult = $state<PiiEntity[] | null>(null);
	let syntaxResult = $state<SyntaxToken[] | null>(null);
	let toxicityResult = $state<ToxicContent[] | null>(null);

	function clearResults() {
		sentimentResult = null;
		entitiesResult = null;
		keyPhrasesResult = null;
		languageResult = null;
		piiResult = null;
		syntaxResult = null;
		toxicityResult = null;
	}

	async function runInference() {
		if (!testText.trim()) {
			toast.error('Enter text to analyze');
			return;
		}
		inferring = true;
		clearResults();
		try {
			const lang = testLang as LanguageCode;
			if (testOp === 'sentiment') {
				const r = await client().send(new DetectSentimentCommand({ Text: testText, LanguageCode: lang }));
				sentimentResult = { sentiment: r.Sentiment, score: r.SentimentScore };
			} else if (testOp === 'entities') {
				const r = await client().send(new DetectEntitiesCommand({ Text: testText, LanguageCode: lang }));
				entitiesResult = r.Entities ?? [];
			} else if (testOp === 'keyphrases') {
				const r = await client().send(new DetectKeyPhrasesCommand({ Text: testText, LanguageCode: lang }));
				keyPhrasesResult = r.KeyPhrases ?? [];
			} else if (testOp === 'pii') {
				const r = await client().send(new DetectPiiEntitiesCommand({ Text: testText, LanguageCode: lang }));
				piiResult = r.Entities ?? [];
			} else if (testOp === 'syntax') {
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				const r = await client().send(new DetectSyntaxCommand({ Text: testText, LanguageCode: lang as any }));
				syntaxResult = r.SyntaxTokens ?? [];
			} else if (testOp === 'toxicity') {
				const r = await client().send(new DetectToxicContentCommand({ TextSegments: [{ Text: testText }], LanguageCode: lang }));
				toxicityResult = r.ResultList?.[0]?.Labels ?? [];
			} else {
				const r = await client().send(new DetectDominantLanguageCommand({ Text: testText }));
				languageResult = r.Languages ?? [];
			}
		} catch (e) {
			toast.error('Inference failed: ' + describeError(e));
		} finally {
			inferring = false;
		}
	}

	const pct = (v: number | undefined) => (v === undefined || v === null ? '-' : (v * 100).toFixed(1) + '%');

	// ── Classifiers: create / delete (no Update op exists) ──────────────────
	let createClassifierModal = $state<Modal | null>(null);
	let creatingClassifier = $state(false);
	let createClassifierError = $state<string | null>(null);
	let newClassifierName = $state('');
	let newClassifierRoleArn = $state('');
	let newClassifierLanguage = $state<string>(LanguageCode.EN);
	let newClassifierInputJson = $state('');

	function openCreateClassifierModal(): void {
		createClassifierError = null;
		newClassifierName = '';
		newClassifierRoleArn = '';
		newClassifierLanguage = LanguageCode.EN;
		newClassifierInputJson = '';
		createClassifierModal?.open();
	}

	async function submitCreateClassifier(): Promise<void> {
		if (!newClassifierName || !newClassifierRoleArn) {
			createClassifierError = 'Name and data access role ARN are required.';
			return;
		}
		let inputDataConfig: Record<string, unknown>;
		try {
			inputDataConfig = parseJson(newClassifierInputJson);
		} catch {
			createClassifierError = 'Input data config must be valid JSON.';
			return;
		}
		creatingClassifier = true;
		createClassifierError = null;
		try {
			await client().send(
				new CreateDocumentClassifierCommand({
					DocumentClassifierName: newClassifierName,
					DataAccessRoleArn: newClassifierRoleArn,
					LanguageCode: newClassifierLanguage as LanguageCode,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					InputDataConfig: inputDataConfig as any
				})
			);
			toast.success('Document classifier created');
			createClassifierModal?.close();
			await tabLoader.refresh('classifiers');
		} catch (e) {
			const msg = describeError(e);
			createClassifierError = msg;
			toast.error(msg);
		} finally {
			creatingClassifier = false;
		}
	}

	async function deleteClassifier(c: DocumentClassifierProperties): Promise<void> {
		if (!c.DocumentClassifierArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete document classifier',
			message: `Delete document classifier "${c.DocumentClassifierArn}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDocumentClassifierCommand({ DocumentClassifierArn: c.DocumentClassifierArn }));
			toast.success('Document classifier deleted');
			await tabLoader.refresh('classifiers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Entity Recognizers: create / delete (no Update op exists) ───────────
	let createRecognizerModal = $state<Modal | null>(null);
	let creatingRecognizer = $state(false);
	let createRecognizerError = $state<string | null>(null);
	let newRecognizerName = $state('');
	let newRecognizerRoleArn = $state('');
	let newRecognizerLanguage = $state<string>(LanguageCode.EN);
	let newRecognizerInputJson = $state('');

	function openCreateRecognizerModal(): void {
		createRecognizerError = null;
		newRecognizerName = '';
		newRecognizerRoleArn = '';
		newRecognizerLanguage = LanguageCode.EN;
		newRecognizerInputJson = '';
		createRecognizerModal?.open();
	}

	async function submitCreateRecognizer(): Promise<void> {
		if (!newRecognizerName || !newRecognizerRoleArn) {
			createRecognizerError = 'Name and data access role ARN are required.';
			return;
		}
		let inputDataConfig: Record<string, unknown>;
		try {
			inputDataConfig = parseJson(newRecognizerInputJson);
		} catch {
			createRecognizerError = 'Input data config must be valid JSON.';
			return;
		}
		creatingRecognizer = true;
		createRecognizerError = null;
		try {
			await client().send(
				new CreateEntityRecognizerCommand({
					RecognizerName: newRecognizerName,
					DataAccessRoleArn: newRecognizerRoleArn,
					LanguageCode: newRecognizerLanguage as LanguageCode,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					InputDataConfig: inputDataConfig as any
				})
			);
			toast.success('Entity recognizer created');
			createRecognizerModal?.close();
			await tabLoader.refresh('recognizers');
		} catch (e) {
			const msg = describeError(e);
			createRecognizerError = msg;
			toast.error(msg);
		} finally {
			creatingRecognizer = false;
		}
	}

	async function deleteRecognizer(r: EntityRecognizerProperties): Promise<void> {
		if (!r.EntityRecognizerArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete entity recognizer',
			message: `Delete entity recognizer "${r.EntityRecognizerArn}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteEntityRecognizerCommand({ EntityRecognizerArn: r.EntityRecognizerArn }));
			toast.success('Entity recognizer deleted');
			await tabLoader.refresh('recognizers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Endpoints: create / detail / edit / delete ───────────────────────────
	let createEndpointModal = $state<Modal | null>(null);
	let creatingEndpoint = $state(false);
	let createEndpointError = $state<string | null>(null);
	let newEndpointName = $state('');
	let newEndpointModelArn = $state('');
	let newEndpointUnits = $state('1');

	function openCreateEndpointModal(): void {
		createEndpointError = null;
		newEndpointName = '';
		newEndpointModelArn = '';
		newEndpointUnits = '1';
		createEndpointModal?.open();
	}

	async function submitCreateEndpoint(): Promise<void> {
		if (!newEndpointName || !newEndpointModelArn) {
			createEndpointError = 'Name and model ARN are required.';
			return;
		}
		creatingEndpoint = true;
		createEndpointError = null;
		try {
			await client().send(
				new CreateEndpointCommand({
					EndpointName: newEndpointName,
					ModelArn: newEndpointModelArn,
					DesiredInferenceUnits: Number(newEndpointUnits) || 1
				})
			);
			toast.success('Endpoint created');
			createEndpointModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (e) {
			const msg = describeError(e);
			createEndpointError = msg;
			toast.error(msg);
		} finally {
			creatingEndpoint = false;
		}
	}

	let endpointDetailModal = $state<Modal | null>(null);
	let viewedEndpoint = $state<EndpointProperties | null>(null);
	let endpointDetailLoading = $state(false);
	let endpointDetailError = $state<string | null>(null);

	async function openEndpointDetail(e: EndpointProperties): Promise<void> {
		viewedEndpoint = null;
		endpointDetailError = null;
		endpointDetailModal?.open();
		if (!e.EndpointArn) return;
		endpointDetailLoading = true;
		try {
			const resp = await client().send(new DescribeEndpointCommand({ EndpointArn: e.EndpointArn }));
			viewedEndpoint = resp.EndpointProperties ?? null;
		} catch (err) {
			endpointDetailError = describeError(err);
		} finally {
			endpointDetailLoading = false;
		}
	}

	let editEndpointModal = $state<Modal | null>(null);
	let editingEndpoint = $state(false);
	let editEndpointError = $state<string | null>(null);
	let editEndpointArn = $state('');
	let editEndpointUnits = $state('1');

	function openEditEndpointModal(e: EndpointProperties): void {
		editEndpointError = null;
		editEndpointArn = e.EndpointArn ?? '';
		editEndpointUnits = String(e.DesiredInferenceUnits ?? 1);
		editEndpointModal?.open();
	}

	async function submitEditEndpoint(): Promise<void> {
		if (!editEndpointArn) return;
		editingEndpoint = true;
		editEndpointError = null;
		try {
			await client().send(
				new UpdateEndpointCommand({ EndpointArn: editEndpointArn, DesiredInferenceUnits: Number(editEndpointUnits) || 1 })
			);
			toast.success('Endpoint updated');
			editEndpointModal?.close();
			await tabLoader.refresh('endpoints');
			const resp = await client().send(new DescribeEndpointCommand({ EndpointArn: editEndpointArn }));
			viewedEndpoint = resp.EndpointProperties ?? viewedEndpoint;
		} catch (e) {
			const msg = describeError(e);
			editEndpointError = msg;
			toast.error(msg);
		} finally {
			editingEndpoint = false;
		}
	}

	async function deleteEndpoint(e: EndpointProperties): Promise<void> {
		if (!e.EndpointArn) return;
		const confirmed = await confirmDestructive({ title: 'Delete endpoint', message: `Delete endpoint "${e.EndpointArn}"?` });
		if (!confirmed) return;
		try {
			await client().send(new DeleteEndpointCommand({ EndpointArn: e.EndpointArn }));
			toast.success('Endpoint deleted');
			endpointDetailModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	// ── Flywheels: create / detail (+ nested iterations) / edit / delete ────
	let createFlywheelModal = $state<Modal | null>(null);
	let creatingFlywheel = $state(false);
	let createFlywheelError = $state<string | null>(null);
	let newFlywheelName = $state('');
	let newFlywheelRoleArn = $state('');
	let newFlywheelDataLakeS3Uri = $state('');
	let newFlywheelModelType = $state<string>(ModelType.DOCUMENT_CLASSIFIER);

	function openCreateFlywheelModal(): void {
		createFlywheelError = null;
		newFlywheelName = '';
		newFlywheelRoleArn = '';
		newFlywheelDataLakeS3Uri = '';
		newFlywheelModelType = ModelType.DOCUMENT_CLASSIFIER;
		createFlywheelModal?.open();
	}

	async function submitCreateFlywheel(): Promise<void> {
		if (!newFlywheelName || !newFlywheelRoleArn || !newFlywheelDataLakeS3Uri) {
			createFlywheelError = 'Name, data access role ARN and data lake S3 URI are required.';
			return;
		}
		creatingFlywheel = true;
		createFlywheelError = null;
		try {
			await client().send(
				new CreateFlywheelCommand({
					FlywheelName: newFlywheelName,
					DataAccessRoleArn: newFlywheelRoleArn,
					DataLakeS3Uri: newFlywheelDataLakeS3Uri,
					ModelType: newFlywheelModelType as ModelType
				})
			);
			toast.success('Flywheel created');
			createFlywheelModal?.close();
			await tabLoader.refresh('flywheels');
		} catch (e) {
			const msg = describeError(e);
			createFlywheelError = msg;
			toast.error(msg);
		} finally {
			creatingFlywheel = false;
		}
	}

	let flywheelDetailModal = $state<Modal | null>(null);
	let viewedFlywheel = $state<FlywheelProperties | null>(null);
	let flywheelDetailLoading = $state(false);
	let flywheelDetailError = $state<string | null>(null);
	let flywheelIterations = $state<FlywheelIterationProperties[]>([]);
	let flywheelIterationsLoading = $state(false);
	let startingIteration = $state(false);

	async function loadFlywheelIterations(): Promise<void> {
		if (!viewedFlywheel?.FlywheelArn) return;
		flywheelIterationsLoading = true;
		try {
			const resp = await client().send(new ListFlywheelIterationHistoryCommand({ FlywheelArn: viewedFlywheel.FlywheelArn }));
			flywheelIterations = resp.FlywheelIterationPropertiesList ?? [];
		} catch (e) {
			toast.error('Failed to load iterations: ' + describeError(e));
		} finally {
			flywheelIterationsLoading = false;
		}
	}

	async function openFlywheelDetail(f: FlywheelSummary): Promise<void> {
		viewedFlywheel = null;
		flywheelDetailError = null;
		flywheelIterations = [];
		flywheelDetailModal?.open();
		if (!f.FlywheelArn) return;
		flywheelDetailLoading = true;
		try {
			const resp = await client().send(new DescribeFlywheelCommand({ FlywheelArn: f.FlywheelArn }));
			viewedFlywheel = resp.FlywheelProperties ?? null;
			await loadFlywheelIterations();
		} catch (e) {
			flywheelDetailError = describeError(e);
		} finally {
			flywheelDetailLoading = false;
		}
	}

	async function startFlywheelIteration(): Promise<void> {
		if (!viewedFlywheel?.FlywheelArn) return;
		startingIteration = true;
		try {
			await client().send(new StartFlywheelIterationCommand({ FlywheelArn: viewedFlywheel.FlywheelArn }));
			toast.success('Flywheel iteration started');
			await loadFlywheelIterations();
		} catch (e) {
			toast.error('Failed to start iteration: ' + describeError(e));
		} finally {
			startingIteration = false;
		}
	}

	let editFlywheelModal = $state<Modal | null>(null);
	let editingFlywheel = $state(false);
	let editFlywheelError = $state<string | null>(null);
	let editFlywheelArn = $state('');
	let editFlywheelRoleArn = $state('');

	function openEditFlywheelModal(f: FlywheelProperties): void {
		editFlywheelError = null;
		editFlywheelArn = f.FlywheelArn ?? '';
		editFlywheelRoleArn = f.DataAccessRoleArn ?? '';
		editFlywheelModal?.open();
	}

	async function submitEditFlywheel(): Promise<void> {
		if (!editFlywheelArn) return;
		editingFlywheel = true;
		editFlywheelError = null;
		try {
			const resp = await client().send(
				new UpdateFlywheelCommand({ FlywheelArn: editFlywheelArn, DataAccessRoleArn: editFlywheelRoleArn || undefined })
			);
			toast.success('Flywheel updated');
			editFlywheelModal?.close();
			await tabLoader.refresh('flywheels');
			viewedFlywheel = resp.FlywheelProperties ?? viewedFlywheel;
		} catch (e) {
			const msg = describeError(e);
			editFlywheelError = msg;
			toast.error(msg);
		} finally {
			editingFlywheel = false;
		}
	}

	async function deleteFlywheel(f: FlywheelSummary | FlywheelProperties): Promise<void> {
		if (!f.FlywheelArn) return;
		const confirmed = await confirmDestructive({ title: 'Delete flywheel', message: `Delete flywheel "${f.FlywheelArn}"?` });
		if (!confirmed) return;
		try {
			await client().send(new DeleteFlywheelCommand({ FlywheelArn: f.FlywheelArn }));
			toast.success('Flywheel deleted');
			flywheelDetailModal?.close();
			await tabLoader.refresh('flywheels');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Datasets: create / detail (no Update or Delete op exists) ───────────
	let createDatasetModal = $state<Modal | null>(null);
	let creatingDataset = $state(false);
	let createDatasetError = $state<string | null>(null);
	let newDatasetName = $state('');
	let newDatasetFlywheelArn = $state('');
	let newDatasetType = $state<string>(DatasetType.TRAIN);
	let newDatasetInputJson = $state('');

	function openCreateDatasetModal(): void {
		createDatasetError = null;
		newDatasetName = '';
		newDatasetFlywheelArn = '';
		newDatasetType = DatasetType.TRAIN;
		newDatasetInputJson = '';
		createDatasetModal?.open();
	}

	async function submitCreateDataset(): Promise<void> {
		if (!newDatasetName || !newDatasetFlywheelArn) {
			createDatasetError = 'Name and flywheel ARN are required.';
			return;
		}
		let inputDataConfig: Record<string, unknown>;
		try {
			inputDataConfig = parseJson(newDatasetInputJson);
		} catch {
			createDatasetError = 'Input data config must be valid JSON.';
			return;
		}
		creatingDataset = true;
		createDatasetError = null;
		try {
			await client().send(
				new CreateDatasetCommand({
					DatasetName: newDatasetName,
					FlywheelArn: newDatasetFlywheelArn,
					DatasetType: newDatasetType as DatasetType,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					InputDataConfig: inputDataConfig as any
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

	let datasetDetailModal = $state<Modal | null>(null);
	let viewedDataset = $state<DatasetProperties | null>(null);
	let datasetDetailLoading = $state(false);
	let datasetDetailError = $state<string | null>(null);

	async function openDatasetDetail(d: DatasetProperties): Promise<void> {
		viewedDataset = null;
		datasetDetailError = null;
		datasetDetailModal?.open();
		if (!d.DatasetArn) return;
		datasetDetailLoading = true;
		try {
			const resp = await client().send(new DescribeDatasetCommand({ DatasetArn: d.DatasetArn }));
			viewedDataset = resp.DatasetProperties ?? null;
		} catch (e) {
			datasetDetailError = describeError(e);
		} finally {
			datasetDetailLoading = false;
		}
	}

	// ── Jobs: start / detail / stop (where the family supports it) ─────────
	let startJobModal = $state<Modal | null>(null);
	let startingJob = $state(false);
	let startJobError = $state<string | null>(null);
	let jobForm = $state({
		jobName: '',
		dataAccessRoleArn: '',
		inputS3Uri: '',
		outputS3Uri: '',
		languageCode: LanguageCode.EN as string,
		documentClassifierArn: '',
		entityRecognizerArn: '',
		flywheelArn: '',
		piiMode: PiiEntitiesDetectionMode.ONLY_OFFSETS as string,
		numberOfTopics: '',
		targetEventTypes: ''
	});

	function openStartJobModal(): void {
		startJobError = null;
		jobForm = {
			jobName: '',
			dataAccessRoleArn: '',
			inputS3Uri: '',
			outputS3Uri: '',
			languageCode: LanguageCode.EN,
			documentClassifierArn: '',
			entityRecognizerArn: '',
			flywheelArn: '',
			piiMode: PiiEntitiesDetectionMode.ONLY_OFFSETS,
			numberOfTopics: '',
			targetEventTypes: ''
		};
		startJobModal?.open();
	}

	async function submitStartJob(): Promise<void> {
		const fam = selectedJobFamily;
		if (!jobForm.dataAccessRoleArn || !jobForm.inputS3Uri || !jobForm.outputS3Uri) {
			startJobError = 'Data access role ARN and input/output S3 URIs are required.';
			return;
		}
		startingJob = true;
		startJobError = null;
		try {
			const input: Record<string, unknown> = {
				JobName: jobForm.jobName || undefined,
				DataAccessRoleArn: jobForm.dataAccessRoleArn,
				InputDataConfig: { S3Uri: jobForm.inputS3Uri },
				OutputDataConfig: { S3Uri: jobForm.outputS3Uri }
			};
			if (fam.hasLanguageCode) input.LanguageCode = jobForm.languageCode;
			if (fam.hasDocumentClassifierArn && jobForm.documentClassifierArn) input.DocumentClassifierArn = jobForm.documentClassifierArn;
			if (fam.hasEntityRecognizerArn && jobForm.entityRecognizerArn) input.EntityRecognizerArn = jobForm.entityRecognizerArn;
			if (fam.hasFlywheelArn && jobForm.flywheelArn) input.FlywheelArn = jobForm.flywheelArn;
			if (fam.hasPiiMode) input.Mode = jobForm.piiMode;
			if (fam.hasNumberOfTopics && jobForm.numberOfTopics) input.NumberOfTopics = Number(jobForm.numberOfTopics);
			if (fam.hasTargetEventTypes) {
				input.TargetEventTypes = jobForm.targetEventTypes
					.split(',')
					.map((s) => s.trim())
					.filter(Boolean);
			}
			await client().send(new fam.startCmd(input));
			toast.success('Job started');
			startJobModal?.close();
			await tabLoader.refresh('jobs');
		} catch (e) {
			const msg = describeError(e);
			startJobError = msg;
			toast.error(msg);
		} finally {
			startingJob = false;
		}
	}

	let jobDetailModal = $state<Modal | null>(null);
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let viewedJob = $state<any | null>(null);
	let jobDetailLoading = $state(false);
	let jobDetailError = $state<string | null>(null);
	let stoppingJob = $state(false);

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	async function openJobDetail(j: any): Promise<void> {
		const fam = selectedJobFamily;
		viewedJob = null;
		jobDetailError = null;
		jobDetailModal?.open();
		if (!j.JobId) return;
		jobDetailLoading = true;
		try {
			const resp = await client().send(new fam.describeCmd({ JobId: j.JobId }));
			viewedJob = (resp as unknown as Record<string, unknown>)[fam.objectField] ?? null;
		} catch (e) {
			jobDetailError = describeError(e);
		} finally {
			jobDetailLoading = false;
		}
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	async function stopJob(j: any): Promise<void> {
		const fam = selectedJobFamily;
		if (!fam.stopCmd || !j.JobId) return;
		stoppingJob = true;
		try {
			await client().send(new fam.stopCmd({ JobId: j.JobId }));
			toast.success('Job stop requested');
			jobDetailModal?.close();
			await tabLoader.refresh('jobs');
		} catch (e) {
			toast.error('Failed to stop job: ' + describeError(e));
		} finally {
			stoppingJob = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={MessageSquare} title="Amazon Comprehend" description="Natural language processing to find insights in text" onRefresh={handleRefresh} color="orange">
		{#snippet actions()}
			{#if activeTab === 'classifiers'}
				<button onclick={openCreateClassifierModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Create classifier</button>
			{:else if activeTab === 'recognizers'}
				<button onclick={openCreateRecognizerModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Create recognizer</button>
			{:else if activeTab === 'endpoints'}
				<button onclick={openCreateEndpointModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Create endpoint</button>
			{:else if activeTab === 'flywheels'}
				<button onclick={openCreateFlywheelModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Create flywheel</button>
			{:else if activeTab === 'datasets'}
				<button onclick={openCreateDatasetModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Create dataset</button>
			{:else if activeTab === 'jobs'}
				<button onclick={openStartJobModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"><Plus class="w-4 h-4" /> Start job</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><FileText class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{classifiers.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Document Classifiers</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Tag class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{recognizers.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Entity Recognizers</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{jobs.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">{selectedJobFamily.label} Jobs</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			{#if activeTab === 'jobs'}
				<label class="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-2">
					Job family:
					<select bind:value={selectedJobFamilyId} onchange={changeJobFamily} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1.5">
						{#each jobFamilies as fam (fam.id)}
							<option value={fam.id}>{fam.label}</option>
						{/each}
					</select>
				</label>
			{:else if activeTab !== 'detection'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>
		{#if activeTab === 'detection'}
			<div class="p-4 space-y-4">
				<div class="flex flex-wrap items-center gap-3">
					<select bind:value={testOp} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-3 py-2">
						<option value="sentiment">Detect Sentiment</option>
						<option value="entities">Detect Entities</option>
						<option value="keyphrases">Detect Key Phrases</option>
						<option value="language">Detect Dominant Language</option>
						<option value="pii">Detect PII Entities</option>
						<option value="syntax">Detect Syntax</option>
						<option value="toxicity">Detect Toxic Content</option>
					</select>
					{#if testOp !== 'language'}
						<select bind:value={testLang} class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-3 py-2">
							{#each [['en', 'English'], ['es', 'Spanish'], ['fr', 'French'], ['de', 'German'], ['it', 'Italian'], ['pt', 'Portuguese'], ['ja', 'Japanese'], ['ko', 'Korean'], ['zh', 'Chinese'], ['ar', 'Arabic'], ['hi', 'Hindi']] as [code, name] (code)}
							<option value={code}>{name}</option>
						{/each}
						</select>
					{/if}
					<button onclick={runInference} disabled={inferring} class="ml-auto flex items-center gap-2 px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 text-sm font-medium disabled:opacity-50">
						<Play class="w-4 h-4" /> {inferring ? 'Analyzing...' : 'Analyze'}
					</button>
				</div>
				<textarea bind:value={testText} rows="5" placeholder="Enter sample text to analyze..." class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>

				{#if sentimentResult}
					<div class="space-y-2">
						<p class="text-sm font-medium text-gray-900 dark:text-white">Sentiment: <span class="font-bold">{sentimentResult.sentiment}</span></p>
						{#each [['Positive', sentimentResult.score?.Positive], ['Negative', sentimentResult.score?.Negative], ['Neutral', sentimentResult.score?.Neutral], ['Mixed', sentimentResult.score?.Mixed]] as [label, val] (label)}
							<div class="flex items-center gap-2">
								<span class="text-xs w-16 text-gray-500 dark:text-gray-400">{label}</span>
								<div class="flex-1 h-2 bg-gray-100 dark:bg-slate-700 rounded-full overflow-hidden"><div class="h-full bg-orange-500" style="width: {(Number(val) || 0) * 100}%"></div></div>
								<span class="text-xs w-14 text-right font-mono text-gray-600 dark:text-gray-300">{pct(val as number | undefined)}</span>
							</div>
						{/each}
					</div>
				{:else if entitiesResult}
					{#if entitiesResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No entities detected</p>
					{:else}
						<div class="flex flex-wrap gap-2">
							{#each entitiesResult as ent, i (i)}
								<span class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-blue-50 dark:bg-blue-900/20 text-sm">
									<span class="font-medium text-gray-900 dark:text-white">{ent.Text}</span>
									<span class="text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-300">{ent.Type}</span>
									<span class="text-xs text-gray-500">{pct(ent.Score)}</span>
								</span>
							{/each}
						</div>
					{/if}
				{:else if keyPhrasesResult}
					{#if keyPhrasesResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No key phrases detected</p>
					{:else}
						<div class="flex flex-wrap gap-2">
							{#each keyPhrasesResult as kp, i (i)}
								<span class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-green-50 dark:bg-green-900/20 text-sm">
									<span class="font-medium text-gray-900 dark:text-white">{kp.Text}</span>
									<span class="text-xs text-gray-500">{pct(kp.Score)}</span>
								</span>
							{/each}
						</div>
					{/if}
				{:else if piiResult}
					{#if piiResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No PII entities detected</p>
					{:else}
						<div class="flex flex-wrap gap-2">
							{#each piiResult as ent, i (i)}
								<span class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-red-50 dark:bg-red-900/20 text-sm">
									<span class="text-xs px-1.5 py-0.5 rounded bg-red-100 dark:bg-red-900/40 text-red-700 dark:text-red-300">{ent.Type}</span>
									<span class="text-xs text-gray-500">{pct(ent.Score)}</span>
								</span>
							{/each}
						</div>
					{/if}
				{:else if syntaxResult}
					{#if syntaxResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No syntax tokens detected</p>
					{:else}
						<div class="flex flex-wrap gap-2">
							{#each syntaxResult as tok, i (i)}
								<span class="inline-flex items-center gap-1 px-2 py-1 rounded-lg bg-purple-50 dark:bg-purple-900/20 text-sm">
									<span class="font-medium text-gray-900 dark:text-white">{tok.Text}</span>
									<span class="text-xs px-1.5 py-0.5 rounded bg-purple-100 dark:bg-purple-900/40 text-purple-700 dark:text-purple-300">{tok.PartOfSpeech?.Tag}</span>
								</span>
							{/each}
						</div>
					{/if}
				{:else if toxicityResult}
					{#if toxicityResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No toxicity labels detected</p>
					{:else}
						<div class="space-y-2">
							{#each toxicityResult as label, i (i)}
								<div class="flex items-center gap-2">
									<span class="text-xs w-32 text-gray-500 dark:text-gray-400">{label.Name}</span>
									<div class="flex-1 h-2 bg-gray-100 dark:bg-slate-700 rounded-full overflow-hidden"><div class="h-full bg-orange-500" style="width: {(label.Score || 0) * 100}%"></div></div>
									<span class="text-xs w-14 text-right font-mono text-gray-600 dark:text-gray-300">{pct(label.Score)}</span>
								</div>
							{/each}
						</div>
					{/if}
				{:else if languageResult}
					{#if languageResult.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400">No languages detected</p>
					{:else}
						<div class="space-y-2">
							{#each languageResult as lang, i (i)}
								<div class="flex items-center gap-2">
									<span class="text-xs w-16 font-mono text-gray-700 dark:text-gray-200">{lang.LanguageCode}</span>
									<div class="flex-1 h-2 bg-gray-100 dark:bg-slate-700 rounded-full overflow-hidden"><div class="h-full bg-orange-500" style="width: {(lang.Score || 0) * 100}%"></div></div>
									<span class="text-xs w-14 text-right font-mono text-gray-600 dark:text-gray-300">{pct(lang.Score)}</span>
								</div>
							{/each}
						</div>
					{/if}
				{/if}
			</div>
		{/if}
		<div class="p-4">
			{#if activeTabError}
				<div role="alert" class="mb-4 rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}
			{#if activeTab === 'classifiers'}
				{#if filteredClassifiers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No document classifiers found</div>
				{:else}
					{#if compareSelection.length >= 2}
						<div class="mb-3 flex items-center justify-between">
							<p class="text-sm text-gray-600 dark:text-gray-300">{compareSelection.length} versions selected</p>
							<button onclick={() => (showCompare = true)} class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-orange-600 text-white text-sm hover:bg-orange-700">
								<GitCompare class="w-4 h-4" /> Compare metrics
							</button>
						</div>
					{/if}
					{#if showCompare && compareRows.length >= 2}
						<div class="mb-4 overflow-x-auto rounded-lg border border-slate-200 dark:border-slate-700">
							<table class="w-full text-sm">
								<thead class="bg-gray-50 dark:bg-slate-700/50">
									<tr>
										<th class="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-200">Metric</th>
										{#each compareRows as c (c.DocumentClassifierArn)}
											<th class="px-3 py-2 text-left font-medium text-gray-700 dark:text-gray-200 truncate max-w-[12rem]" title={c.DocumentClassifierArn}>{c.VersionName || (c.DocumentClassifierArn ?? '').split('/').pop()}</th>
										{/each}
									</tr>
								</thead>
								<tbody class="divide-y divide-slate-200 dark:divide-slate-700">
									{#each classifierMetrics(compareRows[0]) as [label] (label)}
										<tr>
											<td class="px-3 py-2 text-gray-500 dark:text-gray-400">{label}</td>
											{#each compareRows as c (c.DocumentClassifierArn)}
												{@const v = (classifierMetrics(c).find(([l]) => l === label) ?? [])[1]}
												<td class="px-3 py-2 font-mono text-gray-900 dark:text-white">{label === 'Hamming Loss' ? (v == null ? '-' : (v as number).toFixed(4)) : pct(v as number | undefined)}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
							<div class="p-2 text-right"><button onclick={() => (showCompare = false)} class="text-xs text-gray-500 hover:text-gray-700 dark:hover:text-gray-300">Close comparison</button></div>
						</div>
					{/if}
					<div class="space-y-2">
						{#each filteredClassifiers as clf (clf.DocumentClassifierArn)}
							{@const arn = clf.DocumentClassifierArn ?? ''}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center justify-between p-3">
									<button onclick={() => (expandedClassifier = expandedClassifier === arn ? null : arn)} class="flex items-center gap-3 min-w-0 text-left">
										{#if expandedClassifier === arn}<ChevronDown class="w-4 h-4 text-gray-400 flex-shrink-0" />{:else}<ChevronRight class="w-4 h-4 text-gray-400 flex-shrink-0" />{/if}
										<FileText class="w-5 h-5 text-orange-500 flex-shrink-0" />
										<div class="min-w-0">
											<p class="text-sm font-medium text-gray-900 dark:text-white truncate max-w-sm">{arn}</p>
											{#if clf.VersionName}<p class="text-xs text-gray-500 dark:text-gray-400">{clf.VersionName} · F1 {pct(clf.ClassifierMetadata?.EvaluationMetrics?.F1Score)}</p>{/if}
										</div>
									</button>
									<div class="flex items-center gap-3 flex-shrink-0">
										<label class="flex items-center gap-1 text-xs text-gray-500 dark:text-gray-400">
											<input type="checkbox" checked={compareSelection.includes(arn)} onchange={() => toggleCompare(arn)} class="rounded" /> Compare
										</label>
										<span class="text-xs px-2 py-1 rounded-full {clf.Status === 'TRAINED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{clf.Status}</span>
										<button onclick={() => deleteClassifier(clf)} title="Delete" aria-label="Delete classifier {arn}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
									</div>
								</div>
								{#if expandedClassifier === arn}
									<div class="px-3 pb-3 border-t border-slate-200 dark:border-slate-600 pt-3">
										{#if clf.ClassifierMetadata?.EvaluationMetrics}
											<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
												{#each classifierMetrics(clf) as [label, val] (label)}
													<div>
														<p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
														<div class="flex items-center gap-2">
															{#if label !== 'Hamming Loss'}
																<div class="flex-1 h-1.5 bg-gray-200 dark:bg-slate-600 rounded-full overflow-hidden"><div class="h-full bg-orange-500" style="width: {(Number(val) || 0) * 100}%"></div></div>
															{/if}
															<span class="text-xs font-mono text-gray-900 dark:text-white">{label === 'Hamming Loss' ? (val == null ? '-' : val.toFixed(4)) : pct(val)}</span>
														</div>
													</div>
												{/each}
											</div>
											{#if clf.ClassifierMetadata?.NumberOfTrainedDocuments != null}
												<p class="mt-2 text-xs text-gray-500 dark:text-gray-400">Trained on {clf.ClassifierMetadata.NumberOfTrainedDocuments} documents · {clf.ClassifierMetadata.NumberOfLabels ?? 0} labels</p>
											{/if}
										{:else}
											<p class="text-sm text-gray-500 dark:text-gray-400">No evaluation metrics available (training not complete).</p>
										{/if}
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'recognizers'}
				{#if filteredRecognizers.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No entity recognizers found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRecognizers as rec (rec.EntityRecognizerArn)}
							{@const rarn = rec.EntityRecognizerArn ?? ''}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center justify-between p-3">
									<button onclick={() => (expandedRecognizer = expandedRecognizer === rarn ? null : rarn)} class="flex items-center gap-3 min-w-0 text-left">
										{#if expandedRecognizer === rarn}<ChevronDown class="w-4 h-4 text-gray-400 flex-shrink-0" />{:else}<ChevronRight class="w-4 h-4 text-gray-400 flex-shrink-0" />{/if}
										<Tag class="w-5 h-5 text-blue-500 flex-shrink-0" />
										<div class="min-w-0">
											<p class="text-sm font-medium text-gray-900 dark:text-white truncate max-w-sm">{rarn}</p>
											{#if rec.VersionName}<p class="text-xs text-gray-500 dark:text-gray-400">{rec.VersionName} · F1 {pct(rec.RecognizerMetadata?.EvaluationMetrics?.F1Score)}</p>{/if}
										</div>
									</button>
									<div class="flex items-center gap-3 flex-shrink-0">
										<span class="text-xs px-2 py-1 rounded-full {rec.Status === 'TRAINED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{rec.Status}</span>
										<button onclick={() => deleteRecognizer(rec)} title="Delete" aria-label="Delete recognizer {rarn}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
									</div>
								</div>
								{#if expandedRecognizer === rarn}
									<div class="px-3 pb-3 border-t border-slate-200 dark:border-slate-600 pt-3">
										{#if rec.RecognizerMetadata?.EvaluationMetrics}
											<div class="grid grid-cols-3 gap-3">
												{#each [['Precision', rec.RecognizerMetadata.EvaluationMetrics.Precision], ['Recall', rec.RecognizerMetadata.EvaluationMetrics.Recall], ['F1 Score', rec.RecognizerMetadata.EvaluationMetrics.F1Score]] as [label, val] (label)}
													<div>
														<p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
														<div class="flex items-center gap-2">
															<div class="flex-1 h-1.5 bg-gray-200 dark:bg-slate-600 rounded-full overflow-hidden"><div class="h-full bg-blue-500" style="width: {(Number(val) || 0) * 100}%"></div></div>
															<span class="text-xs font-mono text-gray-900 dark:text-white">{pct(val as number | undefined)}</span>
														</div>
													</div>
												{/each}
											</div>
											{#if rec.RecognizerMetadata?.NumberOfTrainedDocuments != null}
												<p class="mt-2 text-xs text-gray-500 dark:text-gray-400">Trained on {rec.RecognizerMetadata.NumberOfTrainedDocuments} documents</p>
											{/if}
										{:else}
											<p class="text-sm text-gray-500 dark:text-gray-400">No evaluation metrics available (training not complete).</p>
										{/if}
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'endpoints'}
				{#snippet endpointActionsCell(e: EndpointProperties)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openEndpointDetail(e)} title="View" aria-label="View endpoint {e.EndpointArn}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteEndpoint(e)} title="Delete" aria-label="Delete endpoint {e.EndpointArn}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				<DataTable
					rows={filteredEndpoints}
					rowKey={(e) => e.EndpointArn ?? ''}
					columns={defineColumns<EndpointProperties>([
						{ key: 'EndpointArn', label: 'ARN' },
						{ key: 'Status', label: 'Status' },
						{ key: 'DesiredInferenceUnits', label: 'Inference Units' },
						{ key: 'actions', label: '', render: endpointActionsCell }
					])}
					loading={tabLoader.isLoading('endpoints')}
					emptyMessage="No endpoints found"
				/>
			{:else if activeTab === 'flywheels'}
				{#snippet flywheelActionsCell(f: FlywheelSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openFlywheelDetail(f)} title="View" aria-label="View flywheel {f.FlywheelArn}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteFlywheel(f)} title="Delete" aria-label="Delete flywheel {f.FlywheelArn}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				<DataTable
					rows={filteredFlywheels}
					rowKey={(f) => f.FlywheelArn ?? ''}
					columns={defineColumns<FlywheelSummary>([
						{ key: 'FlywheelArn', label: 'ARN' },
						{ key: 'ModelType', label: 'Model Type' },
						{ key: 'Status', label: 'Status' },
						{ key: 'actions', label: '', render: flywheelActionsCell }
					])}
					loading={tabLoader.isLoading('flywheels')}
					emptyMessage="No flywheels found"
				/>
			{:else if activeTab === 'datasets'}
				{#snippet datasetActionsCell(d: DatasetProperties)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDatasetDetail(d)} title="View" aria-label="View dataset {d.DatasetName}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				<DataTable
					rows={filteredDatasets}
					rowKey={(d) => d.DatasetArn ?? ''}
					columns={defineColumns<DatasetProperties>([
						{ key: 'DatasetName', label: 'Name' },
						{ key: 'DatasetType', label: 'Type' },
						{ key: 'Status', label: 'Status' },
						{ key: 'actions', label: '', render: datasetActionsCell }
					])}
					loading={tabLoader.isLoading('datasets')}
					emptyMessage="No datasets found. Datasets are immutable once created -- there is no Delete operation."
				/>
			{:else if activeTab === 'jobs'}
				{#snippet jobStatusCell(j: Record<string, unknown>)}
					<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{String(j.JobStatus ?? '—')}</span>
				{/snippet}
				{#snippet jobActionsCell(j: Record<string, unknown>)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openJobDetail(j)} title="View" aria-label="View job {j.JobId}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				<DataTable
					rows={filteredJobs}
					rowKey={(j) => (j.JobId as string) ?? ''}
					columns={defineColumns<Record<string, unknown>>([
						{ key: 'JobName', label: 'Name' },
						{ key: 'JobId', label: 'Job ID' },
						{ key: 'JobStatus', label: 'Status', render: jobStatusCell },
						{ key: 'actions', label: '', render: jobActionsCell }
					])}
					loading={tabLoader.isLoading('jobs')}
					emptyMessage="No {selectedJobFamily.label} jobs found"
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create Document Classifier -->
<Modal bind:this={createClassifierModal} title="Create Document Classifier">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="clf-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="clf-name" bind:value={newClassifierName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="clf-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="clf-role" bind:value={newClassifierRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="clf-lang" class="text-sm text-slate-600 dark:text-slate-300">Language</label>
				<select id="clf-lang" bind:value={newClassifierLanguage} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(LanguageCode) as l (l)}
						<option value={l}>{l}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="clf-input" class="text-sm text-slate-600 dark:text-slate-300">Input data config (JSON)</label>
				<textarea id="clf-input" bind:value={newClassifierInputJson} rows={3} placeholder={'{\n  "DataFormat": "COMPREHEND_CSV",\n  "S3Uri": "s3://bucket/training.csv"\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createClassifierError}
				<p class="text-sm text-red-600 dark:text-red-400">{createClassifierError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createClassifierModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateClassifier} disabled={creatingClassifier} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingClassifier ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Create Entity Recognizer -->
<Modal bind:this={createRecognizerModal} title="Create Entity Recognizer">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rec-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rec-name" bind:value={newRecognizerName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rec-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="rec-role" bind:value={newRecognizerRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rec-lang" class="text-sm text-slate-600 dark:text-slate-300">Language</label>
				<select id="rec-lang" bind:value={newRecognizerLanguage} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(LanguageCode) as l (l)}
						<option value={l}>{l}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="rec-input" class="text-sm text-slate-600 dark:text-slate-300">Input data config (JSON)</label>
				<textarea id="rec-input" bind:value={newRecognizerInputJson} rows={3} placeholder={'{\n  "EntityTypes": [{ "Type": "PRODUCT" }],\n  "Documents": { "S3Uri": "s3://bucket/docs/" }\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createRecognizerError}
				<p class="text-sm text-red-600 dark:text-red-400">{createRecognizerError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRecognizerModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateRecognizer} disabled={creatingRecognizer} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingRecognizer ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Create Endpoint -->
<Modal bind:this={createEndpointModal} title="Create Endpoint">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ep-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ep-name" bind:value={newEndpointName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-model" class="text-sm text-slate-600 dark:text-slate-300">Model ARN</label>
				<input id="ep-model" bind:value={newEndpointModelArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-units" class="text-sm text-slate-600 dark:text-slate-300">Desired inference units</label>
				<input id="ep-units" type="number" min="1" bind:value={newEndpointUnits} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createEndpointError}
				<p class="text-sm text-red-600 dark:text-red-400">{createEndpointError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createEndpointModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateEndpoint} disabled={creatingEndpoint} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingEndpoint ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Endpoint detail -->
<Modal bind:this={endpointDetailModal} title="Endpoint">
	{#snippet children()}
		{#if endpointDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if endpointDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{endpointDetailError}</p>
		{:else if viewedEndpoint}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedEndpoint.EndpointArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Model ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedEndpoint.ModelArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Inference units</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.DesiredInferenceUnits ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedEndpoint.CreationTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => endpointDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedEndpoint}
			<button type="button" onclick={() => viewedEndpoint && openEditEndpointModal(viewedEndpoint)} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedEndpoint && deleteEndpoint(viewedEndpoint)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Endpoint -->
<Modal bind:this={editEndpointModal} title="Edit Endpoint">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ep-edit-units" class="text-sm text-slate-600 dark:text-slate-300">Desired inference units</label>
				<input id="ep-edit-units" type="number" min="1" bind:value={editEndpointUnits} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editEndpointError}
				<p class="text-sm text-red-600 dark:text-red-400">{editEndpointError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editEndpointModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditEndpoint} disabled={editingEndpoint} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingEndpoint ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Flywheel -->
<Modal bind:this={createFlywheelModal} title="Create Flywheel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fw-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fw-name" bind:value={newFlywheelName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fw-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="fw-role" bind:value={newFlywheelRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fw-datalake" class="text-sm text-slate-600 dark:text-slate-300">Data lake S3 URI</label>
				<input id="fw-datalake" bind:value={newFlywheelDataLakeS3Uri} placeholder="s3://bucket/data-lake/" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fw-model-type" class="text-sm text-slate-600 dark:text-slate-300">Model type</label>
				<select id="fw-model-type" bind:value={newFlywheelModelType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(ModelType) as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			{#if createFlywheelError}
				<p class="text-sm text-red-600 dark:text-red-400">{createFlywheelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createFlywheelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateFlywheel} disabled={creatingFlywheel} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingFlywheel ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Flywheel detail (with nested iteration history) -->
<Modal bind:this={flywheelDetailModal} title="Flywheel">
	{#snippet children()}
		{#if flywheelDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if flywheelDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{flywheelDetailError}</p>
		{:else if viewedFlywheel}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedFlywheel.FlywheelArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedFlywheel.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Model type</dt><dd class="text-slate-900 dark:text-white">{viewedFlywheel.ModelType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Active model ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedFlywheel.ActiveModelArn ?? '—'}</dd></div>
				<div>
					<div class="flex items-center justify-between">
						<dt class="text-slate-500 dark:text-slate-400">Iteration history</dt>
						<button type="button" onclick={startFlywheelIteration} disabled={startingIteration} class="flex items-center gap-1 text-xs text-orange-600 hover:text-orange-800 dark:text-orange-400 disabled:opacity-50">
							<RotateCw class="w-3 h-3" /> {startingIteration ? 'Starting…' : 'Start iteration'}
						</button>
					</div>
					<dd class="text-slate-900 dark:text-white mt-1">
						{#if flywheelIterationsLoading}
							<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
						{:else if flywheelIterations.length === 0}
							<p class="text-xs text-slate-500 dark:text-slate-400">No iterations yet</p>
						{:else}
							<ul class="space-y-1">
								{#each flywheelIterations as it (it.FlywheelIterationId)}
									<li class="flex items-center justify-between p-2 rounded bg-gray-50 dark:bg-slate-700/50">
										<span class="text-xs font-mono">{it.FlywheelIterationId}</span>
										<span class="text-xs px-2 py-0.5 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{it.Status}</span>
									</li>
								{/each}
							</ul>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => flywheelDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedFlywheel}
			<button type="button" onclick={() => viewedFlywheel && openEditFlywheelModal(viewedFlywheel)} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedFlywheel && deleteFlywheel(viewedFlywheel)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Flywheel -->
<Modal bind:this={editFlywheelModal} title="Edit Flywheel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fw-edit-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="fw-edit-role" bind:value={editFlywheelRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editFlywheelError}
				<p class="text-sm text-red-600 dark:text-red-400">{editFlywheelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editFlywheelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditFlywheel} disabled={editingFlywheel} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingFlywheel ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Dataset -->
<Modal bind:this={createDatasetModal} title="Create Dataset">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ds-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ds-name" bind:value={newDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ds-flywheel" class="text-sm text-slate-600 dark:text-slate-300">Flywheel ARN</label>
				<input id="ds-flywheel" bind:value={newDatasetFlywheelArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ds-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select id="ds-type" bind:value={newDatasetType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(DatasetType) as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="ds-input" class="text-sm text-slate-600 dark:text-slate-300">Input data config (JSON)</label>
				<textarea id="ds-input" bind:value={newDatasetInputJson} rows={3} placeholder={'{\n  "DataFormat": "COMPREHEND_CSV",\n  "DocumentClassifierInputDataConfig": { "S3Uri": "s3://bucket/data.csv" }\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createDatasetError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDatasetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDatasetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDataset} disabled={creatingDataset} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingDataset ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Dataset detail (no Update or Delete op exists) -->
<Modal bind:this={datasetDetailModal} title="Dataset">
	{#snippet children()}
		{#if datasetDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if datasetDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{datasetDetailError}</p>
		{:else if viewedDataset}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.DatasetName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedDataset.DatasetArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.DatasetType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Documents</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.NumberOfDocuments ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDataset.CreationTime)}</dd></div>
			</dl>
			<p class="mt-3 text-xs text-slate-500 dark:text-slate-400">Datasets are immutable once created -- the real API has no Update or Delete operation for this resource.</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => datasetDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- Start Job -->
<Modal bind:this={startJobModal} title="Start {selectedJobFamily.label} Job">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="job-name" class="text-sm text-slate-600 dark:text-slate-300">Job name (optional)</label>
				<input id="job-name" bind:value={jobForm.jobName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-role" class="text-sm text-slate-600 dark:text-slate-300">Data access role ARN</label>
				<input id="job-role" bind:value={jobForm.dataAccessRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-input" class="text-sm text-slate-600 dark:text-slate-300">Input S3 URI</label>
				<input id="job-input" bind:value={jobForm.inputS3Uri} placeholder="s3://bucket/input/" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="job-output" class="text-sm text-slate-600 dark:text-slate-300">Output S3 URI</label>
				<input id="job-output" bind:value={jobForm.outputS3Uri} placeholder="s3://bucket/output/" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if selectedJobFamily.hasLanguageCode}
				<div>
					<label for="job-lang" class="text-sm text-slate-600 dark:text-slate-300">Language</label>
					<select id="job-lang" bind:value={jobForm.languageCode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each Object.values(LanguageCode) as l (l)}
							<option value={l}>{l}</option>
						{/each}
					</select>
				</div>
			{/if}
			{#if selectedJobFamily.hasDocumentClassifierArn}
				<div>
					<label for="job-doc-clf" class="text-sm text-slate-600 dark:text-slate-300">Document classifier ARN (optional)</label>
					<input id="job-doc-clf" bind:value={jobForm.documentClassifierArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if selectedJobFamily.hasEntityRecognizerArn}
				<div>
					<label for="job-entity-rec" class="text-sm text-slate-600 dark:text-slate-300">Entity recognizer ARN (optional)</label>
					<input id="job-entity-rec" bind:value={jobForm.entityRecognizerArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if selectedJobFamily.hasFlywheelArn}
				<div>
					<label for="job-flywheel" class="text-sm text-slate-600 dark:text-slate-300">Flywheel ARN (optional)</label>
					<input id="job-flywheel" bind:value={jobForm.flywheelArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if selectedJobFamily.hasPiiMode}
				<div>
					<label for="job-pii-mode" class="text-sm text-slate-600 dark:text-slate-300">Mode</label>
					<select id="job-pii-mode" bind:value={jobForm.piiMode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each Object.values(PiiEntitiesDetectionMode) as m (m)}
							<option value={m}>{m}</option>
						{/each}
					</select>
				</div>
			{/if}
			{#if selectedJobFamily.hasNumberOfTopics}
				<div>
					<label for="job-topics" class="text-sm text-slate-600 dark:text-slate-300">Number of topics (optional)</label>
					<input id="job-topics" type="number" min="1" bind:value={jobForm.numberOfTopics} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if selectedJobFamily.hasTargetEventTypes}
				<div>
					<label for="job-event-types" class="text-sm text-slate-600 dark:text-slate-300">Target event types (comma-separated)</label>
					<input id="job-event-types" bind:value={jobForm.targetEventTypes} placeholder="BANKRUPTCY,SPINOFF" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if !selectedJobFamily.noStop}
				<p class="text-xs text-slate-500 dark:text-slate-400">This job family supports Stop once running.</p>
			{:else}
				<p class="text-xs text-slate-500 dark:text-slate-400">This job family has no Stop operation -- once started it cannot be cancelled.</p>
			{/if}
			{#if startJobError}
				<p class="text-sm text-red-600 dark:text-red-400">{startJobError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => startJobModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStartJob} disabled={startingJob} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{startingJob ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<!-- Job detail -->
<Modal bind:this={jobDetailModal} title="{selectedJobFamily.label} Job">
	{#snippet children()}
		{#if jobDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if jobDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{jobDetailError}</p>
		{:else if viewedJob}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Job ID</dt><dd class="text-slate-900 dark:text-white">{viewedJob.JobId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedJob.JobName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedJob.JobStatus ?? '—'}</dd></div>
				{#if viewedJob.Message}
					<div><dt class="text-slate-500 dark:text-slate-400">Message</dt><dd class="text-red-600 dark:text-red-400">{viewedJob.Message}</dd></div>
				{/if}
				<div><dt class="text-slate-500 dark:text-slate-400">Submit time</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedJob.SubmitTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => jobDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedJob && !selectedJobFamily.noStop && viewedJob.JobStatus !== 'STOPPED' && viewedJob.JobStatus !== 'COMPLETED' && viewedJob.JobStatus !== 'FAILED'}
			<button type="button" onclick={() => stopJob(viewedJob)} disabled={stoppingJob} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{stoppingJob ? 'Stopping…' : 'Stop job'}</button>
		{/if}
	{/snippet}
</Modal>
