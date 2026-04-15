<script lang="ts">
	let { servicePath, fallbackLabel }: { servicePath: string; fallbackLabel: string } = $props();

	const headingByService: Record<string, string> = {
		cognitoidp: 'Cognito User Pools',
		iam: 'IAM',
		rds: 'RDS Instances',
		scheduler: 'Scheduler',
		shield: 'Shield Advanced',
		sns: 'SNS Topics',
		sqs: 'SQS Queues',
		ssm: 'SSM Parameter Store'
	};

	const heading = $derived(headingByService[servicePath] ?? fallbackLabel);
</script>

<section class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-slate-100">{heading}</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
			This service page is not fully implemented in the new UI yet.
		</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h2 class="text-lg font-semibold text-slate-900 dark:text-slate-100">What You Can Do</h2>
		<ul class="mt-3 list-disc space-y-2 pl-6 text-sm text-slate-700 dark:text-slate-300">
			<li>Use the sidebar to switch to another fully implemented service.</li>
			<li>Use the top-right Metrics and Console tabs for visibility and API traces.</li>
			<li>Return here later as this service gets a native page.</li>
		</ul>
	</div>
</section>
