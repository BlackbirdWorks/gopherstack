<script lang="ts">
import { Archive, Info, CheckCircle, Database } from 'lucide-svelte';

const storageClasses = [
{ name: 'S3 Glacier Instant Retrieval', retrieval: 'Milliseconds', minStorage: '90 days', cost: 'Low', useCase: 'Rarely accessed data requiring immediate retrieval' },
{ name: 'S3 Glacier Flexible Retrieval', retrieval: '3-5 hours (Standard)', minStorage: '90 days', cost: 'Very Low', useCase: 'Archive data, 1-2 retrievals per year' },
{ name: 'S3 Glacier Deep Archive', retrieval: '12 hours (Standard)', minStorage: '180 days', cost: 'Lowest', useCase: 'Long-term retention, accessed 1-2x per year' }
];
</script>

<div class="p-6 space-y-6">
<div class="flex items-center gap-3">
<Archive class="w-7 h-7 text-blue-800 dark:text-blue-300" />
<div>
<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Glacier</h1>
<p class="text-sm text-gray-500 dark:text-gray-400">Secure, durable, and extremely low-cost cloud storage for data archiving and long-term backup</p>
</div>
</div>

<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-lg p-4 flex items-start gap-3">
<Info class="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
<div>
<p class="text-sm font-medium text-blue-800 dark:text-blue-300">Managed via Amazon S3</p>
<p class="text-sm text-blue-700 dark:text-blue-400 mt-1">Amazon S3 Glacier storage classes are managed through Amazon S3. Use the S3 console to configure lifecycle policies to automatically archive objects.</p>
</div>
</div>

<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
<div class="p-4 border-b border-slate-200 dark:border-slate-700">
<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Glacier Storage Classes</h2>
</div>
<div class="p-4 space-y-4">
{#each storageClasses as cls}
<div class="p-4 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
<h3 class="font-semibold text-gray-900 dark:text-white mb-2">{cls.name}</h3>
<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
<div>
<p class="text-gray-500 dark:text-gray-400 text-xs">Retrieval Time</p>
<p class="text-gray-900 dark:text-white font-medium">{cls.retrieval}</p>
</div>
<div>
<p class="text-gray-500 dark:text-gray-400 text-xs">Min Storage Duration</p>
<p class="text-gray-900 dark:text-white font-medium">{cls.minStorage}</p>
</div>
<div>
<p class="text-gray-500 dark:text-gray-400 text-xs">Cost</p>
<p class="text-gray-900 dark:text-white font-medium">{cls.cost}</p>
</div>
<div>
<p class="text-gray-500 dark:text-gray-400 text-xs">Use Case</p>
<p class="text-gray-700 dark:text-gray-300">{cls.useCase}</p>
</div>
</div>
</div>
{/each}
</div>
</div>
</div>
