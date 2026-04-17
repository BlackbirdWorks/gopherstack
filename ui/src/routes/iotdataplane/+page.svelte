<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IoT Data Plane</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Publish and shadow operations</p>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Supported Operations</h2>
		<ul class="mt-3 space-y-2 text-sm text-slate-700 dark:text-slate-300">
			<li>Publish</li>
			<li>GetThingShadow</li>
			<li>UpdateThingShadow</li>
			<li>DeleteThingShadow</li>
		</ul>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<pre class="overflow-auto rounded-lg bg-slate-950 p-4 text-xs text-slate-100">// iot-data example
await client.publish(&#123; topic: 'demo/topic', payload: new TextEncoder().encode('hello') &#125;);</pre>
	</div>
</div>
