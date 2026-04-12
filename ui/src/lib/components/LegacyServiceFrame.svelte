<script lang="ts">
  import { toast } from "svelte-sonner";

  let { title, description, src } = $props<{ title: string; description: string; src: string }>();

  let frameKey = $state(0);

  function refreshFrame(): void {
    frameKey += 1;
    toast.info(`${title} reloaded`);
  }
</script>

<section class="space-y-4">
  <div class="flex items-center justify-between gap-3">
    <div>
      <p class="text-xs font-semibold uppercase tracking-[0.18em] text-zinc-500 dark:text-zinc-400">{title}</p>
      <h2 class="text-2xl font-semibold">{title}</h2>
      <p class="text-sm text-zinc-600 dark:text-zinc-300">{description}</p>
    </div>
    <button
      type="button"
      class="rounded-lg border border-zinc-300 bg-white/70 px-4 py-2 text-sm font-semibold hover:bg-zinc-100 dark:border-zinc-700 dark:bg-zinc-900/70 dark:hover:bg-zinc-800"
      onclick={refreshFrame}
    >
      Reload
    </button>
  </div>

  <div class="overflow-hidden rounded-xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-900">
    <iframe
      title={title}
      src={src}
      class="h-[82vh] w-full bg-white dark:bg-slate-900"
      key={frameKey}
    ></iframe>
  </div>
</section>
