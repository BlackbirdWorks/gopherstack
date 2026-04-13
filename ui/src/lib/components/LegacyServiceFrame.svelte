<script lang="ts">
  let { title, src } = $props<{ title: string; src: string }>();
</script>

<section>
  <iframe
    title={title}
    src={src}
    class="h-[88vh] w-full rounded-xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-900"
  ></iframe>
</section>
