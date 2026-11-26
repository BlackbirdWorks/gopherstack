<script setup lang="ts">
import stats from '../generated/stats.json'
</script>

<template>
  <div class="gs-stats-grid" role="region" aria-label="Gopherstack metrics">
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.totalServices }}</div>
      <div class="gs-stat-label">AWS &amp; Cloud Services</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.totalOps.toLocaleString() }}</div>
      <div class="gs-stat-label">Audited PARITY Entries</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.gradeA }} / {{ stats.auditedServices }}</div>
      <div class="gs-stat-label">Grade A Audited Services</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">&lt;100 ms</div>
      <div class="gs-stat-label">Cold Start · ~30 MB RAM</div>
    </div>
  </div>
</template>
