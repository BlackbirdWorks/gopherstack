<script setup lang="ts">
import stats from '../generated/stats.json'
</script>

<template>
  <div class="gs-stats-grid" role="region" aria-label="Site statistics">
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.totalServices }}</div>
      <div class="gs-stat-label">Services</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.totalOps.toLocaleString() }}</div>
      <div class="gs-stat-label">Audited operations</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.gradeA }} / {{ stats.auditedServices }}</div>
      <div class="gs-stat-label">Grade A of audited</div>
    </div>
    <div class="gs-stat-card">
      <div class="gs-stat-value">{{ stats.exampleCount }}</div>
      <div class="gs-stat-label">Runnable examples</div>
    </div>
  </div>
</template>
