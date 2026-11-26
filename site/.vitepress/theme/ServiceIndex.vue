<script setup lang="ts">
import { computed, ref } from 'vue'
import { withBase } from 'vitepress'
import rawServices from '../generated/services.json'

interface ServiceRecord {
  slug: string
  displayName: string
  category: string
  grade: string
  protocol: string
  sdkModule: string
  lastAuditDate: string
  leaksStatus: string
  opsTotal: number
  opsOK: number
  opsPartial: number
  opsGap: number
  opsDeferred: number
  familiesTotal: number
  knownGapsCount: number
  structuralGapsCount: number
  deferredCount: number
  hasGuide: boolean
  removed: boolean
}

type SortKey = 'displayName' | 'category' | 'grade' | 'opsTotal' | 'knownGapsCount'

const services = rawServices as ServiceRecord[]

const search = ref('')
const selectedCategory = ref('All')
const selectedGrade = ref('All')
const onlyWithGuide = ref(false)
const sortKey = ref<SortKey>('displayName')
const sortAsc = ref(true)

const categories = computed(() => {
  const seen = new Set<string>()
  for (const s of services) {
    seen.add(s.category)
  }
  return ['All', ...Array.from(seen).sort((a, b) => a.localeCompare(b))]
})

const filteredServices = computed(() => {
  const q = search.value.trim().toLowerCase()
  const list = services.filter((s) => {
    if (selectedCategory.value !== 'All' && s.category !== selectedCategory.value) {
      return false
    }
    if (selectedGrade.value !== 'All' && s.grade !== selectedGrade.value) {
      return false
    }
    if (onlyWithGuide.value && !s.hasGuide) {
      return false
    }
    if (!q) {
      return true
    }
    return (
      s.displayName.toLowerCase().includes(q) ||
      s.slug.toLowerCase().includes(q) ||
      s.category.toLowerCase().includes(q) ||
      s.protocol.toLowerCase().includes(q)
    )
  })

  return list.sort((a, b) => {
    const dir = sortAsc.value ? 1 : -1
    const av = a[sortKey.value]
    const bv = b[sortKey.value]
    if (typeof av === 'number' && typeof bv === 'number') {
      return (av - bv) * dir
    }
    return String(av).localeCompare(String(bv)) * dir
  })
})

function toggleSort(key: SortKey) {
  if (sortKey.value === key) {
    sortAsc.value = !sortAsc.value
    return
  }
  sortKey.value = key
  sortAsc.value = key === 'displayName' || key === 'category' || key === 'grade'
}

function sortIndicator(key: SortKey): string {
  if (sortKey.value !== key) {
    return ''
  }
  return sortAsc.value ? ' ↑' : ' ↓'
}

function gradeClass(grade: string): string {
  if (grade === 'Removed') {
    return 'gs-grade gs-grade-removed'
  }
  if (grade.startsWith('B')) {
    return 'gs-grade gs-grade-b'
  }
  return 'gs-grade gs-grade-a'
}

function resetFilters() {
  search.value = ''
  selectedCategory.value = 'All'
  selectedGrade.value = 'All'
  onlyWithGuide.value = false
}
</script>

<template>
  <div class="gs-index">
    <div class="gs-controls">
      <label class="gs-field">
        <span class="gs-field-label">Search</span>
        <input
          v-model="search"
          type="search"
          placeholder="Filter by name, slug, protocol, or category…"
          class="gs-input"
        />
      </label>

      <label class="gs-field">
        <span class="gs-field-label">Category</span>
        <select v-model="selectedCategory" class="gs-select">
          <option v-for="cat in categories" :key="cat" :value="cat">{{ cat }}</option>
        </select>
      </label>

      <label class="gs-field">
        <span class="gs-field-label">Grade</span>
        <select v-model="selectedGrade" class="gs-select">
          <option value="All">All</option>
          <option value="A">A</option>
          <option value="B">B</option>
          <option value="Removed">Removed</option>
        </select>
      </label>

      <label class="gs-checkbox">
        <input v-model="onlyWithGuide" type="checkbox" />
        <span>Has usage guide</span>
      </label>

      <button v-if="search || selectedCategory !== 'All' || selectedGrade !== 'All' || onlyWithGuide" type="button" class="gs-reset" @click="resetFilters">
        Reset
      </button>
    </div>

    <p class="gs-result-count">
      Showing <strong>{{ filteredServices.length }}</strong> of {{ services.length }} services
    </p>

    <div tabindex="0" role="region" aria-labelledby="service-index-caption" class="gs-table-wrapper">
      <table class="gs-table">
        <caption id="service-index-caption" class="sr-only">
          Gopherstack supported AWS and cloud services index
        </caption>
        <thead>
          <tr>
            <th scope="col">
              <button type="button" class="gs-sort-btn" @click="toggleSort('displayName')">
                Service{{ sortIndicator('displayName') }}
              </button>
            </th>
            <th scope="col">
              <button type="button" class="gs-sort-btn" @click="toggleSort('category')">
                Category{{ sortIndicator('category') }}
              </button>
            </th>
            <th scope="col">
              <button type="button" class="gs-sort-btn" @click="toggleSort('grade')">
                Grade{{ sortIndicator('grade') }}
              </button>
            </th>
            <th scope="col">
              <button type="button" class="gs-sort-btn" @click="toggleSort('opsTotal')">
                PARITY Entries{{ sortIndicator('opsTotal') }}
              </button>
            </th>
            <th scope="col">Protocol</th>
            <th scope="col">
              <button type="button" class="gs-sort-btn" @click="toggleSort('knownGapsCount')">
                Open Gaps{{ sortIndicator('knownGapsCount') }}
              </button>
            </th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="svc in filteredServices" :key="svc.slug">
            <th scope="row">
              <a :href="withBase(`/services/${svc.slug}`)" class="gs-svc-link">{{ svc.displayName }}</a>
              <span v-if="svc.hasGuide" class="gs-guide-pill">Guide</span>
            </th>
            <td>{{ svc.category }}</td>
            <td>
              <span :class="gradeClass(svc.grade)">{{ svc.grade }}</span>
            </td>
            <td>
              <template v-if="svc.opsTotal > 0">{{ svc.opsTotal }}</template>
              <template v-else-if="svc.familiesTotal > 0">{{ svc.familiesTotal }} families</template>
              <template v-else>—</template>
            </td>
            <td>
              <code v-if="svc.protocol">{{ svc.protocol }}</code>
              <span v-else>—</span>
            </td>
            <td>
              <template v-if="svc.removed">Removed</template>
              <template v-else-if="svc.knownGapsCount === 0">None</template>
              <template v-else>{{ svc.knownGapsCount }}</template>
            </td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>

<style scoped>
.gs-controls {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem 1rem;
  align-items: flex-end;
  margin: 1.25rem 0 0.75rem;
}

.gs-field {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  min-width: 150px;
  flex: 1 1 180px;
}

.gs-field-label {
  font-size: 0.78rem;
  font-weight: 600;
  color: var(--vp-c-text-2);
}

.gs-input,
.gs-select {
  border: 1px solid var(--vp-c-border);
  background: var(--vp-c-bg-soft);
  color: var(--vp-c-text-1);
  border-radius: 8px;
  padding: 0.45rem 0.7rem;
  font-size: 0.9rem;
}

.gs-checkbox {
  display: inline-flex;
  align-items: center;
  gap: 0.45rem;
  padding-bottom: 0.45rem;
  font-size: 0.88rem;
  cursor: pointer;
}

.gs-reset {
  padding: 0.45rem 0.85rem;
  border-radius: 8px;
  border: 1px solid var(--vp-c-border);
  background: var(--vp-c-bg-soft);
  font-size: 0.85rem;
  cursor: pointer;
}

.gs-reset:hover {
  border-color: var(--vp-c-brand-1);
  color: var(--vp-c-brand-1);
}

.gs-result-count {
  margin: 0 0 0.75rem;
  font-size: 0.88rem;
  color: var(--vp-c-text-2);
}

.gs-table-wrapper {
  overflow: auto;
  max-inline-size: 100%;
  max-block-size: min(680px, 75vh);
  container-type: inline-size;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg);
}

.gs-table {
  width: 100%;
  margin: 0;
  border-collapse: separate;
  border-spacing: 0;
  --label-2: "Category";
  --label-3: "Grade";
  --label-4: "PARITY Entries";
  --label-5: "Protocol";
  --label-6: "Open Gaps";
}

.gs-table thead {
  position: sticky;
  inset-block-start: 0;
  z-index: 2;
  background: var(--vp-c-bg-soft);
}

.gs-table th,
.gs-table td {
  padding: 0.6rem 0.85rem;
  text-align: left;
  border-bottom: 1px solid var(--vp-c-divider);
  font-size: 0.9rem;
}

.gs-table tbody th {
  position: sticky;
  inset-inline-start: 0;
  z-index: 1;
  background: var(--vp-c-bg);
  font-weight: 600;
}

.gs-sort-btn {
  background: none;
  border: none;
  padding: 0;
  font: inherit;
  font-weight: 600;
  color: var(--vp-c-text-1);
  cursor: pointer;
  white-space: nowrap;
}

.gs-sort-btn:hover {
  color: var(--vp-c-brand-1);
}

.gs-svc-link {
  font-weight: 600;
  text-decoration: none;
}

.gs-svc-link:hover {
  text-decoration: underline;
}

.gs-guide-pill {
  display: inline-block;
  margin-left: 0.45rem;
  padding: 0.08rem 0.45rem;
  border-radius: 999px;
  font-size: 0.72rem;
  font-weight: 600;
  background: var(--vp-c-brand-soft);
  color: var(--vp-c-brand-1);
}

@container (width < 640px) {
  .gs-table,
  .gs-table thead,
  .gs-table tbody,
  .gs-table tr,
  .gs-table th,
  .gs-table td {
    display: block;
  }

  .gs-table thead {
    display: none;
  }

  .gs-table tr {
    margin-block-end: 0.85rem;
    border-bottom: 2px solid var(--vp-c-divider);
  }

  .gs-table tbody th {
    position: sticky;
    inset-block-start: 0;
    z-index: 2;
    background: var(--vp-c-bg-soft);
  }

  .gs-table td:nth-child(2)::before {
    content: var(--label-2) ": ";
    font-weight: 600;
    color: var(--vp-c-text-2);
  }
  .gs-table td:nth-child(3)::before {
    content: var(--label-3) ": ";
    font-weight: 600;
    color: var(--vp-c-text-2);
  }
  .gs-table td:nth-child(4)::before {
    content: var(--label-4) ": ";
    font-weight: 600;
    color: var(--vp-c-text-2);
  }
  .gs-table td:nth-child(5)::before {
    content: var(--label-5) ": ";
    font-weight: 600;
    color: var(--vp-c-text-2);
  }
  .gs-table td:nth-child(6)::before {
    content: var(--label-6) ": ";
    font-weight: 600;
    color: var(--vp-c-text-2);
  }
}
</style>
