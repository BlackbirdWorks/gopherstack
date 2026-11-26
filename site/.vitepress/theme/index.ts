import type { Theme } from 'vitepress'
import DefaultTheme from 'vitepress/theme'
import ServiceIndex from './ServiceIndex.vue'
import SiteStats from './SiteStats.vue'
import './style.css'

export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('ServiceIndex', ServiceIndex)
    app.component('SiteStats', SiteStats)
  },
} satisfies Theme
