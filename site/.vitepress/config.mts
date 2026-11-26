import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'
import sidebar from './generated/sidebar.json' with { type: 'json' }
import stats from './generated/stats.json' with { type: 'json' }

const base = process.env.DOCS_BASE || '/'

const releasesUrl = 'https://github.com/BlackbirdWorks/gopherstack/releases'

const navItems: any[] = [
  {
    text: 'Guide',
    activeMatch: '^/guide/',
    items: [
      { text: 'Quickstart', link: '/guide/quickstart' },
      { text: 'Docker & Compose', link: '/guide/docker' },
      { text: 'Migrate from LocalStack', link: '/guide/migration' },
      { text: 'Performance', link: '/guide/performance' },
      { text: 'Testcontainers', link: '/guide/testcontainers' },
    ],
  },
  { text: 'Services', link: '/services/', activeMatch: '^/services/' },
  { text: 'Examples', link: '/examples/', activeMatch: '^/examples/' },
  {
    text: 'Reference',
    items: [
      { text: 'Limitations & Parity Gaps', link: '/limitations' },
      { text: 'Architecture', link: '/architecture/' },
      { text: 'Engine Modes', link: '/guide/engines' },
      { text: 'Chaos Testing', link: '/guide/chaos' },
    ],
  },
  {
    text: stats.version || 'Releases',
    items: [
      {
        text: 'Release notes',
        link: stats.version
          ? `${releasesUrl}/tag/${encodeURIComponent(stats.version)}`
          : releasesUrl,
      },
      { text: 'All releases', link: releasesUrl },
      { text: 'BlackbirdWorks', link: 'https://blackbirdworks.dev' },
    ],
  },
]

const blackbirdLogoSvg =
  '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64">' +
  '<defs>' +
  '<linearGradient id="bb-nav-bg" x1="0" y1="0" x2="1" y2="1">' +
  '<stop offset="0%" stop-color="#0f172a"/>' +
  '<stop offset="100%" stop-color="#1e1b4b"/>' +
  '</linearGradient>' +
  '<linearGradient id="bb-nav-sr71" x1="0" y1="0" x2="0.4" y2="1">' +
  '<stop offset="0%" stop-color="#f8fafc"/>' +
  '<stop offset="55%" stop-color="#a5b4fc"/>' +
  '<stop offset="100%" stop-color="#22d3ee"/>' +
  '</linearGradient>' +
  '</defs>' +
  '<rect width="64" height="64" rx="14" fill="url(#bb-nav-bg)"/>' +
  '<g transform="translate(16.4 4.7) scale(0.13)">' +
  '<path fill="url(#bb-nav-sr71)" d="M 119 22 L 119 9 L 121 9 L 121 22 C 129 36 139 64 143.5 113 L 143.5 210 L 161 241 L 167 232 L 170.5 207 L 173 228 L 182 244 C 190 258 192.5 266 192.5 306 L 221 361 C 223 366 222 372 218 375 L 189 384 L 184 378 L 179 378 L 178 382 L 170 378 L 169 382 L 162 377 L 156 389 L 136 396 L 123.5 399 L 120 410 L 116.5 399 L 104 396 L 84 389 L 78 377 L 71 382 L 70 378 L 62 382 L 61 378 L 56 378 L 51 384 L 22 375 C 18 372 17 366 19 361 L 47.5 306 C 47.5 266 50 258 58 244 L 67 228 L 69.5 207 L 73 232 L 79 241 L 96.5 210 L 96.5 113 C 101 64 111 36 119 22 Z"/>' +
  '</g></svg>'

export default withMermaid(
  defineConfig({
    title: 'Gopherstack',
    description: `${stats.totalServices}+ AWS & cloud services in a single Go binary. Milliseconds, not minutes.`,
    base,
    cleanUrls: true,
    lastUpdated: true,
    ignoreDeadLinks: [/^https?:\/\/localhost/],

    head: [
      ['meta', { name: 'color-scheme', content: 'light dark' }],
      ['link', { rel: 'icon', type: 'image/png', href: `${base}favicon.png` }],
      ['meta', { property: 'og:type', content: 'website' }],
      ['meta', { property: 'og:url', content: 'https://gopherstack.blackbirdworks.dev/' }],
      ['meta', { property: 'og:title', content: 'Gopherstack Documentation' }],
      [
        'meta',
        {
          property: 'og:description',
          content: `${stats.totalServices}+ AWS & cloud services in a single Go binary. Milliseconds, not minutes.`,
        },
      ],
      ['meta', { property: 'og:image', content: 'https://gopherstack.blackbirdworks.dev/logo.png' }],
    ],

    themeConfig: {
      logo: '/logo.png',
      siteTitle: 'Gopherstack',

      nav: navItems,
      sidebar,

      search: {
        provider: 'local',
      },

      editLink: {
        pattern: ({ filePath }) => {
          const baseEdit = 'https://github.com/BlackbirdWorks/gopherstack/edit/main/'
          if (filePath === 'index.md') return `${baseEdit}site/index.md`
          if (filePath === 'limitations.md') return `${baseEdit}cmd/gendocs/site_pages.go`
          if (filePath === 'guide/performance.md') return `${baseEdit}cmd/gendocs/site.go`
          if (filePath.startsWith('guide/')) return `${baseEdit}docs/${filePath.slice('guide/'.length)}`
          if (filePath === 'architecture/index.md') return `${baseEdit}docs/architecture/README.md`
          if (filePath.startsWith('architecture/')) return `${baseEdit}docs/${filePath}`
          if (filePath === 'services/index.md') return `${baseEdit}cmd/gendocs/site_pages.go`
          if (filePath.startsWith('services/')) {
            const slug = filePath.slice('services/'.length).replace(/\.md$/, '')
            return `${baseEdit}services/${slug}/PARITY.md`
          }
          if (filePath === 'examples/index.md') return `${baseEdit}examples/README.md`
          if (filePath.startsWith('examples/')) {
            const slug = filePath.slice('examples/'.length).replace(/\.md$/, '')
            return `${baseEdit}examples/${slug}/README.md`
          }
          return `${baseEdit}${filePath}`
        },
        text: 'Edit source on GitHub',
      },

      socialLinks: [
        { icon: { svg: blackbirdLogoSvg }, link: 'https://blackbirdworks.dev', ariaLabel: 'BlackbirdWorks' },
        { icon: 'github', link: 'https://github.com/BlackbirdWorks/gopherstack' },
      ],

      footer: {
        message: 'Released under the MIT License.',
        copyright: 'Copyright © BlackbirdWorks',
      },
    },
  })
)
