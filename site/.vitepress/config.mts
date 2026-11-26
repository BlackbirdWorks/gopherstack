import { defineConfig } from 'vitepress'
import { withMermaid } from 'vitepress-plugin-mermaid'
import sidebar from './generated/sidebar.json' with { type: 'json' }
import stats from './generated/stats.json' with { type: 'json' }

const base = process.env.DOCS_BASE || '/'

const navItems = [
  { text: 'Guide', link: '/guide/quickstart', activeMatch: '^/guide/' },
  { text: 'Services', link: '/services/', activeMatch: '^/services/' },
  { text: 'Examples', link: '/examples/', activeMatch: '^/examples/' },
  { text: 'Limitations', link: '/limitations' },
  { text: 'Migrate from LocalStack', link: '/guide/migration' },
  { text: 'Architecture', link: '/architecture/', activeMatch: '^/architecture/' },
]

if (stats.version) {
  navItems.push({
    text: stats.version,
    link: `https://github.com/BlackbirdWorks/gopherstack/releases/tag/${encodeURIComponent(stats.version)}`,
  })
}

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
        { icon: 'github', link: 'https://github.com/BlackbirdWorks/gopherstack' },
      ],

      footer: {
        message: 'Released under the MIT License.',
        copyright: 'Copyright © BlackbirdWorks',
      },
    },
  })
)
