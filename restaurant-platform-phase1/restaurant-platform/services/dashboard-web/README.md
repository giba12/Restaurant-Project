# dashboard-web

The React dashboard for the platform: station load, staff on shift, anomaly counts and the narrated-findings feed, all polled from `dashboard-api`. Built with Vite and served by nginx, which also proxies `/api/` to `dashboard-api` so the browser only ever talks to one origin.

- `src/App.jsx` is the whole UI (a `useApi` polling hook and four panels).
- `src/App.css` styles the panels; `src/index.css` centres the page and sets base heading styles.
- `nginx.conf.template` serves the build and proxies `/api/`.
- `src/App.test.jsx` holds the component tests: the real `<App/>` against canned `dashboard-api` answers (Vitest, React Testing Library, jsdom). `npm test` runs them; the `dashboard-web` CI job also lints and builds.

## Local development

```bash
npm install
VITE_API_BASE=http://localhost:8000/api npm run dev   # point at a running dashboard-api
npm test                                               # component tests, no API needed
npm run build                                          # static output in dist/
```

In the platform it runs as a container (see `Dockerfile`), on host port 8080 in Compose.
