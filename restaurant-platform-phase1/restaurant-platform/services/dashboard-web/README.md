# dashboard-web

The React dashboard for the platform: station load, staff on shift, anomaly counts and the narrated-findings feed, all polled from `dashboard-api`. Built with Vite and served by nginx, which also proxies `/api/` to `dashboard-api` so the browser only ever talks to one origin.

- `src/App.jsx` is the whole UI (a `useApi` polling hook and four panels).
- `src/App.css` styles the panels; `src/index.css` centres the page and sets base heading styles.
- `nginx.conf` serves the build and proxies `/api/`.

## Local development

```bash
npm install
VITE_API_BASE=http://localhost:8000/api npm run dev   # point at a running dashboard-api
npm run build                                          # static output in dist/
```

In the platform it runs as a container (see `Dockerfile`), on host port 8080 in Compose.
