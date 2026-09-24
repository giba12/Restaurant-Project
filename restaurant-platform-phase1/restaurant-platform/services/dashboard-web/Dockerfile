# Multi-stage: build the static React app, then serve it with nginx
# (which also reverse-proxies /api/ to the dashboard-api service, so the
# browser only ever talks to one origin).
FROM docker.io/library/node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install
COPY . .
RUN npm run build

FROM docker.io/library/nginx:1.27-alpine
COPY --from=build /app/dist /usr/share/nginx/html
# A template, not a static conf: the base image's own entrypoint runs
# envsubst over every /etc/nginx/templates/*.template at container start,
# substituting DASHBOARD_API_KEY (see nginx.conf.template's own header
# comment) with no extra tooling. k8s mounts a different template over
# this same path to add HTTP Basic Auth; Compose uses this one as-is.
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
EXPOSE 80
