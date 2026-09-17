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
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
