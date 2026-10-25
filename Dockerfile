FROM node:22-bookworm-slim
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
      git ca-certificates curl tini python3 make g++ ffmpeg imagemagick webp chromium fonts-noto-color-emoji \
    && rm -rf /var/lib/apt/lists/*
# Chromium wrapper for whatsapp-web.js / puppeteer bots (container-safe flags)
RUN printf '#!/bin/sh\nexec /usr/bin/chromium --no-sandbox --disable-dev-shm-usage --disable-gpu "$@"\n' > /usr/local/bin/chromium-safe && chmod +x /usr/local/bin/chromium-safe
ENV PUPPETEER_EXECUTABLE_PATH=/usr/local/bin/chromium-safe CHROME_PATH=/usr/local/bin/chromium-safe \
    PUPPETEER_SKIP_DOWNLOAD=true NODE_ENV=production PORT=7860 DATA_DIR=/data
WORKDIR /app
COPY package.json ./
RUN npm install --omit=dev --no-audit --no-fund
COPY . .
RUN mkdir -p /data && chown -R node:node /data /app
USER node
EXPOSE 7860
HEALTHCHECK --interval=60s --timeout=5s CMD curl -fs http://127.0.0.1:${PORT}/health || exit 1
ENTRYPOINT ["/usr/bin/tini","--"]
CMD ["node","server.js"]
