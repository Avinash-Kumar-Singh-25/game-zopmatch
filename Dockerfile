# ZopMatch — zero-dependency Node server. No build step, no npm install.
FROM node:20-alpine

# app dir owned by the non-root "node" user so runtime state (rooms.json) is writable
RUN mkdir -p /app && chown node:node /app
WORKDIR /app
USER node

COPY --chown=node:node package.json server.js ./
COPY --chown=node:node public ./public

# server reads process.env.PORT (falls back to 3000)
ENV PORT=3000
EXPOSE 3000

CMD ["node", "server.js"]
