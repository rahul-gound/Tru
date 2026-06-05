FROM node:20-alpine

WORKDIR /app

COPY server.js ./
RUN npm init -y \
  && npm install --omit=dev express express-rate-limit

ENV PORT=7860
EXPOSE 7860

CMD ["node", "server.js"]
