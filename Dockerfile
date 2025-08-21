FROM node:20-alpine
ENV NODE_ENV=production
WORKDIR /app

COPY package*.json ./
RUN npm ci

COPY . .

USER node
EXPOSE 8080
CMD ["node", "src/server.js"]
