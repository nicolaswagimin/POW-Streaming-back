# Contexto de build = raíz de este repo (contiene /proto y /src).
FROM node:20-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY proto ./proto
COPY src ./src
RUN npx tsc

FROM node:20-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY package.json package-lock.json ./
RUN npm ci --omit=dev
COPY proto ./proto
COPY --from=build /app/dist ./dist
# Render inyecta $PORT; el gRPC queda interno en localhost:50051.
EXPOSE 8080
CMD ["node", "dist/main.js"]
