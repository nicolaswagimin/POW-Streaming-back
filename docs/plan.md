# Observatorio Galeras — Plan de implementación

> Ejecución: inline (executing-plans), según lo pidió el usuario.

**Goal:** Demo de gRPC (4 tipos de RPC) con monitoreo sísmico simulado del Galeras, dashboard Angular en vivo.

**Architecture:** Un contenedor Node: servidor gRPC en `localhost:50051`, simulador (cliente gRPC, client-streaming) y gateway Express+WS (cliente gRPC) en `$PORT`. El navegador habla JSON por WebSocket con el gateway.

**Tech Stack:** Node 20, TypeScript, @grpc/grpc-js, @grpc/proto-loader, express, ws, tsx; Angular 22 standalone + signals.

**Spec:** prompt del usuario (sección "Proyecto: Observatorio Galeras").

## Repos (adaptación de la estructura del spec a los dos repos entregados)
- `backend/` → POW-Streaming-back: `proto/galeras.proto`, `src/*.ts`, `Dockerfile` (contexto = raíz del repo), `render.yaml`, `README.md`.
- `Streaming/` → POW-Streaming-front: Angular sin SSR, `vercel.json` con output `dist/Streaming/browser`.
- Gateway sirve estáticos desde `STATIC_DIR` (por defecto `../Streaming/dist/Streaming/browser`) si existe.

## Global Constraints
- proto-loader: `keepCase:true, longs:Number, enums:String, defaults:true, oneofs:true`.
- Rama del oneof por campo virtual (`c.accion`, `e.payload`, `r.resultado`); nunca por truthiness de enums.
- Umbrales por defecto: amplitud 60, amarillo 5, naranja 12, rojo 25. Ventana 5 min, recálculo 2 s, enfriamiento 1,5 s, sin señal a los 5 s, TREMOR si ≥2 sismos previos en 10 s.
- Colores: fondo #F6F7F3, tinta #1E2B35, retícula #CBD4D3, trazas #1F4E8C #A3262A #2E6B3A #A8740F. Barlow / Barlow Condensed.

## Review Focus
1. Cliente WS que se desconecta → listener del EventEmitter debe quitarse (`cancelled` y `close`) o hay fuga. Cubierto: test cancela la suscripción y verifica `listenerCount` 0.
2. Enfriamiento y TREMOR usan `timestamp_ms` de la lectura (no el reloj), así el test de picos cada 2 s es determinista.
3. Umbrales inválidos (amarillo ≥ naranja, amplitud ≤ 0) → `error`, estado intacto. Cubierto en test bidi.
4. Backend dormido en Render → el front reintenta con backoff y muestra "Reintentando…".
5. Canvas en grid → `position:absolute; inset:0` + `min-width:0` para evitar bucle de ResizeObserver.

---

### Task 1: Contrato y test de los 4 RPC (rojo)
Files: `proto/galeras.proto`, `src/proto.ts`, `src/test-rpcs.ts`, `package.json`, `tsconfig.json`.
- `proto.ts` exporta `cargarPaquete()` → `galeras.v1` y `OPCIONES_PROTO`.
- `test-rpcs.ts`: `iniciarServidorGrpc(50999)` y asserts: unario VERDE + 4 estaciones; client-stream 10 lecturas, 5 picos (amp 150) cada 2 s → `sismos_detectados===5`, `lecturas_recibidas===10`; server-stream recibe `lectura`, `sismo` y `cambio_alerta.nuevo==='AMARILLO'`; bidi: estado, ack umbrales, ack confirmación, error por umbrales inválidos; listener count 0 tras cancelar.
- Run `npm test` → falla (no existe grpc-server). Commit.

### Task 2: Servidor gRPC + dominio (verde)
Files: `src/grpc-server.ts`. Produce `iniciarServidorGrpc(puerto:number): Promise<{server, dominio}>`; clase `Dominio` (estado, `registrarLectura`, `recalcular`, `validarUmbrales`, `eventos: EventEmitter`). Run `npm test` → PASS. Commit.

### Task 3: Simulador, métricas, gateway, main, Docker, Render
Files: `src/simulador.ts` (`Simulador.iniciar()`, `enjambre(ms=20000)`), `src/metricas.ts` (`contar(rpc, 'llamadas'|'mensajes')`, `snapshot()`), `src/gateway.ts`, `src/main.ts`, `Dockerfile`, `render.yaml`. Verificar con `curl /health` y `/api/estado`. Commit.

### Task 4: Frontend Angular
Quitar SSR; `config.ts`, `galeras.service.ts` (signals, backoff), `helicorder.ts` (canvas), `app.*`, `styles.css`, `vercel.json`. `ng build` OK. Commit.

### Task 5: Verificación end-to-end + README
Backend sirviendo el build; en navegador: trazas, enjambre sube nivel, comandos responden. README en español. Commit.
