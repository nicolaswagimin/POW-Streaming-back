# Observatorio Galeras — demostración de gRPC (backend)

Monitoreo sísmico **simulado** del volcán Galeras (Pasto, Nariño, Colombia), hecho para el taller de
Programación Orientada a la Web. El foco es **gRPC**: el sistema usa de verdad los cuatro tipos de RPC.

- Backend (este repo): https://github.com/nicolaswagimin/POW-Streaming-back
- Frontend Angular: https://github.com/nicolaswagimin/POW-Streaming-front

> Los datos son simulados con fines académicos. No es información oficial del Servicio Geológico Colombiano.

## Arquitectura

Render no expone gRPC público y los navegadores no hablan gRPC nativo. Por eso el backend es **un
solo contenedor** con tres componentes que se comunican **solo por gRPC**:

```
┌──────────────┐  WebSocket (JSON)   ┌──────────────────────── contenedor Docker (Render) ────────────────────────┐
│  Navegador   │ ◄─────────────────► │  Gateway Express + ws  ($PORT)                                              │
│  Angular     │   /ws, /api/*       │   cliente gRPC · sin lógica de negocio · traduce WS ⇄ gRPC                  │
│  (Vercel)    │                     │        │  ObtenerEstado (unario, deadline 3 s)                               │
└──────────────┘                     │        │  SuscribirEventos (stream del servidor)                             │
                                     │        │  SesionOperador (bidireccional)                                     │
                                     │        ▼            gRPC · HTTP/2 · Protocol Buffers                         │
                                     │  Servidor gRPC  galeras.v1.MonitoreoVolcanico  (localhost:50051)             │
                                     │   clase Observatorio: estado en memoria, detección, nivel de alerta          │
                                     │        ▲                                                                     │
                                     │        │  EnviarLecturas (stream del cliente) · metadata x-estacion-id       │
                                     │  Simulador: 4 estaciones (ANG, CRA, URC, CUF), cada una es un cliente gRPC   │
                                     └──────────────────────────────────────────────────────────────────────────────┘
```

`proto/galeras.proto` es la **única fuente de verdad**. Se carga en tiempo de ejecución con
`@grpc/proto-loader` (sin codegen) usando `keepCase, longs: Number, enums: String, defaults, oneofs`.

## Los 4 tipos de RPC

| Tipo | Firma | Quién llama | Uso en el sistema |
|---|---|---|---|
| **Unario** | `ObtenerEstado(ObtenerEstadoRequest) → EstadoVolcan` | Gateway → Servidor | Foto del volcán al conectar cada navegador, cada 2 s y en `GET /api/estado`. Deadline de 3 s; si vence, el gateway responde **504** (`DEADLINE_EXCEEDED`). |
| **Streaming del cliente** | `EnviarLecturas(stream LecturaSismica) → ResumenEnvio` | Simulador → Servidor | Cada estación envía 8 lecturas (una cada 400 ms), hace `end()` y recibe **un** resumen. Una estación desconocida responde `NOT_FOUND`; una amplitud o frecuencia negativa responde `INVALID_ARGUMENT`. En ambos casos el servidor ignora el resto del stream. |
| **Streaming del servidor** | `SuscribirEventos(SuscripcionRequest) → stream Evento` | Servidor → Gateway | Una suscripción por navegador que nunca se cierra desde el servidor. Empuja `lectura`, `sismo` y `cambio_alerta` (`oneof payload`). El listener se quita en `cancelled`, `close` y `error`. |
| **Bidireccional** | `SesionOperador(stream ComandoOperador) → stream RespuestaOperador` | Gateway ⇄ Servidor | Una sesión por navegador para ajustar umbrales, confirmar el nivel o pedir el estado. Responde `ack` y luego `estado`, o `error`. Un error **no** corta la sesión. |

**Metadata:** el simulador envía `x-estacion-id`; el gateway envía `x-cliente: gateway-web` y
`x-operador`. El servidor las lee con `call.metadata.get(...)` y las registra en el log.

**Interceptor:** todos los clientes gRPC del contenedor usan un interceptor de cliente
(`src/metricas.ts`). Este cuenta las llamadas, los mensajes y los streams abiertos por tipo de RPC,
y el gateway publica esas cifras cada 1 s.

## Lógica de dominio (`Observatorio`, en `src/grpc-server.ts`)

- **Estaciones:** Anganoy (ANG), Cráter (CRA), Urcunina (URC) y Cufiño (CUF). Una estación pasa a «sin señal» si no envía lecturas durante 5 s.
- **Sismo:** se registra si `amplitud ≥ umbral` (60 µm/s por defecto), con un enfriamiento de 1,5 s por estación.
- **Tipo:** frecuencia < 5 Hz → **LP**; ≥ 5 Hz → **VT**. Si la estación ya tuvo 2 sismos en los últimos 10 s → **TREMOR**.
- **Nivel**, según los sismos en una ventana deslizante de 5 min: ≥ 25 ROJO · ≥ 12 NARANJA · ≥ 5 AMARILLO · si no, VERDE. Se recalcula cada 2 s y con cada sismo, y emite `cambio_alerta` cuando cambia.
- **Validación de umbrales:** amplitud > 0 y 1 ≤ amarillo < naranja < rojo.
- **Simulador:** ruido de fondo de 5 a 25 µm/s y picos ocasionales (1,5 %). Un enjambre de 20 s sube la probabilidad de picos al 30 %, con amplitudes de 80 a 300 µm/s.

## Correr en local

Requisitos: Node 20 o superior.

```bash
# backend
cd POW-Streaming-back
npm install
npm test          # prueba los 4 RPC contra un servidor en el puerto 50999
npm run dev       # gRPC :50051 + simulador + gateway :8080

# frontend (en otra terminal)
cd POW-Streaming-front
npm install
npm start         # http://localhost:4200 → se conecta a http://localhost:8080
```

Si el build de Angular está en `../POW-Streaming-front/dist/Streaming/browser` (o en la ruta que
indique `STATIC_DIR`), el gateway también lo sirve en http://localhost:8080.

Endpoints: `GET /health`, `GET /api/estado`, `GET /api/metricas` y el WebSocket `/ws?operador=<nombre>`.

## Despliegue paso a paso (gratis, sin tarjeta)

1. **GitHub:** sube los dos repos (`POW-Streaming-back` y `POW-Streaming-front`).
2. **Render (backend):** en https://dashboard.render.com elige **New → Blueprint**, conecta
   `POW-Streaming-back` y acepta. `render.yaml` crea un web service Docker en plan **free** con
   health check en `/health`. Copia la URL que te asigne (por ejemplo `https://galeras-grpc.onrender.com`).
3. **Configurar el frontend:** en `POW-Streaming-front/src/app/config.ts` cambia
   `BACKEND_URL_PRODUCCION` por la URL de Render. Luego haz commit y push.
4. **Vercel (frontend):** en https://vercel.com elige **Add New → Project** e importa
   `POW-Streaming-front`. El framework se detecta como Angular y `vercel.json` ya fija la salida
   `dist/Streaming/browser`. Despliega.

> El plan gratuito de Render duerme el servicio tras un rato sin uso. La primera visita puede tardar
> cerca de 1 minuto; mientras tanto el dashboard muestra «Reintentando…» y se reconecta solo.

## Estructura

```
proto/galeras.proto   contrato gRPC (package galeras.v1)
src/proto.ts          carga dinámica del proto, tipos y fábrica de clientes
src/grpc-server.ts    Observatorio (dominio) + handlers de los 4 RPC
src/simulador.ts      estaciones simuladas (cliente gRPC, streaming del cliente)
src/gateway.ts        Express + WebSocket (cliente gRPC)
src/metricas.ts       interceptor de cliente con contadores por tipo de RPC
src/main.ts           arranca todo en un solo proceso
src/test-rpcs.ts      prueba de punta a punta de los 4 RPC (npm test)
Dockerfile, render.yaml
```
