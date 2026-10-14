// Gateway HTTP + WebSocket: traduce gRPC ⇄ JSON para el navegador.
import fs from 'node:fs';
import http from 'node:http';
import path from 'node:path';
import express from 'express';
import cors from 'cors';
import { WebSocket, WebSocketServer } from 'ws';
import * as grpc from '@grpc/grpc-js';
import { crearCliente, EstadoVolcan, Evento, RespuestaOperador } from './proto';
import { snapshot } from './metricas';
import { Simulador } from './simulador';

const DEADLINE_UNARIO_MS = 3000;

/** Metadata que el gateway adjunta a cada llamada gRPC. */
function metadataGateway(operador = 'gateway'): grpc.Metadata {
  const md = new grpc.Metadata();
  md.set('x-cliente', 'gateway-web');
  md.set('x-operador', operador);
  return md;
}

const DIR_ESTATICO = process.env.STATIC_DIR ?? path.resolve(__dirname, '..', '..', 'Streaming', 'dist', 'Streaming', 'browser');

export function iniciarGateway(puerto: number, direccionGrpc: string, simulador: Simulador): http.Server {
  const cliente = crearCliente(direccionGrpc);
  // Unario con deadline de 3 s: si vence, el error llega con status DEADLINE_EXCEEDED.
  const obtenerEstado = (operador?: string) =>
    new Promise<EstadoVolcan>((res, rej) =>
      cliente.ObtenerEstado({}, metadataGateway(operador), { deadline: Date.now() + DEADLINE_UNARIO_MS },
        (err: grpc.ServiceError | null, r: EstadoVolcan) => (err ? rej(err) : res(r))),
    );

  const app = express();
  app.use(cors());
  app.get('/health', (_req, res) => { res.json({ ok: true }); });
  app.get('/api/estado', async (_req, res) => {
    try {
      res.json(await obtenerEstado());
    } catch (err) {
      const e = err as grpc.ServiceError;
      res.status(e.code === grpc.status.DEADLINE_EXCEEDED ? 504 : 502).json({ error: e.details ?? e.message, codigo: grpc.status[e.code] });
    }
  });
  app.get('/api/metricas', (_req, res) => { res.json(snapshot()); });
  if (fs.existsSync(path.join(DIR_ESTATICO, 'index.html'))) {
    app.use(express.static(DIR_ESTATICO));
    console.log(`[gateway] sirviendo el frontend desde ${DIR_ESTATICO}`);
  }

  const server = http.createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  const enviar = (ws: WebSocket, msg: unknown) => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); };
  const difundir = (msg: unknown) => wss.clients.forEach((ws) => enviar(ws, msg));

  wss.on('connection', (ws, req) => {
    const operador = (new URL(req.url ?? '/ws', 'http://x').searchParams.get('operador') || 'anónimo').slice(0, 40);

    // 1) Unario: foto inicial del estado.
    obtenerEstado(operador).then((estado) => enviar(ws, { tipo: 'estado', estado })).catch(() => {});

    // 2) Streaming del servidor: eventos en vivo. El CANCELLED al cerrar es esperado.
    const sub = cliente.SuscribirEventos({ incluir_lecturas: true }, metadataGateway(operador));
    sub.on('data', (evento: Evento) => enviar(ws, { tipo: 'evento', evento }));
    sub.on('error', (err: grpc.ServiceError) => { if (err.code !== grpc.status.CANCELLED) console.error('[gateway] sub', err.message); });

    // 3) Bidireccional: sesión del operador de este navegador.
    const sesion = cliente.SesionOperador(metadataGateway(operador));
    sesion.on('data', (respuesta: RespuestaOperador) => enviar(ws, { tipo: 'operador', respuesta }));
    sesion.on('error', (err: grpc.ServiceError) => { if (err.code !== grpc.status.CANCELLED) console.error('[gateway] sesión', err.message); });

    enviar(ws, { tipo: 'metricas', metricas: snapshot(), enjambreHasta: simulador.enjambreActivoHasta });

    ws.on('message', (datos) => {
      let msg: any;
      try { msg = JSON.parse(String(datos)); } catch { return; }
      if (msg?.tipo === 'comando' && msg.comando && typeof msg.comando === 'object') {
        sesion.write(msg.comando);
      } else if (msg?.tipo === 'enjambre') {
        const hasta = simulador.enjambre(20_000);
        difundir({ tipo: 'enjambre', hasta });
      }
    });

    ws.on('close', () => {
      sub.cancel();
      sesion.end();
    });
  });

  // Métricas cada 1 s y estado (unario) cada 2 s para todos los navegadores.
  setInterval(() => { if (wss.clients.size) difundir({ tipo: 'metricas', metricas: snapshot(), enjambreHasta: simulador.enjambreActivoHasta }); }, 1000);
  setInterval(() => {
    if (wss.clients.size) obtenerEstado().then((estado) => difundir({ tipo: 'estado', estado })).catch(() => {});
  }, 2000);

  server.listen(puerto, () => console.log(`[gateway] HTTP + WS en :${puerto} (WS en /ws)`));
  return server;
}
