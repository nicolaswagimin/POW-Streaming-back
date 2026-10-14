// Prueba de punta a punta de los 4 tipos de RPC contra un servidor en un puerto de prueba.
import assert from 'node:assert/strict';
import * as grpc from '@grpc/grpc-js';
import { crearCliente, EstadoVolcan, Evento, LecturaSismica, ResumenEnvio, RespuestaOperador } from './proto';
import { iniciarServidorGrpc } from './grpc-server';

const PUERTO = 50999;
let fallos = 0;

async function prueba(nombre: string, fn: () => Promise<void>) {
  try {
    await fn();
    console.log(`  ✔ ${nombre}`);
  } catch (err) {
    fallos++;
    console.error(`  ✘ ${nombre}\n    ${(err as Error).message}`);
  }
}

function conTiempoLimite<T>(p: Promise<T>, ms: number, que: string): Promise<T> {
  return Promise.race([
    p,
    new Promise<T>((_, rej) => setTimeout(() => rej(new Error(`Tiempo agotado: ${que}`)), ms)),
  ]);
}

async function main() {
  const { server, observatorio } = await iniciarServidorGrpc(PUERTO);
  const cliente = crearCliente(`localhost:${PUERTO}`);
  console.log('Observatorio Galeras — prueba de los 4 RPC');

  await prueba('Unario ObtenerEstado: VERDE y 4 estaciones', async () => {
    const estado = await new Promise<EstadoVolcan>((res, rej) =>
      cliente.ObtenerEstado({}, (err: Error | null, r: EstadoVolcan) => (err ? rej(err) : res(r))),
    );
    assert.equal(estado.nivel, 'VERDE');
    assert.equal(estado.estaciones.length, 4);
    assert.deepEqual(estado.estaciones.map((e) => e.id).sort(), ['ANG', 'CRA', 'CUF', 'URC']);
    assert.equal(estado.umbrales.amplitud_sismo, 60);
  });

  // La suscripción se abre antes de enviar lecturas para observar todo el flujo.
  const recibidos: Evento[] = [];
  const sub = cliente.SuscribirEventos({ incluir_lecturas: true });
  sub.on('data', (e: Evento) => recibidos.push(e));
  sub.on('error', () => {});
  await new Promise((r) => setTimeout(r, 150));

  await prueba('Streaming cliente EnviarLecturas: 10 lecturas, 5 picos → 5 sismos', async () => {
    const resumen = await new Promise<ResumenEnvio>((res, rej) => {
      const md = new grpc.Metadata();
      md.set('x-estacion-id', 'CRA');
      const llamada = cliente.EnviarLecturas(md, (err: Error | null, r: ResumenEnvio) => (err ? rej(err) : res(r)));
      const base = Date.now();
      for (let i = 0; i < 10; i++) {
        const pico = i % 2 === 0;
        llamada.write({
          estacion_id: 'CRA',
          timestamp_ms: base + i * 1000, // los picos (i par) quedan espaciados 2 s
          amplitud_um_s: pico ? 150 : 12,
          frecuencia_hz: pico ? 8 : 3,
        });
      }
      llamada.end();
    });
    assert.equal(resumen.estacion_id, 'CRA');
    assert.equal(resumen.lecturas_recibidas, 10);
    assert.equal(resumen.sismos_detectados, 5);
    assert.equal(resumen.amplitud_maxima, 150);
  });

  const enviarLote = (lecturas: LecturaSismica[]) =>
    new Promise<grpc.ServiceError | null>((res) => {
      const llamada = cliente.EnviarLecturas(new grpc.Metadata(), (err: grpc.ServiceError | null) => res(err));
      lecturas.forEach((l) => llamada.write(l));
      llamada.end();
    });

  await prueba('Streaming cliente: estación desconocida → NOT_FOUND', async () => {
    const err = await enviarLote([{ estacion_id: 'XXX', timestamp_ms: Date.now(), amplitud_um_s: 10, frecuencia_hz: 3 }]);
    assert.equal(err?.code, grpc.status.NOT_FOUND);
  });

  await prueba('Streaming cliente: amplitud negativa → INVALID_ARGUMENT y no procesa el resto', async () => {
    const antes = recibidos.length;
    const err = await enviarLote([
      { estacion_id: 'ANG', timestamp_ms: Date.now(), amplitud_um_s: -1, frecuencia_hz: 3 },
      { estacion_id: 'ANG', timestamp_ms: Date.now(), amplitud_um_s: 10, frecuencia_hz: 3 },
    ]);
    assert.equal(err?.code, grpc.status.INVALID_ARGUMENT);
    await new Promise((r) => setTimeout(r, 100));
    assert.equal(recibidos.length, antes, 'ninguna lectura del stream inválido llega al bus');
  });

  await prueba('Streaming servidor SuscribirEventos: lecturas, sismos y cambio a AMARILLO', async () => {
    await conTiempoLimite(
      (async () => {
        while (!recibidos.some((e) => e.payload === 'cambio_alerta')) await new Promise((r) => setTimeout(r, 50));
      })(),
      3000,
      'esperando cambio_alerta',
    );
    assert.equal(recibidos.filter((e) => e.payload === 'lectura').length, 10);
    const sismos = recibidos.filter((e) => e.payload === 'sismo');
    assert.equal(sismos.length, 5);
    assert.ok(sismos.some((e) => e.sismo!.tipo === 'TREMOR'), 'el 3.er pico seguido en 10 s es TREMOR');
    const cambio = recibidos.find((e) => e.payload === 'cambio_alerta')!.cambio_alerta!;
    assert.equal(cambio.anterior, 'VERDE');
    assert.equal(cambio.nuevo, 'AMARILLO');
  });

  await prueba('Cancelar la suscripción quita el listener del EventEmitter', async () => {
    sub.cancel();
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(observatorio.eventos.listenerCount('evento'), 0);
  });

  await prueba('Bidireccional SesionOperador: estado, ack umbrales, ack confirmación, error', async () => {
    const respuestas: RespuestaOperador[] = [];
    const sesion = cliente.SesionOperador(new grpc.Metadata());
    sesion.on('data', (r: RespuestaOperador) => respuestas.push(r));
    sesion.on('error', () => {});
    sesion.write({ operador: 'prueba', solicitar_estado: true });
    sesion.write({ operador: 'prueba', ajustar_umbrales: { amplitud_sismo: 70, sismos_amarillo: 4, sismos_naranja: 10, sismos_rojo: 20 } });
    sesion.write({ operador: 'prueba', confirmar_alerta: 'AMARILLO' });
    sesion.write({ operador: 'prueba', ajustar_umbrales: { amplitud_sismo: 70, sismos_amarillo: 10, sismos_naranja: 5, sismos_rojo: 20 } });
    sesion.write({ operador: 'prueba', solicitar_estado: true }); // el error no corta la sesión
    await conTiempoLimite(
      (async () => {
        while (respuestas.length < 7) await new Promise((r) => setTimeout(r, 50));
      })(),
      3000,
      'esperando 7 respuestas',
    );
    const terminada = new Promise<void>((r) => sesion.on('end', () => r()));
    sesion.end();
    await conTiempoLimite(terminada, 2000, 'el servidor cierra la sesión con call.end()');
    assert.deepEqual(respuestas.map((r) => r.resultado), ['estado', 'ack', 'estado', 'ack', 'estado', 'error', 'estado']);
    assert.equal(respuestas[0].estado!.estaciones.length, 4);
    assert.equal(observatorio.estado().umbrales.amplitud_sismo, 70, 'el umbral inválido no sobrescribe el válido');
    assert.equal(observatorio.estado().nivel_confirmado, 'AMARILLO');
  });

  cliente.close();
  server.forceShutdown();
  observatorio.detener();
  console.log(fallos === 0 ? 'Todas las pruebas pasaron.' : `${fallos} prueba(s) fallaron.`);
  process.exit(fallos === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
