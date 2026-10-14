// Servidor gRPC del Observatorio Galeras: dominio en memoria + implementación de los 4 RPC.
import { EventEmitter } from 'node:events';
import * as grpc from '@grpc/grpc-js';
import {
  cargarPaquete, ComandoOperador, Estacion, EstadoVolcan, Evento, LecturaSismica,
  NivelAlerta, RespuestaOperador, ResumenEnvio, TipoSismo, Umbrales,
} from './proto';

const VENTANA_MS = 5 * 60 * 1000;
const RECALCULO_MS = 2000;
const ENFRIAMIENTO_MS = 1500;
const SIN_SENAL_MS = 5000;
const VENTANA_TREMOR_MS = 10_000;
const NIVELES_VALIDOS: NivelAlerta[] = ['VERDE', 'AMARILLO', 'NARANJA', 'ROJO'];

const ESTACIONES_BASE: Omit<Estacion, 'en_linea' | 'ultima_amplitud'>[] = [
  { id: 'ANG', nombre: 'Anganoy', lat: 1.2167, lon: -77.3010 },
  { id: 'CRA', nombre: 'Cráter', lat: 1.2208, lon: -77.3592 },
  { id: 'URC', nombre: 'Urcunina', lat: 1.2307, lon: -77.3456 },
  { id: 'CUF', nombre: 'Cufiño', lat: 1.2090, lon: -77.3792 },
];

interface EstadoEstacion { ultimaLecturaReloj: number; ultimaAmplitud: number; ultimoSismoTs: number; sismosTs: number[]; }

/** Dominio: estado del volcán y reglas de detección (solo lo usa el servidor gRPC). Emite `evento` (Evento) por cada novedad. */
export class Observatorio {
  readonly eventos = new EventEmitter();
  private umbrales: Umbrales = { amplitud_sismo: 60, sismos_amarillo: 5, sismos_naranja: 12, sismos_rojo: 25 };
  private nivel: NivelAlerta = 'VERDE';
  private nivelConfirmado: NivelAlerta = 'NIVEL_ALERTA_UNSPECIFIED';
  private sismosVentana: number[] = []; // timestamps de lectura de cada sismo
  private estaciones = new Map<string, EstadoEstacion>();
  private temporizador: NodeJS.Timeout;

  constructor() {
    this.eventos.setMaxListeners(0);
    for (const e of ESTACIONES_BASE) {
      this.estaciones.set(e.id, { ultimaLecturaReloj: 0, ultimaAmplitud: 0, ultimoSismoTs: -Infinity, sismosTs: [] });
    }
    this.temporizador = setInterval(() => this.recalcular('Recálculo periódico de la ventana de 5 min'), RECALCULO_MS);
  }

  detener(): void {
    clearInterval(this.temporizador);
  }

  estado(): EstadoVolcan {
    const ahora = Date.now();
    this.podarVentana(ahora);
    return {
      nivel: this.nivel,
      nivel_confirmado: this.nivelConfirmado,
      sismos_ventana: this.sismosVentana.length,
      umbrales: { ...this.umbrales },
      estaciones: ESTACIONES_BASE.map((base) => {
        const e = this.estaciones.get(base.id)!;
        return { ...base, en_linea: ahora - e.ultimaLecturaReloj < SIN_SENAL_MS, ultima_amplitud: e.ultimaAmplitud };
      }),
    };
  }

  existeEstacion(id: string): boolean {
    return this.estaciones.has(id);
  }

  /** Procesa una lectura. Devuelve true si se detectó un sismo. */
  registrarLectura(l: LecturaSismica): boolean {
    const est = this.estaciones.get(l.estacion_id);
    if (!est) return false;
    est.ultimaLecturaReloj = Date.now();
    est.ultimaAmplitud = l.amplitud_um_s;
    this.emitir({ lectura: l });

    const ts = l.timestamp_ms;
    if (l.amplitud_um_s < this.umbrales.amplitud_sismo || ts - est.ultimoSismoTs < ENFRIAMIENTO_MS) return false;

    est.sismosTs = est.sismosTs.filter((t) => ts - t < VENTANA_TREMOR_MS);
    const tipo: TipoSismo = est.sismosTs.length >= 2 ? 'TREMOR' : l.frecuencia_hz < 5 ? 'LP' : 'VT';
    est.sismosTs.push(ts);
    est.ultimoSismoTs = ts;
    this.sismosVentana.push(ts);
    this.emitir({ sismo: { estacion_id: l.estacion_id, amplitud: l.amplitud_um_s, frecuencia_hz: l.frecuencia_hz, tipo } });
    this.recalcular(`Sismo ${tipo} en ${l.estacion_id}`);
    return true;
  }

  /** Recalcula el nivel con la ventana deslizante y emite `cambio_alerta` si cambió. */
  recalcular(motivo: string): void {
    this.podarVentana(Date.now());
    const n = this.sismosVentana.length;
    const u = this.umbrales;
    const nuevo: NivelAlerta = n >= u.sismos_rojo ? 'ROJO' : n >= u.sismos_naranja ? 'NARANJA' : n >= u.sismos_amarillo ? 'AMARILLO' : 'VERDE';
    if (nuevo === this.nivel) return;
    const anterior = this.nivel;
    this.nivel = nuevo;
    this.emitir({ cambio_alerta: { anterior, nuevo, motivo: `${motivo} (${n} sismos en 5 min)` } });
  }

  /** Devuelve un mensaje de error, o null si los umbrales son válidos. */
  static validarUmbrales(u: Umbrales | undefined): string | null {
    if (!u) return 'Faltan los umbrales';
    if (!(u.amplitud_sismo > 0)) return 'La amplitud de sismo debe ser mayor que 0';
    if (!(u.sismos_amarillo >= 1 && u.sismos_amarillo < u.sismos_naranja && u.sismos_naranja < u.sismos_rojo)) {
      return 'Se requiere 1 ≤ amarillo < naranja < rojo';
    }
    return null;
  }

  ajustarUmbrales(u: Umbrales): void {
    this.umbrales = { ...u };
    this.recalcular('Umbrales ajustados por el operador');
  }

  confirmarNivel(n: NivelAlerta): void {
    this.nivelConfirmado = n;
  }

  private podarVentana(ahora: number): void {
    this.sismosVentana = this.sismosVentana.filter((t) => ahora - t < VENTANA_MS);
  }

  private emitir(payload: Omit<Evento, 'timestamp_ms'>): void {
    this.eventos.emit('evento', { timestamp_ms: Date.now(), ...payload } as Evento);
  }
}

/** Lee una cabecera de metadata como texto ('' si no viene). */
function leerMetadata(call: { metadata: grpc.Metadata }, clave: string): string {
  return String(call.metadata.get(clave)[0] ?? '');
}

/** Construye las implementaciones de los 4 RPC sobre el Observatorio. */
function crearImplementacion(obs: Observatorio): grpc.UntypedServiceImplementation {
  const lotesPorEstacion = new Map<string, number>();

  return {
    // 1) Unario: una petición → una respuesta.
    ObtenerEstado(call: grpc.ServerUnaryCall<unknown, EstadoVolcan>, callback: grpc.sendUnaryData<EstadoVolcan>) {
      const cliente = leerMetadata(call, 'x-cliente') || 'desconocido';
      if (cliente !== 'gateway-web') console.log(`[grpc] ObtenerEstado ← ${cliente}`);
      callback(null, obs.estado());
    },

    // 2) Streaming del cliente: muchas lecturas → un ResumenEnvio al cerrar el stream.
    EnviarLecturas(call: grpc.ServerReadableStream<LecturaSismica, ResumenEnvio>, callback: grpc.sendUnaryData<ResumenEnvio>) {
      const estacionMeta = leerMetadata(call, 'x-estacion-id');
      const n = (lotesPorEstacion.get(estacionMeta) ?? 0) + 1;
      lotesPorEstacion.set(estacionMeta, n);
      if (n === 1 || n % 25 === 0) console.log(`[grpc] EnviarLecturas ← x-estacion-id=${estacionMeta || '(sin metadata)'} (lote ${n})`);

      const resumen: ResumenEnvio = { estacion_id: estacionMeta, lecturas_recibidas: 0, amplitud_maxima: 0, sismos_detectados: 0 };
      let terminado = false;
      const fallar = (code: grpc.status, details: string) => {
        terminado = true;
        console.warn(`[grpc] EnviarLecturas ✘ ${grpc.status[code]}: ${details}`);
        callback({ code, details } as grpc.ServiceError);
      };

      call.on('data', (l: LecturaSismica) => {
        if (terminado) return; // tras un error se ignora el resto del stream
        if (!obs.existeEstacion(l.estacion_id)) return fallar(grpc.status.NOT_FOUND, `Estación desconocida: ${l.estacion_id}`);
        if (l.amplitud_um_s < 0 || l.frecuencia_hz < 0) {
          return fallar(grpc.status.INVALID_ARGUMENT, `Lectura inválida de ${l.estacion_id}: amplitud y frecuencia deben ser ≥ 0`);
        }
        resumen.estacion_id = l.estacion_id;
        resumen.lecturas_recibidas++;
        resumen.amplitud_maxima = Math.max(resumen.amplitud_maxima, l.amplitud_um_s);
        if (obs.registrarLectura(l)) resumen.sismos_detectados++;
      });
      call.on('end', () => {
        if (!terminado) callback(null, resumen);
      });
    },

    // 3) Streaming del servidor: una suscripción → eventos indefinidamente (nunca se llama call.end()).
    SuscribirEventos(call: grpc.ServerWritableStream<{ incluir_lecturas: boolean }, Evento>) {
      console.log(`[grpc] SuscribirEventos ← x-cliente=${leerMetadata(call, 'x-cliente')} x-operador=${leerMetadata(call, 'x-operador')}`);
      const incluirLecturas = call.request.incluir_lecturas;
      const alEvento = (e: Evento) => {
        if (e.payload === 'lectura' && !incluirLecturas) return;
        call.write(e);
      };
      obs.eventos.on('evento', alEvento);
      const limpiar = () => obs.eventos.off('evento', alEvento);
      call.on('cancelled', limpiar);
      call.on('close', limpiar);
      call.on('error', limpiar);
    },

    // 4) Bidireccional: comandos y respuestas fluyen de forma independiente en el mismo stream.
    SesionOperador(call: grpc.ServerDuplexStream<ComandoOperador, RespuestaOperador>) {
      const operadorMeta = leerMetadata(call, 'x-operador') || 'operador';
      console.log(`[grpc] SesionOperador abierta ← x-cliente=${leerMetadata(call, 'x-cliente')} x-operador=${operadorMeta}`);
      const responder = (r: RespuestaOperador) => call.write(r);

      call.on('data', (c: ComandoOperador) => {
        const quien = c.operador || operadorMeta;
        if (c.accion === 'solicitar_estado') {
          responder({ estado: obs.estado() });
        } else if (c.accion === 'ajustar_umbrales') {
          const error = Observatorio.validarUmbrales(c.ajustar_umbrales);
          if (error) return responder({ error }); // el error viaja dentro del stream; la sesión sigue abierta
          const u = c.ajustar_umbrales!;
          obs.ajustarUmbrales(u);
          responder({ ack: `${quien} ajustó umbrales: ${u.amplitud_sismo} µm/s · ${u.sismos_amarillo}/${u.sismos_naranja}/${u.sismos_rojo}` });
          responder({ estado: obs.estado() });
        } else if (c.accion === 'confirmar_alerta') {
          const n = c.confirmar_alerta!;
          if (!NIVELES_VALIDOS.includes(n)) return responder({ error: 'Nivel de alerta no válido' });
          obs.confirmarNivel(n);
          responder({ ack: `${quien} confirmó el nivel ${n}` });
          responder({ estado: obs.estado() });
        } else {
          responder({ error: 'Comando vacío: indique una acción' });
        }
      });
      call.on('end', () => {
        console.log(`[grpc] SesionOperador cerrada (${operadorMeta})`);
        call.end();
      });
    },
  };
}

/** Arranca el servidor gRPC en el puerto indicado (solo localhost). */
export function iniciarServidorGrpc(puerto: number, host = '127.0.0.1'): Promise<{ server: grpc.Server; observatorio: Observatorio }> {
  const paquete = cargarPaquete();
  const observatorio = new Observatorio();
  const server = new grpc.Server();
  server.addService(paquete.MonitoreoVolcanico.service, crearImplementacion(observatorio));
  return new Promise((resolve, reject) => {
    server.bindAsync(`${host}:${puerto}`, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) return reject(err);
      resolve({ server, observatorio });
    });
  });
}
