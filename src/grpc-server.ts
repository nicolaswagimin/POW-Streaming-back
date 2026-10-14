// Servidor gRPC del Observatorio Galeras: dominio en memoria + implementación de los 4 RPC.
import { EventEmitter } from 'node:events';
import * as grpc from '@grpc/grpc-js';
import {
  cargarPaquete, ComandoOperador, Estacion, EstadoVolcan, Evento, LecturaSismica,
  NivelAlerta, RespuestaOperador, ResumenEnvio, TipoSismo, Umbrales,
} from './proto';
import { contarMensajes, iniciarLlamada, terminarLlamada, TipoRpc } from './metricas';

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

/** Estado del volcán y reglas de detección. Emite `evento` (Evento) por cada novedad. */
export class Dominio {
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

/** Construye las implementaciones de los 4 RPC sobre un Dominio. */
function crearImplementacion(dominio: Dominio): grpc.UntypedServiceImplementation {
  const medir = (tipo: TipoRpc, call: EventEmitter) => {
    iniciarLlamada(tipo);
    let terminada = false;
    const fin = () => { if (!terminada) { terminada = true; terminarLlamada(tipo); } };
    call.on('cancelled', fin);
    call.on('close', fin);
    call.on('finish', fin);
    return fin;
  };

  return {
    ObtenerEstado(call: grpc.ServerUnaryCall<unknown, EstadoVolcan>, callback: grpc.sendUnaryData<EstadoVolcan>) {
      iniciarLlamada('unario');
      contarMensajes('unario', 2);
      callback(null, dominio.estado());
      terminarLlamada('unario');
    },

    EnviarLecturas(call: grpc.ServerReadableStream<LecturaSismica, ResumenEnvio>, callback: grpc.sendUnaryData<ResumenEnvio>) {
      const fin = medir('streaming_cliente', call);
      const resumen: ResumenEnvio = { estacion_id: '', lecturas_recibidas: 0, amplitud_maxima: 0, sismos_detectados: 0 };
      call.on('data', (l: LecturaSismica) => {
        contarMensajes('streaming_cliente');
        resumen.estacion_id = l.estacion_id;
        resumen.lecturas_recibidas++;
        resumen.amplitud_maxima = Math.max(resumen.amplitud_maxima, l.amplitud_um_s);
        if (dominio.registrarLectura(l)) resumen.sismos_detectados++;
      });
      call.on('end', () => {
        contarMensajes('streaming_cliente');
        callback(null, resumen);
        fin();
      });
    },

    SuscribirEventos(call: grpc.ServerWritableStream<{ incluir_lecturas: boolean }, Evento>) {
      const fin = medir('streaming_servidor', call);
      contarMensajes('streaming_servidor');
      const incluirLecturas = call.request.incluir_lecturas;
      const alEvento = (e: Evento) => {
        if (e.lectura && !incluirLecturas) return;
        contarMensajes('streaming_servidor');
        call.write(e);
      };
      dominio.eventos.on('evento', alEvento);
      const limpiar = () => { dominio.eventos.off('evento', alEvento); fin(); };
      call.on('cancelled', limpiar);
      call.on('close', limpiar);
    },

    SesionOperador(call: grpc.ServerDuplexStream<ComandoOperador, RespuestaOperador>) {
      const fin = medir('bidireccional', call);
      const responder = (r: RespuestaOperador) => { contarMensajes('bidireccional'); call.write(r); };
      call.on('data', (c: ComandoOperador) => {
        contarMensajes('bidireccional');
        const quien = c.operador || 'operador';
        if (c.accion === 'solicitar_estado') {
          responder({ estado: dominio.estado() });
        } else if (c.accion === 'ajustar_umbrales') {
          const error = Dominio.validarUmbrales(c.ajustar_umbrales);
          if (error) return responder({ error });
          dominio.ajustarUmbrales(c.ajustar_umbrales!);
          const u = c.ajustar_umbrales!;
          responder({ ack: `${quien} ajustó umbrales: ${u.amplitud_sismo} µm/s · ${u.sismos_amarillo}/${u.sismos_naranja}/${u.sismos_rojo}` });
        } else if (c.accion === 'confirmar_alerta') {
          const n = c.confirmar_alerta!;
          if (!NIVELES_VALIDOS.includes(n)) return responder({ error: 'Nivel de alerta no válido' });
          dominio.confirmarNivel(n);
          responder({ ack: `${quien} confirmó el nivel ${n}` });
        } else {
          responder({ error: 'Comando vacío: indique una acción' });
        }
      });
      call.on('end', () => { call.end(); fin(); });
    },
  };
}

/** Arranca el servidor gRPC en el puerto indicado (solo localhost). */
export function iniciarServidorGrpc(puerto: number, host = '127.0.0.1'): Promise<{ server: grpc.Server; dominio: Dominio }> {
  const paquete = cargarPaquete();
  const dominio = new Dominio();
  const server = new grpc.Server();
  server.addService(paquete.MonitoreoVolcanico.service, crearImplementacion(dominio));
  return new Promise((resolve, reject) => {
    server.bindAsync(`${host}:${puerto}`, grpc.ServerCredentials.createInsecure(), (err) => {
      if (err) return reject(err);
      resolve({ server, dominio });
    });
  });
}
