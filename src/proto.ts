// Carga dinámica del contrato (sin codegen) y tipos TypeScript a mano que lo reflejan.
import path from 'node:path';
import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import { interceptorMetricas } from './metricas';

export const RUTA_PROTO = path.join(__dirname, '..', 'proto', 'galeras.proto');

export const OPCIONES_PROTO: protoLoader.Options = {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
};

export type NivelAlerta = 'NIVEL_ALERTA_UNSPECIFIED' | 'VERDE' | 'AMARILLO' | 'NARANJA' | 'ROJO';
export type TipoSismo = 'TIPO_SISMO_UNSPECIFIED' | 'VT' | 'LP' | 'TREMOR';

export interface LecturaSismica { estacion_id: string; timestamp_ms: number; amplitud_um_s: number; frecuencia_hz: number; }
export interface ResumenEnvio { estacion_id: string; lecturas_recibidas: number; amplitud_maxima: number; sismos_detectados: number; }
export interface Estacion { id: string; nombre: string; lat: number; lon: number; en_linea: boolean; ultima_amplitud: number; }
export interface Umbrales { amplitud_sismo: number; sismos_amarillo: number; sismos_naranja: number; sismos_rojo: number; }
export interface EstadoVolcan { nivel: NivelAlerta; estaciones: Estacion[]; sismos_ventana: number; umbrales: Umbrales; nivel_confirmado: NivelAlerta; }
export interface SismoDetectado { estacion_id: string; amplitud: number; frecuencia_hz: number; tipo: TipoSismo; }
export interface CambioAlerta { anterior: NivelAlerta; nuevo: NivelAlerta; motivo: string; }
export interface Evento {
  timestamp_ms: number;
  payload?: 'lectura' | 'sismo' | 'cambio_alerta';
  lectura?: LecturaSismica;
  sismo?: SismoDetectado;
  cambio_alerta?: CambioAlerta;
}
export interface ComandoOperador {
  operador: string;
  accion?: 'ajustar_umbrales' | 'confirmar_alerta' | 'solicitar_estado';
  ajustar_umbrales?: Umbrales;
  confirmar_alerta?: NivelAlerta;
  solicitar_estado?: boolean;
}
export interface RespuestaOperador {
  resultado?: 'ack' | 'error' | 'estado';
  ack?: string;
  error?: string;
  estado?: EstadoVolcan;
}

/** Devuelve el paquete `galeras.v1` cargado con proto-loader. */
export function cargarPaquete(): any {
  const definicion = protoLoader.loadSync(RUTA_PROTO, OPCIONES_PROTO);
  return (grpc.loadPackageDefinition(definicion) as any).galeras.v1;
}

/** Crea un cliente del servicio MonitoreoVolcanico con el interceptor de métricas. */
export function crearCliente(direccion: string): any {
  const paquete = cargarPaquete();
  return new paquete.MonitoreoVolcanico(direccion, grpc.credentials.createInsecure(), {
    interceptors: [interceptorMetricas],
  });
}
