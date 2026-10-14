// Contadores en memoria por tipo de RPC. Los incrementa el servidor gRPC, así cubren
// a todos los clientes del contenedor (simulador y gateway).
export type TipoRpc = 'unario' | 'streaming_cliente' | 'streaming_servidor' | 'bidireccional';

export interface ContadorRpc { llamadas: number; mensajes: number; activas: number; }
export type Metricas = Record<TipoRpc, ContadorRpc>;

const metricas: Metricas = {
  unario: { llamadas: 0, mensajes: 0, activas: 0 },
  streaming_cliente: { llamadas: 0, mensajes: 0, activas: 0 },
  streaming_servidor: { llamadas: 0, mensajes: 0, activas: 0 },
  bidireccional: { llamadas: 0, mensajes: 0, activas: 0 },
};

/** Registra el inicio de una llamada (activas sube hasta que se llama a `terminar`). */
export function iniciarLlamada(tipo: TipoRpc): void {
  metricas[tipo].llamadas++;
  metricas[tipo].activas++;
}

export function terminarLlamada(tipo: TipoRpc): void {
  metricas[tipo].activas = Math.max(0, metricas[tipo].activas - 1);
}

/** Cuenta mensajes en cualquier dirección (petición o respuesta). */
export function contarMensajes(tipo: TipoRpc, n = 1): void {
  metricas[tipo].mensajes += n;
}

export function snapshot(): Metricas {
  return structuredClone(metricas);
}
