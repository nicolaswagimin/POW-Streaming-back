// Métricas del lado cliente: un interceptor gRPC cuenta llamadas y mensajes por tipo de RPC.
// Lo usan todos los clientes del contenedor (simulador y gateway); el gateway publica el snapshot.
import * as grpc from '@grpc/grpc-js';

export type TipoRpc = 'unario' | 'streaming_cliente' | 'streaming_servidor' | 'bidireccional';

export interface ContadorRpc { llamadas: number; mensajes: number; activas: number; }
export type Metricas = Record<TipoRpc, ContadorRpc>;

const metricas: Metricas = {
  unario: { llamadas: 0, mensajes: 0, activas: 0 },
  streaming_cliente: { llamadas: 0, mensajes: 0, activas: 0 },
  streaming_servidor: { llamadas: 0, mensajes: 0, activas: 0 },
  bidireccional: { llamadas: 0, mensajes: 0, activas: 0 },
};

function tipoDe(def: grpc.InterceptorOptions['method_definition']): TipoRpc {
  if (def.requestStream) return def.responseStream ? 'bidireccional' : 'streaming_cliente';
  return def.responseStream ? 'streaming_servidor' : 'unario';
}

/** Interceptor de cliente: llamada al iniciar, mensaje por cada envío/recepción, fin al recibir el status. */
export const interceptorMetricas: grpc.Interceptor = (options, nextCall) => {
  const tipo = tipoDe(options.method_definition);
  const c = metricas[tipo];
  c.llamadas++;
  c.activas++;
  return new grpc.InterceptingCall(nextCall(options), {
    start(metadata, _listener, next) {
      next(metadata, {
        onReceiveMessage(mensaje, siguiente) { c.mensajes++; siguiente(mensaje); },
        onReceiveStatus(status, siguiente) { c.activas = Math.max(0, c.activas - 1); siguiente(status); },
      });
    },
    sendMessage(mensaje, siguiente) { c.mensajes++; siguiente(mensaje); },
  });
};

export function snapshot(): Metricas {
  return structuredClone(metricas);
}
