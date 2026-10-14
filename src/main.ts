// Un solo proceso: servidor gRPC (localhost), simulador (cliente gRPC) y gateway (cliente gRPC).
import { iniciarServidorGrpc } from './grpc-server';
import { Simulador } from './simulador';
import { iniciarGateway } from './gateway';

const PUERTO_GRPC = Number(process.env.GRPC_PORT ?? 50051);
const PUERTO_HTTP = Number(process.env.PORT ?? 8080);
const DIRECCION_GRPC = `127.0.0.1:${PUERTO_GRPC}`;

async function main() {
  await iniciarServidorGrpc(PUERTO_GRPC);
  console.log(`[grpc] MonitoreoVolcanico escuchando en ${DIRECCION_GRPC}`);

  const simulador = new Simulador(DIRECCION_GRPC);
  simulador.iniciar();
  console.log('[simulador] 4 estaciones enviando lecturas');

  iniciarGateway(PUERTO_HTTP, DIRECCION_GRPC, simulador);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
