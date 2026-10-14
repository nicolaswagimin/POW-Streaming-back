// Simulador de estaciones: cada una es un cliente gRPC que usa streaming del cliente.
import * as grpc from '@grpc/grpc-js';
import { crearCliente, LecturaSismica, ResumenEnvio } from './proto';

const ESTACIONES = ['ANG', 'CRA', 'URC', 'CUF'];
const LECTURAS_POR_LOTE = 8;
const INTERVALO_MS = 400;
const PROB_PICO_NORMAL = 0.015;
const PROB_PICO_ENJAMBRE = 0.3;

const azar = (min: number, max: number) => min + Math.random() * (max - min);
const esperar = (ms: number) => new Promise((r) => setTimeout(r, ms));

export class Simulador {
  private cliente: any;
  private activo = false;
  private enjambreHasta = 0;

  constructor(direccionGrpc: string) {
    this.cliente = crearCliente(direccionGrpc);
  }

  iniciar(): void {
    this.activo = true;
    ESTACIONES.forEach((id, i) => setTimeout(() => this.bucleEstacion(id), i * 100));
  }

  detener(): void {
    this.activo = false;
    this.cliente.close();
  }

  /** Durante `ms` milisegundos sube la probabilidad de picos al 30 %. Devuelve el fin del enjambre. */
  enjambre(ms = 20_000): number {
    this.enjambreHasta = Date.now() + ms;
    return this.enjambreHasta;
  }

  get enjambreActivoHasta(): number {
    return this.enjambreHasta > Date.now() ? this.enjambreHasta : 0;
  }

  private generarLectura(estacion_id: string): LecturaSismica {
    const enEnjambre = Date.now() < this.enjambreHasta;
    const pico = Math.random() < (enEnjambre ? PROB_PICO_ENJAMBRE : PROB_PICO_NORMAL);
    return {
      estacion_id,
      timestamp_ms: Date.now(),
      amplitud_um_s: Math.round((pico ? (enEnjambre ? azar(80, 300) : azar(70, 160)) : azar(5, 25)) * 10) / 10,
      frecuencia_hz: Math.round((pico ? azar(1.5, 12) : azar(1, 8)) * 10) / 10,
    };
  }

  /** Abre EnviarLecturas, envía un lote de 8 lecturas (una cada 400 ms), cierra y repite. */
  private async bucleEstacion(id: string): Promise<void> {
    while (this.activo) {
      try {
        await new Promise<ResumenEnvio>(async (resolve, reject) => {
          const metadata = new grpc.Metadata();
          metadata.set('x-estacion-id', id);
          const llamada = this.cliente.EnviarLecturas(metadata, (err: Error | null, r: ResumenEnvio) => (err ? reject(err) : resolve(r)));
          for (let i = 0; i < LECTURAS_POR_LOTE && this.activo; i++) {
            llamada.write(this.generarLectura(id));
            await esperar(INTERVALO_MS);
          }
          llamada.end();
        });
      } catch (err) {
        console.error(`[simulador] ${id}: ${(err as Error).message}`);
        await esperar(2000);
      }
    }
  }
}
