/** Shared HTTP contract for the liaison UDS endpoint and CLI. */
import { z } from 'zod'

/** Stable service marker carried by every health response. */
export const LIAISON_SERVICE = 'dsh-liaison'

/** Current loopback operator state shown by `status`. */
export const LoopbackHealthSchema = z.object({
  connected: z.boolean(),
  generation: z.number().int().positive().nullable(),
  reauthentications: z.number().int().nonnegative(),
}).strict()

/** `GET /health` response, also printed by `dsh-liaison status`. */
export const HealthResponseSchema = z.object({
  ok: z.literal(true),
  service: z.literal(LIAISON_SERVICE),
  pid: z.number().int().nonnegative(),
  dshHome: z.string().min(1),
  origin: z.string().nullable(),
  socket: z.string().min(1),
  startedAt: z.string().min(1),
  loopback: LoopbackHealthSchema,
}).strict()

/** `POST /ping` request body, also accepted as optional CLI input. */
export const PingRequestSchema = z.object({
  nonce: z.string().optional(),
}).strict()

/** `POST /ping` response, printed by `dsh-liaison ping`. */
export const PingResponseSchema = z.object({
  ok: z.literal(true),
  pong: z.literal(true),
  nonce: z.string().nullable(),
  generation: z.number().int().positive(),
  reauthentications: z.number().int().nonnegative(),
  models: z.number().int().nonnegative(),
}).strict()

/** Structured failure printed by the CLI and served by the UDS endpoint. */
export const ErrorResponseSchema = z.object({
  ok: z.literal(false),
  error: z.object({
    code: z.string().min(1),
    message: z.string().min(1),
  }).strict(),
}).strict()

/** Validated `GET /health` document. */
export type HealthResponse = z.infer<typeof HealthResponseSchema>
/** Validated `POST /ping` request. */
export type PingRequest = z.infer<typeof PingRequestSchema>
/** Validated `POST /ping` response. */
export type PingResponse = z.infer<typeof PingResponseSchema>
/** Validated structured failure. */
export type ErrorResponse = z.infer<typeof ErrorResponseSchema>
