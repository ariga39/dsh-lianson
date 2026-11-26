/** Wire schemas shared by the UDS server and the CLI. */

import { z } from 'zod'

/** Non-empty trimmed identifier. */
const identifier = z.string().trim().min(1).max(200)

/** `provider/model` route. */
const modelRoute = z.string().trim().min(3).max(300)

export const statusRequestSchema = z.object({}).strict()

export const createRequestSchema = z.object({
  delegationId: identifier,
  cwd: z.string().min(1),
  title: z.string().trim().min(1).max(4_000).optional(),
  model: modelRoute.optional(),
}).strict()

export const getRequestSchema = z.object({ delegationId: identifier }).strict()

export const listRequestSchema = z.object({}).strict()

export const promptRequestSchema = z.object({
  delegationId: identifier,
  requestId: identifier.optional(),
  text: z.string().min(1).max(2_000_000),
}).strict()

export const verifyModelRequestSchema = z.object({
  delegationId: identifier,
  requestId: identifier.optional(),
}).strict()

export type StatusRequest = z.infer<typeof statusRequestSchema>
export type CreateRequest = z.infer<typeof createRequestSchema>
export type GetRequest = z.infer<typeof getRequestSchema>
export type ListRequest = z.infer<typeof listRequestSchema>
export type PromptRequest = z.infer<typeof promptRequestSchema>
export type VerifyModelRequest = z.infer<typeof verifyModelRequestSchema>

/** Successful response envelope. */
export interface SuccessEnvelope {
  readonly ok: true
  readonly value: unknown
}

/** Failed response envelope. */
export interface FailureEnvelope {
  readonly ok: false
  readonly error: {
    readonly code: string
    readonly message: string
    readonly details?: Readonly<Record<string, unknown>>
  }
}

/** One server document. */
export type ServerEnvelope = SuccessEnvelope | FailureEnvelope

/** HTTP routes served over the Unix-domain socket. */
export const ROUTES = {
  status: '/status',
  create: '/delegations/create',
  get: '/delegations/get',
  list: '/delegations/list',
  prompt: '/prompts',
  verifyModel: '/verify-model',
} as const

/** Route pathname. */
export type RoutePath = (typeof ROUTES)[keyof typeof ROUTES]
