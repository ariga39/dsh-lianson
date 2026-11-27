/** Wire schemas shared by the UDS server and the CLI. */

import { z } from 'zod'

/** Non-empty trimmed identifier. */
const identifier = z.string().trim().min(1).max(200)

/** `provider/model` route. */
const modelRoute = z.string().trim().min(3).max(300)

/** Inclusive or exclusive event cursor. */
const cursor = z.number().int().min(-1)

/** Positive timer/page bound. */
const positiveInteger = z.number().int().positive()

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

export const waitRequestSchema = z.object({
  delegationId: identifier,
  afterSeq: cursor.optional(),
  timeoutMs: positiveInteger.optional(),
  sliceTimeoutMs: positiveInteger.optional(),
  maxMessages: positiveInteger.optional(),
}).strict()

export const eventsRequestSchema = z.object({
  delegationId: identifier,
  afterSeq: cursor.optional(),
  throughSeq: cursor.optional(),
  pageSize: positiveInteger.optional(),
}).strict()

export const steerRequestSchema = z.object({
  delegationId: identifier,
  requestId: identifier.optional(),
  text: z.string().min(1).max(2_000_000),
}).strict()

export const queueActionSchema = z.discriminatedUnion('kind', [
  z.object({ kind: z.literal('edit'), text: z.string().min(1).max(2_000_000) }).strict(),
  z.object({ kind: z.literal('remove') }).strict(),
  z.object({ kind: z.literal('steer') }).strict(),
])

export const queueRequestSchema = z.object({
  delegationId: identifier,
  itemId: identifier,
  action: queueActionSchema,
}).strict()

export const stopRequestSchema = z.object({
  delegationId: identifier,
  timeoutMs: positiveInteger.optional(),
}).strict()

export const reworkRequestSchema = z.object({
  delegationId: identifier,
  requestId: identifier.optional(),
  text: z.string().min(1).max(2_000_000),
}).strict()

export const interactionListRequestSchema = z.object({
  delegationId: identifier.optional(),
}).strict()

export const interactionApproveRequestSchema = z.object({
  interactionId: identifier,
}).strict()

export const interactionRejectRequestSchema = z.object({
  interactionId: identifier,
}).strict()

export const answerItemSchema = z.object({
  id: z.string().min(1).max(400),
  selected: z.array(z.string().max(2_000)).max(100),
  custom: z.string().max(20_000).optional(),
}).strict()

export const interactionAnswerRequestSchema = z.object({
  interactionId: identifier.optional(),
  delegationId: identifier.optional(),
  callId: identifier.optional(),
  answers: z.array(answerItemSchema).min(1).max(100),
}).strict()

export type StatusRequest = z.infer<typeof statusRequestSchema>
export type CreateRequest = z.infer<typeof createRequestSchema>
export type GetRequest = z.infer<typeof getRequestSchema>
export type ListRequest = z.infer<typeof listRequestSchema>
export type PromptRequest = z.infer<typeof promptRequestSchema>
export type VerifyModelRequest = z.infer<typeof verifyModelRequestSchema>
export type WaitRequest = z.infer<typeof waitRequestSchema>
export type EventsRequest = z.infer<typeof eventsRequestSchema>
export type SteerRequest = z.infer<typeof steerRequestSchema>
export type QueueRequest = z.infer<typeof queueRequestSchema>
export type StopRequest = z.infer<typeof stopRequestSchema>
export type ReworkRequest = z.infer<typeof reworkRequestSchema>
export type InteractionListRequest = z.infer<typeof interactionListRequestSchema>
export type InteractionApproveRequest = z.infer<typeof interactionApproveRequestSchema>
export type InteractionRejectRequest = z.infer<typeof interactionRejectRequestSchema>
export type InteractionAnswerRequest = z.infer<typeof interactionAnswerRequestSchema>

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
  wait: '/wait',
  events: '/events',
  steer: '/steer',
  queue: '/queue',
  stop: '/stop',
  rework: '/rework',
  interactionList: '/interactions/list',
  interactionApprove: '/interactions/approve',
  interactionReject: '/interactions/reject',
  interactionAnswer: '/interactions/answer',
} as const

/** Route pathname. */
export type RoutePath = (typeof ROUTES)[keyof typeof ROUTES]
