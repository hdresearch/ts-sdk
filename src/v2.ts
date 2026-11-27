import type {
  BranchComputersRequest,
  BranchComputersResponse,
  ComputerResponse,
  CreateComputerCheckpointRequest,
  CreateComputerRequest,
  ListCheckpointsParams,
  ListCheckpointsResponse,
  ListComputersParams,
  ListComputersResponse,
  Operation,
  OperationEvent,
  RestoreComputerFromCheckpointRequest,
  VmExecRequest,
  VmExecResponse,
  VmExecStreamEvent,
  VmReadFileResponse,
  WakeComputerResponse,
  WhoamiResponse,
} from "./models";
import type { RequestOptions } from "./request-options";
import {
  APIError,
  APIConnectionError,
  APIConnectionTimeoutError,
  AuthenticationError,
  NotFoundError,
  PermissionDeniedError,
  VersOperationError,
} from "./errors";
import { detectRuntime, getDefaultFetch, getEnv } from "./shims";

const SDK_VERSION = "1.20.1";
const DEFAULT_BASE_URL = "https://api.vers.sh";
const DEFAULT_TIMEOUT_MS = 30_000;
const DEFAULT_MAX_RETRIES = 2;
const DEFAULT_WAIT_TIMEOUT_MS = 300_000;
const DEFAULT_POLL_INITIAL_DELAY_MS = 500;
const DEFAULT_POLL_MAX_DELAY_MS = 5_000;
const MAX_STREAM_RECONNECT_FAILURES = 2;

const RETRYABLE_STATUS_CODES = new Set([408, 429]);

function isRetryableStatus(status: number): boolean {
  return status >= 500 || RETRYABLE_STATUS_CODES.has(status);
}

function retryDelay(retryCount: number): number {
  const base = 500 * Math.pow(2, retryCount);
  const jitter = Math.random() * base * 0.25;
  return base + jitter;
}

function parseRetryAfter(headerValue: string | null): number | null {
  if (headerValue == null) return null;
  const seconds = Number(headerValue);
  if (!Number.isNaN(seconds)) {
    return Math.min(Math.max(seconds * 1000, 0), 60_000);
  }
  const date = new Date(headerValue);
  if (!Number.isNaN(date.getTime())) {
    return Math.min(Math.max(date.getTime() - Date.now(), 0), 60_000);
  }
  return null;
}

function hasHeader(
  headers: Record<string, string | null | undefined> | undefined,
  name: string,
): boolean {
  if (!headers) return false;
  const lower = name.toLowerCase();
  return Object.keys(headers).some((k) => k.toLowerCase() === lower);
}

function mergeHeaders(
  ...sources: (Record<string, string | null | undefined> | undefined)[]
): Record<string, string> {
  const merged: Record<string, string> = {};
  for (const source of sources) {
    if (!source) continue;
    for (const [key, value] of Object.entries(source)) {
      const lower = key.toLowerCase();
      for (const existingKey of Object.keys(merged)) {
        if (existingKey.toLowerCase() === lower) delete merged[existingKey];
      }
      if (value !== null && value !== undefined) {
        merged[key] = value;
      }
    }
  }
  return merged;
}

function isBinaryBody(body: unknown): body is Uint8Array | ArrayBuffer | ArrayBufferView {
  return body instanceof Uint8Array || body instanceof ArrayBuffer || ArrayBuffer.isView(body);
}

function decodeBase64(b64: string): Uint8Array {
  if (typeof Buffer !== "undefined") {
    const buf = Buffer.from(b64, "base64");
    // Return a plain Uint8Array copy so strict Uint8Array checks match across runtimes/test runners.
    return new Uint8Array(buf.buffer.slice(buf.byteOffset, buf.byteOffset + buf.byteLength));
  }
  const binary = globalThis.atob(b64);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  if (ms <= 0) {
    if (signal?.aborted) {
      return Promise.reject(new APIConnectionError({ message: "Operation wait aborted." }));
    }
    return Promise.resolve();
  }
  return new Promise((resolve, reject) => {
    if (signal?.aborted) {
      reject(new APIConnectionError({ message: "Operation wait aborted." }));
      return;
    }
    const onAbort = () => {
      clearTimeout(timer);
      reject(new APIConnectionError({ message: "Operation wait aborted." }));
    };
    const timer = setTimeout(() => {
      signal?.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    signal?.addEventListener("abort", onAbort, { once: true });
  });
}

async function* parseNdjson<T>(
  stream: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<T> {
  const reader = stream.getReader();
  const decoder = new TextDecoder("utf-8");
  let buffer = "";

  const onAbort = () => {
    reader.cancel().catch(() => {});
  };
  if (signal) {
    if (signal.aborted) {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
      throw new APIConnectionError({ message: "Stream aborted." });
    }
    signal.addEventListener("abort", onAbort, { once: true });
  }

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let newlineIdx = buffer.indexOf("\n");
      while (newlineIdx !== -1) {
        const line = buffer.slice(0, newlineIdx).trim();
        buffer = buffer.slice(newlineIdx + 1);
        if (line.length > 0) {
          yield JSON.parse(line) as T;
        }
        newlineIdx = buffer.indexOf("\n");
      }
    }

    buffer += decoder.decode();
    const trailing = buffer.trim();
    if (trailing.length > 0) {
      yield JSON.parse(trailing) as T;
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export interface VersClientOptions {
  /** Base URL for the Vers API. Defaults to `VERS_BASE_URL` or `"https://api.vers.sh"`. */
  baseUrl?: string;
  /** API key sent as `Authorization: Bearer <apiKey>`. Defaults to `VERS_API_KEY`. */
  apiKey?: string;
  /** Default headers included with every request. */
  headers?: Record<string, string | null>;
  /** Maximum retries on 408/429/5xx and transient network errors. Default: `2`. */
  maxRetries?: number;
  /** Default per-request HTTP timeout in milliseconds. Default: `30000`. */
  timeout?: number;
  /** Custom `fetch` implementation. Defaults to native `globalThis.fetch`. */
  fetch?: typeof fetch;
  /** Additional `RequestInit` options passed to every `fetch` call. */
  fetchOptions?: RequestInit;
}

export interface WaitForOperationOptions {
  /** Overall wait timeout in milliseconds. Default: `300000` (5 minutes). */
  timeoutMs?: number;
  /** Called whenever a newer operation snapshot cursor is observed. */
  onProgress?: (operation: Operation) => void;
  /** AbortSignal to cancel waiting early. */
  signal?: AbortSignal;
  /**
   * For checkpoint operations:
   * - `"durable"` (default): wait until `status === "succeeded"` (portable in S3 with `result`).
   * - `"local"`: return as soon as `local_result` or `result` is populated (or `status === "succeeded"`).
   */
  waitFor?: "durable" | "local";
  /** Initial backoff delay in milliseconds when reconnecting or polling. Default: `500`. */
  pollInitialDelayMs?: number;
  /** Maximum backoff delay in milliseconds when polling. Default: `5000`. */
  pollMaxDelayMs?: number;
  /** Optional initial operation snapshot already returned by a mutation call. */
  initialOperation?: Operation;
}

export interface CreateComputerOptions extends Omit<WaitForOperationOptions, "waitFor" | "initialOperation"> {
  /** Wait for the create operation to succeed and fetch the `ComputerResponse`. Default: `true`. */
  wait?: boolean;
  /** Explicit UUIDv4 `Idempotency-Key` header. Auto-generated if omitted. */
  idempotencyKey?: string;
  /** Per-request HTTP options. */
  requestOptions?: RequestOptions;
}

export interface RestoreComputerOptions extends CreateComputerOptions {
  /** Destination project ID. Required when restoring a project-network checkpoint. */
  projectId?: string;
  /** Snake-case alias for `projectId`. */
  project_id?: string;
  /** Exempt the restored computer from inactivity auto-sleep. */
  keepAlive?: boolean;
  /** Snake-case alias for `keepAlive`. */
  keep_alive?: boolean;
  /** Override individual labels inherited from the checkpoint. */
  labels?: Record<string, string>;
}

export interface CheckpointComputerOptions extends Omit<WaitForOperationOptions, "initialOperation"> {
  /** Human-readable name for the checkpoint. */
  name?: string;
  /** Optional description for the checkpoint. */
  description?: string;
  /** Leave the source computer paused after capturing the checkpoint. */
  keepPaused?: boolean;
  /** Snake-case alias for `keepPaused`. */
  keep_paused?: boolean;
  /** Refuse immediately if the source computer has not finished booting. */
  skipWaitBoot?: boolean;
  /** Snake-case alias for `skipWaitBoot`. */
  skip_wait_boot?: boolean;
  /** Wait for checkpoint completion. Default: `true`. */
  wait?: boolean;
  /** Explicit UUIDv4 `Idempotency-Key` header. Auto-generated if omitted. */
  idempotencyKey?: string;
  /** Per-request HTTP options. */
  requestOptions?: RequestOptions;
}

export interface SleepComputerOptions {
  /** Refuse rather than waiting for boot completion. */
  skipWaitBoot?: boolean;
  /** Snake-case alias for `skipWaitBoot`. */
  skip_wait_boot?: boolean;
  /** Per-request HTTP options. */
  requestOptions?: RequestOptions;
}

export interface WriteFileOptions {
  /** POSIX file mode (e.g. `0o644`). Defaults to `0o644` on the server if omitted. */
  mode?: number;
  /** Create missing parent directories. */
  createDirs?: boolean;
  /** Snake-case alias for `createDirs`. */
  create_dirs?: boolean;
  /** Per-request HTTP options. */
  requestOptions?: RequestOptions;
}

export interface DeleteComputerOptions extends Omit<WaitForOperationOptions, "waitFor" | "initialOperation"> {
  /** Refuse rather than waiting for boot completion. */
  skipWaitBoot?: boolean;
  /** Snake-case alias for `skipWaitBoot`. */
  skip_wait_boot?: boolean;
  /** Wait for the deletion operation to finish. Default: `true`. */
  wait?: boolean;
  /** Explicit UUIDv4 `Idempotency-Key` header. Auto-generated if omitted. */
  idempotencyKey?: string;
  /** Per-request HTTP options. */
  requestOptions?: RequestOptions;
}

/**
 * High-level TypeScript client for the Vers `/api/v2` API.
 *
 * Uses native `fetch` and Web Crypto (`crypto.randomUUID`) with zero runtime
 * dependencies on Node.js 20+, Deno, Bun, and edge runtimes.
 */
export class VersClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly headers: Record<string, string | null>;
  private readonly maxRetries: number;
  private readonly timeout: number;
  private readonly fetchFn?: typeof fetch;
  private readonly fetchOptions: RequestInit;

  constructor(options: VersClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? getEnv("VERS_BASE_URL") ?? DEFAULT_BASE_URL).replace(/\/+$/, "");
    this.apiKey = options.apiKey ?? getEnv("VERS_API_KEY");
    this.headers = options.headers ?? {};
    this.maxRetries = options.maxRetries ?? DEFAULT_MAX_RETRIES;
    this.timeout = options.timeout ?? DEFAULT_TIMEOUT_MS;
    this.fetchFn = options.fetch;
    this.fetchOptions = options.fetchOptions ?? {};
  }

  private get userAgent(): string {
    const runtime = detectRuntime();
    let rt: string;
    switch (runtime) {
      case "deno":
        rt = `deno/${(globalThis as any).Deno?.version?.deno ?? "unknown"}`;
        break;
      case "bun":
        rt = `bun/${(globalThis as any).Bun?.version ?? "unknown"}`;
        break;
      case "node":
        rt = `node/${process.versions?.node ?? "unknown"}`;
        break;
      case "edge":
        rt = "edge-runtime";
        break;
      case "browser":
        rt = "browser";
        break;
      default:
        rt = "unknown-runtime";
    }
    const os =
      typeof process !== "undefined"
        ? `${process.platform ?? "unknown"}/${process.arch ?? "unknown"}`
        : "unknown-os";
    return `vers-sdk/${SDK_VERSION} ${rt} ${os}`;
  }

  private withIdempotencyKey(
    requestOptions: RequestOptions | undefined,
    idempotencyKey: string | undefined,
  ): RequestOptions | undefined {
    if (!idempotencyKey) return requestOptions;
    return {
      ...requestOptions,
      headers: {
        ...requestOptions?.headers,
        "Idempotency-Key": idempotencyKey,
      },
    };
  }

  private async request(
    method: string,
    path: string,
    body?: unknown,
    options?: RequestOptions,
    internalOpts?: {
      retries?: number;
      applyDefaultTimeout?: boolean;
      mutatingIdempotency?: boolean;
    },
  ): Promise<Response> {
    const upperMethod = method.toUpperCase();
    const binary = isBinaryBody(body);

    const defaultHeaders: Record<string, string> = {
      "User-Agent": this.userAgent,
    };
    if (body !== undefined) {
      defaultHeaders["Content-Type"] = binary ? "application/octet-stream" : "application/json";
    }
    if (this.apiKey) {
      defaultHeaders["Authorization"] = `Bearer ${this.apiKey}`;
    }

    const needsIdempotencyKey =
      internalOpts?.mutatingIdempotency ?? (upperMethod === "POST" || upperMethod === "DELETE");
    const idempotencyExplicitlyHandled =
      hasHeader(this.headers, "Idempotency-Key") || hasHeader(options?.headers, "Idempotency-Key");
    if (needsIdempotencyKey && !idempotencyExplicitlyHandled) {
      defaultHeaders["Idempotency-Key"] = globalThis.crypto.randomUUID();
    }

    const headers = mergeHeaders(defaultHeaders, this.headers, options?.headers);

    let serializedBody: BodyInit | undefined;
    if (body === undefined) {
      serializedBody = undefined;
    } else if (body instanceof ArrayBuffer) {
      serializedBody = body;
    } else if (ArrayBuffer.isView(body)) {
      const copy = new Uint8Array(body.byteLength);
      copy.set(new Uint8Array(body.buffer, body.byteOffset, body.byteLength));
      serializedBody = copy;
    } else {
      serializedBody = JSON.stringify(body);
    }

    const maxRetries = internalOpts?.retries ?? this.maxRetries;
    const applyDefaultTimeout = internalOpts?.applyDefaultTimeout ?? true;
    const effectiveTimeout =
      options?.timeout !== undefined
        ? options.timeout
        : applyDefaultTimeout
          ? this.timeout
          : undefined;
    const fetchImpl = this.fetchFn ?? getDefaultFetch();

    let lastError: unknown;
    let lastRetryAfterUsed = false;

    for (let attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0 && !lastRetryAfterUsed) {
        await sleep(retryDelay(attempt - 1), options?.signal ?? undefined);
      }
      lastRetryAfterUsed = false;

      const controller = new AbortController();
      let timedOut = false;
      const timeoutId =
        effectiveTimeout !== undefined
          ? setTimeout(() => {
              timedOut = true;
              controller.abort();
            }, effectiveTimeout)
          : undefined;

      const externalSignal = options?.signal ?? undefined;
      const onExternalAbort = () => controller.abort();
      if (externalSignal) {
        if (externalSignal.aborted) {
          controller.abort();
        } else {
          externalSignal.addEventListener("abort", onExternalAbort, { once: true });
        }
      }

      try {
        const mergedFetchOptions = { ...this.fetchOptions, ...options?.fetchOptions };
        const response = await fetchImpl(`${this.baseUrl}${path}`, {
          ...mergedFetchOptions,
          method: upperMethod,
          headers,
          body: serializedBody,
          signal: controller.signal,
        });

        if (isRetryableStatus(response.status) && attempt < maxRetries) {
          const retryAfterDelay = parseRetryAfter(response.headers.get("retry-after"));
          await response.body?.cancel().catch(() => {});
          if (retryAfterDelay != null) {
            lastRetryAfterUsed = true;
            await sleep(retryAfterDelay, externalSignal);
          }
          lastError = new Error(`HTTP ${response.status}`);
          continue;
        }

        if (!response.ok) {
          let errorBody: object | undefined;
          let errorMessage: string | undefined;
          try {
            const text = await response.text();
            const ct = response.headers.get("content-type") ?? "";
            if (ct.includes("application/json")) {
              try {
                errorBody = JSON.parse(text);
              } catch {
                errorMessage = text;
              }
            } else {
              errorMessage = text;
            }
          } catch {}
          throw APIError.generate(response.status, errorBody, errorMessage, response.headers);
        }

        return response;
      } catch (error) {
        if (error instanceof APIError) throw error;
        if (externalSignal?.aborted) {
          throw new APIConnectionError({
            message: "Request aborted.",
            cause: error instanceof Error ? error : undefined,
          });
        }
        lastError = error;
        if (attempt === maxRetries) {
          if (timedOut) {
            throw new APIConnectionTimeoutError();
          }
          throw new APIConnectionError({ cause: error instanceof Error ? error : undefined });
        }
      } finally {
        if (timeoutId !== undefined) clearTimeout(timeoutId);
        if (externalSignal) {
          externalSignal.removeEventListener("abort", onExternalAbort);
        }
      }
    }

    throw lastError;
  }

  private async requestJson<T>(
    method: string,
    path: string,
    body?: unknown,
    options?: RequestOptions,
    internalOpts?: {
      retries?: number;
      applyDefaultTimeout?: boolean;
      mutatingIdempotency?: boolean;
    },
  ): Promise<T> {
    const resp = await this.request(method, path, body, options, internalOpts);
    const text = await resp.text();
    if (!text) return undefined as unknown as T;
    return JSON.parse(text) as T;
  }

  // ── Computers ───────────────────────────────────────────────────────────

  /**
   * Create a new computer (`POST /api/v2/computers`).
   *
   * By default (`wait: true`), waits for the durable creation operation to
   * succeed and fetches the resulting `ComputerResponse`. Pass `{ wait: false }`
   * to return the `202` `Operation` immediately.
   */
  create(req: CreateComputerRequest, opts: CreateComputerOptions & { wait: false }): Promise<Operation>;
  create(req: CreateComputerRequest, opts?: CreateComputerOptions & { wait?: true }): Promise<ComputerResponse>;
  create(req: CreateComputerRequest, opts?: CreateComputerOptions): Promise<ComputerResponse | Operation>;
  async create(
    req: CreateComputerRequest,
    opts?: CreateComputerOptions,
  ): Promise<ComputerResponse | Operation> {
    const reqOpts = this.withIdempotencyKey(opts?.requestOptions, opts?.idempotencyKey);
    const op = await this.requestJson<Operation>("POST", "/api/v2/computers", req, reqOpts);

    if (opts?.wait === false) {
      return op;
    }

    const finalOp = await this.waitForOperation(op.operation_id, {
      timeoutMs: opts?.timeoutMs,
      onProgress: opts?.onProgress,
      signal: opts?.signal,
      pollInitialDelayMs: opts?.pollInitialDelayMs,
      pollMaxDelayMs: opts?.pollMaxDelayMs,
      initialOperation: op,
    });

    return this.get(finalOp.resource.id, opts?.requestOptions);
  }

  /**
   * Restore a new computer from a checkpoint (`POST /api/v2/computers` with
   * `{ source: { type: "checkpoint", checkpoint_id } }`).
   *
   * Waits for the restore operation to succeed and returns `ComputerResponse`
   * by default; pass `{ wait: false }` to return the `Operation` immediately.
   */
  restore(checkpointId: string, opts: RestoreComputerOptions & { wait: false }): Promise<Operation>;
  restore(checkpointId: string, opts?: RestoreComputerOptions & { wait?: true }): Promise<ComputerResponse>;
  restore(checkpointId: string, opts?: RestoreComputerOptions): Promise<ComputerResponse | Operation>;
  async restore(
    checkpointId: string,
    opts?: RestoreComputerOptions,
  ): Promise<ComputerResponse | Operation> {
    const body: RestoreComputerFromCheckpointRequest = {
      source: {
        type: "checkpoint",
        checkpoint_id: checkpointId,
      },
    };
    const projectId = opts?.projectId ?? opts?.project_id;
    const keepAlive = opts?.keepAlive ?? opts?.keep_alive;
    if (projectId !== undefined) body.project_id = projectId;
    if (keepAlive !== undefined) body.keep_alive = keepAlive;
    if (opts?.labels !== undefined) body.labels = opts.labels;

    return this.create(body, opts);
  }

  /**
   * Fetch one computer's durable metadata and current runtime state (`GET /api/v2/computers/{id}`).
   */
  async get(computerId: string, options?: RequestOptions): Promise<ComputerResponse> {
    return this.requestJson<ComputerResponse>(
      "GET",
      `/api/v2/computers/${encodeURIComponent(computerId)}`,
      undefined,
      options,
    );
  }

  /**
   * List one bounded page of durable, non-deleted computers (`GET /api/v2/computers`).
   */
  async list(params?: ListComputersParams, options?: RequestOptions): Promise<ListComputersResponse> {
    let url = "/api/v2/computers";
    const searchParams = new URLSearchParams();
    if (params?.limit !== undefined) searchParams.append("limit", String(params.limit));
    if (params?.cursor !== undefined) searchParams.append("cursor", String(params.cursor));
    const qs = searchParams.toString();
    if (qs) url += "?" + qs;
    return this.requestJson<ListComputersResponse>("GET", url, undefined, options);
  }

  /**
   * Execute one bounded process inside a computer (`POST /api/v2/computers/{id}/exec`).
   *
   * `req.command` is executed directly as argv (no implicit shell wrapper).
   */
  async exec(
    computerId: string,
    req: VmExecRequest,
    options?: RequestOptions,
  ): Promise<VmExecResponse> {
    let effectiveOptions = options;
    if (options?.timeout === undefined && req.timeout_secs && req.timeout_secs > 0) {
      effectiveOptions = {
        ...options,
        timeout: Math.max(this.timeout, req.timeout_secs * 1000 + 15_000),
      };
    }
    return this.requestJson<VmExecResponse>(
      "POST",
      `/api/v2/computers/${encodeURIComponent(computerId)}/exec`,
      req,
      effectiveOptions,
    );
  }

  /**
   * Execute a process and stream NDJSON stdout/stderr chunks and terminal exit
   * event (`POST /api/v2/computers/{id}/exec/stream`).
   */
  async *execStream(
    computerId: string,
    req: VmExecRequest,
    options?: RequestOptions,
  ): AsyncGenerator<VmExecStreamEvent> {
    const resp = await this.request(
      "POST",
      `/api/v2/computers/${encodeURIComponent(computerId)}/exec/stream`,
      req,
      {
        ...options,
        headers: {
          Accept: "application/x-ndjson",
          ...options?.headers,
        },
      },
      { applyDefaultTimeout: false },
    );

    if (!resp.body) {
      return;
    }

    yield* parseNdjson<VmExecStreamEvent>(resp.body, options?.signal ?? undefined);
  }

  /**
   * Branch one or more computers synchronously from a computer, checkpoint, or tag
   * (`POST /api/v2/branches`).
   */
  async branch(
    req: BranchComputersRequest,
    options?: RequestOptions,
  ): Promise<BranchComputersResponse> {
    return this.requestJson<BranchComputersResponse>("POST", "/api/v2/branches", req, options);
  }

  /**
   * Capture a whole-computer checkpoint (`POST /api/v2/computers/{id}/checkpoints`).
   *
   * By default (`wait: true`, `waitFor: "durable"`), waits until the checkpoint
   * operation succeeds and is durably published in S3 (`operation.result` set).
   * Pass `{ waitFor: "local" }` to return as soon as `local_result` or `result`
   * is available on the source node. Pass `{ wait: false }` to return the `202`
   * `Operation` immediately.
   */
  async checkpoint(
    computerId: string,
    opts?: CheckpointComputerOptions,
  ): Promise<Operation> {
    const body: CreateComputerCheckpointRequest = {};
    if (opts?.name !== undefined) body.name = opts.name;
    if (opts?.description !== undefined) body.description = opts.description;
    const keepPaused = opts?.keepPaused ?? opts?.keep_paused;
    const skipWaitBoot = opts?.skipWaitBoot ?? opts?.skip_wait_boot;
    if (keepPaused !== undefined) body.keep_paused = keepPaused;
    if (skipWaitBoot !== undefined) body.skip_wait_boot = skipWaitBoot;

    const reqOpts = this.withIdempotencyKey(opts?.requestOptions, opts?.idempotencyKey);
    const op = await this.requestJson<Operation>(
      "POST",
      `/api/v2/computers/${encodeURIComponent(computerId)}/checkpoints`,
      body,
      reqOpts,
    );

    if (opts?.wait === false) {
      return op;
    }

    return this.waitForOperation(op.operation_id, {
      timeoutMs: opts?.timeoutMs,
      onProgress: opts?.onProgress,
      signal: opts?.signal,
      waitFor: opts?.waitFor ?? "durable",
      pollInitialDelayMs: opts?.pollInitialDelayMs,
      pollMaxDelayMs: opts?.pollMaxDelayMs,
      initialOperation: op,
    });
  }

  /**
   * Put a running computer to sleep (`POST /api/v2/computers/{id}/sleep`, `204`).
   */
  async sleep(computerId: string, opts?: SleepComputerOptions): Promise<void> {
    let url = `/api/v2/computers/${encodeURIComponent(computerId)}/sleep`;
    const skipWaitBoot = opts?.skipWaitBoot ?? opts?.skip_wait_boot;
    if (skipWaitBoot !== undefined) {
      const searchParams = new URLSearchParams();
      searchParams.append("skip_wait_boot", String(skipWaitBoot));
      url += "?" + searchParams.toString();
    }
    await this.request("POST", url, undefined, opts?.requestOptions);
  }

  /**
   * Wake a sleeping computer (`POST /api/v2/computers/{id}/wake`, returns `{ ip }`).
   */
  async wake(computerId: string, options?: RequestOptions): Promise<WakeComputerResponse> {
    return this.requestJson<WakeComputerResponse>(
      "POST",
      `/api/v2/computers/${encodeURIComponent(computerId)}/wake`,
      undefined,
      options,
    );
  }

  /**
   * Read a file from a running computer as raw bytes (`GET /api/v2/computers/{id}/files`).
   */
  async readFile(
    computerId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<Uint8Array> {
    const searchParams = new URLSearchParams({ path });
    const resp = await this.requestJson<VmReadFileResponse>(
      "GET",
      `/api/v2/computers/${encodeURIComponent(computerId)}/files?${searchParams.toString()}`,
      undefined,
      options,
    );
    return decodeBase64(resp.content_b64);
  }

  /**
   * Read a UTF-8 text file from a running computer.
   */
  async readText(
    computerId: string,
    path: string,
    options?: RequestOptions,
  ): Promise<string> {
    const bytes = await this.readFile(computerId, path, options);
    return new TextDecoder("utf-8").decode(bytes);
  }

  /**
   * Write a file into a running computer using binary `PUT application/octet-stream`
   * (`PUT /api/v2/computers/{id}/files?path=&mode=&create_dirs=`).
   */
  async writeFile(
    computerId: string,
    path: string,
    data: Uint8Array | string,
    opts?: WriteFileOptions,
  ): Promise<void> {
    const searchParams = new URLSearchParams({ path });
    if (opts?.mode !== undefined) searchParams.append("mode", String(opts.mode));
    const createDirs = opts?.createDirs ?? opts?.create_dirs;
    if (createDirs !== undefined) searchParams.append("create_dirs", String(createDirs));

    const payload = typeof data === "string" ? new TextEncoder().encode(data) : data;
    await this.request(
      "PUT",
      `/api/v2/computers/${encodeURIComponent(computerId)}/files?${searchParams.toString()}`,
      payload,
      opts?.requestOptions,
      { mutatingIdempotency: false },
    );
  }

  /**
   * Delete a computer (`DELETE /api/v2/computers/{id}`).
   *
   * By default (`wait: true`), waits for the durable deletion operation to
   * succeed. Pass `{ wait: false }` to return the `202` `Operation` immediately.
   */
  async delete(computerId: string, opts?: DeleteComputerOptions): Promise<Operation> {
    let url = `/api/v2/computers/${encodeURIComponent(computerId)}`;
    const skipWaitBoot = opts?.skipWaitBoot ?? opts?.skip_wait_boot;
    if (skipWaitBoot !== undefined) {
      const searchParams = new URLSearchParams();
      searchParams.append("skip_wait_boot", String(skipWaitBoot));
      url += "?" + searchParams.toString();
    }

    const reqOpts = this.withIdempotencyKey(opts?.requestOptions, opts?.idempotencyKey);
    const op = await this.requestJson<Operation>("DELETE", url, undefined, reqOpts);

    if (opts?.wait === false) {
      return op;
    }

    return this.waitForOperation(op.operation_id, {
      timeoutMs: opts?.timeoutMs,
      onProgress: opts?.onProgress,
      signal: opts?.signal,
      pollInitialDelayMs: opts?.pollInitialDelayMs,
      pollMaxDelayMs: opts?.pollMaxDelayMs,
      initialOperation: op,
    });
  }

  // ── Operations ──────────────────────────────────────────────────────────

  /**
   * Fetch the current durable state of an operation (`GET /api/v2/operations/{id}`).
   */
  async getOperation(operationId: string, options?: RequestOptions): Promise<Operation> {
    return this.requestJson<Operation>(
      "GET",
      `/api/v2/operations/${encodeURIComponent(operationId)}`,
      undefined,
      options,
    );
  }

  /**
   * Follow a durable operation to completion.
   *
   * Streams `GET /api/v2/operations/{id}/events?follow=true` (`application/x-ndjson`)
   * with cursor resumption on disconnect and automatic exponential-backoff polling
   * fallback to `GET /api/v2/operations/{id}`.
   *
   * Throws `VersOperationError` if the operation finishes in `"failed"` or
   * `"quarantined"` status.
   */
  async waitForOperation(
    operationId: string,
    opts: WaitForOperationOptions = {},
  ): Promise<Operation> {
    const waitFor = opts.waitFor ?? "durable";
    const timeoutMs = opts.timeoutMs ?? DEFAULT_WAIT_TIMEOUT_MS;
    const pollInitialDelayMs = opts.pollInitialDelayMs ?? DEFAULT_POLL_INITIAL_DELAY_MS;
    const pollMaxDelayMs = opts.pollMaxDelayMs ?? DEFAULT_POLL_MAX_DELAY_MS;
    const deadline = Date.now() + timeoutMs;

    let lastCursor: number | undefined;
    let lastOp: Operation | undefined;

    const observe = (op: Operation): Operation | null => {
      const isNewer = lastOp === undefined || op.cursor > lastOp.cursor;
      if (lastCursor === undefined || op.cursor > lastCursor) {
        lastCursor = op.cursor;
      }
      if (isNewer) {
        lastOp = op;
        opts.onProgress?.(op);
      }

      if (op.status === "failed" || op.status === "quarantined") {
        throw new VersOperationError(op);
      }

      if (waitFor === "local") {
        if (op.local_result != null || op.result != null || op.status === "succeeded") {
          return op;
        }
        return null;
      }

      if (op.status === "succeeded") {
        if (op.kind === "checkpoint" && op.result == null) {
          return null;
        }
        return op;
      }

      return null;
    };

    if (opts.initialOperation) {
      const ready = observe(opts.initialOperation);
      if (ready) return ready;
    }

    const remainingMs = (): number => {
      const rem = deadline - Date.now();
      if (rem <= 0) {
        throw new APIConnectionTimeoutError({
          message: `Timed out waiting for operation ${operationId} after ${timeoutMs}ms.`,
        });
      }
      return rem;
    };

    let usePolling = false;
    let streamFailures = 0;
    let pollDelayMs = pollInitialDelayMs;

    while (true) {
      const rem = remainingMs();
      if (opts.signal?.aborted) {
        throw new APIConnectionError({ message: `Aborted waiting for operation ${operationId}.` });
      }

      if (usePolling) {
        const op = await this.getOperation(operationId, {
          timeout: Math.min(this.timeout, rem),
          signal: opts.signal,
        });
        const ready = observe(op);
        if (ready) return ready;

        const waitMs = Math.min(pollDelayMs, remainingMs());
        await sleep(waitMs, opts.signal);
        pollDelayMs = Math.min(pollDelayMs * 2, pollMaxDelayMs);
        continue;
      }

      // Build resumable NDJSON event stream URL.
      const searchParams = new URLSearchParams({ follow: "true" });
      if (lastCursor !== undefined) {
        searchParams.set("cursor", String(lastCursor));
      }
      const eventsPath = `/api/v2/operations/${encodeURIComponent(operationId)}/events?${searchParams.toString()}`;

      let response: Response;
      try {
        response = await this.request(
          "GET",
          eventsPath,
          undefined,
          {
            headers: { Accept: "application/x-ndjson" },
            timeout: rem,
            signal: opts.signal,
          },
          { retries: 0, applyDefaultTimeout: false },
        );
      } catch (err) {
        if (
          err instanceof AuthenticationError ||
          err instanceof PermissionDeniedError
        ) {
          throw err;
        }
        if (opts.signal?.aborted) {
          throw err;
        }
        if (err instanceof APIConnectionTimeoutError) {
          remainingMs(); // Throws wait timeout if overall deadline expired
        }

        // If the events endpoint itself is unavailable/unsupported (404/405/501)
        // or returns 400 (e.g. cursor mismatch), switch directly to polling
        // GET /api/v2/operations/{id}. If the operation truly doesn't exist,
        // getOperation will surface the 404 NotFoundError.
        if (
          err instanceof NotFoundError ||
          (err instanceof APIError && (err.status === 400 || err.status === 405 || err.status === 501))
        ) {
          usePolling = true;
          continue;
        }

        streamFailures++;
        if (streamFailures > MAX_STREAM_RECONNECT_FAILURES) {
          usePolling = true;
          continue;
        }

        const backoff = Math.min(
          pollInitialDelayMs * Math.pow(2, streamFailures - 1),
          pollMaxDelayMs,
          remainingMs(),
        );
        await sleep(backoff, opts.signal);
        continue;
      }

      if (!response.body) {
        usePolling = true;
        continue;
      }

      let sawEnd = false;
      let sawTerminalEnd = false;
      try {
        for await (const event of parseNdjson<OperationEvent>(response.body, opts.signal)) {
          streamFailures = 0;

          if (event.type === "gap") {
            // Do not advance lastCursor on a gap alone; wait for the promised
            // baseline snapshot so reconnects never skip compacted state.
            continue;
          }

          if (event.type === "snapshot") {
            const ready = observe(event.operation);
            if (ready) return ready;
            continue;
          }

          if (event.type === "end") {
            sawEnd = true;
            sawTerminalEnd = event.terminal;
            break;
          }
        }
      } catch (err) {
        if (err instanceof VersOperationError) throw err;
        if (opts.signal?.aborted) throw err;

        streamFailures++;
        if (streamFailures > MAX_STREAM_RECONNECT_FAILURES) {
          usePolling = true;
          continue;
        }

        const backoff = Math.min(
          pollInitialDelayMs * Math.pow(2, streamFailures - 1),
          pollMaxDelayMs,
          remainingMs(),
        );
        await sleep(backoff, opts.signal);
        continue;
      }

      if (sawTerminalEnd) {
        // Server reported terminal end; confirm via GET /operations/{id} so we
        // always return or throw with the authoritative terminal snapshot.
        const op = await this.getOperation(operationId, {
          timeout: Math.min(this.timeout, remainingMs()),
          signal: opts.signal,
        });
        const ready = observe(op);
        if (ready) return ready;
        // Defensive fallback if server marked terminal before result landed.
        usePolling = true;
        continue;
      }

      if (sawEnd) {
        // Normal bounded follow window ended with terminal=false: reconnect
        // immediately from lastCursor without growing backoff.
        continue;
      }

      // Stream reached EOF without an `end` frame (mid-stream disconnect):
      // resume from lastCursor with backoff, falling back to polling if repeated.
      streamFailures++;
      if (streamFailures > MAX_STREAM_RECONNECT_FAILURES) {
        usePolling = true;
        continue;
      }
      const backoff = Math.min(
        pollInitialDelayMs * Math.pow(2, streamFailures - 1),
        pollMaxDelayMs,
        remainingMs(),
      );
      await sleep(backoff, opts.signal);
    }
  }

  // ── Checkpoints & Identity ──────────────────────────────────────────────

  /**
   * List checkpoints visible to the caller (`GET /api/v2/checkpoints`).
   */
  async listCheckpoints(
    params?: ListCheckpointsParams,
    options?: RequestOptions,
  ): Promise<ListCheckpointsResponse> {
    let url = "/api/v2/checkpoints";
    const searchParams = new URLSearchParams();
    if (params?.limit !== undefined) searchParams.append("limit", String(params.limit));
    if (params?.cursor !== undefined) searchParams.append("cursor", String(params.cursor));
    if (params?.computer_id !== undefined) searchParams.append("computer_id", String(params.computer_id));
    const qs = searchParams.toString();
    if (qs) url += "?" + qs;
    return this.requestJson<ListCheckpointsResponse>("GET", url, undefined, options);
  }

  /**
   * Delete a published checkpoint (`DELETE /api/v2/checkpoints/{id}`).
   */
  async deleteCheckpoint(checkpointId: string, options?: RequestOptions): Promise<void> {
    await this.request(
      "DELETE",
      `/api/v2/checkpoints/${encodeURIComponent(checkpointId)}`,
      undefined,
      options,
    );
  }

  /**
   * Get authenticated user, org, and API key metadata (`GET /api/v2/whoami`).
   */
  async whoami(options?: RequestOptions): Promise<WhoamiResponse> {
    return this.requestJson<WhoamiResponse>("GET", "/api/v2/whoami", undefined, options);
  }
}
