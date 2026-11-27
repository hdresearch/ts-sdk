import { describe, expect, it, vi } from "vitest";
import {
  APIConnectionTimeoutError,
  AuthenticationError,
  ConflictError,
  NotFoundError,
  VersClient,
  VersOperationError,
  VersSdkClient,
  type ComputerResponse,
  type Operation,
  type OperationEvent,
  type VmExecStreamEvent,
} from "../src";

const UUID_V4_RE =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

function jsonResponse(body: unknown, status = 200, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json", ...headers },
  });
}

function emptyResponse(status = 204): Response {
  return new Response(null, { status });
}

function ndjsonResponse(
  chunks: string[],
  status = 200,
  opts?: { errorAfterChunks?: Error },
): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      if (opts?.errorAfterChunks) {
        controller.error(opts.errorAfterChunks);
      } else {
        controller.close();
      }
    },
  });
  return new Response(stream, {
    status,
    headers: { "content-type": "application/x-ndjson" },
  });
}

function makeOp(overrides: Partial<Operation> = {}): Operation {
  return {
    operation_id: "11111111-1111-4111-8111-111111111111",
    kind: "create",
    status: "pending",
    resource: {
      kind: "computer",
      id: "22222222-2222-4222-8222-222222222222",
    },
    cursor: 1,
    stage: "admitting",
    created_at: "2026-11-27T00:00:00Z",
    updated_at: "2026-11-27T00:00:00Z",
    ...overrides,
  };
}

function makeComputer(overrides: Partial<ComputerResponse> = {}): ComputerResponse {
  return {
    computer_id: "22222222-2222-4222-8222-222222222222",
    created_at: "2026-11-27T00:00:00Z",
    ip: "10.0.0.42",
    keep_alive: false,
    mem_size_mib: 512,
    owner_id: "org-1",
    state: "running",
    vcpu_count: 1,
    ...overrides,
  };
}

function getHeader(init: RequestInit | undefined, name: string): string | undefined {
  const headers = init?.headers as Record<string, string> | undefined;
  if (!headers) return undefined;
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === lower) return v;
  }
  return undefined;
}

describe("VersClient auth & Idempotency-Key", () => {
  it("sends Authorization: Bearer and auto-generates Idempotency-Key (never X-Idempotency-Key)", async () => {
    const calls: Array<{ url: string; init?: RequestInit }> = [];
    const mockFetch: typeof fetch = async (input, init) => {
      calls.push({ url: String(input), init });
      return jsonResponse(makeOp({ status: "succeeded", completed_at: "2026-11-27T00:00:01Z" }), 202);
    };

    const client = new VersClient({
      baseUrl: "https://api.example.test/",
      apiKey: "sk-vers-secret",
      fetch: mockFetch,
      maxRetries: 0,
    });

    const op = await client.create({ vm_config: { vcpu_count: 1 } }, { wait: false });
    expect(op.operation_id).toBe("11111111-1111-4111-8111-111111111111");
    expect(calls).toHaveLength(1);
    expect(calls[0].url).toBe("https://api.example.test/api/v2/computers");
    expect(getHeader(calls[0].init, "Authorization")).toBe("Bearer sk-vers-secret");
    expect(getHeader(calls[0].init, "X-Idempotency-Key")).toBeUndefined();
    const idemKey = getHeader(calls[0].init, "Idempotency-Key");
    expect(idemKey).toMatch(UUID_V4_RE);
  });

  it("reuses the exact same Idempotency-Key across retries on 503", async () => {
    const seenKeys: Array<string | undefined> = [];
    let attempt = 0;
    const mockFetch: typeof fetch = async (_input, init) => {
      attempt++;
      seenKeys.push(getHeader(init, "Idempotency-Key"));
      expect(getHeader(init, "X-Idempotency-Key")).toBeUndefined();
      if (attempt < 3) {
        return jsonResponse({ error: "transient" }, 503, { "retry-after": "0" });
      }
      return jsonResponse(makeOp(), 202);
    };

    const client = new VersClient({
      baseUrl: "https://api.example.test",
      apiKey: "test-key",
      fetch: mockFetch,
      maxRetries: 2,
    });

    await client.create({ vm_config: {} }, { wait: false });
    expect(seenKeys).toHaveLength(3);
    expect(seenKeys[0]).toMatch(UUID_V4_RE);
    expect(seenKeys[1]).toBe(seenKeys[0]);
    expect(seenKeys[2]).toBe(seenKeys[0]);
  });

  it("preserves caller-supplied idempotencyKey across retries", async () => {
    const explicitKey = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    const seenKeys: Array<string | undefined> = [];
    let attempt = 0;
    const mockFetch: typeof fetch = async (_input, init) => {
      attempt++;
      seenKeys.push(getHeader(init, "Idempotency-Key"));
      if (attempt === 1) {
        return jsonResponse({ error: "retry me" }, 503, { "retry-after": "0" });
      }
      return jsonResponse(makeOp(), 202);
    };

    const client = new VersClient({
      baseUrl: "https://api.example.test",
      apiKey: "test-key",
      fetch: mockFetch,
      maxRetries: 2,
    });

    await client.create({ vm_config: {} }, { wait: false, idempotencyKey: explicitKey });
    expect(seenKeys).toEqual([explicitKey, explicitKey]);
  });
});

describe("VersClient create, restore, get, list, delete", () => {
  it("create() waits for operation and fetches ComputerResponse by default", async () => {
    const urls: string[] = [];
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      urls.push(`${init?.method ?? "GET"} ${url}`);
      if (url.endsWith("/api/v2/computers") && init?.method === "POST") {
        return jsonResponse(makeOp({ status: "pending", cursor: 1 }), 202);
      }
      if (url.includes("/api/v2/operations/") && url.includes("/events")) {
        const events: OperationEvent[] = [
          { type: "snapshot", operation: makeOp({ status: "pending", cursor: 2, stage: "booting" }) },
          {
            type: "snapshot",
            operation: makeOp({
              status: "succeeded",
              cursor: 3,
              stage: "completed",
              completed_at: "2026-11-27T00:00:03Z",
            }),
          },
          { type: "end", cursor: 3, terminal: true },
        ];
        return ndjsonResponse(events.map((e) => JSON.stringify(e) + "\n"));
      }
      if (url.endsWith("/api/v2/computers/22222222-2222-4222-8222-222222222222")) {
        return jsonResponse(makeComputer());
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const stages: Array<string | null | undefined> = [];
    const computer = await client.create(
      { vm_config: { vcpu_count: 2, mem_size_mib: 1024 } },
      { onProgress: (op) => stages.push(op.stage) },
    );

    expect(computer.computer_id).toBe("22222222-2222-4222-8222-222222222222");
    expect(computer.state).toBe("running");
    expect(stages).toEqual(["admitting", "booting", "completed"]);
    expect(urls[0]).toBe("POST https://api.example.test/api/v2/computers");
    expect(urls[1]).toBe(
      "GET https://api.example.test/api/v2/operations/11111111-1111-4111-8111-111111111111/events?follow=true&cursor=1",
    );
    expect(urls[2]).toBe("GET https://api.example.test/api/v2/computers/22222222-2222-4222-8222-222222222222");
  });

  it("restore() sends checkpoint source payload with optional fields", async () => {
    let capturedBody: any;
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      if (url.endsWith("/api/v2/computers") && init?.method === "POST") {
        capturedBody = JSON.parse(String(init.body));
        return jsonResponse(
          makeOp({ status: "succeeded", cursor: 2, completed_at: "2026-11-27T00:00:01Z" }),
          202,
        );
      }
      if (url.endsWith("/api/v2/computers/22222222-2222-4222-8222-222222222222")) {
        return jsonResponse(makeComputer({ parent_checkpoint_id: "ckpt-123" }));
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const computer = await client.restore("ckpt-123", {
      projectId: "proj-1",
      keepAlive: true,
      labels: { env: "test" },
    });

    expect(capturedBody).toEqual({
      source: { type: "checkpoint", checkpoint_id: "ckpt-123" },
      project_id: "proj-1",
      keep_alive: true,
      labels: { env: "test" },
    });
    expect(computer.parent_checkpoint_id).toBe("ckpt-123");
  });

  it("list() and get() hit /api/v2/computers endpoints with query params", async () => {
    const urls: string[] = [];
    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      urls.push(url);
      if (url.includes("/api/v2/computers?")) {
        return jsonResponse({ computers: [makeComputer()], next_cursor: "cur-2" });
      }
      return jsonResponse(makeComputer());
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const page = await client.list({ limit: 25, cursor: "cur-1" });
    expect(page.computers).toHaveLength(1);
    expect(page.next_cursor).toBe("cur-2");
    expect(urls[0]).toBe("https://api.example.test/api/v2/computers?limit=25&cursor=cur-1");

    const single = await client.get("22222222-2222-4222-8222-222222222222");
    expect(single.ip).toBe("10.0.0.42");
  });

  it("delete() sends DELETE with Idempotency-Key and skip_wait_boot and waits by default", async () => {
    let deleteMethod: string | undefined;
    let deleteUrl = "";
    let deleteIdemKey: string | undefined;
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      if (init?.method === "DELETE") {
        deleteMethod = init.method;
        deleteUrl = url;
        deleteIdemKey = getHeader(init, "Idempotency-Key");
        return jsonResponse(makeOp({ kind: "delete", status: "pending", cursor: 1 }), 202);
      }
      if (url.includes("/events")) {
        return ndjsonResponse([
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              kind: "delete",
              status: "succeeded",
              cursor: 2,
              completed_at: "2026-11-27T00:00:02Z",
            }),
          }) + "\n",
          JSON.stringify({ type: "end", cursor: 2, terminal: true }) + "\n",
        ]);
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const op = await client.delete("22222222-2222-4222-8222-222222222222", { skipWaitBoot: true });
    expect(deleteMethod).toBe("DELETE");
    expect(deleteUrl).toBe(
      "https://api.example.test/api/v2/computers/22222222-2222-4222-8222-222222222222?skip_wait_boot=true",
    );
    expect(deleteIdemKey).toMatch(UUID_V4_RE);
    expect(op.status).toBe("succeeded");
  });
});

describe("VersClient operation streaming, cursor resumption, polling fallback, and errors", () => {
  it("handles gap events, terminal=false follow window end, and cursor resumption on disconnect", async () => {
    const eventRequests: string[] = [];
    let call = 0;

    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      eventRequests.push(url);
      call++;

      if (call === 1) {
        // First window: gap 0..4, snapshot at 4, then normal non-terminal end
        return ndjsonResponse([
          JSON.stringify({ type: "gap", requested_cursor: 0, available_cursor: 4 }) + "\n",
          JSON.stringify({ type: "snapshot", operation: makeOp({ cursor: 4, stage: "snapshotting" }) }) + "\n",
          JSON.stringify({ type: "end", cursor: 4, terminal: false }) + "\n",
        ]);
      }

      if (call === 2) {
        // Second window: emit cursor 5 then drop connection without `end` frame
        return ndjsonResponse([
          JSON.stringify({ type: "snapshot", operation: makeOp({ cursor: 5, stage: "uploading" }) }) + "\n",
        ]);
      }

      if (call === 3) {
        // Third window: resumes at cursor=5, emits succeeded snapshot at cursor 6
        return ndjsonResponse([
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              cursor: 6,
              status: "succeeded",
              stage: "done",
              completed_at: "2026-11-27T00:00:06Z",
            }),
          }) + "\n",
          JSON.stringify({ type: "end", cursor: 6, terminal: true }) + "\n",
        ]);
      }

      throw new Error(`unexpected call ${call}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const observedCursors: number[] = [];
    const op = await client.waitForOperation("11111111-1111-4111-8111-111111111111", {
      pollInitialDelayMs: 5,
      pollMaxDelayMs: 20,
      onProgress: (snap) => observedCursors.push(snap.cursor),
    });

    expect(op.status).toBe("succeeded");
    expect(op.cursor).toBe(6);
    expect(observedCursors).toEqual([4, 5, 6]);
    expect(eventRequests).toEqual([
      "https://api.example.test/api/v2/operations/11111111-1111-4111-8111-111111111111/events?follow=true",
      "https://api.example.test/api/v2/operations/11111111-1111-4111-8111-111111111111/events?follow=true&cursor=4",
      "https://api.example.test/api/v2/operations/11111111-1111-4111-8111-111111111111/events?follow=true&cursor=5",
    ]);
  });

  it("falls back to exponential-backoff polling GET /operations/{id} when /events repeatedly fails", async () => {
    const requestedUrls: string[] = [];
    let pollCount = 0;

    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      requestedUrls.push(url);
      if (url.includes("/events")) {
        return jsonResponse({ error: "stream unavailable" }, 503);
      }
      pollCount++;
      if (pollCount < 3) {
        return jsonResponse(makeOp({ cursor: pollCount, status: "pending" }));
      }
      return jsonResponse(
        makeOp({ cursor: 3, status: "succeeded", completed_at: "2026-11-27T00:00:03Z" }),
      );
    };

    const client = new VersClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: mockFetch,
      maxRetries: 0,
    });

    const op = await client.waitForOperation("11111111-1111-4111-8111-111111111111", {
      pollInitialDelayMs: 5,
      pollMaxDelayMs: 25,
    });

    expect(op.status).toBe("succeeded");
    expect(pollCount).toBe(3);
    expect(requestedUrls.filter((u) => u.includes("/events")).length).toBe(3);
    expect(
      requestedUrls.filter((u) => u.endsWith("/api/v2/operations/11111111-1111-4111-8111-111111111111")).length,
    ).toBe(3);
  });

  it("throws typed VersOperationError on failed and quarantined operations", async () => {
    const failedOp = makeOp({
      status: "failed",
      stage: "booting",
      failure: { code: "insufficient_resources", message: "No capacity available" },
      cleanup_operation_id: "99999999-9999-4999-8999-999999999999",
      completed_at: "2026-11-27T00:00:05Z",
      cursor: 2,
    });

    const clientFailed = new VersClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: async () =>
        ndjsonResponse([
          JSON.stringify({ type: "snapshot", operation: failedOp }) + "\n",
          JSON.stringify({ type: "end", cursor: 2, terminal: true }) + "\n",
        ]),
    });

    await expect(clientFailed.waitForOperation(failedOp.operation_id)).rejects.toSatisfy((err: unknown) => {
      expect(err).toBeInstanceOf(VersOperationError);
      const opErr = err as VersOperationError;
      expect(opErr.operationId).toBe(failedOp.operation_id);
      expect(opErr.status).toBe("failed");
      expect(opErr.stage).toBe("booting");
      expect(opErr.code).toBe("insufficient_resources");
      expect(opErr.message).toContain("No capacity available");
      expect(opErr.cleanupOperationId).toBe("99999999-9999-4999-8999-999999999999");
      return true;
    });

    const quarantinedOp = makeOp({
      status: "quarantined",
      stage: "publishing",
      failure: { code: "storage_corrupt", message: "Quarantined artifact" },
      completed_at: "2026-11-27T00:00:05Z",
      cursor: 4,
    });

    const clientQuarantined = new VersClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: async () =>
        ndjsonResponse([JSON.stringify({ type: "snapshot", operation: quarantinedOp }) + "\n"]),
    });

    await expect(clientQuarantined.waitForOperation(quarantinedOp.operation_id)).rejects.toSatisfy(
      (err: unknown) => {
        expect(err).toBeInstanceOf(VersOperationError);
        const opErr = err as VersOperationError;
        expect(opErr.status).toBe("quarantined");
        expect(opErr.stage).toBe("publishing");
        expect(opErr.code).toBe("storage_corrupt");
        expect(opErr.cleanupOperationId).toBeUndefined();
        return true;
      },
    );
  });

  it("throws AuthenticationError immediately without falling back to polling", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () => jsonResponse({ error: "unauthorized" }, 401));
    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "bad", fetch: fetchSpy });

    await expect(client.waitForOperation("11111111-1111-4111-8111-111111111111")).rejects.toBeInstanceOf(
      AuthenticationError,
    );
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("throws APIConnectionTimeoutError when waitForOperation exceeds timeoutMs", async () => {
    const mockFetch: typeof fetch = async (input) => {
      const url = String(input);
      if (url.includes("/events")) return jsonResponse({ error: "not implemented" }, 501);
      return jsonResponse(makeOp({ status: "pending", cursor: 1 }));
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    await expect(
      client.waitForOperation("11111111-1111-4111-8111-111111111111", {
        timeoutMs: 30,
        pollInitialDelayMs: 10,
        pollMaxDelayMs: 10,
      }),
    ).rejects.toBeInstanceOf(APIConnectionTimeoutError);
  });
});

describe("VersClient checkpoint local vs durable wait", () => {
  it("waitFor: 'local' returns as soon as local_result is set while status is still pending", async () => {
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      if (init?.method === "POST") {
        expect(getHeader(init, "Idempotency-Key")).toMatch(UUID_V4_RE);
        return jsonResponse(makeOp({ kind: "checkpoint", status: "pending", cursor: 1 }), 202);
      }
      if (url.includes("/events")) {
        return ndjsonResponse([
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              kind: "checkpoint",
              status: "pending",
              cursor: 2,
              stage: "publishing",
              local_result: { kind: "checkpoint", id: "ckpt-local-1" },
            }),
          }) + "\n",
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              kind: "checkpoint",
              status: "succeeded",
              cursor: 3,
              stage: "completed",
              local_result: { kind: "checkpoint", id: "ckpt-local-1" },
              result: { kind: "checkpoint", id: "ckpt-local-1" },
              completed_at: "2026-11-27T00:00:05Z",
            }),
          }) + "\n",
        ]);
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const localOp = await client.checkpoint("comp-1", { name: "snap", waitFor: "local" });
    expect(localOp.status).toBe("pending");
    expect(localOp.local_result?.id).toBe("ckpt-local-1");
    expect(localOp.result).toBeUndefined();
  });

  it("waitFor: 'durable' (default) waits past local_result until succeeded with result", async () => {
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      if (init?.method === "POST") {
        return jsonResponse(makeOp({ kind: "checkpoint", status: "pending", cursor: 1 }), 202);
      }
      if (url.includes("/events")) {
        return ndjsonResponse([
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              kind: "checkpoint",
              status: "pending",
              cursor: 2,
              stage: "publishing",
              local_result: { kind: "checkpoint", id: "ckpt-durable-1" },
            }),
          }) + "\n",
          JSON.stringify({
            type: "snapshot",
            operation: makeOp({
              kind: "checkpoint",
              status: "succeeded",
              cursor: 3,
              stage: "completed",
              local_result: { kind: "checkpoint", id: "ckpt-durable-1" },
              result: { kind: "checkpoint", id: "ckpt-durable-1" },
              completed_at: "2026-11-27T00:00:05Z",
            }),
          }) + "\n",
          JSON.stringify({ type: "end", cursor: 3, terminal: true }) + "\n",
        ]);
      }
      throw new Error(`unexpected fetch: ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const durableOp = await client.checkpoint("comp-1", { name: "snap" });
    expect(durableOp.status).toBe("succeeded");
    expect(durableOp.result?.id).toBe("ckpt-durable-1");
  });
});

describe("VersClient exec, execStream, branch, sleep, wake, files", () => {
  it("exec() sends direct argv command and returns VmExecResponse", async () => {
    let sentBody: unknown;
    const mockFetch: typeof fetch = async (input, init) => {
      expect(String(input)).toBe("https://api.example.test/api/v2/computers/comp-1/exec");
      sentBody = JSON.parse(String(init?.body));
      return jsonResponse({ exit_code: 0, stdout: "hello\n", stderr: "", exec_id: "exec-1" });
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const res = await client.exec("comp-1", {
      command: ["echo", "hello"],
      working_dir: "/tmp",
      env: { FOO: "bar" },
    });

    expect(sentBody).toEqual({ command: ["echo", "hello"], working_dir: "/tmp", env: { FOO: "bar" } });
    expect(res).toEqual({ exit_code: 0, stdout: "hello\n", stderr: "", exec_id: "exec-1" });
  });

  it("execStream() parses NDJSON events split across arbitrary chunk boundaries", async () => {
    const line1 = JSON.stringify({
      type: "chunk",
      cursor: 1,
      exec_id: "exec-1",
      stream: "stdout",
      data_b64: Buffer.from("hello ").toString("base64"),
    });
    const line2 = JSON.stringify({
      type: "chunk",
      cursor: 2,
      exec_id: "exec-1",
      stream: "stderr",
      data_b64: Buffer.from("warn").toString("base64"),
    });
    const line3 = JSON.stringify({
      type: "exit",
      cursor: 3,
      exec_id: "exec-1",
      exit_code: 0,
      infrastructure_error: null,
    });
    const full = `${line1}\n${line2}\r\n${line3}\n`;
    // Split mid-JSON line to verify incremental buffering
    const chunks = [full.slice(0, 17), full.slice(17, 90), full.slice(90)];

    const mockFetch: typeof fetch = async (input, init) => {
      expect(String(input)).toBe("https://api.example.test/api/v2/computers/comp-1/exec/stream");
      expect(getHeader(init, "Accept")).toBe("application/x-ndjson");
      return ndjsonResponse(chunks);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const events: VmExecStreamEvent[] = [];
    for await (const ev of client.execStream("comp-1", { command: ["sh", "-c", "echo hi"] })) {
      events.push(ev);
    }

    expect(events).toHaveLength(3);
    expect(events[0].type).toBe("chunk");
    if (events[0].type === "chunk") {
      expect(events[0].stream).toBe("stdout");
      expect(Buffer.from(events[0].data_b64, "base64").toString("utf8")).toBe("hello ");
    }
    expect(events[1].type).toBe("chunk");
    expect(events[2]).toEqual({
      type: "exit",
      cursor: 3,
      exec_id: "exec-1",
      exit_code: 0,
      infrastructure_error: null,
    });
  });

  it("branch() sends synchronous POST /api/v2/branches and returns 201 computers", async () => {
    let sentBody: unknown;
    const mockFetch: typeof fetch = async (input, init) => {
      expect(String(input)).toBe("https://api.example.test/api/v2/branches");
      expect(init?.method).toBe("POST");
      sentBody = JSON.parse(String(init?.body));
      return jsonResponse(
        {
          computers: [
            { computer_id: "b-1", ip: "10.0.0.2", name: "branch-1", volume_backend: "cowfswal" },
            { computer_id: "b-2", ip: "10.0.0.3", name: "branch-2", volume_backend: "cowfswal" },
          ],
        },
        201,
      );
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    const res = await client.branch({
      source: { type: "computer", computer_id: "comp-1" },
      count: 2,
      project_id: "proj-1",
    });

    expect(sentBody).toEqual({
      source: { type: "computer", computer_id: "comp-1" },
      count: 2,
      project_id: "proj-1",
    });
    expect(res.computers).toHaveLength(2);
    expect(res.computers[0].computer_id).toBe("b-1");
  });

  it("sleep() and wake() send empty POST bodies and handle 204 / 200 { ip }", async () => {
    const calls: Array<{ url: string; method?: string; body?: BodyInit | null; contentType?: string }> = [];
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      calls.push({
        url,
        method: init?.method,
        body: init?.body,
        contentType: getHeader(init, "Content-Type"),
      });
      if (url.includes("/sleep")) return emptyResponse(204);
      if (url.includes("/wake")) return jsonResponse({ ip: "10.0.0.77" }, 200);
      throw new Error(`unexpected ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });
    await client.sleep("comp-1", { skipWaitBoot: true });
    const wakeRes = await client.wake("comp-1");

    expect(wakeRes).toEqual({ ip: "10.0.0.77" });
    expect(calls[0]).toEqual({
      url: "https://api.example.test/api/v2/computers/comp-1/sleep?skip_wait_boot=true",
      method: "POST",
      body: undefined,
      contentType: undefined,
    });
    expect(calls[1]).toEqual({
      url: "https://api.example.test/api/v2/computers/comp-1/wake",
      method: "POST",
      body: undefined,
      contentType: undefined,
    });
  });

  it("writeFile() and readFile() / readText() roundtrip arbitrary binary bytes and text", async () => {
    const store = new Map<string, Uint8Array>();
    const capturedWrites: Array<{ url: string; contentType?: string }> = [];

    const mockFetch: typeof fetch = async (input, init) => {
      const url = new URL(String(input));
      const filePath = url.searchParams.get("path")!;
      if (init?.method === "PUT") {
        capturedWrites.push({
          url: url.toString(),
          contentType: getHeader(init, "Content-Type"),
        });
        const body = init.body;
        expect(body).toBeInstanceOf(Uint8Array);
        store.set(filePath, new Uint8Array(body as Uint8Array));
        return emptyResponse(204);
      }
      if (init?.method === "GET") {
        const bytes = store.get(filePath);
        if (!bytes) return jsonResponse({ error: "not found" }, 404);
        return jsonResponse({ content_b64: Buffer.from(bytes).toString("base64") });
      }
      throw new Error(`unexpected ${init?.method} ${url}`);
    };

    const client = new VersClient({ baseUrl: "https://api.example.test", apiKey: "k", fetch: mockFetch });

    // Binary payload with null bytes and invalid UTF-8 sequences
    const bin = new Uint8Array([0x00, 0xff, 0x10, 0x80, 0xfe, 0x01, 0x42]);
    await client.writeFile("comp-1", "/tmp/data/blob.bin", bin, { mode: 0o600, createDirs: true });

    expect(capturedWrites[0].contentType).toBe("application/octet-stream");
    const parsedUrl = new URL(capturedWrites[0].url);
    expect(parsedUrl.pathname).toBe("/api/v2/computers/comp-1/files");
    expect(parsedUrl.searchParams.get("path")).toBe("/tmp/data/blob.bin");
    expect(parsedUrl.searchParams.get("mode")).toBe(String(0o600));
    expect(parsedUrl.searchParams.get("create_dirs")).toBe("true");

    const readBack = await client.readFile("comp-1", "/tmp/data/blob.bin");
    expect(readBack).toBeInstanceOf(Uint8Array);
    expect(readBack).toEqual(bin);

    // UTF-8 text roundtrip
    await client.writeFile("comp-1", "/tmp/hello.txt", "héllo 🚀\n");
    const text = await client.readText("comp-1", "/tmp/hello.txt");
    expect(text).toBe("héllo 🚀\n");
  });
});

describe("VersSdkClient backwards-compatible bug fixes", () => {
  it("sends Idempotency-Key (never X-Idempotency-Key) and reuses it across retries", async () => {
    const keys: Array<string | undefined> = [];
    let attempt = 0;
    const mockFetch: typeof fetch = async (_input, init) => {
      attempt++;
      expect(getHeader(init, "X-Idempotency-Key")).toBeUndefined();
      keys.push(getHeader(init, "Idempotency-Key"));
      if (attempt === 1) {
        return jsonResponse({ error: "busy" }, 503, { "retry-after": "0" });
      }
      return jsonResponse(makeOp(), 202);
    };

    const sdk = new VersSdkClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: mockFetch,
      maxRetries: 2,
      logLevel: "off",
    });

    await sdk.createComputer({ vm_config: {} });
    expect(keys).toHaveLength(2);
    expect(keys[0]).toMatch(UUID_V4_RE);
    expect(keys[1]).toBe(keys[0]);
  });

  it("does not retry 409 ConflictError and extracts { error } message cleanly", async () => {
    const fetchSpy = vi.fn<typeof fetch>(async () =>
      jsonResponse({ error: "computer is already sleeping" }, 409),
    );
    const sdk = new VersSdkClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: fetchSpy,
      maxRetries: 2,
      logLevel: "off",
    });

    await expect(sdk.sleepComputer("comp-1")).rejects.toSatisfy((err: unknown) => {
      expect(err).toBeInstanceOf(ConflictError);
      expect((err as ConflictError).message).toBe("409 computer is already sleeping");
      return true;
    });
    expect(fetchSpy).toHaveBeenCalledTimes(1);
  });

  it("sleepComputer and wakeComputer omit request body and handle 204 / 200", async () => {
    const captured: Array<{ url: string; body: unknown; contentType?: string }> = [];
    const mockFetch: typeof fetch = async (input, init) => {
      const url = String(input);
      captured.push({ url, body: init?.body, contentType: getHeader(init, "Content-Type") });
      if (url.includes("/sleep")) return emptyResponse(204);
      return jsonResponse({ ip: "10.0.0.5" });
    };

    const sdk = new VersSdkClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: mockFetch,
      maxRetries: 0,
      logLevel: "off",
    });

    await sdk.sleepComputer("comp-1", { skip_wait_boot: true });
    const res = await sdk.wakeComputer("comp-1");

    expect(res).toEqual({ ip: "10.0.0.5" });
    expect(captured[0]).toEqual({
      url: "https://api.example.test/api/v2/computers/comp-1/sleep?skip_wait_boot=true",
      body: undefined,
      contentType: undefined,
    });
    expect(captured[1]).toEqual({
      url: "https://api.example.test/api/v2/computers/comp-1/wake",
      body: undefined,
      contentType: undefined,
    });
  });

  it("writeComputerFile supports raw binary application/octet-stream and JSON contracts", async () => {
    const captured: Array<{ url: string; contentType?: string; body: unknown }> = [];
    const mockFetch: typeof fetch = async (input, init) => {
      captured.push({
        url: String(input),
        contentType: getHeader(init, "Content-Type"),
        body: init?.body,
      });
      return emptyResponse(204);
    };

    const sdk = new VersSdkClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: mockFetch,
      maxRetries: 0,
      logLevel: "off",
    });

    const bytes = new Uint8Array([1, 2, 3, 4]);
    await sdk.writeComputerFile("comp-1", bytes, {
      path: "/tmp/a.bin",
      mode: 0o644,
      create_dirs: true,
    });
    expect(captured[0].contentType).toBe("application/octet-stream");
    expect(captured[0].url).toBe(
      "https://api.example.test/api/v2/computers/comp-1/files?path=%2Ftmp%2Fa.bin&mode=420&create_dirs=true",
    );
    expect(captured[0].body).toEqual(bytes);

    await sdk.writeComputerFile("comp-1", {
      path: "/tmp/b.txt",
      content_b64: Buffer.from("hi").toString("base64"),
    });
    expect(captured[1].contentType).toBe("application/json");
    expect(captured[1].body).toBe(
      JSON.stringify({ path: "/tmp/b.txt", content_b64: Buffer.from("hi").toString("base64") }),
    );
  });

  it("maps 404 to NotFoundError", async () => {
    const sdk = new VersSdkClient({
      baseUrl: "https://api.example.test",
      apiKey: "k",
      fetch: async () => jsonResponse({ error: "not found" }, 404),
      maxRetries: 0,
      logLevel: "off",
    });

    await expect(sdk.getComputer("missing")).rejects.toBeInstanceOf(NotFoundError);
  });
});
