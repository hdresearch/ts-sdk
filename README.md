# Vers TypeScript SDK (`vers-sdk`)

Official TypeScript / JavaScript SDK for the [Vers](https://vers.sh) `/api/v2` REST API.

- **Zero runtime dependencies** — uses native `fetch` and `crypto.randomUUID()` on Node.js 20+, Deno, Bun, and edge runtimes.
- **High-level `VersClient`** — ergonomic methods for computers, exec & NDJSON streaming, binary file I/O, branching, durable/local checkpoints, sleep/wake, and resumable operation following.
- **Full low-level `VersSdkClient`** — direct access to every `/api/v2` endpoint.

## Installation

```sh
npm install vers-sdk
```

## Quick Start

```ts
import { VersClient } from "vers-sdk";

const vers = new VersClient({
  apiKey: process.env["VERS_API_KEY"], // default; can be omitted when VERS_API_KEY is set
});

// Create a computer (automatically waits for the durable operation to complete)
const computer = await vers.create({
  vm_config: { vcpu_count: 2, mem_size_mib: 1024 },
});

// Write and read files (binary PUT application/octet-stream)
await vers.writeFile(computer.computer_id, "/tmp/hello.txt", "hello from vers v2\n", {
  mode: 0o644,
  createDirs: true,
});
const text = await vers.readText(computer.computer_id, "/tmp/hello.txt");

// Execute a command directly (argv array, no implicit shell)
const result = await vers.exec(computer.computer_id, {
  command: ["cat", "/tmp/hello.txt"],
});
console.log(result.exit_code, result.stdout);

// Clean up
await vers.delete(computer.computer_id);
```

## Streaming Command Execution

```ts
for await (const event of vers.execStream(computer.computer_id, {
  command: ["sh", "-c", "for i in 1 2 3; do echo line $i; sleep 0.2; done"],
})) {
  if (event.type === "chunk") {
    const chunk = Buffer.from(event.data_b64, "base64").toString("utf8");
    if (event.stream === "stdout") process.stdout.write(chunk);
    else process.stderr.write(chunk);
  } else if (event.type === "exit") {
    console.log("Exited with code:", event.exit_code);
  }
}
```

## Checkpoints, Restore, and Branching

```ts
// Capture a durable (S3-portable) checkpoint
const ckptOp = await vers.checkpoint(computer.computer_id, {
  name: "after-setup",
  waitFor: "durable", // default; use "local" to return as soon as local_result is ready
});
const checkpointId = ckptOp.result!.id;

// Restore a new computer from the checkpoint
const restored = await vers.restore(checkpointId, {
  keepAlive: true,
  labels: { role: "worker" },
});

// Synchronously branch multiple computers from a computer or checkpoint
const branches = await vers.branch({
  source: { type: "checkpoint", checkpoint_id: checkpointId },
  count: 3,
});
console.log(branches.computers.map((c) => c.computer_id));
```

## Sleep and Wake

```ts
// Put a running computer to sleep (preserves execution state)
await vers.sleep(computer.computer_id);

// Wake it later and get its guest IP
const { ip } = await vers.wake(computer.computer_id);
console.log("Awake at", ip);
```

## Durable Operations & Async Mode

Mutating lifecycle methods (`create`, `restore`, `checkpoint`, `delete`) automatically generate a UUIDv4 `Idempotency-Key` header once per call, reuse the exact same key across retries, and follow `GET /api/v2/operations/{id}/events?follow=true` (NDJSON) with cursor resumption and exponential-backoff polling fallback to `GET /api/v2/operations/{id}`.

Pass `{ wait: false }` to get the `202` `Operation` immediately and wait later:

```ts
const op = await vers.create({ vm_config: { vcpu_count: 1 } }, { wait: false });
const done = await vers.waitForOperation(op.operation_id, {
  onProgress: (snap) => console.log(snap.status, snap.stage),
});
const computer = await vers.get(done.resource.id);
```

## Configuration

```ts
const vers = new VersClient({
  apiKey: "your-api-key",         // defaults to VERS_API_KEY
  baseUrl: "https://api.vers.sh", // defaults to VERS_BASE_URL or https://api.vers.sh
  maxRetries: 2,                  // retries 408, 429, 5xx, and connection errors
  timeout: 30_000,                // per-request timeout in ms
  fetch: customFetch,             // optional custom fetch implementation
});
```

## Error Handling

Non-2xx responses throw typed subclasses of `APIError`, and failed or quarantined durable operations throw `VersOperationError`:

```ts
import { NotFoundError, VersOperationError } from "vers-sdk";

try {
  await vers.create({ vm_config: { vcpu_count: 1 } });
} catch (err) {
  if (err instanceof VersOperationError) {
    console.error(err.operationId, err.status, err.stage, err.code, err.message);
    if (err.cleanupOperationId) {
      console.error("Cleanup operation:", err.cleanupOperationId);
    }
  } else if (err instanceof NotFoundError) {
    console.error(err.status, err.errorMessage);
  }
}
```

| Error Type | When Thrown |
| --- | --- |
| `VersOperationError` | Durable operation reaches `"failed"` or `"quarantined"` |
| `BadRequestError` | HTTP `400` |
| `AuthenticationError` | HTTP `401` |
| `PermissionDeniedError` | HTTP `403` |
| `NotFoundError` | HTTP `404` |
| `ConflictError` | HTTP `409` |
| `UnprocessableEntityError` | HTTP `422` |
| `RateLimitError` | HTTP `429` |
| `InternalServerError` | HTTP `>= 500` |
| `APIConnectionTimeoutError` | Client request or operation wait timed out |
| `APIConnectionError` | Network / transport failure or abort |

## Low-Level Client (`VersSdkClient`)

For endpoints not wrapped on `VersClient` (such as projects, domains, env vars, repositories, and registry endpoints), `VersSdkClient` remains exported with full `/api/v2` coverage:

```ts
import { VersSdkClient } from "vers-sdk";

const sdk = new VersSdkClient({ apiKey: process.env["VERS_API_KEY"] });
const projects = await sdk.listProjects();
```

## Development & Verification

```sh
npm install
npm run build
npm test
VERS_API_KEY=... npm run smoke
```

## Requirements

- Node.js >= 20 (or Deno, Bun, Cloudflare Workers)
- TypeScript >= 5.0

## License

MIT
