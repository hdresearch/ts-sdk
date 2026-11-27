/**
 * Live production end-to-end smoke test for the Vers v2 TypeScript SDK.
 *
 * Usage:
 *   VERS_API_KEY=... npm run smoke
 *   VERS_API_KEY=... VERS_BASE_URL=https://api.vers.sh npx tsx scripts/smoke.ts
 */

import { VersClient } from "../src";

async function main(): Promise<void> {
  const apiKey = process.env["VERS_API_KEY"];
  if (!apiKey) {
    console.log("Skipping live smoke test: VERS_API_KEY is not set.");
    return;
  }

  const client = new VersClient({
    apiKey,
    baseUrl: process.env["VERS_BASE_URL"],
  });

  const createdComputerIds: string[] = [];
  const createdCheckpointIds: string[] = [];

  try {
    console.log("1. whoami...");
    const identity = await client.whoami();
    console.log(`   Authenticated as ${identity.user.email} (org: ${identity.org.name})`);

    console.log("2. create computer...");
    const computer = await client.create(
      {
        vm_config: {
          vcpu_count: 1,
          mem_size_mib: 512,
          labels: { "vers-sdk-smoke": "true" },
        },
      },
      {
        onProgress: (op) => console.log(`   [create] status=${op.status} stage=${op.stage ?? "-"}`),
      },
    );
    createdComputerIds.push(computer.computer_id);
    console.log(`   Created computer ${computer.computer_id} (state=${computer.state}, ip=${computer.ip})`);

    console.log("3. get & list...");
    const fetched = await client.get(computer.computer_id);
    if (fetched.computer_id !== computer.computer_id) {
      throw new Error("get() returned mismatched computer_id");
    }
    const listed = await client.list({ limit: 50 });
    if (!listed.computers.some((c) => c.computer_id === computer.computer_id)) {
      throw new Error("list() did not include newly created computer");
    }

    console.log("4. writeFile / readFile (binary) / readText...");
    const binaryPayload = new Uint8Array([0x00, 0x01, 0x02, 0xfe, 0xff, 0x42]);
    await client.writeFile(computer.computer_id, "/tmp/vers-smoke/blob.bin", binaryPayload, {
      mode: 0o644,
      createDirs: true,
    });
    const readBinary = await client.readFile(computer.computer_id, "/tmp/vers-smoke/blob.bin");
    if (
      readBinary.length !== binaryPayload.length ||
      readBinary.some((b, i) => b !== binaryPayload[i])
    ) {
      throw new Error("Binary file roundtrip mismatch");
    }

    await client.writeFile(computer.computer_id, "/tmp/vers-smoke/hello.txt", "hello vers v2\n");
    const readStr = await client.readText(computer.computer_id, "/tmp/vers-smoke/hello.txt");
    if (readStr !== "hello vers v2\n") {
      throw new Error(`Text file roundtrip mismatch: ${JSON.stringify(readStr)}`);
    }

    console.log("5. exec & execStream...");
    const execRes = await client.exec(computer.computer_id, {
      command: ["cat", "/tmp/vers-smoke/hello.txt"],
    });
    if (execRes.exit_code !== 0 || execRes.stdout !== "hello vers v2\n") {
      throw new Error(`Unexpected exec output: ${JSON.stringify(execRes)}`);
    }

    let streamedOut = "";
    let exitCode: number | undefined;
    for await (const ev of client.execStream(computer.computer_id, {
      command: ["echo", "streamed-ok"],
    })) {
      if (ev.type === "chunk" && ev.stream === "stdout") {
        streamedOut += Buffer.from(ev.data_b64, "base64").toString("utf8");
      } else if (ev.type === "exit") {
        exitCode = ev.exit_code;
      }
    }
    if (exitCode !== 0 || streamedOut.trim() !== "streamed-ok") {
      throw new Error(`Unexpected execStream output: exit=${exitCode}, out=${JSON.stringify(streamedOut)}`);
    }

    console.log("6. sleep & wake...");
    await client.sleep(computer.computer_id);
    const woken = await client.wake(computer.computer_id);
    console.log(`   Woke computer at ip=${woken.ip}`);

    console.log("7. checkpoint (durable) & restore...");
    const ckptOp = await client.checkpoint(computer.computer_id, {
      name: "ts-sdk-smoke",
      waitFor: "durable",
      onProgress: (op) => console.log(`   [checkpoint] status=${op.status} stage=${op.stage ?? "-"}`),
    });
    const checkpointId = ckptOp.result?.id;
    if (!checkpointId) {
      throw new Error("Durable checkpoint operation did not return result.id");
    }
    createdCheckpointIds.push(checkpointId);
    console.log(`   Published checkpoint ${checkpointId}`);

    const restored = await client.restore(checkpointId);
    createdComputerIds.push(restored.computer_id);
    const restoredText = await client.readText(restored.computer_id, "/tmp/vers-smoke/hello.txt");
    if (restoredText !== "hello vers v2\n") {
      throw new Error(`Restored computer missing file: ${JSON.stringify(restoredText)}`);
    }

    console.log("8. branch...");
    const branched = await client.branch({
      source: { type: "checkpoint", checkpoint_id: checkpointId },
      count: 1,
    });
    for (const c of branched.computers) {
      createdComputerIds.push(c.computer_id);
      console.log(`   Branched computer ${c.computer_id} (ip=${c.ip})`);
    }

    console.log("All live smoke checks passed!");
  } finally {
    // Delete all computers first so checkpoints can be cleanly deleted afterwards.
    for (const id of createdComputerIds.reverse()) {
      try {
        console.log(`cleanup: deleting computer ${id}...`);
        await client.delete(id, { skipWaitBoot: true });
      } catch (err) {
        console.error(`cleanup: failed to delete computer ${id}:`, err);
      }
    }
    for (const ckptId of createdCheckpointIds.reverse()) {
      try {
        console.log(`cleanup: deleting checkpoint ${ckptId}...`);
        await client.deleteCheckpoint(ckptId);
      } catch (err) {
        console.error(`cleanup: failed to delete checkpoint ${ckptId}:`, err);
      }
    }
  }
}

main().catch((err) => {
  console.error("Smoke test failed:", err);
  process.exit(1);
});
