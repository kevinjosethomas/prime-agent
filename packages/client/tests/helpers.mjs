import { spawn } from 'node:child_process';
import { existsSync, mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

export function tmpSocket(name) {
  const dir = mkdtempSync(join(tmpdir(), 'pa-client-node-'));
  return join(dir, `${name}.sock`);
}

export async function startMock(socketPath, mode = 'echo', arg = '') {
  // Inside Electron's main process, process.execPath is the Electron
  // binary: ELECTRON_RUN_AS_NODE runs the mock as plain Node.
  const electron = process.versions.electron !== undefined;
  const child = spawn(process.execPath, ['mock/daemon.mjs', socketPath, mode, arg], {
    stdio: ['ignore', 'inherit', 'inherit'],
    env: electron ? { ...process.env, ELECTRON_RUN_AS_NODE: '1' } : process.env,
  });
  const deadline = Date.now() + 5000;
  while (!existsSync(socketPath)) {
    if (Date.now() > deadline) throw new Error('mock daemon socket never appeared');
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  return async function stop() {
    child.kill();
    await new Promise((resolve) => {
      if (child.exitCode !== null) resolve(undefined);
      else child.on('exit', resolve);
    });
  };
}

export async function drainEnd(connection, limit = 8) {
  const frames = [];
  for (let i = 0; i < limit; i += 1) {
    const frame = await connection.nextFrame();
    if (frame === null) return frames;
    frames.push(frame);
  }
  throw new Error('end-of-stream never arrived');
}
