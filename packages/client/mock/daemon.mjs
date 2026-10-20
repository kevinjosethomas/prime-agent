/**
 * Mock daemon for the Node test battery: a child process speaking the wire
 * JSONL protocol over a unix socket. Modes control the failure scenarios
 * the error matrix needs.
 *
 * usage: node mock/daemon.mjs <socketPath> <mode> [arg]
 * modes:
 *  echo  — hello; answer every command envelope with success + the command
 *           object echoed back (unknown fields included).
 *  events — hello; push a known sequence of application frames (known
 *           frame types, full meta, and an unknown frame type).
 *  silent — hello; read commands but never respond (client timeouts).
 *  refuse — hello; answer success:false with a typed errorInfo.
 *  flood  — hello; answer commands, then push session_event frames fast.
 *  die    — hello; close the socket after reading one command envelope.
 */
import net from 'node:net';
import fs from 'node:fs';

const [socketPath, mode = 'echo', arg = ''] = process.argv.slice(2);
fs.rmSync(socketPath, { force: true });

const server = net.createServer((socket) => {
  socket.setEncoding('utf8');
  let buffer = '';
  socket.write(
    JSON.stringify({
      type: 'daemon_hello',
      protocol: { name: 'prime-agent.daemon', version: 7 },
      serverCapabilities: ['direct_peer_transport'],
    }) + '\n',
  );
  if (mode === 'events') {
    const frames = [
      {
        type: 'session_event',
        activeSessionId: 'active-1',
        event: { type: 'assistant_text', text: 'hello' },
        meta: { sequence: 1 },
      },
      {
        type: 'session_list_item',
        id: 'daemon_list',
        session: { sessionId: 'saved-1', name: 'Saved session' },
      },
      { type: 'roster_update', changed: [], removed: [], resync: true },
      {
        type: 'a_future_frame_the_tui_does_not_know',
        payload: { anything: { nested: [1, 2] } },
        meta: { sequence: 2, source: 'worker' },
      },
    ];
    for (const frame of frames) {
      socket.write(JSON.stringify(frame) + '\n');
    }
  }
  if (mode === 'flood') {
    let seq = 0;
    const timer = setInterval(() => {
      for (let i = 0; i < 64; i += 1) {
        socket.write(
          JSON.stringify({
            type: 'session_event',
            activeSessionId: 'active-1',
            event: { type: 'tick', seq: seq++ },
            meta: { sequence: seq },
          }) + '\n',
        );
      }
      if (seq > 100000) clearInterval(timer);
    }, 5);
    socket.on('close', () => clearInterval(timer));
  }
  socket.on('data', (chunk) => {
    buffer += chunk;
    let newline;
    while ((newline = buffer.indexOf('\n')) !== -1) {
      const line = buffer.slice(0, newline);
      buffer = buffer.slice(newline + 1);
      if (line.trim() === '') continue;
      const frame = JSON.parse(line);
      if (frame.type !== 'command') continue;
      const id = frame.id;
      if (mode === 'silent') continue;
      if (mode === 'die') {
        socket.destroy();
        return;
      }
      if (mode === 'refuse') {
        socket.write(
          JSON.stringify({
            type: 'response',
            id,
            command: frame.command.type,
            success: false,
            error: 'the daemon refuses this command',
            errorInfo: {
              code: 'session_already_active',
              sessionPath: '/tmp/refused-session.jsonl',
            },
          }) + '\n',
        );
        continue;
      }
      if (mode === 'echo' || mode === 'events') {
        socket.write(
          JSON.stringify({
            type: 'response',
            id,
            command: frame.command.type,
            success: true,
            data: { echoed: frame.command },
          }) + '\n',
        );
      }
      if (mode === 'flood' && arg === 'respond') {
        socket.write(
          JSON.stringify({
            type: 'response',
            id,
            command: frame.command.type,
            success: true,
            data: { ok: true },
          }) + '\n',
        );
      }
    }
  });
});
server.listen(socketPath);
