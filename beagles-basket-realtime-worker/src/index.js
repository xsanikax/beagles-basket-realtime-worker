const KEY = "shared-state";
const encoder = new TextEncoder();

const json = (value, status = 200) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });

function sse(value) {
  return encoder.encode(`data: ${JSON.stringify(value)}\n\n`);
}

export class BasketRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Map();
  }

  async readShared() {
    return (await this.state.storage.get(KEY)) || { revision: 0, state: null };
  }

  async writeShared(value) {
    await this.state.storage.put(KEY, value);
  }

  async broadcast(value) {
    const payload = sse(value);
    await Promise.all([...this.clients].map(async ([id, writer]) => {
      try {
        await writer.write(payload);
      } catch {
        this.clients.delete(id);
      }
    }));
  }

  async handleEvents(request) {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const id = crypto.randomUUID();
    this.clients.set(id, writer);

    const current = await this.readShared();
    await writer.write(encoder.encode(`retry: 1000\n`));
    await writer.write(sse({ connected: true, revision: current.revision || 0, updatedAt: current.updatedAt || Date.now() }));

    request.signal.addEventListener("abort", () => {
      this.clients.delete(id);
      try { writer.close(); } catch {}
    });

    return new Response(readable, {
      headers: {
        "content-type": "text/event-stream; charset=utf-8",
        "cache-control": "no-cache, no-transform",
        "x-accel-buffering": "no",
      },
    });
  }

  async fetch(request) {
    const url = new URL(request.url);

    if (request.method === "GET" && url.pathname === "/api/state/events") {
      return this.handleEvents(request);
    }

    if (request.method === "GET" && url.pathname === "/api/state") {
      return json(await this.readShared());
    }

    if (request.method === "PUT" && url.pathname === "/api/state") {
      try {
        const body = await request.json();
        if (!body.state || !Array.isArray(body.state.items) || !Array.isArray(body.state.history)) {
          return json({ error: "Invalid shared state" }, 400);
        }

        const current = await this.readShared();
        const next = { revision: (current?.revision || 0) + 1, updatedAt: Date.now(), state: body.state };
        await this.writeShared(next);
        await this.broadcast({ revision: next.revision, updatedAt: next.updatedAt });
        return json({ revision: next.revision, updatedAt: next.updatedAt, realtime: true });
      } catch (error) {
        return json({ error: error.message }, 500);
      }
    }

    return json({ error: "Not found" }, 404);
  }
}

export default {
  async fetch(request, env) {
    const id = env.BASKET_ROOM.idFromName("household");
    return env.BASKET_ROOM.get(id).fetch(request);
  },
};
