const KEY = "shared-state";
const ACTIONS_KEY = "recent-actions";
const encoder = new TextEncoder();

const json = (value, status = 200) =>
  new Response(JSON.stringify(value), {
    status,
    headers: { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" },
  });

function sse(value) {
  return encoder.encode(`data: ${JSON.stringify(value)}\n\n`);
}

const normalize = value => String(value || "").toLowerCase().trim().replace(/^\d+\s*/, "");
const numericQty = qty => Math.max(1, Number.parseInt(qty, 10) || 1);

function ensureShape(value) {
  const state = value && Array.isArray(value.items) && Array.isArray(value.history) ? value : {
    items: [], history: [], trips: 0, selectedStore: "morrisons", prices: { morrisons: {}, asda: {} }
  };
  state.items ||= [];
  state.history ||= [];
  state.trips ||= 0;
  state.selectedStore ||= "morrisons";
  state.prices ||= { morrisons: {}, asda: {} };
  state.prices.morrisons ||= {};
  state.prices.asda ||= {};
  state.priceSources ||= { morrisons: {}, asda: {} };
  state.priceSources.morrisons ||= {};
  state.priceSources.asda ||= {};
  state.productCatalog ||= { morrisons: {} };
  state.productCatalog.morrisons ||= {};
  state.productSelections ||= { morrisons: {} };
  state.productSelections.morrisons ||= {};
  return state;
}

function mergePriceData(state, patch = {}) {
  if (patch.prices) state.prices = patch.prices;
  if (patch.priceSources) state.priceSources = patch.priceSources;
  if (patch.productCatalog) state.productCatalog = patch.productCatalog;
  if (patch.productSelections) state.productSelections = patch.productSelections;
  if (patch.lastMorrisonsRefresh) state.lastMorrisonsRefresh = patch.lastMorrisonsRefresh;
  if (patch.lastMorrisonsError) state.lastMorrisonsError = patch.lastMorrisonsError;
  if (patch.lastMorrisonsError === null) delete state.lastMorrisonsError;
  ensureShape(state);
}

function applyAction(state, action) {
  state = ensureShape(state);
  const type = action?.type;
  const p = action?.payload || {};
  if (p.state && Array.isArray(p.state.items) && Array.isArray(p.state.history)) {
    return ensureShape(p.state);
  }
  if (type === "initState") {
    return ensureShape(p.state || state);
  }
  if (type === "addItem") {
    mergePriceData(state, p.pricePatch);
    const key = p.key || normalize(p.name || p.item?.name);
    const amount = Math.max(1, Number(p.amount) || numericQty(p.item?.qty));
    const existing = state.items.find(item => !item.done && normalize(item.name) === key);
    if (existing) {
      existing.qty = String(numericQty(existing.qty) + amount);
      if (p.item?.category) existing.category = p.item.category;
    } else if (p.item?.id && p.item?.name) {
      state.items.unshift({ ...p.item, done: Boolean(p.item.done) });
    }
    return state;
  }
  if (type === "setDone") {
    const item = state.items.find(item => item.id === p.id);
    if (item) item.done = Boolean(p.done);
    return state;
  }
  if (type === "setQty") {
    const item = state.items.find(item => item.id === p.id);
    if (item) item.qty = String(Math.max(1, numericQty(p.qty)));
    return state;
  }
  if (type === "deleteItem") {
    state.items = state.items.filter(item => item.id !== p.id);
    return state;
  }
  if (type === "clearBought") {
    const ids = new Set(Array.isArray(p.ids) ? p.ids : []);
    state.items = state.items.filter(item => !(item.done || ids.has(item.id)));
    return state;
  }
  if (type === "completeQuest") {
    const now = Number(p.now) || Date.now();
    const ids = new Set(Array.isArray(p.ids) ? p.ids : []);
    const bought = state.items.filter(item => item.done || ids.has(item.id));
    for (const item of bought) {
      const key = normalize(item.name);
      const source = state.priceSources?.[state.selectedStore]?.[key];
      const price = state.prices?.[state.selectedStore]?.[key] ?? null;
      state.history.push({ name: item.name, boughtAt: now, store: state.selectedStore, price, productName: source?.productName || null });
    }
    state.items = state.items.filter(item => !(item.done || ids.has(item.id)));
    state.trips = (Number(state.trips) || 0) + 1;
    return state;
  }
  if (type === "setSelectedStore") {
    state.selectedStore = p.selectedStore || state.selectedStore || "morrisons";
    return state;
  }
  if (type === "mergePriceData") {
    mergePriceData(state, p);
    return state;
  }
  if (type === "setProductSelection") {
    mergePriceData(state, p);
    return state;
  }
  if (type === "setManualPrice") {
    state.prices ||= { morrisons: {}, asda: {} };
    state.prices[p.store || state.selectedStore || "morrisons"] ||= {};
    state.prices[p.store || state.selectedStore || "morrisons"][p.key] = Number(p.price);
    if (state.priceSources?.[p.store || state.selectedStore]?.[p.key]) delete state.priceSources[p.store || state.selectedStore][p.key];
    mergePriceData(state, p);
    return state;
  }
  return state;
}

export class BasketRoom {
  constructor(state, env) {
    this.state = state;
    this.env = env;
    this.clients = new Map();
    this.sockets = new Set();
  }

  async readShared() {
    return (await this.state.storage.get(KEY)) || { revision: 0, state: null };
  }

  async writeShared(value) {
    await this.state.storage.put(KEY, value);
  }

  async isDuplicate(actionId) {
    if (!actionId) return false;
    const recent = (await this.state.storage.get(ACTIONS_KEY)) || [];
    if (recent.includes(actionId)) return true;
    recent.push(actionId);
    while (recent.length > 200) recent.shift();
    await this.state.storage.put(ACTIONS_KEY, recent);
    return false;
  }

  async broadcast(value) {
    const payload = sse(value);
    await Promise.all([...this.clients].map(async ([id, writer]) => {
      try { await writer.write(payload); }
      catch { this.clients.delete(id); }
    }));
    const message = JSON.stringify(value);
    for (const socket of [...this.sockets]) {
      try { socket.send(message); }
      catch { this.sockets.delete(socket); }
    }
  }

  async handleSocket(request) {
    if (request.headers.get("Upgrade") !== "websocket") return json({ error: "Expected websocket" }, 426);
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    server.accept();
    this.sockets.add(server);
    const current = await this.readShared();
    server.send(JSON.stringify({ ...current, connected: true }));
    server.addEventListener("message", async (event) => {
      try {
        const message = JSON.parse(event.data || "{}");
        if (message.type !== "replaceState" || !message.state) return;
        const response = await this.replaceState(message);
        const result = await response.json();
        server.send(JSON.stringify({ ack: true, revision: result.revision, clientMutation: message.clientMutation || 0 }));
      } catch (error) {
        try { server.send(JSON.stringify({ error: error.message })); } catch {}
      }
    });
    server.addEventListener("close", () => this.sockets.delete(server));
    server.addEventListener("error", () => this.sockets.delete(server));
    return new Response(null, { status: 101, webSocket: client });
  }

  async handleEvents(request) {
    const { readable, writable } = new TransformStream();
    const writer = writable.getWriter();
    const id = crypto.randomUUID();
    this.clients.set(id, writer);
    const current = await this.readShared();
    await writer.write(encoder.encode(`retry: 800\n`));
    await writer.write(sse({ connected: true, revision: current.revision || 0, updatedAt: current.updatedAt || Date.now(), state: current.state || null }));
    request.signal.addEventListener("abort", () => {
      this.clients.delete(id);
      try { writer.close(); } catch {}
    });
    return new Response(readable, { headers: { "content-type": "text/event-stream; charset=utf-8", "cache-control": "no-cache, no-transform", "x-accel-buffering": "no" } });
  }

  async commit(action) {
    const current = await this.readShared();
    if (await this.isDuplicate(action.actionId)) return current;
    const baseState = current.state || action.payload?.state || null;
    const nextState = applyAction(baseState, action);
    const next = { revision: (Number(current.revision) || 0) + 1, updatedAt: Date.now(), state: nextState, lastAction: { type: action.type, actionId: action.actionId, clientId: action.clientId, createdAt: action.createdAt || Date.now() } };
    await this.writeShared(next);
    await this.broadcast(next);
    return next;
  }

  async replaceState(body = {}) {
    if (!body.state || !Array.isArray(body.state.items) || !Array.isArray(body.state.history)) {
      return json({ error: "Invalid shared state" }, 400);
    }
    const current = await this.readShared();
    const next = {
      revision: (Number(current.revision) || 0) + 1,
      updatedAt: Date.now(),
      state: ensureShape(body.state),
      lastAction: { type: "replaceState", clientId: body.clientId || null, clientMutation: body.clientMutation || 0, createdAt: body.updatedAt || Date.now() },
    };
    await this.writeShared(next);
    await this.broadcast(next);
    return json(next);
  }

  async fetch(request) {
    const url = new URL(request.url);
    if (request.method === "GET" && url.pathname === "/api/sync") return this.handleSocket(request);
    if (request.method === "GET" && url.pathname === "/api/state/events") return this.handleEvents(request);
    if (request.method === "GET" && url.pathname === "/api/state") return json(await this.readShared());
    if (request.method === "POST" && url.pathname === "/api/action") {
      try {
        const action = await request.json();
        if (!action || !action.type) return json({ error: "Missing action type" }, 400);
        return json(await this.commit(action));
      } catch (error) { return json({ error: error.message }, 500); }
    }
    if (request.method === "PUT" && url.pathname === "/api/state") {
      try {
        return this.replaceState(await request.json());
      } catch (error) { return json({ error: error.message }, 500); }
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
