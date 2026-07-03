// Browser-direct live quote stream for the Live page.
//
// Connects straight from the user's browser to Yahoo's public streaming
// WebSocket (WebSockets are not subject to CORS, and residential IPs are
// served happily — unlike datacenter relays, which get rate-limited). No
// backend, no serverless proxy: the HF Space sees none of this traffic.
//
// Frames arrive as base64-encoded protobuf `PricingData` messages (v2 wraps
// them in a small JSON envelope). The decoder below is a minimal hand-rolled
// protobuf reader for the fields the page needs — field numbers are stable
// and match the widely-documented PricingData schema.

export type LiveTick = {
  symbol: string; // NSE symbol, ".NS" stripped
  price: number | null;
  changePercent: number | null;
  dayVolume: number | null;
  dayHigh: number | null;
  dayLow: number | null;
  previousClose: number | null;
  time: number | null; // ms epoch
  marketHours: number | null; // 1 = regular market
};

export type LiveFeedStatus = "connecting" | "open" | "closed" | "error";

const STREAM_URL = "wss://streamer.finance.yahoo.com/?version=2";

// ── minimal protobuf reader ─────────────────────────────────────────────────

class Reader {
  private view: DataView;
  private pos = 0;
  constructor(private bytes: Uint8Array) {
    this.view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  }
  eof(): boolean {
    return this.pos >= this.bytes.length;
  }
  varint(): number {
    let result = 0;
    let shift = 0;
    for (;;) {
      const b = this.bytes[this.pos++];
      result += (b & 0x7f) * Math.pow(2, shift);
      if ((b & 0x80) === 0) return result;
      shift += 7;
      if (shift > 63) throw new Error("varint too long");
    }
  }
  zigzag(): number {
    const n = this.varint();
    return n % 2 === 0 ? n / 2 : -(n + 1) / 2;
  }
  float32(): number {
    const v = this.view.getFloat32(this.pos, true);
    this.pos += 4;
    return v;
  }
  fixed64skip(): void {
    this.pos += 8;
  }
  bytesField(): Uint8Array {
    const len = this.varint();
    const out = this.bytes.subarray(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }
  skip(wireType: number): void {
    if (wireType === 0) this.varint();
    else if (wireType === 1) this.fixed64skip();
    else if (wireType === 2) this.bytesField();
    else if (wireType === 5) this.pos += 4;
    else throw new Error("unsupported wire type " + wireType);
  }
}

const textDecoder = new TextDecoder();

function decodePricingData(bytes: Uint8Array): LiveTick | null {
  const r = new Reader(bytes);
  const tick: LiveTick = {
    symbol: "",
    price: null,
    changePercent: null,
    dayVolume: null,
    dayHigh: null,
    dayLow: null,
    previousClose: null,
    time: null,
    marketHours: null,
  };
  try {
    while (!r.eof()) {
      const tag = r.varint();
      const field = tag >>> 3;
      const wire = tag & 7;
      if (field === 1 && wire === 2) tick.symbol = textDecoder.decode(r.bytesField());
      else if (field === 2 && wire === 5) tick.price = r.float32();
      else if (field === 3 && wire === 0) tick.time = r.zigzag();
      else if (field === 8 && wire === 5) tick.changePercent = r.float32();
      else if (field === 9 && wire === 0) tick.dayVolume = r.zigzag();
      else if (field === 10 && wire === 5) tick.dayHigh = r.float32();
      else if (field === 11 && wire === 5) tick.dayLow = r.float32();
      else if (field === 12 && wire === 0) tick.marketHours = r.varint();
      else if (field === 16 && wire === 5) tick.previousClose = r.float32();
      else r.skip(wire);
    }
  } catch {
    return null; // torn frame — drop it
  }
  if (!tick.symbol) return null;
  tick.symbol = tick.symbol.replace(/\.(NS|BO)$/, "");
  return tick;
}

function base64ToBytes(b64: string): Uint8Array {
  const bin = atob(b64);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ── feed client ─────────────────────────────────────────────────────────────

export class LiveFeed {
  private ws: WebSocket | null = null;
  private symbols: string[] = [];
  private reconnectDelay = 2000;
  private reconnectTimer: number | null = null;
  private closedByUser = false;

  constructor(
    private onTick: (tick: LiveTick) => void,
    private onStatus: (status: LiveFeedStatus) => void,
  ) {}

  start(symbols: string[]): void {
    this.closedByUser = false;
    this.symbols = symbols;
    this.connect();
  }

  setSymbols(symbols: string[]): void {
    const prev = new Set(this.symbols);
    this.symbols = symbols;
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      const added = symbols.filter((s) => !prev.has(s));
      const removed = [...prev].filter((s) => !symbols.includes(s));
      if (removed.length) this.ws.send(JSON.stringify({ unsubscribe: removed.map(tick2yahoo) }));
      if (added.length) this.ws.send(JSON.stringify({ subscribe: added.map(tick2yahoo) }));
    }
  }

  stop(): void {
    this.closedByUser = true;
    if (this.reconnectTimer !== null) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.ws?.close();
    this.ws = null;
    this.onStatus("closed");
  }

  private connect(): void {
    if (this.closedByUser) return;
    this.onStatus("connecting");
    let ws: WebSocket;
    try {
      ws = new WebSocket(STREAM_URL);
    } catch {
      this.scheduleReconnect();
      return;
    }
    this.ws = ws;

    ws.onopen = () => {
      this.reconnectDelay = 2000;
      this.onStatus("open");
      if (this.symbols.length) {
        ws.send(JSON.stringify({ subscribe: this.symbols.map(tick2yahoo) }));
      }
    };
    ws.onmessage = (event) => {
      const data = String(event.data || "");
      let b64 = data;
      if (data.startsWith("{")) {
        try {
          const doc = JSON.parse(data);
          b64 = String(doc.message || "");
        } catch {
          return;
        }
      }
      if (!b64) return;
      try {
        const tick = decodePricingData(base64ToBytes(b64));
        if (tick) this.onTick(tick);
      } catch {
        // ignore undecodable frames
      }
    };
    ws.onerror = () => {
      this.onStatus("error");
    };
    ws.onclose = () => {
      this.ws = null;
      if (!this.closedByUser) this.scheduleReconnect();
    };
  }

  private scheduleReconnect(): void {
    if (this.closedByUser) return;
    this.onStatus("connecting");
    if (this.reconnectTimer !== null) window.clearTimeout(this.reconnectTimer);
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null;
      this.connect();
    }, this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 60_000);
  }
}

function tick2yahoo(symbol: string): string {
  // Non-NSE special instruments (crypto pairs, FX, indices) pass through
  // untouched; bare equity symbols get the NSE suffix.
  if (symbol.includes(".") || symbol.includes("-") || symbol.includes("=") || symbol.startsWith("^")) {
    return symbol;
  }
  return `${symbol}.NS`;
}
