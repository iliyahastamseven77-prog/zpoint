/**
 * Zpoint GAS Relay v1 — ephemeral encrypted-frame relay on Google Apps Script.
 *
 * Property of the @ily_bio research channel (@iliyahsatam).
 *
 * PURPOSE
 *   Carrier for Zpoint when ONLY *.google.com / script.google.com is
 *   reachable.  The script is a dumb, short-lived RAM buffer:
 *     - client POSTs a batch of encrypted frames  -> stored in CacheService
 *     - exit long-polls GET ?mode=up              -> receives them
 *     - exit POSTs response batches (?dir=dn)     -> stored in CacheService
 *     - client long-polls GET ?mode=dn            -> receives them
 *   It never sees plaintext: bodies are AES-256-GCM ciphertext produced by
 *   core/crypto.py (nonce = dir-prefix + seq, AAD = stream+type).
 *
 * WIRE CONTRACT (mirrored by tests/test_gas.py::FakeGAS — keep in sync!)
 *   POST /exec?k=<token>&dir=up|dn    body: {"v":1,"c":<batchseq>,"f":[frames]}
 *                                     -> {"ok":true,"c":n} | {"ok":false,"err":...}
 *   GET  /exec?k=<token>&mode=ping    -> {"ok":true,"pong":true,"hb":...}
 *   GET  /exec?k=<token>&mode=up|dn&after=<lastbatchseq>&wait=<seconds>
 *        [&resync=1]                  -> {"ok":true,"b":[{"c":n,"f":[...]},...]}
 *     long-poll: holds up to `wait` seconds (cap MAX_WAIT_S) polling the ring
 *     every TICK_MS; &resync=1 returns immediately. after=-1 => whole ring.
 *
 * STORAGE = CacheService.getScriptCache()  (shared script-scope cache)
 *   ring of RING=64 keys per direction ("up:<n mod 64>" / "dn:<n mod 64>"),
 *   value = JSON {"c":n,"f":[...],"ts":...}, TTL 6h (entries self-GC —
 *   this REPLACES the old RTDB consumer-delete GC).
 *   meta counter per direction ("up:meta"/"dn:meta") allocated under
 *   LockService so two concurrent POSTs can never take the same slot.
 *   Ring wrap = oldest entries are overwritten; consumers detect gaps via
 *   the batch seq `c` and re-sync with after=<last c> (see core/gas.py).
 *
 * QUOTA NOTES (developers.google.com/apps-script/guides/services/quotas):
 *   - UrlFetchApp is NOT used at all => the 20k/day URL Fetch quota is
 *     irrelevant; doGet/doPost web invocations have no published daily cap.
 *   - 6 min/execution (long-poll caps at 240s), 30 simultaneous executions
 *     per user (pool uses ~2 per script), cache value 100KB/key (client
 *     seals batches at <=96KB wire), cache 1000 entries (we use ~130).
 *
 * DEPLOY (per script of the pool):
 *   1. New project -> paste this file.
 *   2. Project Settings -> Script Properties -> add ZP_TOKEN = <shared secret>
 *      (same token on every script of the pool; matches config gas_token).
 *   3. Deploy -> New deployment -> Web app:
 *        Execute as: Me        Who has access: Anyone
 *      -> copy the /exec URL into the kit config's gas_urls list.
 *   4. Repeat on other accounts/projects for more pool members.
 */
var RING = 64;                 // cache keys per direction (overwrite = evict oldest)
var TTL = 21600;               // 6h — max allowed by CacheService
var TICK_MS = 250;             // long-poll wake-up granularity
var MAX_WAIT_S = 240;          // keep executions well under the 6-min cap
var MAX_FRAMES = 128;          // defensive cap on frames per batch
// Hardening (v1.1): hard ceiling for ONE doGet execution.  Quotas are
// 6 min/execution (consumer) and ~30 min total/day for ALL web-app
// executions on personal accounts.  The poller therefore always asks for
// wait <= MAX_WAIT_S (240s), but clock skew / slow ticks must never let a
// single invocation push past this ceiling: doGet stops early and answers
// cleanly ({"ok":true,"b":[]}), so the client's next long-poll continues
// the work in a FRESH execution instead of the script being killed by
// Google mid-response ("script function unexpectedly finished", unhandled
// execution error on the dashboard).
var EXEC_DEADLINE_MS = 255000; // 255s < 240s wait + reserve < 6-min quota

function zpToken() {
  var p = PropertiesService.getScriptProperties().getProperty('ZP_TOKEN');
  return p || 'REPLACE_WITH_TOKEN';
}

function jsonOut(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
      .setMimeType(ContentService.MimeType.JSON);
}

function authed(e) {
  var p = (e && e.parameter) || {};
  return typeof p.k === 'string' && p.k === zpToken();
}

// ---------- ring storage ----------

function metaN(dir) {
  var raw = CacheService.getScriptCache().get(dir + ':meta');
  if (!raw) return 0;
  try { return Math.max(0, parseInt(JSON.parse(raw).n, 10) || 0); }
  catch (err) { return 0; }
}

function metaPut(dir, n) {
  CacheService.getScriptCache().put(dir + ':meta', JSON.stringify({ n: n }), TTL);
}

/** Store one batch; returns {"ok":true,"c":n} or {"ok":false,"err":...}. */
function storeBatch(dir, batch) {
  var lock = LockService.getScriptLock();
  if (!lock.tryLock(8000)) return { ok: false, err: 'busy' };
  try {
    var n = metaN(dir);
    var val = JSON.stringify({ c: n, f: batch.f, ts: Date.now() });
    if (val.length > 100 * 1024) return { ok: false, err: 'too_big' };
    CacheService.getScriptCache().put(dir + ':' + (n % RING), val, TTL);
    metaPut(dir, n + 1);
    return { ok: true, c: n };
  } catch (err) {
    return { ok: false, err: 'store:' + err };
  } finally {
    lock.releaseLock();
  }
}

/** All readable batches with c > after (stale wrapped slots are filtered). */
function readSince(dir, after) {
  var n = metaN(dir);
  var lo = Math.max(after + 1, n - RING);   // evicted-window guard
  var cache = CacheService.getScriptCache();
  var out = [];
  for (var x = lo; x < n; x++) {
    var raw = cache.get(dir + ':' + (x % RING));
    if (!raw) continue;
    var b;
    try { b = JSON.parse(raw); } catch (err) { continue; }
    if (b && typeof b.c === 'number' && b.c >= after + 1) out.push({ c: b.c, f: b.f });
  }
  return out;
}

// ---------- entry points ----------

function doPost(e) {
  try {
    if (!authed(e)) return jsonOut({ ok: false, err: 'auth' });
    var p = (e && e.parameter) || {};
    // Parse body once (relay batches AND simple-test endpoints)
    var body = null;
    try { body = JSON.parse(e.postData.contents); } catch (err) {}

    // === SIMPLE HANDSHAKE (plain-JSON probe for Windows-client testing;
    //     additive — the Android relay protocol is untouched) ===
    if (body && body.action === 'handshake') {
      return jsonOut({
        ok: true,
        proxyHost: '127.0.0.1',   // client-side SOCKS5 loopback (Zpoint model)
        proxyPort: 1080,          // desktop default listen port
        selectedServer: body.server || '',
        servers: ['SE', 'DE', 'FR']
      });
    }

    // === EXISTING RELAY LOGIC (Android/Python clients) ===
    var dir = p.dir === 'dn' ? 'dn' : 'up';
    if (!body || !Array.isArray(body.f) || body.f.length === 0 ||
        body.f.length > MAX_FRAMES) {
      return jsonOut({ ok: false, err: 'badbatch' });
    }
    return jsonOut(storeBatch(dir, body));
  } catch (err) {
    return jsonOut({ ok: false, err: 'post:' + err });
  }
}

function doGet(e) {
  // Hardening (v1.1): start the execution clock FIRST — a long-poll must
  // never outlive EXEC_DEADLINE_MS even if the client asked for more.
  var execStart = Date.now();
  try {
    if (!authed(e)) return jsonOut({ ok: false, err: 'auth' });
    var p = (e && e.parameter) || {};
    var mode = p.mode || 'ping';
    if (mode === 'ping') return jsonOut({ ok: true, pong: true, hb: Date.now() });
    if (mode !== 'up' && mode !== 'dn') return jsonOut({ ok: false, err: 'mode' });

    var after = parseInt(p.after, 10);
    if (isNaN(after)) after = -1;

    if (p.resync) return jsonOut({ ok: true, b: readSince(mode, after) });

    var waitS = parseInt(p.wait, 10);
    if (isNaN(waitS)) waitS = 0;
    waitS = Math.min(Math.max(waitS, 0), MAX_WAIT_S);

    var out = readSince(mode, after);
    if (out.length === 0) {
      // Hardening (v1.1): never hold this execution past its ceiling.
      // - deadline: client's wait, but clipped so that the TOTAL execution
      //   time stops at EXEC_DEADLINE_MS (trailing reserve covers auth,
      //   reads and response serialization).  A wait clamped to <= 0
      //   answers immediately — a single-shot read, never a sleep.
      var waitMs = waitS * 1000;
      var leftMs = EXEC_DEADLINE_MS - (Date.now() - execStart);
      var deadline = Date.now() + Math.max(0, Math.min(waitMs - 400, leftMs));
      while (out.length === 0 && Date.now() < deadline) {
        Utilities.sleep(Math.min(TICK_MS, deadline - Date.now()));
        out = readSince(mode, after);
        if (out.length !== 0) break;
        // graceful stop: answering empty now is ALWAYS clean — the next
        // long-poll of the same client continues from its `after` cursor
        // in a fresh execution.  Google never sees this script die.
        if (Date.now() - execStart >= EXEC_DEADLINE_MS) break;
      }
    }
    return jsonOut({ ok: true, b: out });
  } catch (err) {
    return jsonOut({ ok: false, err: 'get:' + err });
  }
}
