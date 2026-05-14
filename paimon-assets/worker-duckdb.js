// worker-duckdb.js — Custom DuckDB+Paimon worker
// Uses our duckdb_wasm.js (software exceptions) directly.
// Implements the old web-shell protocol:
//   IN:  { op, tag, ...params }
//   OUT: { tag, status, data?, error?, duration? }
'use strict';

// Set up DUCKDB_RUNTIME before loading duckdb_wasm.js.
// For BROWSER_BUFFER (protocol 0) files: the WASM manages data internally;
// JS callbacks (openFile, readFile, etc.) are no-ops.
globalThis.DUCKDB_RUNTIME = {
    // Protocol 0 = BROWSER_BUFFER (in-memory). WASM handles I/O internally.
    getDefaultDataProtocol: () => 0,
    openFile: (mod, fileId, flags) => 0,
    closeFile: (mod, fileId) => {},
    readFile: (mod, fileId, buf, size, offset) => 0,
    writeFile: (mod, fileId, buf, size, offset) => 0,
    truncateFile: (mod, fileId, newSize) => {},
    checkFile: (mod, fileId, path) => false,
    dropFile: (mod, fileId, path) => {},
    moveFile: (mod, fromId, fromPath, toId, toPath) => true,
    glob: (mod, path, outBuf) => {},
    syncFile: (mod, fileId) => {},
    removeFile: (mod, fileId, path) => {},
    checkDirectory: (mod, path, opts) => false,
    createDirectory: (mod, path, opts) => {},
    removeDirectory: (mod, path, opts) => {},
    listDirectoryEntries: (mod, path, outBuf) => false,
    testPlatformFeature: (mod, feature) => {
        if (feature === 1) return typeof BigInt64Array !== 'undefined';
        return false;
    },
    callScalarUDF: () => {},
};

// Patch aborting syscall stubs before duckdb_wasm.js loads them.
// duckdb_wasm.js is compiled with !SYSCALLS_REQUIRE_FILESYSTEM so these call abort().
// We intercept WebAssembly.instantiate* and replace them with ENOSYS (-1) stubs.
(function patchSyscallStubs() {
    const ABORTING_SYSCALLS = [
        '__syscall_faccessat', '__syscall_fstat64', '__syscall_ftruncate64',
        '__syscall_getcwd', '__syscall_getdents64', '__syscall_lstat64',
        '__syscall_mkdirat', '__syscall_newfstatat', '__syscall_openat',
        '__syscall_poll', '__syscall_readlinkat', '__syscall_renameat',
        '__syscall_rmdir', '__syscall_sendto', '__syscall_socket',
        '__syscall_stat64', '__syscall_statfs64', '__syscall_symlinkat',
        '__syscall_unlinkat',
        // WASI fd functions (same wasmImports object, imported as fd_* not __syscall_*)
        'fd_close', 'fd_pread', 'fd_pwrite', 'fd_read', 'fd_sync',
    ];
    function patchEnv(imports) {
        if (!imports?.env) return;
        for (const name of ABORTING_SYSCALLS) {
            if (name in imports.env) imports.env[name] = () => -1;
        }
    }
    const origStreaming = WebAssembly.instantiateStreaming;
    WebAssembly.instantiateStreaming = async (src, imports) => {
        patchEnv(imports);
        return origStreaming(src, imports);
    };
    const origInstantiate = WebAssembly.instantiate;
    WebAssembly.instantiate = async (bufferOrModule, imports) => {
        patchEnv(imports);
        return origInstantiate(bufferOrModule, imports);
    };
})();

// Load duckdb_wasm.js which exports globalThis.DuckDB (async factory function)
importScripts('./duckdb_wasm.js');

let Se = null;  // DuckDB module instance (Emscripten Module)
let Mt = 0;     // Current connection ID

// Initialize once on first 'instantiate' message
let _dbInitPromise = null;
function ensureInit() {
    if (!_dbInitPromise) {
        _dbInitPromise = globalThis.DuckDB({
            locateFile: name => './' + name,
            print: () => {},
            printErr: msg => { if (msg && !msg.includes('wasm streaming')) console.warn('[duckdb]', msg); },
        }).then(mod => { Se = mod; });
    }
    return _dbInitPromise;
}

// ── Helpers (mirrors worker.js from duckdb-web-shell) ─────────────────────────

// Call a C function with a 3×f64 output parameter (status, ptr, size).
function sn(mod, name, types, args) {
    const sp = mod.stackSave();
    const out = mod.stackAlloc(3 * 8);
    types.unshift('number');
    args.unshift(out);
    mod.ccall(name, null, types, args);
    const f = mod.HEAPF64;
    const o = out >> 3;
    const status = f[o], ptr = f[o + 1], size = f[o + 2];
    mod.stackRestore(sp);
    return [status, ptr, size];
}

// Copy WASM memory to a new Uint8Array.
function Jc(mod, ptr, size) {
    const src = mod.HEAPU8.subarray(ptr, ptr + size);
    const dst = new Uint8Array(new ArrayBuffer(src.byteLength));
    dst.set(src);
    return dst;
}

// Clear the DuckDB response buffer.
function Ir(mod) {
    mod.ccall('duckdb_web_clear_response', null, [], []);
}

// Post a response to the main thread.
function Ce(tag, status, data, error, duration) {
    const msg = { tag, status };
    if (duration !== undefined) msg.duration = duration;
    if (data) { msg.data = data; globalThis.postMessage(msg, [data.buffer]); }
    else { if (error) msg.error = error; globalThis.postMessage(msg); }
}

// Call sn, capture output, clear response buffer.
function Bn(name, types, args) {
    const t0 = performance.now();
    const [, ptr, size] = sn(Se, name, types, args);
    const data = size > 0 ? Jc(Se, ptr, size) : null;
    Ir(Se);
    return { data, duration: performance.now() - t0 };
}

// ── Message handler ────────────────────────────────────────────────────────────
async function Md(o) {
    const { op: e, tag: t } = o;
    try {
        switch (e) {
            case 'instantiate': {
                await ensureInit();
                Ce(t, 0, null);
                break;
            }
            case 'open': {
                sn(Se, 'duckdb_web_open', ['string'], [o.config || '']);
                Ir(Se);
                Ce(t, 0, null);
                break;
            }
            case 'connect': {
                Mt = Se.ccall('duckdb_web_connect', 'number', [], []);
                Ce(t, 0, null);
                break;
            }
            case 'disconnect': {
                Se.ccall('duckdb_web_disconnect', null, ['number'], [Mt]);
                Mt = 0;
                Ce(t, 0, null);
                break;
            }
            case 'query': {
                let dur = 0;
                const r = Bn(
                    'duckdb_web_experimental_query_start',
                    ['number', 'string', 'number'],
                    [Mt, o.sql, o.castMode ?? 0],
                );
                dur += r.duration;
                if (r.data) { Ce(t, 0, r.data, undefined, dur); break; }
                // Poll loop via MessageChannel to avoid blocking
                const ch = new MessageChannel();
                ch.port1.onmessage = () => {
                    const p = Bn('duckdb_web_experimental_query_poll', ['number'], [Mt]);
                    dur += p.duration;
                    if (p.data) { Ce(t, 0, p.data, undefined, dur); ch.port1.close(); }
                    else ch.port2.postMessage(null);
                };
                ch.port2.postMessage(null);
                break;
            }
            case 'send_query': {
                const r = Bn(
                    'duckdb_web_experimental_send_query',
                    ['number', 'string', 'number'],
                    [Mt, o.sql, o.castMode ?? 0],
                );
                Ce(t, 0, r.data, undefined, r.duration);
                break;
            }
            case 'poll_pending_query': {
                const r = Bn('duckdb_web_experimental_poll_pending_query', ['number'], [Mt]);
                Ce(t, 0, r.data, undefined, r.duration);
                break;
            }
            case 'fetch': {
                const r = Bn('duckdb_web_experimental_fetch', ['number'], [Mt]);
                Ce(t, 0, r.data, undefined, r.duration);
                break;
            }
            case 'fetch_chunk_at': {
                const r = Bn(
                    'duckdb_web_experimental_fetch_chunk_at',
                    ['number', 'number'],
                    [Mt, o.chunkIdx ?? 0],
                );
                Ce(t, 0, r.data, undefined, r.duration);
                break;
            }
            case 'interrupt': {
                Se.ccall('duckdb_web_experimental_interrupt', null, ['number'], [Mt]);
                Ce(t, 0, null);
                break;
            }
            case 'clear_interrupt': {
                Se.ccall('duckdb_web_experimental_clear_interrupt', null, ['number'], [Mt]);
                Ce(t, 0, null);
                break;
            }
            case 'register_file': {
                const buf = o.buffer;
                const ptr = Se._malloc(buf.length);
                Se.HEAPU8.set(buf, ptr);
                sn(Se, 'duckdb_web_fs_register_file_buffer',
                    ['string', 'number', 'number'],
                    [o.fileName, ptr, buf.length]);
                // Do NOT free ptr here: C++ takes ownership of this malloc'd buffer
                // via unique_ptr<char[]> inside duckdb_web_fs_register_file_buffer.
                // Freeing it here causes a double-free and corrupts file data in DuckDB's
                // WebFileSystem (read_csv etc.). PaimonWasmFileSystem makes its own copy.
                Ir(Se);
                Ce(t, 0, null);
                break;
            }
            case 'list_files': {
                const [, ptr, size] = sn(Se, 'duckdb_web_fs_glob_file_infos', ['string'], ['*']);
                const json = new TextDecoder().decode(Se.HEAPU8.subarray(ptr, ptr + size));
                Ir(Se);
                Ce(t, 0, new TextEncoder().encode(json));
                break;
            }
            case 'download_file': {
                const [status, ptr, size] = sn(Se, 'duckdb_web_copy_file_to_buffer', ['string'], [o.fileName]);
                if (status !== 0) {
                    const err = size > 0 ? new TextDecoder().decode(Se.HEAPU8.subarray(ptr, ptr + size)) : 'unknown error';
                    Ir(Se); Ce(t, 1, null, err);
                } else {
                    const data = size > 0 ? Jc(Se, ptr, size) : null;
                    Ir(Se); Ce(t, 0, data);
                }
                break;
            }
            case 'drop_file': {
                sn(Se, 'duckdb_web_fs_drop_file', ['string'], [o.fileName]);
                Ir(Se);
                Ce(t, 0, null);
                break;
            }
            case 'drop_files': {
                sn(Se, 'duckdb_web_fs_drop_files', [], []);
                Ir(Se);
                Ce(t, 0, null);
                break;
            }
            default:
                Ce(t, 1, null, 'unknown op: ' + e);
        }
    } catch (err) {
        Ce(t, 1, null, err.message || String(err));
    }
}

globalThis.onmessage = async o => { await Md(o.data); };
