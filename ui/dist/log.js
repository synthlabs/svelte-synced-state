import { getDefaultClient } from './client.js';
import { LogLevel } from './protocol.js';
const consoleMethods = ['trace', 'debug', 'info', 'log', 'warn', 'error'];
const consoleLevels = {
    trace: LogLevel.Trace,
    debug: LogLevel.Debug,
    info: LogLevel.Info,
    log: LogLevel.Info,
    warn: LogLevel.Warn,
    error: LogLevel.Error
};
const levelMethods = {
    trace: 'trace',
    debug: 'debug',
    info: 'info',
    warn: 'warn',
    error: 'error'
};
let baselineConsole;
let activeOwner;
let activeForwarder;
let patched = false;
export function createLogger(options = {}) {
    captureBaselineConsole();
    const owner = Symbol('svelte-synced-state logger');
    const scope = options.scope ?? 'app';
    const client = options.client ?? getDefaultClient(options);
    const emit = (level, message, scopeOverride) => {
        client.log({
            level,
            message,
            timestamp: new Date().toISOString(),
            scope: scopeOverride ?? scope
        });
    };
    const print = (levelName, args) => {
        printBaseline(levelMethods[levelName], args);
    };
    const logger = {
        trace(message, scopeOverride) {
            print('trace', [message]);
            emit(LogLevel.Trace, message, scopeOverride);
        },
        debug(message, scopeOverride) {
            print('debug', [message]);
            emit(LogLevel.Debug, message, scopeOverride);
        },
        info(message, scopeOverride) {
            print('info', [message]);
            emit(LogLevel.Info, message, scopeOverride);
        },
        warn(message, scopeOverride) {
            print('warn', [message]);
            emit(LogLevel.Warn, message, scopeOverride);
        },
        error(message, scopeOverride) {
            print('error', [message]);
            emit(LogLevel.Error, message, scopeOverride);
        },
        forwardConsole() {
            installConsolePatch();
            activeOwner = owner;
            activeForwarder = (method, args) => {
                emit(consoleLevels[method], stringifyArgs(args));
            };
            return () => {
                if (activeOwner !== owner) {
                    return;
                }
                activeOwner = undefined;
                activeForwarder = undefined;
                restoreConsolePatch();
            };
        }
    };
    return logger;
}
function captureBaselineConsole() {
    if (baselineConsole) {
        return;
    }
    const currentConsole = globalThis.console;
    const fallback = currentConsole?.log?.bind(currentConsole) ?? (() => { });
    baselineConsole = Object.fromEntries(consoleMethods.map((method) => {
        const fn = currentConsole?.[method]?.bind(currentConsole) ?? fallback;
        return [method, fn];
    }));
}
function installConsolePatch() {
    if (patched) {
        return;
    }
    captureBaselineConsole();
    for (const method of consoleMethods) {
        globalThis.console[method] = (...args) => {
            printBaseline(method, args);
            activeForwarder?.(method, args);
        };
    }
    patched = true;
}
function restoreConsolePatch() {
    if (!patched || activeOwner) {
        return;
    }
    const baseline = baselineConsole;
    if (!baseline) {
        return;
    }
    for (const method of consoleMethods) {
        globalThis.console[method] = baseline[method];
    }
    patched = false;
}
function printBaseline(method, args) {
    captureBaselineConsole();
    baselineConsole?.[method](...args);
}
function stringifyArgs(args) {
    return args.map(stringifyValue).join(' ');
}
function stringifyValue(value) {
    if (typeof value === 'string') {
        return value;
    }
    if (value instanceof Error) {
        return value.stack ?? value.message;
    }
    if (value === undefined) {
        return 'undefined';
    }
    try {
        const json = JSON.stringify(value);
        return json ?? String(value);
    }
    catch {
        return String(value);
    }
}
