// Temporary. Collects facts from the macOS lanes for the watcher change of this branch, and is removed
// before the branch is ready.
import type { Subprocess } from "bun";
import { dlopen, ptr } from "bun:ffi";
import { expect, test } from "bun:test";
import { bunEnv, bunExe, isMacOS, isWindows, tempDir } from "harness";
import {
  appendFileSync,
  chmodSync,
  closeSync,
  ftruncateSync,
  mkdirSync,
  openSync,
  readFileSync,
  rmSync,
  truncateSync,
  utimesSync,
  writeFileSync,
  writeSync,
} from "node:fs";
import { release } from "node:os";
import { join } from "node:path";

const NOTE: [number, string][] = [
  [0x1, "delete"],
  [0x2, "write"],
  [0x4, "extend"],
  [0x8, "attrib"],
  [0x10, "link"],
  [0x20, "rename"],
  [0x40, "revoke"],
];

// What the fix of this branch rests on: a truncation is `attrib` and never `write`.
test.skipIf(!isMacOS)("kqueue: what a vnode reports for each change", () => {
  const lib = dlopen("libSystem.B.dylib", {
    kqueue: { args: [], returns: "i32" },
    kevent: { args: ["i32", "ptr", "i32", "ptr", "i32", "ptr"], returns: "i32" },
  });
  using dir = tempDir("kqueue-probe", { "file.txt": Buffer.alloc(100, "x").toString(), "sub/child.txt": "child" });
  const file = join(String(dir), "file.txt");
  const sub = join(String(dir), "sub");
  const kq = lib.symbols.kqueue();
  expect(kq).toBeGreaterThan(0);
  const watched = new Map<number, string>();
  const watch = (path: string, name: string) => {
    const fd = openSync(path, "r");
    // struct kevent: ident u64, filter i16, flags u16, fflags u32, data i64, udata u64.
    const change = Buffer.alloc(32);
    change.writeBigUInt64LE(BigInt(fd), 0);
    change.writeInt16LE(-4, 8); // EVFILT_VNODE
    change.writeUInt16LE(0x1 | 0x4 | 0x20, 10); // EV_ADD | EV_ENABLE | EV_CLEAR
    change.writeUInt32LE(0x7f, 12);
    expect(lib.symbols.kevent(kq, ptr(change), 1, null, 0, null)).toBe(0);
    watched.set(fd, name);
    return fd;
  };
  const fds = [watch(file, "file"), watch(sub, "dir")];
  const zero = Buffer.alloc(16);
  const out = Buffer.alloc(32 * 8);
  const drain = () => {
    const seen: Record<string, string> = {};
    for (;;) {
      const n = lib.symbols.kevent(kq, null, 0, ptr(out), 8, ptr(zero));
      if (n <= 0) break;
      for (let i = 0; i < n; i++) {
        const name = watched.get(Number(out.readBigUInt64LE(i * 32)))!;
        const fflags = out.readUInt32LE(i * 32 + 12);
        const names = NOTE.filter(([bit]) => fflags & bit).map(([, n]) => n);
        seen[name] = [...new Set([...(seen[name]?.split("+") ?? []), ...names])].sort().join("+");
      }
    }
    return seen;
  };
  const table: Record<string, Record<string, string>> = {};
  const step = (name: string, change: () => void) => {
    change();
    table[name] = drain();
  };
  try {
    step("overwrite the start", () => {
      const fd = openSync(file, "r+");
      writeSync(fd, "yyyy");
      closeSync(fd);
    });
    step("truncate to a shorter length", () => truncateSync(file, 50));
    step("ftruncate to a shorter length", () => {
      const fd = openSync(file, "r+");
      ftruncateSync(fd, 40);
      closeSync(fd);
    });
    step("truncate to a longer length", () => truncateSync(file, 80));
    step("truncate to the same length", () => truncateSync(file, 80));
    step("append", () => appendFileSync(file, "zzz"));
    step("chmod", () => chmodSync(file, 0o600));
    step("utimes", () => utimesSync(file, new Date(2020, 0, 1), new Date(2020, 0, 1)));
    step("open with O_TRUNC", () => closeSync(openSync(file, "w")));
    step("mkdir in the folder", () => mkdirSync(join(sub, "made")));
    step("new file in the folder", () => writeFileSync(join(sub, "new.txt"), "a"));
    step("write to a file in the folder", () => {
      const fd = openSync(join(sub, "child.txt"), "r+");
      writeSync(fd, "CHILD");
      closeSync(fd);
    });
    step("chmod of the folder", () => chmodSync(sub, 0o700));
  } finally {
    for (const fd of fds) closeSync(fd);
    closeSync(kq);
    lib.close();
  }
  expect(table).toEqual({
    "overwrite the start": { file: "write" },
    "truncate to a shorter length": { file: "attrib" },
    "ftruncate to a shorter length": { file: "attrib" },
    "truncate to a longer length": { file: "attrib" },
    "truncate to the same length": { file: "attrib" },
    "append": { file: "extend+write" },
    "chmod": { file: "attrib" },
    "utimes": { file: "attrib" },
    "open with O_TRUNC": { file: "attrib" },
    "mkdir in the folder": { dir: "link+write" },
    "new file in the folder": { dir: "write" },
    "write to a file in the folder": {},
    "chmod of the folder": { dir: "attrib" },
  });
});

const env = { ...bunEnv, BUN_FEATURE_FLAG_EXPERIMENTAL_BAKE: "1", NO_COLOR: "1" };
const fixed = `export default {};`;
const badConfig = `export default { plugins: [{ name: "a", framework: { plugins: [], fileSystemRouterTypes: [] } }] };`;
const server = `export default { fetch: () => new Response("fixed") };`;

function collectOutput(proc: Subprocess<"ignore", "pipe", "pipe">) {
  let text = "";
  let changed = Promise.withResolvers<void>();
  const pump = async (stream: ReadableStream<Uint8Array>) => {
    const decoder = new TextDecoder();
    for await (const chunk of stream) {
      text += decoder.decode(chunk, { stream: true });
      changed.resolve();
      changed = Promise.withResolvers<void>();
    }
  };
  const done = Promise.all([pump(proc.stdout), pump(proc.stderr)]);
  return {
    get text() {
      return text;
    },
    /** False when `ms` passed, or the process ended, first. */
    async waitFor(pattern: RegExp, ms: number) {
      const deadline = Bun.sleep(ms).then(() => "late" as const);
      while (!pattern.test(text)) {
        const first = await Promise.race([
          changed.promise.then(() => "more" as const),
          done.then(() => "end" as const),
          deadline,
        ]);
        if (first !== "more" && !pattern.test(text)) return false;
      }
      return true;
    },
  };
}

const variants = [
  "first write makes app/",
  "first write makes app/, then 5 ms",
  "app/ is there",
  "app/ is there, then 5 ms",
  "app/ has a page",
  "app/ has a page, then 5 ms",
] as const;
type Variant = (typeof variants)[number];
type Hang = {
  variant: Variant;
  stage: string;
  out: string;
  trace: string;
  config: string;
  files: string;
  /** Whether one more save of the same text starts the server. */
  startsAfterOneMoreSave?: boolean;
};
const counts = Object.fromEntries(variants.map(v => [v, { runs: 0, hangs: 0, ms: [] as number[] }]));
const hangs: Hang[] = [];
const split: { before: string; recovered: boolean; after: string }[] = [];
let sequence = 0;

async function withBake<T>(
  files: Record<string, string>,
  body: (root: string, output: ReturnType<typeof collectOutput>, trace: () => string) => Promise<T>,
) {
  using dir = tempDir("bake-diag", files);
  const root = String(dir);
  // Outside the project, so that the watcher does not report its own trace.
  const traceFile = join(root, "..", `bake-diag-trace-${process.pid}-${sequence++}.jsonl`);
  try {
    await using proc = Bun.spawn({
      cmd: [bunExe(), "bake", "--port=0"],
      env: { ...env, BUN_WATCHER_TRACE: traceFile },
      cwd: root,
      killSignal: "SIGKILL",
      stdin: "ignore",
      stdout: "pipe",
      stderr: "pipe",
    });
    return await body(root, collectOutput(proc), () => {
      try {
        return readFileSync(traceFile, "utf8").replaceAll(root, "");
      } catch {
        return "(no trace)";
      }
    });
  } finally {
    rmSync(traceFile, { force: true });
  }
}

async function natural(variant: Variant) {
  const files: Record<string, string> = { "bake.config.ts": badConfig };
  if (variant.startsWith("app/ is there")) files["app/pages/.keep"] = "";
  if (variant.startsWith("app/ has a page"))
    files["app/pages/index.html"] = `<!doctype html><html><body></body></html>`;
  await withBake(files, async (root, output, trace) => {
    const hang = (stage: string): Hang | undefined => {
      counts[variant].hangs++;
      if (hangs.length < 4) {
        let config = "(unreadable)";
        try {
          config = readFileSync(join(root, "bake.config.ts"), "utf8");
        } catch {}
        const listing = [...new Bun.Glob("**/*").scanSync({ cwd: root, dot: true })].sort().join(",");
        const entry: Hang = {
          variant,
          stage,
          out: output.text.replaceAll(root, "").slice(0, 500),
          trace: trace().slice(0, 500),
          config: config.slice(0, 120),
          files: listing.slice(0, 200),
        };
        hangs.push(entry);
        return entry;
      }
    };
    counts[variant].runs++;
    if (!(await output.waitFor(/error: /, 30_000))) return hang("no first error");
    const late = Symbol();
    const first = await Promise.race([
      Bun.write(join(root, "app/server.ts"), server),
      Bun.sleep(10_000).then(() => late),
    ]);
    if (first === late) return hang("the first Bun.write did not resolve");
    if (variant.endsWith("then 5 ms")) await Bun.sleep(5);
    const start = performance.now();
    const second = await Promise.race([
      Bun.write(join(root, "bake.config.ts"), fixed),
      Bun.sleep(10_000).then(() => late),
    ]);
    if (second === late) return hang("the second Bun.write did not resolve");
    if (!(await output.waitFor(/url: /, 10_000))) {
      const entry = hang("no url 10 s after the writes");
      // A save that is one `write()` after an `open()` that truncates.
      writeFileSync(join(root, "bake.config.ts"), fixed);
      const started = await output.waitFor(/url: /, 5_000);
      if (entry) entry.startsAfterOneMoreSave = started;
      return;
    }
    counts[variant].ms.push(performance.now() - start);
  });
}

// The rows of test/cli/bake/bake.test.ts "bun bake rejects ..." that time out, as they are, many times.
(isWindows ? test.skip : test.concurrent).each(Array.from({ length: 15 }, (_, i) => i))(
  "bun bake starts after the project is repaired, group %d",
  async group => {
    const until = performance.now() + 35_000;
    for (let i = 0; i < 90 && performance.now() < until; i++) {
      await natural(variants[(group + i) % variants.length]);
    }
  },
);

// The two steps of `Bun.write`, with the reload in between.
(isWindows ? test.skip : test.concurrent).each([0, 1, 2])(
  "bun bake, repaired with a save that stops before its truncation, %d",
  async () => {
    await withBake({ "bake.config.ts": badConfig }, async (root, output) => {
      expect(await output.waitFor(/error: /, 30_000)).toBe(true);
      await Bun.write(join(root, "app/server.ts"), server);
      const before = output.text.length;
      const fd = openSync(join(root, "bake.config.ts"), "r+");
      writeSync(fd, fixed);
      // The reload that reads the file as it is now.
      await output.waitFor(/(?:error: [^\n]*\n[\s\S]*){2}/, 10_000);
      const between = output.text.slice(before);
      ftruncateSync(fd, fixed.length);
      closeSync(fd);
      const recovered = await output.waitFor(/url: /, 10_000);
      split.push({
        before: between.replaceAll(root, "").slice(0, 300),
        recovered,
        after: output.text
          .slice(before + between.length)
          .replaceAll(root, "")
          .slice(0, 200),
      });
    });
  },
);

test.skipIf(isWindows)("report", () => {
  const stats = (ms: number[]) => {
    const sorted = ms.toSorted((a, b) => a - b);
    const at = (q: number) => Math.round(sorted[Math.min(sorted.length - 1, Math.floor(sorted.length * q))] ?? -1);
    return `min ${at(0)} p50 ${at(0.5)} p90 ${at(0.9)} p99 ${at(0.99)} max ${at(1)}`;
  };
  const report = {
    platform: `${process.platform} ${process.arch} ${release()}`,
    counts: Object.fromEntries(
      Object.entries(counts).map(([name, { runs, hangs, ms }]) => [
        name,
        `${hangs} of ${runs} hang; ms to url: ${stats(ms)}`,
      ]),
    ),
    hangs,
    split,
  };
  // On macOS this fails on purpose, to show the numbers. On Linux it fails only for a hang.
  if (isMacOS || process.env.WATCHER_DIAG_PRINT)
    throw new Error("DIAGNOSTIC REPORT\n" + JSON.stringify(report, null, 1));
  expect(report.hangs).toEqual([]);
  expect(report.split.map(s => s.recovered)).toEqual([true, true, true]);
});
