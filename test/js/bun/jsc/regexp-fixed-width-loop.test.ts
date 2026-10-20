import { describe, expect, test } from "bun:test";
import { bunEnv, bunExe } from "harness";

// A quantified group whose body has a fixed width and nothing to backtrack into, such as (?:ab)* or
// ([0-9a-f]{2})+, runs without a per-iteration backtracking context. Before, every iteration kept
// one, and a search with about two million iterations ran out of room and reported "no match".
// The long inputs run in a child process with the RegExp JIT on and off, so both engines are covered.

const BIG = 3_000_000;

// [source, flags, input expression, expected [index, length, capture 1 indices]]
const cases: [string, string, string, [number, number, [number, number]?] | null][] = [
  // The report: a group followed by $ on an input longer than the engine had room for.
  ["^(?:a)*$", "", `rep("a", ${BIG})`, [0, BIG]],
  ["(?:a)*$", "", `rep("a", ${BIG})`, [0, BIG]],
  ["(?:ab)*$", "", `rep("ab", ${BIG / 2})`, [0, BIG]],
  ["^(?:[0-9a-f]{2})+$", "", `rep("deadbeef", ${BIG / 8})`, [0, BIG]],
  ["^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$", "", `rep("aGVs", ${BIG / 4}) + "bG8="`, [0, BIG + 4]],
  // The group's capture is the last iteration.
  ["(a)*$", "d", `rep("a", ${BIG})`, [0, BIG, [BIG - 1, BIG]]],
  ["(ab)*$", "d", `rep("ab", ${BIG / 2})`, [0, BIG, [BIG - 2, BIG]]],
  // A bounded or lazy group, and one the search has to give iterations back from.
  ["(?:ab){2,}c", "", `rep("ab", ${BIG / 2}) + "c"`, [0, BIG + 1]],
  ["(?:ab)*?c", "", `rep("ab", ${BIG / 2}) + "c"`, [0, BIG + 1]],
  ["(?:ab)*abab$", "", `rep("ab", ${BIG / 2})`, [0, BIG]],
  ["(ab)*abab$", "d", `rep("ab", ${BIG / 2})`, [0, BIG, [BIG - 6, BIG - 4]]],
  ["^(?:ab)*$", "", `rep("ab", ${BIG / 2}) + "a"`, null],
  // A body that is two code units wide.
  ["^(?:\\u{1F600})+$", "u", `rep("\\u{1F600}", ${BIG / 2})`, [0, BIG]],
];

const script = `
  const rep = (s, n) => Buffer.alloc(Buffer.byteLength(s) * n, s).toString();
  const cases = ${JSON.stringify(cases.map(([source, flags, input]) => [source, flags, input]))};
  const out = [];
  for (const [source, flags, input] of cases) {
    const m = new RegExp(source, flags).exec(eval(input));
    out.push(m && [m.index, m[0].length, ...(m.indices ? [m.indices[1]] : [])]);
  }
  console.log(JSON.stringify(out));
`;

describe.concurrent("a quantified fixed-width group matches inputs longer than the engine's backtracking pool", () => {
  for (const [label, env] of [
    ["JIT", {}],
    ["interpreter", { BUN_JSC_useRegExpJIT: "false" }],
  ] as const) {
    test(label, async () => {
      await using proc = Bun.spawn({
        cmd: [bunExe(), "-e", script],
        env: { ...bunEnv, ...env },
        stderr: "pipe",
      });
      const [stdout, stderr, exitCode] = await Promise.all([proc.stdout.text(), proc.stderr.text(), proc.exited]);
      expect(stderr).toBe("");
      expect(JSON.parse(stdout)).toEqual(cases.map(([, , , expected]) => expected));
      expect(exitCode).toBe(0);
    });
  }
});

describe("a quantified fixed-width group backtracks like any other group", () => {
  test("greedy groups give iterations back", () => {
    expect(Array.from(/(?:ab)*abc/.exec("ababababc")!)).toEqual(["ababababc"]);
    expect(Array.from(/(ab)*abc/.exec("ababababc")!)).toEqual(["ababababc", "ab"]);
    expect(Array.from(/(ab)*abc/.exec("abc")!)).toEqual(["abc", undefined]);
    expect(Array.from(/(?:ab){2,3}c/.exec("ababababc")!)).toEqual(["abababc"]);
    expect(/(?:ab){2,}c/.exec("abc")).toBeNull();
    expect(Array.from(/(ab){2,4}/.exec("ababababababab")!)).toEqual(["abababab", "ab"]);
    expect(/^(?:ab)*$/.test("abababa")).toBe(false);
  });

  test("lazy groups take iterations on demand", () => {
    expect(Array.from(/(?:ab)*?c/.exec("ababc")!)).toEqual(["ababc"]);
    expect(Array.from(/(ab)*?abc/.exec("ababc")!)).toEqual(["ababc", "ab"]);
    expect(Array.from(/(ab)*?$/.exec("abab")!)).toEqual(["abab", "ab"]);
    expect(Array.from(/(?:ab){2,3}?/.exec("abababab")!)).toEqual(["abab"]);
    expect(/(ab)+?b/.exec("abab")).toBeNull();
    expect(Array.from(/(ab)+?b/.exec("ababb")!)).toEqual(["ababb", "ab"]);
  });

  test("the capture, a backreference, and the groups after it see the last iteration", () => {
    expect(Array.from(/(ab)*\1/.exec("ababab")!)).toEqual(["ababab", "ab"]);
    expect(Array.from(/(ab)*\1c/.exec("ababc")!)).toEqual(["ababc", "ab"]);
    expect(Array.from(/(ab)*(ab)c/.exec("ababc")!)).toEqual(["ababc", "ab", "ab"]);
    expect(/(?<p>ab)*c/.exec("ababc")!.groups).toEqual({ p: "ab" });
    expect(/^(?:(?<p>ab)*c|(?<p>cd)*e)$/.exec("cdcde")!.groups).toEqual({ p: "cd" });
    expect(/^(?:(?<p>ab)*c|(?<p>cd)*e)$/.exec("ababc")!.groups).toEqual({ p: "ab" });
    expect("ababababc".replace(/(ab)*c/, "[$1]")).toBe("[ab]");
    expect(Array.from("ababc ababc".matchAll(/(ab)*c/g)).map(m => [m[0], m[1], m.index])).toEqual([
      ["ababc", "ab", 0],
      ["ababc", "ab", 6],
    ]);
  });

  test("assertions and wide characters in the body", () => {
    expect(/^(?:a\B)*a$/.test("aaaa")).toBe(true);
    expect(Array.from(/(?:\bab)*$/.exec("ab ab")!)).toEqual(["ab"]);
    expect(Array.from(/(?:\u{1F600}a)*$/u.exec("\u{1F600}a\u{1F600}a")!)).toEqual(["\u{1F600}a\u{1F600}a"]);
    expect(Array.from(/(?:[\u{1F600}\u{1F601}])+b/u.exec("\u{1F600}\u{1F601}b")!)).toEqual(["\u{1F600}\u{1F601}b"]);
    expect(Array.from(/(?:\u{1F600}){2,3}b/u.exec("\u{1F600}\u{1F600}\u{1F600}\u{1F600}b")!)).toEqual([
      "\u{1F600}\u{1F600}\u{1F600}b",
    ]);
    expect(Array.from(/(?:AB)*c/i.exec("ababC")!)).toEqual(["ababC"]);
  });

  test("nested in other groups and assertions", () => {
    expect(Array.from(/((?:ab)*c)+$/.exec("ababcabcc")!)).toEqual(["ababcabcc", "c"]);
    expect(Array.from(/(?:x(?:ab)*)?y/.exec("xababy")!)).toEqual(["xababy"]);
    expect(Array.from(/(?=(?:ab)*c)ab/.exec("ababc")!)).toEqual(["ab"]);
    expect(Array.from(/(?<=x(?:ab)*)c/.exec("xababc")!)).toEqual(["c"]);
    expect("ab ab abab x".split(/(?:ab)+/)).toEqual(["", " ", " ", " x"]);
  });
});
