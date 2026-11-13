#!/usr/bin/env node
// One spec per source file, next to it: every lib/**/x.spec.ts needs a sibling lib/**/x.ts.
// Catches per-feature spec files (x.retry.spec.ts) and specs left behind by a rename.
import { existsSync, readdirSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';

const root = resolve(import.meta.dirname, '..');
const lib = join(root, 'lib');

function* specs(dir) {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) {
      if (entry.name !== '__mocks__') yield* specs(path);
    } else if (entry.name.endsWith('.spec.ts')) {
      yield path;
    }
  }
}

const all = [...specs(lib)];
const orphans = all.filter((p) => !existsSync(p.replace(/\.spec\.ts$/, '.ts')));

if (orphans.length > 0) {
  console.error(`check:specs FAILED: ${orphans.length} spec(s) without a source file of the same name:`);
  for (const p of orphans) console.error(`  - ${relative(root, p)}`);
  console.error('Put the tests in the spec of the file they test (one spec per source file).');
  process.exit(1);
}
console.log(`check:specs OK (${all.length} specs)`);
