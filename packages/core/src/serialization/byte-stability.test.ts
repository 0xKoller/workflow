/**
 * Wire-format byte stability.
 *
 * Pins the exact devalue string produced for a corpus of representative
 * values against a snapshot recorded BEFORE the hardened stringify
 * operations existed (see operations.ts). A mismatch here means the wire
 * format changed: payloads already stored in event logs were written by the
 * old code, so any drift breaks replay of in-flight runs.
 *
 * To intentionally change the wire format, regenerate the snapshot with:
 *
 *   RECORD_BYTE_CORPUS=1 pnpm vitest run src/serialization/byte-stability.test.ts
 *
 * and justify the change in the PR description.
 */

import { readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';
import { dehydrateStepArguments } from '../serialization.js';
import { buildByteCorpus, buildThrowingCorpus } from './byte-corpus.js';
import { decodeFormatPrefix } from './format.js';
import { SerializationFormat } from './types.js';

const snapshotPath = fileURLToPath(
  new URL('./byte-corpus.snapshot.json', import.meta.url)
);

const recording = process.env.RECORD_BYTE_CORPUS === '1';

/**
 * Serialize through the real workflow→step boundary path (the one the
 * suspension handler uses), with encryption and compression off so the
 * devalue string is recoverable from the format-prefixed payload.
 */
async function serializeToDevalueString(value: unknown): Promise<string> {
  const bytes = (await dehydrateStepArguments(
    value,
    'wrun_byte_corpus',
    undefined,
    globalThis,
    false,
    false
  )) as Uint8Array;
  const { format, payload } = decodeFormatPrefix(bytes);
  expect(format).toBe(SerializationFormat.DEVALUE_V1);
  return new TextDecoder().decode(payload);
}

describe('serialization byte stability', () => {
  it('produces byte-identical devalue strings for the recorded corpus', async () => {
    const corpus = buildByteCorpus();
    const actual: Record<string, string> = {};
    for (const { name, value } of corpus) {
      actual[name] = await serializeToDevalueString(value);
    }

    const throwing = buildThrowingCorpus();
    for (const { name, value } of throwing) {
      const message = await serializeToDevalueString(value).then(
        () => {
          throw new Error(`expected "${name}" to fail serialization`);
        },
        (error: unknown) => (error as Error).message
      );
      actual[`throws: ${name}`] = message;
    }

    if (recording) {
      writeFileSync(snapshotPath, `${JSON.stringify(actual, null, 2)}\n`);
      return;
    }

    const recorded: Record<string, string> = JSON.parse(
      readFileSync(snapshotPath, 'utf8')
    );
    // Compare entry-by-entry for readable failures, then in full so added
    // or removed corpus entries are caught too.
    for (const [name, expected] of Object.entries(recorded)) {
      expect(actual[name], name).toBe(expected);
    }
    expect(Object.keys(actual).sort()).toEqual(Object.keys(recorded).sort());
  });
});
