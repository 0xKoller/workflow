import { type CreateEventRequest, SPEC_VERSION_CURRENT } from '@workflow/world';
import { describe, expect, it } from 'vitest';
import {
  assembleSuspensionBatch,
  MAX_SUSPENSION_BATCH_ITEMS,
  type SuspensionBatchComponents,
} from './suspension-batch.js';

const frame = (eventType: string, correlationId?: string): CreateEventRequest =>
  ({
    eventType,
    specVersion: SPEC_VERSION_CURRENT,
    ...(correlationId ? { correlationId } : {}),
    eventData: {},
  }) as CreateEventRequest;

const inlinePair = (correlationId: string) => ({
  created: frame('step_created', correlationId),
  started: frame('step_started', correlationId),
});

// The server grammar (workflow-server classifySuspensionBatch): a valid batch
// has an optional leading step_completed|step_failed at index 0, a terminal
// run_completed|run_failed only as the final element, and every step_started
// immediately preceded by a step_created with the same correlationId.
function assertGrammarValid(events: CreateEventRequest[]): void {
  expect(events.length).toBeGreaterThanOrEqual(1);
  // Leading outcome only at index 0.
  events.forEach((e, i) => {
    if (e.eventType === 'step_completed' || e.eventType === 'step_failed') {
      expect(i).toBe(0);
    }
  });
  // Terminal only as the final element.
  events.forEach((e, i) => {
    if (e.eventType === 'run_completed' || e.eventType === 'run_failed') {
      expect(i).toBe(events.length - 1);
    }
  });
  // Every step_started is immediately preceded by its step_created.
  events.forEach((e, i) => {
    if (e.eventType === 'step_started') {
      const prev = events[i - 1];
      expect(prev?.eventType).toBe('step_created');
      expect(prev?.correlationId).toBe(e.correlationId);
    }
  });
}

describe('assembleSuspensionBatch', () => {
  it('orders leading outcome first, terminal last, middle in between', () => {
    const { events } = assembleSuspensionBatch({
      leadingOutcome: frame('step_completed', 'n'),
      hookReceiveds: [frame('hook_received', 'h1')],
      inlineSteps: [inlinePair('m1')],
      pendingSteps: [frame('step_created', 'p1')],
      waits: [frame('wait_created', 'w1')],
      terminal: frame('run_completed'),
    });

    expect(events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'hook_received',
      'step_created', // inline pair created
      'step_started', // inline pair started
      'step_created', // bare pending
      'wait_created',
      'run_completed',
    ]);
    assertGrammarValid(events);
  });

  it('keeps each inline pair adjacent and correlated', () => {
    const { events } = assembleSuspensionBatch({
      inlineSteps: [inlinePair('a'), inlinePair('b'), inlinePair('c')],
    });
    expect(events.map((e) => `${e.eventType}:${e.correlationId}`)).toEqual([
      'step_created:a',
      'step_started:a',
      'step_created:b',
      'step_started:b',
      'step_created:c',
      'step_started:c',
    ]);
    assertGrammarValid(events);
  });

  it('emits the v1 single-transition triple unchanged (backward-compat shape)', () => {
    // The v1 batch was exactly [completed(N), created(N+1), started(N+1)]; the
    // generalized builder must still produce that when given one inline pair
    // and a leading completion.
    const { events, itemCount } = assembleSuspensionBatch({
      leadingOutcome: frame('step_completed', 'n'),
      inlineSteps: [inlinePair('n1')],
    });
    expect(events.map((e) => e.eventType)).toEqual([
      'step_completed',
      'step_created',
      'step_started',
    ]);
    // fence(1) + leadingOutcome(2) + inlineStep(3) = 6.
    expect(itemCount).toBe(6);
    assertGrammarValid(events);
  });

  it('assembles an empty batch to zero events but still counts the fence item', () => {
    const { events, itemCount, withinItemBudget } = assembleSuspensionBatch({});
    expect(events).toEqual([]);
    expect(itemCount).toBe(1); // run-fence item only
    expect(withinItemBudget).toBe(true);
  });

  it('counts transaction items exactly as the server opItemCount + fence', () => {
    const components: SuspensionBatchComponents = {
      leadingOutcome: frame('step_failed', 'n'), // 2
      hookReceiveds: [
        frame('hook_received', 'h1'),
        frame('hook_received', 'h2'),
      ], // 1 each = 2
      inlineSteps: [inlinePair('m1'), inlinePair('m2')], // 3 each = 6
      pendingSteps: [frame('step_created', 'p1')], // 2
      waits: [frame('wait_created', 'w1'), frame('wait_completed', 'w2')], // 2 each = 4
      terminal: frame('run_failed'), // 1
    };
    // fence(1) + 2 + 2 + 6 + 2 + 4 + 1 = 18.
    const { itemCount, withinItemBudget } = assembleSuspensionBatch(components);
    expect(itemCount).toBe(18);
    expect(withinItemBudget).toBe(true);
  });

  it('flags the item budget at the 100-item boundary (worst case: all inline pairs)', () => {
    // Inline pairs cost 3 items each; with a leading completion (2) + fence (1)
    // the ceiling is floor((100 - 3) / 3) = 32 pairs (== 99 items), 33 over.
    const leadingOutcome = frame('step_completed', 'n');
    const within = assembleSuspensionBatch({
      leadingOutcome,
      inlineSteps: Array.from({ length: 32 }, (_, i) => inlinePair(`m${i}`)),
    });
    expect(within.itemCount).toBe(1 + 2 + 32 * 3); // 99
    expect(within.withinItemBudget).toBe(true);

    const over = assembleSuspensionBatch({
      leadingOutcome,
      inlineSteps: Array.from({ length: 33 }, (_, i) => inlinePair(`m${i}`)),
    });
    expect(over.itemCount).toBe(1 + 2 + 33 * 3); // 102
    expect(over.withinItemBudget).toBe(false);
    expect(over.itemCount).toBeGreaterThan(MAX_SUSPENSION_BATCH_ITEMS);
  });

  it('a batch of exactly MAX_SUSPENSION_BATCH_ITEMS is within budget; one more is not', () => {
    // hook_received costs 1 item each; 99 of them + the fence = 100 (at cap).
    const at = assembleSuspensionBatch({
      hookReceiveds: Array.from({ length: 99 }, (_, i) =>
        frame('hook_received', `h${i}`)
      ),
    });
    expect(at.itemCount).toBe(MAX_SUSPENSION_BATCH_ITEMS);
    expect(at.withinItemBudget).toBe(true);

    const over = assembleSuspensionBatch({
      hookReceiveds: Array.from({ length: 100 }, (_, i) =>
        frame('hook_received', `h${i}`)
      ),
    });
    expect(over.itemCount).toBe(MAX_SUSPENSION_BATCH_ITEMS + 1);
    expect(over.withinItemBudget).toBe(false);
  });
});
