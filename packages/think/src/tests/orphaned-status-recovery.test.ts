/**
 * Regression tests for cloudflare/agents#1553: "Orphaned 'processing'
 * status: durable state desyncs from turn liveness after DO eviction".
 *
 * The standard pattern is to acquire a durable `status` in `beforeTurn`
 * (persisted via `setState()` → SQLite) and release it in the in-process
 * turn-end hooks (`onChatResponse` / `onChatError`). Durable Objects
 * give no crash callback, so a turn evicted between `beforeTurn` and its
 * first recoverable checkpoint never reaches the in-process terminal
 * path: the lock survives durably while the in-memory liveness gate
 * resets, so nothing ever releases it. Under `messageConcurrency:
 * "queue"` every later message then wedges behind a turn that ended
 * without ending.
 *
 * The fix makes crash reconciliation run the same canonical terminal
 * transition a normal turn-end runs: mark the submission terminal,
 * surface it, and fire the user turn-end hooks so the lock is released
 * and the unanswered turn is not silently dropped.
 */

import { env } from "cloudflare:workers";
import { getServerByName } from "partyserver";
import { describe, expect, it } from "vitest";
import type { ThinkOrphanedStatusTestAgent } from "./agents/think-session";
import type {
  ChatResponseResult,
  ChatRecoveryOptions,
  SubmitMessagesResult,
  ThinkSubmissionInspection,
  ThinkSubmissionStatus
} from "../think";

type OrphanedStatusStub = {
  getStatusForTest(): Promise<string>;
  getChatErrorLogForTest(): Promise<string[]>;
  getResponseLogForTest(): Promise<ChatResponseResult[]>;
  setRecoveryOverrideForTest(options: ChatRecoveryOptions): Promise<void>;
  testSubmitMessages(
    text: string,
    options?: { submissionId?: string }
  ): Promise<SubmitMessagesResult>;
  inspectSubmissionForTest(
    submissionId: string
  ): Promise<ThinkSubmissionInspection | null>;
  recoverSubmissionsForTest(): Promise<void>;
  recoverChatFiberForTest(requestId: string): Promise<void>;
  continueRecoveredChatForTest(requestId: string): Promise<void>;
  fireResponseHookForTest(
    requestId: string,
    status: "completed" | "error" | "aborted",
    error?: string
  ): Promise<void>;
  fireInterruptedTurnHooksForTest(
    requestId: string,
    reason: string
  ): Promise<void>;
  insertEvictedRunningTurnForTest(options: {
    submissionId: string;
    fiberCreatedAt?: number;
    submissionCreatedAt?: number;
    withFiber?: boolean;
  }): Promise<void>;
  waitForSubmissionStatusForTest(
    submissionId: string,
    status: ThinkSubmissionStatus
  ): Promise<ThinkSubmissionInspection | null>;
};

async function freshAgent(
  name = crypto.randomUUID()
): Promise<OrphanedStatusStub> {
  return getServerByName(
    env.ThinkOrphanedStatusTestAgent as unknown as DurableObjectNamespace<ThinkOrphanedStatusTestAgent>,
    name
  ) as unknown as Promise<OrphanedStatusStub>;
}

const THIRTY_MINUTES_MS = 30 * 60 * 1000;

describe("Think — orphaned 'processing' status after DO eviction (#1553)", () => {
  it("releases the durable lock when a turn is reconciled stale past the freshness window", async () => {
    const agent = await freshAgent();

    // A turn evicted while suspended on a long async span: user message
    // applied, submission still `running`, chat fiber row left behind,
    // and the app's durable lock pinned to "processing". The DO is not
    // re-driven until after the recovery freshness window.
    await agent.insertEvictedRunningTurnForTest({
      submissionId: "sub-orphaned",
      submissionCreatedAt: Date.now() - THIRTY_MINUTES_MS,
      fiberCreatedAt: Date.now() - THIRTY_MINUTES_MS
    });

    expect(await agent.getStatusForTest()).toBe("processing");

    // DO re-instantiation runs crash reconciliation.
    await agent.recoverSubmissionsForTest();

    const submission = await agent.inspectSubmissionForTest("sub-orphaned");
    expect(submission?.status).toBe("error");

    // The canonical terminal transition fired the user turn-end hooks,
    // so the standard acquire/release pattern releases the lock.
    expect(await agent.getChatErrorLogForTest()).toHaveLength(1);
    const responses = await agent.getResponseLogForTest();
    expect(responses).toHaveLength(1);
    expect(responses[0].status).toBe("error");

    // The orphaned lock is gone — the chat is no longer wedged.
    expect(await agent.getStatusForTest()).toBe("ready");
  });

  it("releases the lock when the turn died before runFiber recorded a fiber", async () => {
    const agent = await freshAgent();

    // No fiber row and no stream checkpoint: the turn died on the async
    // span before `runFiber` persisted anything recoverable.
    await agent.insertEvictedRunningTurnForTest({
      submissionId: "sub-no-fiber",
      withFiber: false
    });

    expect(await agent.getStatusForTest()).toBe("processing");

    await agent.recoverSubmissionsForTest();

    expect(await agent.inspectSubmissionForTest("sub-no-fiber")).toMatchObject({
      status: "error"
    });
    expect(await agent.getChatErrorLogForTest()).toHaveLength(1);
    expect(await agent.getStatusForTest()).toBe("ready");
  });

  it("releases the lock on the fiber-recovery path when recovery is disabled", async () => {
    const agent = await freshAgent();

    await agent.setRecoveryOverrideForTest({ continue: false });
    await agent.insertEvictedRunningTurnForTest({
      submissionId: "sub-no-continue"
    });

    expect(await agent.getStatusForTest()).toBe("processing");

    // Alarm-driven fiber recovery declares the turn interrupted because
    // the app opted out of continuation.
    await agent.recoverChatFiberForTest("sub-no-continue");

    const submission = await agent.waitForSubmissionStatusForTest(
      "sub-no-continue",
      "error"
    );
    expect(submission?.status).toBe("error");
    expect(await agent.getChatErrorLogForTest()).toHaveLength(1);
    expect(await agent.getStatusForTest()).toBe("ready");
  });

  it("does not prematurely fail a turn that is still genuinely recoverable", async () => {
    const agent = await freshAgent();

    // Fresh fiber, within the freshness window: the turn will be
    // resumed by fiber recovery, so reconciliation must NOT declare it
    // interrupted or fire the terminal hooks. `status` stays
    // "processing" because a live-or-recoverable turn still exists.
    await agent.insertEvictedRunningTurnForTest({
      submissionId: "sub-recoverable",
      submissionCreatedAt: Date.now(),
      fiberCreatedAt: Date.now()
    });

    await agent.recoverSubmissionsForTest();

    expect(
      await agent.inspectSubmissionForTest("sub-recoverable")
    ).toMatchObject({ status: "running" });
    expect(await agent.getChatErrorLogForTest()).toHaveLength(0);
    expect(await agent.getResponseLogForTest()).toHaveLength(0);
    expect(await agent.getStatusForTest()).toBe("processing");
  });

  it("still fires the turn-end hooks exactly once on the normal completion path", async () => {
    const agent = await freshAgent();

    const accepted = await agent.testSubmitMessages("hello", {
      submissionId: "sub-happy"
    });
    expect(accepted.accepted).toBe(true);

    await agent.waitForSubmissionStatusForTest("sub-happy", "completed");

    // Crash reconciliation must not double-fire on a turn that already
    // completed in-process.
    await agent.recoverSubmissionsForTest();

    const responses = await agent.getResponseLogForTest();
    expect(responses).toHaveLength(1);
    expect(responses[0].status).toBe("completed");
    expect(await agent.getChatErrorLogForTest()).toHaveLength(0);
    expect(await agent.getStatusForTest()).toBe("ready");
  });

  it("releases the lock on the continuation path when the recovered turn ends 'skipped'", async () => {
    const agent = await freshAgent();

    // A turn evicted mid-tool-call: user message applied, fiber row
    // still present (fresh), but no assistant leaf was ever persisted
    // because the partial stream never completed. Sweep defers to
    // fiber recovery, fiber recovery schedules a continuation, and the
    // continuation early-returns "skipped" because `getLatestLeaf()` is
    // not an assistant. Without hook parity on the continuation path
    // the submission lands terminal but the durable `beforeTurn` lock
    // stays pinned — the same orphaned-status shape as #1553, reached
    // through the fiber-recovery door rather than the sweep door.
    await agent.insertEvictedRunningTurnForTest({
      submissionId: "sub-skipped-continuation",
      submissionCreatedAt: Date.now(),
      fiberCreatedAt: Date.now()
    });

    expect(await agent.getStatusForTest()).toBe("processing");

    // Sweep sees fresh fiber evidence and defers — the turn is still
    // genuinely recoverable at this point.
    await agent.recoverSubmissionsForTest();
    expect(
      await agent.inspectSubmissionForTest("sub-skipped-continuation")
    ).toMatchObject({ status: "running" });
    expect(await agent.getStatusForTest()).toBe("processing");

    // Alarm-driven fiber recovery + the scheduled continuation drive
    // the recovered submission to a terminal state.
    await agent.recoverChatFiberForTest("sub-skipped-continuation");
    await agent.continueRecoveredChatForTest("sub-skipped-continuation");

    const submission = await agent.waitForSubmissionStatusForTest(
      "sub-skipped-continuation",
      "skipped"
    );
    expect(submission?.status).toBe("skipped");

    // The canonical terminal transition fired on the continuation path
    // too — same hooks, same release contract, regardless of which
    // recovery route the turn took.
    expect(await agent.getChatErrorLogForTest()).toHaveLength(1);
    const responses = await agent.getResponseLogForTest();
    expect(responses).toHaveLength(1);
    expect(responses[0].status).toBe("error");
    expect(await agent.getStatusForTest()).toBe("ready");
  });

  it("dedupes terminal hooks per requestId across the in-process and reconciliation paths", async () => {
    const agent = await freshAgent();

    // In-process completion fires the response hook first; a later
    // reconciliation attempt for the same requestId must be a no-op,
    // since the turn already ended exactly once from the user's view.
    await agent.fireResponseHookForTest("req-completed-first", "completed");
    await agent.fireInterruptedTurnHooksForTest(
      "req-completed-first",
      "late reconciliation attempt"
    );

    let responses = await agent.getResponseLogForTest();
    expect(responses).toHaveLength(1);
    expect(responses[0].status).toBe("completed");
    expect(await agent.getChatErrorLogForTest()).toHaveLength(0);

    // Crash reconciliation fires both hooks first; a later in-process
    // response-hook call (e.g. a stale stream completion) must not
    // double-fire either.
    await agent.fireInterruptedTurnHooksForTest(
      "req-reconciled-first",
      "interrupted"
    );
    await agent.fireResponseHookForTest("req-reconciled-first", "completed");

    responses = await agent.getResponseLogForTest();
    expect(responses).toHaveLength(2);
    expect(responses[1].status).toBe("error");
    const errors = await agent.getChatErrorLogForTest();
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBe("interrupted");
  });
});
