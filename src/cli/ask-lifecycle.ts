import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import type {
  OrchestrationLatestTurn,
  OrchestrationMessage,
  OrchestrationSession,
  OrchestrationThread,
  OrchestrationThreadShell,
} from "@t3tools/contracts";

import type { T3ApplicationService } from "../application/service.ts";
import type { T3CliEnvShape } from "../config/env/env.ts";
import { ThreadSessionError } from "../domain/error.ts";
import { AskThreadArchivedError, AskThreadPendingRequestError } from "./error.ts";
import { formatWaitEventNdjson } from "./format/thread.ts";
import { isInteractiveHumanTerminal } from "./format/output.ts";
import { T3Output } from "./output/service.ts";
import { CliRuntime } from "./runtime/service.ts";

export const archivePolicyChoices = ["never", "always", "on-success", "on-failure"] as const;

export type ArchivePolicy = (typeof archivePolicyChoices)[number];
export type AskFormat = "human" | "json" | "ndjson";

export type AskArchiveResult =
  | {
      readonly policy: ArchivePolicy;
      readonly status: "skipped";
    }
  | {
      readonly policy: ArchivePolicy;
      readonly status: "archived";
      readonly sequence: number;
    }
  | {
      readonly policy: ArchivePolicy;
      readonly status: "failed";
      readonly error: string;
    };

export interface AskExecutionState {
  readonly archivePolicy: ArchivePolicy;
  threadId: string | undefined;
  createdThread: boolean;
  dispatched: boolean;
  askTurnId: string | null;
  archiveResult: AskArchiveResult | undefined;
}

export function resolveAskFormat(
  format: "auto" | AskFormat,
  cliRuntime: CliRuntime["Service"],
  t3CliEnv: T3CliEnvShape,
): AskFormat {
  if (format !== "auto") {
    return format;
  }
  return isInteractiveHumanTerminal(cliRuntime, t3CliEnv) ? "human" : "json";
}

export function ensureAskTargetAvailable(thread: OrchestrationThreadShell) {
  if (thread.archivedAt !== null) {
    return Effect.fail(
      new AskThreadArchivedError({
        message: `thread is archived: ${thread.id}`,
        threadId: thread.id,
      }),
    );
  }
  if (thread.hasPendingApprovals || thread.hasPendingUserInput) {
    return Effect.fail(
      new AskThreadPendingRequestError({
        message: `thread has a pending approval or user-input request: ${thread.id}`,
        threadId: thread.id,
      }),
    );
  }
  return Effect.void;
}

export function waitForBusyThread(application: T3ApplicationService, threadId: string) {
  return application.watchThread(threadId).pipe(
    Stream.tap((event) =>
      event.type === "status"
        ? application.showThread(threadId).pipe(Effect.flatMap(ensureNoPendingRequest))
        : Effect.void,
    ),
    Stream.runLast,
    Effect.flatMap((last) => {
      const event = Option.getOrUndefined(last);
      if (event?.type === "done") {
        return Effect.void;
      }
      return Effect.fail(
        new ThreadSessionError({
          message: `thread wait ended without a terminal event: ${threadId}`,
          threadId,
        }),
      );
    }),
  );
}

export function waitForAskThread(
  application: T3ApplicationService,
  output: T3Output["Service"],
  input: {
    readonly threadId: string;
    readonly format: AskFormat;
    readonly messageId: string;
    readonly state: AskExecutionState;
  },
) {
  let lastStatus = "";
  let askWindowOpen = false;
  let askMessageCreatedAt: string | null = null;
  let askTurnObserved = false;
  let turnComplete = false;
  const observeAskTurnLifecycle = (
    session: OrchestrationSession | null,
    latestTurn: OrchestrationLatestTurn | null,
  ) => {
    if (input.state.askTurnId === null && askWindowOpen) {
      const requestedTurnId =
        askMessageCreatedAt !== null && latestTurn?.requestedAt === askMessageCreatedAt
          ? latestTurn.turnId
          : null;
      input.state.askTurnId = session?.activeTurnId ?? requestedTurnId;
    }
    const askTurnId = input.state.askTurnId;
    if (askTurnId === null) {
      return;
    }
    const askIsActive = session?.activeTurnId === askTurnId;
    const askIsLatest = latestTurn?.turnId === askTurnId;
    if (askIsActive || askIsLatest) {
      askTurnObserved = true;
    }
    if (!askTurnObserved) {
      return;
    }
    if (!askIsLatest) {
      if (
        askMessageCreatedAt !== null &&
        latestTurn !== null &&
        latestTurn.requestedAt > askMessageCreatedAt
      ) {
        turnComplete = true;
      }
      return;
    }
    if (latestTurn.state !== "running") {
      turnComplete = true;
      return;
    }
    if (!askIsActive && session?.status !== "starting" && session?.status !== "running") {
      turnComplete = true;
    }
  };
  return Effect.gen(function* () {
    if (input.format === "human") {
      yield* output.writeStderr(`waiting for ${input.threadId}...\n`);
    }
    const last = yield* application.watchThread(input.threadId).pipe(
      Stream.tap((event) =>
        Effect.gen(function* () {
          if (event.type === "status") {
            const thread = yield* application.showThread(input.threadId);
            observeAskTurnLifecycle(thread.session, thread.latestTurn);
            if (
              thread.session?.status === "error" &&
              input.state.askTurnId !== null &&
              thread.latestTurn?.turnId === input.state.askTurnId
            ) {
              yield* Effect.fail(
                new ThreadSessionError({
                  threadId: input.threadId,
                  message: thread.session.lastError ?? "thread ended with error",
                }),
              );
            }
            if (!turnComplete) {
              yield* ensureNoPendingRequest(thread);
            }
          }
          if (event.type === "thread") {
            const userIndex = event.thread.messages.findIndex(
              (message) => message.id === input.messageId && message.role === "user",
            );
            if (userIndex !== -1) {
              askMessageCreatedAt = event.thread.messages[userIndex]?.createdAt ?? null;
              const following = event.thread.messages.slice(userIndex + 1);
              const nextUserIndex = following.findIndex((message) => message.role === "user");
              const askMessages =
                nextUserIndex === -1 ? following : following.slice(0, nextUserIndex);
              const turnId = askMessages.findLast(
                (message) => message.role === "assistant" && message.turnId !== null,
              )?.turnId;
              if (turnId !== undefined && turnId !== null) {
                input.state.askTurnId = turnId;
                askTurnObserved = true;
              }
              askWindowOpen = nextUserIndex === -1;
              if (!askWindowOpen) {
                turnComplete = true;
              }
            }
            const answer = selectAskAnswer(event.thread, input.messageId, input.state.askTurnId);
            const answerTurnId = answer?.turnId;
            if (answerTurnId !== undefined && answerTurnId !== null) {
              input.state.askTurnId = answerTurnId;
              askTurnObserved = true;
            }
            observeAskTurnLifecycle(event.thread.session, event.thread.latestTurn);
          } else if (event.type === "message") {
            if (event.message.id === input.messageId) {
              askWindowOpen = true;
              askMessageCreatedAt = event.message.createdAt;
            } else if (askWindowOpen && event.message.role === "user") {
              askWindowOpen = false;
              turnComplete = true;
            } else if (
              askWindowOpen &&
              event.message.role === "assistant" &&
              event.message.turnId !== null
            ) {
              if (
                input.state.askTurnId === null ||
                input.state.askTurnId === event.message.turnId
              ) {
                input.state.askTurnId = event.message.turnId;
                askTurnObserved = true;
              }
            }
          }
          if (input.format === "ndjson") {
            yield* output.printNdjson(formatWaitEventNdjson(event));
            return;
          }
          if (input.format === "human" && event.type === "status" && event.status !== lastStatus) {
            lastStatus = event.status;
            yield* output.writeStderr(`${input.threadId}: ${event.status}\n`);
          }
        }),
      ),
      Stream.takeUntil((event) => turnComplete || event.type === "done"),
      Stream.runLast,
    );
    const event = Option.getOrUndefined(last);
    if (!turnComplete && event?.type !== "done") {
      return yield* Effect.fail(
        new ThreadSessionError({
          message: `thread wait ended without a terminal event: ${input.threadId}`,
          threadId: input.threadId,
        }),
      );
    }
    return yield* Effect.void;
  });
}

export function announceQueue(output: T3Output["Service"], format: AskFormat, threadId: string) {
  if (format === "ndjson") {
    return output.printNdjson({ type: "queue", status: "waiting", threadId });
  }
  if (format === "human") {
    return output.writeStderr(`thread ${threadId} is busy; waiting to ask...\n`);
  }
  return Effect.void;
}

export function selectAskAnswer(
  thread: OrchestrationThread,
  messageId: string,
  turnId: string | null,
): OrchestrationMessage | undefined {
  const userIndex = thread.messages.findIndex(
    (message) => message.id === messageId && message.role === "user",
  );
  if (userIndex === -1) {
    return undefined;
  }
  const following = thread.messages.slice(userIndex + 1);
  const nextUserIndex = following.findIndex((message) => message.role === "user");
  const askMessages = nextUserIndex === -1 ? following : following.slice(0, nextUserIndex);
  const candidates = askMessages.filter(
    (message) =>
      message.role === "assistant" &&
      !message.streaming &&
      message.text.trim().length > 0 &&
      (turnId === null || message.turnId === turnId),
  );
  return candidates.at(-1);
}

export function finalizeArchive(
  application: T3ApplicationService,
  output: T3Output["Service"],
  state: AskExecutionState,
  succeeded: boolean,
): Effect.Effect<AskArchiveResult> {
  if (state.archiveResult !== undefined) {
    return Effect.succeed(state.archiveResult);
  }
  const shouldArchive =
    state.archivePolicy === "always" ||
    (state.archivePolicy === "on-success" && succeeded) ||
    (state.archivePolicy === "on-failure" && !succeeded);
  if (
    (!state.createdThread && !state.dispatched) ||
    state.threadId === undefined ||
    !shouldArchive
  ) {
    const result = {
      policy: state.archivePolicy,
      status: "skipped",
    } satisfies AskArchiveResult;
    state.archiveResult = result;
    return Effect.succeed(result);
  }
  const threadId = state.threadId;
  return application.archiveThread(threadId).pipe(
    Effect.matchEffect({
      onFailure: (error) =>
        Effect.gen(function* () {
          const result = {
            policy: state.archivePolicy,
            status: "failed",
            error: error.message,
          } satisfies AskArchiveResult;
          state.archiveResult = result;
          yield* output
            .writeStderr(`warning: failed to archive thread ${threadId}: ${error.message}\n`)
            .pipe(Effect.ignore);
          return result;
        }),
      onSuccess: (dispatch) => {
        const result = {
          policy: state.archivePolicy,
          status: "archived",
          sequence: dispatch.sequence,
        } satisfies AskArchiveResult;
        state.archiveResult = result;
        return Effect.succeed(result);
      },
    }),
  );
}

export function cleanupInterruptedAsk(
  application: T3ApplicationService,
  output: T3Output["Service"],
  state: AskExecutionState,
) {
  if (state.threadId === undefined) {
    return Effect.void;
  }
  const threadId = state.threadId;
  return Effect.gen(function* () {
    if (state.dispatched && state.askTurnId !== null) {
      yield* application.interruptThreadTurn(threadId, state.askTurnId).pipe(
        Effect.matchEffect({
          onFailure: (error) =>
            output
              .writeStderr(`warning: failed to interrupt thread ${threadId}: ${error.message}\n`)
              .pipe(Effect.ignore),
          onSuccess: () => Effect.void,
        }),
      );
    }
    yield* finalizeArchive(application, output, state, false);
  }).pipe(Effect.asVoid);
}

export function ensureTrailingNewline(text: string) {
  return text.endsWith("\n") ? text : `${text}\n`;
}

function ensureNoPendingRequest(thread: {
  readonly id: string;
  readonly hasPendingApprovals: boolean;
  readonly hasPendingUserInput: boolean;
}) {
  if (!thread.hasPendingApprovals && !thread.hasPendingUserInput) {
    return Effect.void;
  }
  return Effect.fail(
    new AskThreadPendingRequestError({
      message: `thread requested approval or user input instead of answering: ${thread.id}`,
      threadId: thread.id,
    }),
  );
}
