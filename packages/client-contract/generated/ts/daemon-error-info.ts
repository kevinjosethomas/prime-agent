/* eslint-disable */
/**
 * Generated from daemon-error-info.schema.json by packages/client-contract/scripts/gen-ts.mjs.
 * Canonical source: the pa-types daemon wire types (crates/pa-types).
 * DO NOT EDIT: regenerate with `npm run gen:ts` and commit.
 */
/**
 * Structured failure info carried on error responses, tagged by `code`.
 */
export type DaemonErrorInfo =
  | {
      code: "missing_session_cwd";
      issue: unknown;
      [k: string]: unknown;
    }
  | {
      code: "session_import_file_not_found";
      filePath: string;
      [k: string]: unknown;
    }
  | {
      code: "session_already_active";
      sessionPath: string;
      activeSessionId?: string | null;
      [k: string]: unknown;
    }
  | {
      code: "session_recovering";
      activeSessionId: string;
      [k: string]: unknown;
    }
  | {
      code: "update_restarting";
      [k: string]: unknown;
    }
  | {
      code: "command_result_uncertain";
      clientId: string;
      commandId: string;
      [k: string]: unknown;
    }
  | {
      code: "update_prepare_refused";
      activeUpdateId: string;
      [k: string]: unknown;
    }
  | {
      code: "model_provider_unauthenticated";
      provider: string;
      [k: string]: unknown;
    }
  | {
      code: "worker_overloaded";
      [k: string]: unknown;
    }
  | {
      code: string;
      [k: string]: unknown;
    };

