import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
  buildCodexLoginManagedBrowserWrapperPath,
  buildAccountFamilyEmail,
  CODEX_ROTATE_ACCOUNT_FLOW_FILE,
  buildCodexRotateOpenAiTempProfileName,
  computeNextAccountFamilySuffix,
  computeNextGmailAliasSuffix,
  ensureCodexLoginManagedBrowserWrapper,
  isRetryableCodexLoginWorkflowErrorMessage,
  normalizeBaseEmailFamily,
  normalizeCredentialStore,
  normalizeGmailBaseEmail,
  readWorkflowFileMetadata,
  resolveCreateBaseEmail,
  resolveManagedProfileNameFromCandidates,
  scoreEmailForManagedProfileName,
  serializeCredentialStore,
  shouldUseDefaultCreateFamilyHint,
  selectBestEmailForManagedProfile,
  selectBestSystemChromeProfileMatch,
  selectPendingBaseEmailHintForProfile,
  selectPendingCredentialForFamily,
  shouldPromptForCodexRotateSecretUnlock,
  selectStoredBaseEmailHint,
} from "./automation.ts";

function makeSecretRef(objectId: string) {
  return {
    type: "secret_ref" as const,
    store: "bitwarden-cli" as const,
    object_id: objectId,
    field_path: null,
    version: null,
  };
}

describe("gmail alias helpers", () => {
  test("normalizes the Gmail base address before suffixing", () => {
    expect(normalizeGmailBaseEmail("Dev.User+17@gmail.com")).toBe(
      "dev.user@gmail.com",
    );
  });

  test("picks the next alias suffix from known emails", () => {
    expect(
      computeNextGmailAliasSuffix("dev.user@gmail.com", 1, [
        "dev.user+1@gmail.com",
        "dev.user+7@gmail.com",
        "other@gmail.com",
      ]),
    ).toBe(2);
  });

  test("respects the persisted family counter when it is ahead of the pool", () => {
    expect(
      computeNextGmailAliasSuffix("dev.user@gmail.com", 5, [
        "dev.user+1@gmail.com",
        "dev.user+2@gmail.com",
      ]),
    ).toBe(5);
  });
});

describe("templated email family helpers", () => {
  test("normalizes a templated family address", () => {
    expect(normalizeBaseEmailFamily("Dev.{N}@HotspotPrime.com")).toBe(
      "dev.{n}@hotspotprime.com",
    );
  });

  test("builds a concrete email from a templated family", () => {
    expect(buildAccountFamilyEmail("dev.{N}@hotspotprime.com", 7)).toBe(
      "dev.7@hotspotprime.com",
    );
  });

  test("picks the next suffix from templated family emails", () => {
    expect(
      computeNextAccountFamilySuffix("dev.{N}@hotspotprime.com", 1, [
        "dev.1@hotspotprime.com",
        "dev.4@hotspotprime.com",
        "other@gmail.com",
      ]),
    ).toBe(2);
  });

  test("respects the persisted family pointer for sparse templated families", () => {
    expect(
      computeNextAccountFamilySuffix("dev.{N}@astronlab.com", 3, [
        "dev.21@astronlab.com",
      ]),
    ).toBe(3);
  });
});

describe("workflow metadata", () => {
  test("reads preferred_profile from the unified local codex-rotate workflow", () => {
    const metadata = readWorkflowFileMetadata(CODEX_ROTATE_ACCOUNT_FLOW_FILE);

    expect(metadata.preferredProfileName).toBe("dev-1");
    expect(metadata.preferredEmail).toBeNull();
  });
});

describe("temporary profile naming", () => {
  test("derives the same retained OpenAI temp profile for the same workflow run stamp", () => {
    expect(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"),
    ).toBe(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"));
  });

  test("changes the retained OpenAI temp profile when the workflow run stamp changes", () => {
    expect(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"),
    ).not.toBe(
      buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:13.000Z"),
    );
  });
});

describe("codex login managed-browser wrapper", () => {
  test("derives a stable wrapper path for the same profile and codex binary", () => {
    expect(buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex")).toBe(
      buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex"),
    );
  });

  test("changes the wrapper path when the profile changes", () => {
    expect(buildCodexLoginManagedBrowserWrapperPath("dev-1", "codex")).not.toBe(
      buildCodexLoginManagedBrowserWrapperPath("dev-2", "codex"),
    );
  });

  test("routes macOS open-based login launches through the managed-profile opener", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-wrapper-"));
    const openerLogPath = join(fixtureRoot, "opener-log.json");
    const openerPath = join(fixtureRoot, "fake-opener.mjs");
    const codexPath = join(fixtureRoot, "fake-codex.sh");

    writeFileSync(
      openerPath,
      [
        "#!/usr/bin/env node",
        'import { writeFileSync } from "node:fs";',
        "const logPath = process.env.CODEX_ROTATE_TEST_OPENER_LOG;",
        "writeFileSync(logPath, JSON.stringify({",
        "  argv: process.argv.slice(2),",
        "  profile: process.env.FAST_BROWSER_PROFILE || null,",
        "  browser: process.env.BROWSER || null,",
        "}));",
      ].join("\n"),
      { encoding: "utf8", mode: 0o700 },
    );
    writeFileSync(
      codexPath,
      [
        "#!/bin/sh",
        'open "https://auth.openai.com/oauth/authorize?state=test-wrapper"',
        "exit 0",
      ].join("\n"),
      { mode: 0o700 },
    );

    const previousOpener = process.env.CODEX_ROTATE_BROWSER_OPENER_BIN;
    const previousLog = process.env.CODEX_ROTATE_TEST_OPENER_LOG;

    try {
      process.env.CODEX_ROTATE_BROWSER_OPENER_BIN = openerPath;
      process.env.CODEX_ROTATE_TEST_OPENER_LOG = openerLogPath;

      const wrapperPath = ensureCodexLoginManagedBrowserWrapper(
        "managed-dev-1",
        codexPath,
      );
      const result = spawnSync(wrapperPath, ["login"], {
        encoding: "utf8",
        env: process.env,
      });

      expect(result.status).toBe(0);
      const logged = JSON.parse(readFileSync(openerLogPath, "utf8")) as {
        argv: string[];
        profile: string | null;
        browser: string | null;
      };
      expect(logged.profile).toBe("managed-dev-1");
      expect(logged.argv).toContain(
        "https://auth.openai.com/oauth/authorize?state=test-wrapper",
      );
      expect(logged.browser).toBe(openerPath);
    } finally {
      if (previousOpener === undefined) {
        delete process.env.CODEX_ROTATE_BROWSER_OPENER_BIN;
      } else {
        process.env.CODEX_ROTATE_BROWSER_OPENER_BIN = previousOpener;
      }
      if (previousLog === undefined) {
        delete process.env.CODEX_ROTATE_TEST_OPENER_LOG;
      } else {
        process.env.CODEX_ROTATE_TEST_OPENER_LOG = previousLog;
      }
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("automation bridge transport", () => {
  test("accepts request payloads through --request-file", () => {
    const fixtureRoot = mkdtempSync(join(tmpdir(), "codex-rotate-bridge-"));
    const requestPath = join(fixtureRoot, "request.json");

    writeFileSync(
      requestPath,
      JSON.stringify({
        command: "read-workflow-metadata",
        payload: {
          filePath: CODEX_ROTATE_ACCOUNT_FLOW_FILE,
        },
      }),
      "utf8",
    );

    try {
      const result = spawnSync(
        "bun",
        [
          "packages/codex-rotate/automation-bridge.ts",
          "--request-file",
          requestPath,
        ],
        {
          cwd: join(import.meta.dir, "..", ".."),
          encoding: "utf8",
        },
      );

      expect(result.status).toBe(0);
      const response = JSON.parse(result.stdout) as {
        ok: boolean;
        result?: { preferredProfileName?: string | null };
      };
      expect(response.ok).toBe(true);
      expect(response.result?.preferredProfileName).toBe("dev-1");
    } finally {
      rmSync(fixtureRoot, { recursive: true, force: true });
    }
  });
});

describe("secret unlock prompt policy", () => {
  test("allows the bridge to force interactive secret unlock prompts", () => {
    const previous = process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK;
    try {
      process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK = "1";
      expect(shouldPromptForCodexRotateSecretUnlock()).toBe(true);
    } finally {
      if (previous === undefined) {
        delete process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK;
      } else {
        process.env.CODEX_ROTATE_ALLOW_INTERACTIVE_SECRET_UNLOCK = previous;
      }
    }
  });
});

describe("codex login retry policy", () => {
  test("retries when verification code collection is not ready yet", () => {
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "signup-verification-code-missing",
      ),
    ).toBe(true);
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "login-verification-submit-stuck:email_verification:https://auth.openai.com/email-verification",
      ),
    ).toBe(true);
  });

  test("does not retry unrelated managed-browser failures", () => {
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "OpenAI rejected the stored password",
      ),
    ).toBe(false);
    expect(
      isRetryableCodexLoginWorkflowErrorMessage(
        "device auth failed with status 429",
      ),
    ).toBe(false);
  });
});

describe("create resolution helpers", () => {
  test("prefers an explicit profile over workflow preferred_profile", () => {
    expect(
      resolveManagedProfileNameFromCandidates(["dev-1", "other"], {
        requestedProfileName: "other",
        preferredProfileName: "dev-1",
        preferredProfileSource: "/tmp/workflow.yaml",
        defaultProfileName: "dev-1",
      }),
    ).toBe("other");
  });

  test("uses workflow preferred_profile when no explicit profile is provided", () => {
    expect(
      resolveManagedProfileNameFromCandidates(["dev-1", "other"], {
        preferredProfileName: "dev-1",
        preferredProfileSource: "/tmp/workflow.yaml",
        defaultProfileName: "other",
      }),
    ).toBe("dev-1");
  });

  test("prefers explicit base email over the discovered profile email", () => {
    expect(
      resolveCreateBaseEmail("other@gmail.com", "dev.user@gmail.com"),
    ).toBe("other@gmail.com");
  });

  test("uses the discovered profile email when no explicit base email is provided", () => {
    expect(resolveCreateBaseEmail(null, "Dev.User+4@gmail.com")).toBe(
      "dev.user@gmail.com",
    );
  });

  test("defaults to the Astronlab template when no hint is available", () => {
    expect(resolveCreateBaseEmail(null, null)).toBe("dev.{n}@astronlab.com");
  });

  test("ignores legacy Gmail hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.user@gmail.com")).toBe(false);
    expect(shouldUseDefaultCreateFamilyHint("dev.user+4@gmail.com")).toBe(
      false,
    );
  });

  test("keeps templated hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.{N}@astronlab.com")).toBe(
      true,
    );
  });

  test("accepts an explicit templated base email family", () => {
    expect(resolveCreateBaseEmail("dev.{N}@hotspotprime.com", null)).toBe(
      "dev.{n}@hotspotprime.com",
    );
  });

  test("matches the most likely Gmail base email for a managed profile name", () => {
    expect(
      selectBestEmailForManagedProfile("dev-1", [
        "arjuda.anjum@gmail.com",
        "dev.2.astronlab@gmail.com",
        "1.dev.astronlab@gmail.com",
      ]),
    ).toBe("1.dev.astronlab@gmail.com");
  });

  test("scores exact profile-token matches above generic matches", () => {
    expect(
      scoreEmailForManagedProfileName("dev-1", "1.dev.astronlab@gmail.com"),
    ).toBeGreaterThan(
      scoreEmailForManagedProfileName("dev-1", "dev.2.astronlab@gmail.com"),
    );
  });

  test("picks the best matching system Chrome profile by its available Gmail accounts", () => {
    const match = selectBestSystemChromeProfileMatch("dev-1", [
      {
        directory: "Profile 1",
        name: "Alamin",
        emails: ["mohammadalamin4512@gmail.com"],
      },
      {
        directory: "Default",
        name: "Arjuda",
        emails: [
          "arjuda.anjum@gmail.com",
          "dev.2.astronlab@gmail.com",
          "1.dev.astronlab@gmail.com",
        ],
      },
    ]);

    expect(match?.directory).toBe("Default");
    expect(match?.matchedEmail).toBe("1.dev.astronlab@gmail.com");
  });
});

describe("credential store normalization", () => {
  test("ignores legacy defaults while keeping family data", () => {
    const store = normalizeCredentialStore({
      version: 1,
      defaults: {
        profile_name: "old-profile",
        base_email: "old@gmail.com",
        updated_at: "2026-03-20T00:00:00.000Z",
      },
      families: {
        "dev-1::dev.user@gmail.com": {
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          next_suffix: 3,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
          last_created_email: "dev.user+2@gmail.com",
        },
      },
      pending: {},
    });

    expect("defaults" in store).toBe(false);
    expect(Object.keys(store.families)).toEqual(["dev-1::dev.user@gmail.com"]);
  });

  test("migrates legacy accounts into families during normalization", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          password: "pw-1",
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect("accounts" in (store as Record<string, unknown>)).toBe(false);
    expect(store.families["dev-1::dev.user@gmail.com"]).toEqual({
      profile_name: "dev-1",
      base_email: "dev.user@gmail.com",
      next_suffix: 2,
      created_at: "2026-03-20T00:00:00.000Z",
      updated_at: "2026-03-20T00:00:00.000Z",
      last_created_email: "dev.user+1@gmail.com",
    });
  });

  test("writes only families and pending after normalizing old account records", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          password: "pw-1",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    const serialized = serializeCredentialStore(store) as Record<
      string,
      unknown
    >;
    expect("accounts" in serialized).toBe(false);
    expect(serialized.families).toEqual({
      "dev-1::dev.user@gmail.com": {
        profile_name: "dev-1",
        base_email: "dev.user@gmail.com",
        next_suffix: 2,
        created_at: "2026-03-20T00:00:00.000Z",
        updated_at: "2026-03-20T00:00:00.000Z",
        last_created_email: "dev.user+1@gmail.com",
      },
    });
  });

  test("ignores persisted Bitwarden refs from old files and relies on runtime lookup instead", () => {
    const store = normalizeCredentialStore({
      accounts: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: "dev.user+1@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect(store.families["dev-1::dev.user@gmail.com"]).toEqual({
      profile_name: "dev-1",
      base_email: "dev.user@gmail.com",
      next_suffix: 2,
      created_at: "2026-03-20T00:00:00.000Z",
      updated_at: "2026-03-20T00:00:00.000Z",
      last_created_email: "dev.user+1@gmail.com",
    });
  });
});

describe("pending credential reuse", () => {
  test("drains the oldest pending credential for the same family first", () => {
    const store = normalizeCredentialStore({
      pending: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T00:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "dev.user+3@gmail.com": {
          email: "dev.user+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-3"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 3,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T03:00:00.000Z",
          started_at: "2026-03-20T03:00:00.000Z",
        },
      },
    });

    expect(
      selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")
        ?.email,
    ).toBe("dev.user+1@gmail.com");
  });

  test("can restrict reuse to a matching alias when provided", () => {
    const store = normalizeCredentialStore({
      pending: {
        "dev.user+2@gmail.com": {
          email: "dev.user+2@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-2"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 2,
          selector: null,
          alias: "team-a",
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T02:00:00.000Z",
          started_at: "2026-03-20T02:00:00.000Z",
        },
        "dev.user+3@gmail.com": {
          email: "dev.user+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-3"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 3,
          selector: null,
          alias: "team-b",
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T03:00:00.000Z",
          started_at: "2026-03-20T03:00:00.000Z",
        },
      },
    });

    expect(
      selectPendingCredentialForFamily(
        store,
        "dev-1",
        "dev.user@gmail.com",
        "team-a",
      )?.email,
    ).toBe("dev.user+2@gmail.com");
  });

  test("still prefers the lowest suffix even if a newer pending entry was touched later", () => {
    const store = normalizeCredentialStore({
      pending: {
        "dev.user+1@gmail.com": {
          email: "dev.user+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-1"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-20T05:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "dev.user+2@gmail.com": {
          email: "dev.user+2@gmail.com",
          account_secret_ref: makeSecretRef("bw-dev-user-2"),
          profile_name: "dev-1",
          base_email: "dev.user@gmail.com",
          suffix: 2,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:10:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-20T00:10:00.000Z",
        },
      },
    });

    expect(
      selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")
        ?.email,
    ).toBe("dev.user+1@gmail.com");
  });

  test("prefers the oldest pending family for a profile before switching to a newly discovered family", () => {
    const store = normalizeCredentialStore({
      pending: {
        "1.dev.astronlab+1@gmail.com": {
          email: "1.dev.astronlab+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-1"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
        "arjuda.anjum+1@gmail.com": {
          email: "arjuda.anjum+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-arjuda-1"),
          profile_name: "dev-1",
          base_email: "arjuda.anjum@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-21T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          started_at: "2026-03-21T00:00:00.000Z",
        },
      },
    });

    expect(selectPendingBaseEmailHintForProfile(store, "dev-1")).toBe(
      "1.dev.astronlab@gmail.com",
    );
  });
});

describe("stored base-email hints", () => {
  test("prefers the most common and recent base email for a managed profile", () => {
    const store = normalizeCredentialStore({
      families: {
        "dev-1::1.dev.astronlab@gmail.com": {
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          next_suffix: 4,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T00:00:00.000Z",
          last_created_email: "1.dev.astronlab+3@gmail.com",
        },
      },
      accounts: {
        "1.dev.astronlab+3@gmail.com": {
          email: "1.dev.astronlab+3@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-3"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 3,
          selector: "1.dev.astronlab+3@gmail.com_free",
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T01:00:00.000Z",
        },
      },
      pending: {
        "1.dev.astronlab+1@gmail.com": {
          email: "1.dev.astronlab+1@gmail.com",
          account_secret_ref: makeSecretRef("bw-1-dev-astronlab-1"),
          profile_name: "dev-1",
          base_email: "1.dev.astronlab@gmail.com",
          suffix: 1,
          selector: null,
          alias: null,
          created_at: "2026-03-20T00:00:00.000Z",
          updated_at: "2026-03-21T02:00:00.000Z",
          started_at: "2026-03-20T00:00:00.000Z",
        },
      },
    });

    expect(selectStoredBaseEmailHint(store, "dev-1")).toBe(
      "1.dev.astronlab@gmail.com",
    );
  });
});
