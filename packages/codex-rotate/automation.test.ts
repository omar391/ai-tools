import { describe, expect, test } from "bun:test";

import {
  buildAccountFamilyEmail,
  CODEX_ROTATE_ACCOUNT_FLOW_FILE,
  buildCodexRotateOpenAiTempProfileName,
  computeNextAccountFamilySuffix,
  computeNextGmailAliasSuffix,
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
    expect(normalizeGmailBaseEmail("Dev.User+17@gmail.com")).toBe("dev.user@gmail.com");
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
    expect(normalizeBaseEmailFamily("Dev.{N}@HotspotPrime.com")).toBe("dev.{n}@hotspotprime.com");
  });

  test("builds a concrete email from a templated family", () => {
    expect(buildAccountFamilyEmail("dev.{N}@hotspotprime.com", 7)).toBe("dev.7@hotspotprime.com");
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
    expect(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"))
      .toBe(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"));
  });

  test("changes the retained OpenAI temp profile when the workflow run stamp changes", () => {
    expect(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:12.000Z"))
      .not.toBe(buildCodexRotateOpenAiTempProfileName("2026-03-22T10:11:13.000Z"));
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
    expect(resolveCreateBaseEmail("other@gmail.com", "dev.user@gmail.com")).toBe("other@gmail.com");
  });

  test("uses the discovered profile email when no explicit base email is provided", () => {
    expect(resolveCreateBaseEmail(null, "Dev.User+4@gmail.com")).toBe("dev.user@gmail.com");
  });

  test("defaults to the Astronlab template when no hint is available", () => {
    expect(resolveCreateBaseEmail(null, null)).toBe("dev.{n}@astronlab.com");
  });

  test("ignores legacy Gmail hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.user@gmail.com")).toBe(false);
    expect(shouldUseDefaultCreateFamilyHint("dev.user+4@gmail.com")).toBe(false);
  });

  test("keeps templated hints on the default create path", () => {
    expect(shouldUseDefaultCreateFamilyHint("dev.{N}@astronlab.com")).toBe(true);
  });

  test("accepts an explicit templated base email family", () => {
    expect(resolveCreateBaseEmail("dev.{N}@hotspotprime.com", null)).toBe("dev.{n}@hotspotprime.com");
  });

  test("matches the most likely Gmail base email for a managed profile name", () => {
    expect(selectBestEmailForManagedProfile("dev-1", [
      "arjuda.anjum@gmail.com",
      "dev.2.astronlab@gmail.com",
      "1.dev.astronlab@gmail.com",
    ])).toBe("1.dev.astronlab@gmail.com");
  });

  test("scores exact profile-token matches above generic matches", () => {
    expect(scoreEmailForManagedProfileName("dev-1", "1.dev.astronlab@gmail.com")).toBeGreaterThan(
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
  test("ignores legacy defaults while keeping account and family data", () => {
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
      accounts: {},
      pending: {},
    });

    expect("defaults" in store).toBe(false);
    expect(Object.keys(store.families)).toEqual(["dev-1::dev.user@gmail.com"]);
  });

  test("keeps legacy passwords in memory until the record is migrated", () => {
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

    expect(store.accounts["dev.user+1@gmail.com"]?.legacy_password).toBe("pw-1");
    expect(store.accounts["dev.user+1@gmail.com"]?.account_secret_ref).toBeNull();
  });

  test("drops legacy passwords from saved records once a Bitwarden ref exists", () => {
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

    const serialized = serializeCredentialStore(store);
    expect(serialized.accounts["dev.user+1@gmail.com"]).toEqual({
      email: "dev.user+1@gmail.com",
      account_secret_ref: makeSecretRef("bw-dev-user-1"),
      profile_name: "dev-1",
      base_email: "dev.user@gmail.com",
      suffix: 1,
      selector: "dev.user+1@gmail.com_free",
      alias: null,
      created_at: "2026-03-20T00:00:00.000Z",
      updated_at: "2026-03-20T00:00:00.000Z",
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

    expect(selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")?.email).toBe("dev.user+1@gmail.com");
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

    expect(selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com", "team-a")?.email).toBe("dev.user+2@gmail.com");
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

    expect(selectPendingCredentialForFamily(store, "dev-1", "dev.user@gmail.com")?.email).toBe("dev.user+1@gmail.com");
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

    expect(selectPendingBaseEmailHintForProfile(store, "dev-1")).toBe("1.dev.astronlab@gmail.com");
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

    expect(selectStoredBaseEmailHint(store, "dev-1")).toBe("1.dev.astronlab@gmail.com");
  });
});
