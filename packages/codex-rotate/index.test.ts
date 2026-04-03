import { describe, expect, test } from "bun:test";

import type { PendingCredential, StoredCredential } from "./automation.ts";
import {
  buildReusableAccountProbeOrder,
  findNextImmediateRoundRobinIndex,
  findNextCachedUsableAccountIndex,
  generateRandomAdultBirthDate,
  resolveCredentialBirthDate,
  shouldUseStoredCredentialRelogin,
} from "./index.ts";

const storedCredential: StoredCredential = {
  email: "dev.user+1@gmail.com",
  account_secret_ref: {
    type: "secret_ref",
    store: "bitwarden-cli",
    object_id: "bw-dev-user-1",
  },
  profile_name: "m-omar",
  base_email: "dev.user@gmail.com",
  suffix: 1,
  selector: "dev.user+1@gmail.com_free",
  alias: null,
  created_at: "2026-03-20T00:00:00.000Z",
  updated_at: "2026-03-20T00:00:00.000Z",
};

const pendingCredential: PendingCredential = {
  email: "dev.user+2@gmail.com",
  account_secret_ref: {
    type: "secret_ref",
    store: "bitwarden-cli",
    object_id: "bw-dev-user-2",
  },
  profile_name: "dev-1",
  base_email: "dev.user@gmail.com",
  suffix: 2,
  selector: null,
  alias: null,
  created_at: "2026-03-20T00:00:00.000Z",
  updated_at: "2026-03-20T00:00:00.000Z",
  started_at: "2026-03-20T00:00:00.000Z",
};

describe("relogin strategy selection", () => {
  test("uses stored credentials by default when they exist", () => {
    expect(
      shouldUseStoredCredentialRelogin(storedCredential, {
        allowEmailChange: false,
        deviceAuth: false,
        logoutFirst: true,
        manualLogin: false,
      }),
    ).toBe(true);
  });

  test("does not use stored credentials when manual login is forced", () => {
    expect(
      shouldUseStoredCredentialRelogin(storedCredential, {
        allowEmailChange: false,
        deviceAuth: false,
        logoutFirst: true,
        manualLogin: true,
      }),
    ).toBe(false);
  });

  test("does not use stored credentials for device auth relogin", () => {
    expect(
      shouldUseStoredCredentialRelogin(storedCredential, {
        allowEmailChange: false,
        deviceAuth: true,
        logoutFirst: true,
        manualLogin: true,
      }),
    ).toBe(false);
  });

  test("falls back to manual login when no stored credentials exist", () => {
    expect(
      shouldUseStoredCredentialRelogin(undefined, {
        allowEmailChange: false,
        deviceAuth: false,
        logoutFirst: true,
        manualLogin: false,
      }),
    ).toBe(false);
  });
});

describe("adult birth date generation", () => {
  test("generates a date that is at least 20 years old", () => {
    const birthDate = generateRandomAdultBirthDate(
      new Date("2026-04-02T00:00:00.000Z"),
      20,
      45,
      () => 0,
    );

    expect(birthDate).toEqual({
      birthMonth: 4,
      birthDay: 2,
      birthYear: 1981,
    });
  });

  test("can generate the newest allowed adult date at the upper edge", () => {
    const birthDate = generateRandomAdultBirthDate(
      new Date("2026-04-02T00:00:00.000Z"),
      20,
      45,
      (maxExclusive) => maxExclusive - 1,
    );

    expect(birthDate).toEqual({
      birthMonth: 4,
      birthDay: 2,
      birthYear: 2006,
    });
  });

  test("reuses an existing stored birth date when present", () => {
    expect(resolveCredentialBirthDate({
      birth_month: 7,
      birth_day: 14,
      birth_year: 1994,
    })).toEqual({
      birthMonth: 7,
      birthDay: 14,
      birthYear: 1994,
    });
  });
});

describe("cached next rotation", () => {
  test("picks the next cached usable account in round-robin order", () => {
    expect(findNextCachedUsableAccountIndex(0, [
      { last_quota_usable: true },
      { last_quota_usable: false },
      { last_quota_usable: true },
    ])).toBe(2);
  });

  test("wraps around when a later slot is not usable but an earlier one is", () => {
    expect(findNextCachedUsableAccountIndex(2, [
      { last_quota_usable: true },
      { last_quota_usable: null },
      { last_quota_usable: false },
    ])).toBe(0);
  });

  test("returns null when no later cached account is marked usable", () => {
    expect(findNextCachedUsableAccountIndex(1, [
      { last_quota_usable: false },
      { last_quota_usable: true },
      { last_quota_usable: null },
    ])).toBeNull();
  });
});

describe("immediate next rotation", () => {
  test("picks the next later account unless it is explicitly marked unusable", () => {
    expect(findNextImmediateRoundRobinIndex(0, [
      { last_quota_usable: true },
      { last_quota_usable: null },
      { last_quota_usable: true },
    ])).toBe(1);
  });

  test("skips entries explicitly marked unusable and wraps around", () => {
    expect(findNextImmediateRoundRobinIndex(1, [
      { last_quota_usable: null },
      { last_quota_usable: true },
      { last_quota_usable: false, last_quota_checked_at: "2026-04-02T00:00:00.000Z" },
    ])).toBe(0);
  });

  test("returns null when every later account is already known unusable", () => {
    expect(findNextImmediateRoundRobinIndex(0, [
      { last_quota_usable: true },
      { last_quota_usable: false, last_quota_checked_at: "2026-04-02T00:00:00.000Z" },
      { last_quota_usable: false, last_quota_checked_at: "2026-04-02T00:00:00.000Z" },
    ])).toBeNull();
  });
});

describe("reusable account probe order", () => {
  test("prefers the current account first for manual create", () => {
    expect(buildReusableAccountProbeOrder(1, 4, "current-first")).toEqual([1, 2, 3, 0]);
  });

  test("prefers later accounts first for next rotation", () => {
    expect(buildReusableAccountProbeOrder(1, 4, "others-first")).toEqual([2, 3, 0, 1]);
  });

  test("can exclude the current account entirely", () => {
    expect(buildReusableAccountProbeOrder(1, 4, "others-only")).toEqual([2, 3, 0]);
  });
});
