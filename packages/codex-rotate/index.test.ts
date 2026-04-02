import { describe, expect, test } from "bun:test";

import type { PendingCredential, StoredCredential } from "./automation.ts";
import {
  generateRandomAdultBirthDate,
  resolveCredentialBirthDate,
  shouldUseStoredCredentialRelogin,
} from "./index.ts";

const storedCredential: StoredCredential = {
  email: "dev.user+1@gmail.com",
  password: "Password123!@#",
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
  password: "Password123!@#",
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
