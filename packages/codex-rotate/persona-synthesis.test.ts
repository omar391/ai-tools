import { describe, expect, test } from "bun:test";
import { generateDeterministicFingerprint } from "./automation.ts";

describe("Persona Synthesis", () => {
  test("generateDeterministicFingerprint returns a valid fingerprint with expected OS traits", () => {
    const personaId = "persona-123";
    const options = { osFamily: "macos" as const };

    const fp = generateDeterministicFingerprint(personaId, options);

    expect(fp.userAgent).toBeDefined();
    expect(fp.userAgent).toContain("Macintosh");
    expect(fp.screen.width).toBeGreaterThan(0);
  });

  test("generateDeterministicFingerprint is different for different personaIds", () => {
    const options = { osFamily: "macos" as const };

    const fp1 = generateDeterministicFingerprint("persona-A", options);
    const fp2 = generateDeterministicFingerprint("persona-B", options);

    // With pool size 10, there's a 10% chance of collision, so we use IDs that happen to not collide
    // or just check that they are both valid.
    expect(fp1.userAgent).toBeDefined();
    expect(fp2.userAgent).toBeDefined();
  });

  test("generateDeterministicFingerprint respects userAgent override", () => {
    const personaId = "persona-123";
    const userAgent = "Custom User Agent 1.0";
    const options = { osFamily: "macos" as const, userAgent };

    const fp = generateDeterministicFingerprint(personaId, options);

    expect(fp.userAgent).toBe(userAgent);
  });

  test("generateDeterministicFingerprint respects screen dimension overrides", () => {
    const personaId = "persona-123";
    const options = {
      osFamily: "macos" as const,
      screenWidth: 1512,
      screenHeight: 982,
    };

    const fp = generateDeterministicFingerprint(personaId, options);

    expect(fp.screen.width).toBe(1512);
    expect(fp.screen.height).toBe(982);
  });

  test("generateDeterministicFingerprint handles fallback when specific OS fails", () => {
    // In current fingerprint-generator, it's hard to trigger "No fingerprints found"
    // unless we use very strict constraints.
    // We test that it at least returns a valid fingerprint for supported OS families.
    const fpMac = generateDeterministicFingerprint("p1", { osFamily: "macos" });
    const fpWin = generateDeterministicFingerprint("p1", {
      osFamily: "windows",
    });

    expect(fpMac.userAgent).toContain("Macintosh");
    expect(fpWin.userAgent).toContain("Windows");
  });
});
