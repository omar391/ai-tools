import { describe, expect, test } from "bun:test";

import { isSuppressedFastBrowserEventLine } from "./automation.ts";

describe("fast-browser daemon event visibility", () => {
  test("suppresses only daemon heartbeats", () => {
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"heartbeat","message":"still alive"}',
      ),
    ).toBe(true);
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"queued","message":"waiting"}',
      ),
    ).toBe(false);
    expect(
      isSuppressedFastBrowserEventLine(
        '__FAST_BROWSER_EVENT__{"phase":"daemon","status":"running","message":"starting"}',
      ),
    ).toBe(false);
  });
});
