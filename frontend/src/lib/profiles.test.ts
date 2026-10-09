import { expect, it } from "vitest";

import { avatarHue, initials, matchesProfile } from "@/lib/profiles";

const profile = { tg_id: 8008930952, username: "jae_xor", display_name: "Жан Ксор", display_username: "@jae_xor" };

it("takes the initials from the name, or from the username without the at sign", () => {
  expect(initials({ ...profile })).toBe("ЖК");
  expect(initials({ ...profile, display_name: "Жан" })).toBe("ЖА");
  expect(initials({ ...profile, display_name: null })).toBe("JX");
  expect(initials({ ...profile, display_name: null, display_username: "@belbro" })).toBe("BE");
  expect(initials({ ...profile, display_name: "  ", display_username: "id 7" })).toBe("I7");
});

it("falls back to a mark when there is nothing to take letters from", () => {
  expect(initials({ ...profile, display_name: null, display_username: "@_" })).toBe("?");
});

it("gives a user the same colour every time, and different users different ones", () => {
  expect(avatarHue(7)).toBe(avatarHue(7));
  expect(avatarHue(7)).not.toBe(avatarHue(8));
  expect(avatarHue(8008930952)).toBeGreaterThanOrEqual(0);
  expect(avatarHue(8008930952)).toBeLessThan(360);
  expect(avatarHue(-5)).toBeGreaterThanOrEqual(0);
});

it("finds a profile by a part of its username, name or id, in any letter case", () => {
  expect(matchesProfile(profile, "")).toBe(true);
  expect(matchesProfile(profile, "   ")).toBe(true);
  expect(matchesProfile(profile, "XOR")).toBe(true);
  expect(matchesProfile(profile, "@jae")).toBe(true);
  expect(matchesProfile(profile, "ксор")).toBe(true);
  expect(matchesProfile(profile, "8930")).toBe(true);
  expect(matchesProfile(profile, "belbro")).toBe(false);
  expect(matchesProfile({ ...profile, display_name: null }, "ксор")).toBe(false);
});
