import { expect, it } from "vitest";

import { channelLines, shrinkQuestion } from "@/lib/channels";

it("counts the non-empty lines, trimmed", () => {
  expect(channelLines(" a \n\n@b\n   \n")).toEqual(["a", "@b"]);
});

it("asks before saving an empty list", () => {
  expect(shrinkQuestion(5, 0)).toBe("Список каналов станет пустым. Сохранить?");
});

it("asks before the list shrinks to less than half", () => {
  expect(shrinkQuestion(10, 4)).toBe("Список уменьшится с 10 до 4 каналов. Сохранить?");
  expect(shrinkQuestion(10, 5)).toBeNull();
  expect(shrinkQuestion(0, 3)).toBeNull();
});
