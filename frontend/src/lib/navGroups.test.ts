import { describe, expect, it } from "vitest";
import { groupOfPage, tabFace } from "./navGroups";

type Page = "today" | "home" | "journal" | "nowhere";
const groups: { id: string; items: { page: Page }[] }[] = [
  { id: "market", items: [{ page: "today" }, { page: "home" }] },
  { id: "journal", items: [{ page: "journal" }] },
];

describe("nav groups", () => {
  it("finds the group a page lives in", () => {
    expect(groupOfPage(groups, "home")?.id).toBe("market");
    expect(groupOfPage(groups, "journal")?.id).toBe("journal");
    expect(groupOfPage(groups, "nowhere")).toBeNull();
  });

  it("shows the active page on its group's tab, else the group's first page", () => {
    expect(tabFace(groups[0].items, "home")?.page).toBe("home");
    expect(tabFace(groups[0].items, "journal")?.page).toBe("today");
    expect(tabFace([], "home")).toBeNull();
  });
});
