/** Which nav group a page belongs to, shared by the desktop menu and the phone bar. */
export type NavGroupShape<P extends string> = { id: string; items: { page: P }[] };

export function groupOfPage<P extends string, G extends NavGroupShape<P>>(groups: G[], page: NoInfer<P>): G | null {
  return groups.find((group) => group.items.some((item) => item.page === page)) ?? null;
}

/** What a phone tab shows: the active page when it is inside the group, else
    the group's first page — so the bar always says where you are. */
export function tabFace<P extends string, I extends { page: P }>(items: I[], activePage: NoInfer<P>): I | null {
  return items.find((item) => item.page === activePage) ?? items[0] ?? null;
}
