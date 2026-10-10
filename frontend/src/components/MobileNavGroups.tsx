import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";
import type { NavGroup } from "./NavGroups";
import { groupOfPage, tabFace } from "../lib/navGroups";
import "./MobileNavGroups.css";

type Props<P extends string> = {
  groups: NavGroup<P>[];
  activePage: P;
  onNavigate: (page: P) => void;
  onPrefetch?: (page: P) => void;
};

/**
 * The phone tab bar: the same four groups as the desktop menu instead of all
 * thirteen pages squeezed into one row (each ~28px wide at 375px, labels cut to
 * "Alike", "Gym", "Lists"). A one-page group is a plain tab; any other opens a
 * sheet above the bar listing its pages with their one-line blurbs.
 *
 * Every page stays one tap from its group — nothing was removed, only grouped.
 */
export function MobileNavGroups<P extends string>({ groups, activePage, onNavigate, onPrefetch }: Props<P>) {
  const [openId, setOpenId] = useState<string | null>(null);
  const sheetRef = useRef<HTMLDivElement | null>(null);
  const barRef = useRef<HTMLElement | null>(null);
  const activeGroup = groupOfPage(groups, activePage);
  const openGroup = groups.find((group) => group.id === openId) ?? null;

  useEffect(() => {
    if (!openId) return;
    const onDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (sheetRef.current?.contains(target) || barRef.current?.contains(target)) return;
      setOpenId(null);
    };
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") setOpenId(null);
    };
    document.addEventListener("pointerdown", onDown);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onDown);
      document.removeEventListener("keydown", onKey);
    };
  }, [openId]);

  // Navigating elsewhere (from a link inside a page, say) closes the sheet.
  useEffect(() => setOpenId(null), [activePage]);

  return (
    <>
      <nav ref={barRef} className="mobile-tabbar" aria-label="Primary">
        {groups.map((group) => {
          const face = tabFace(group.items, activePage);
          if (!face) return null;
          const single = group.items.length === 1;
          const isActive = activeGroup?.id === group.id;
          const Icon = face.Icon;
          return (
            <button
              key={`tabbar-${group.id}`}
              type="button"
              className={`mobile-tab${isActive ? " is-active" : ""}${openId === group.id ? " is-open" : ""}`}
              aria-current={isActive ? "page" : undefined}
              aria-expanded={single ? undefined : openId === group.id}
              aria-haspopup={single ? undefined : "menu"}
              onClick={() => {
                if (single) {
                  setOpenId(null);
                  onNavigate(face.page);
                } else {
                  setOpenId((current) => (current === group.id ? null : group.id));
                  group.items.forEach((item) => onPrefetch?.(item.page));
                }
              }}
            >
              <Icon size={18} strokeWidth={2.1} aria-hidden="true" />
              {/* Inside the group: the page you are on. Outside: the group's name. */}
              <span>{isActive && !single ? face.label : group.label}</span>
            </button>
          );
        })}
      </nav>
      {openGroup
        ? createPortal(
            <div ref={sheetRef} className="mobile-nav-sheet" role="menu" aria-label={openGroup.label}>
              <div className="mobile-nav-sheet-title">{openGroup.label}</div>
              {openGroup.items.map((item) => {
                const Icon = item.Icon;
                const current = item.page === activePage;
                return (
                  <button
                    key={item.page}
                    type="button"
                    role="menuitem"
                    className={`mobile-nav-item${current ? " is-current" : ""}`}
                    aria-current={current ? "page" : undefined}
                    onClick={() => {
                      setOpenId(null);
                      onNavigate(item.page);
                    }}
                  >
                    <Icon size={18} strokeWidth={2} aria-hidden="true" />
                    <span>
                      <strong>{item.label}</strong>
                      <small>{item.blurb}</small>
                    </span>
                  </button>
                );
              })}
            </div>,
            document.body,
          )
        : null}
    </>
  );
}
