import { useCallback, useEffect, useLayoutEffect, useRef, useState, type ComponentType, type KeyboardEvent } from "react";
import { createPortal } from "react-dom";
import { ChevronDown } from "lucide-react";
import "./NavGroups.css";

export type NavGroupItem<P extends string> = {
  page: P;
  label: string;
  blurb: string;
  Icon: ComponentType<{ size?: number; strokeWidth?: number; "aria-hidden"?: boolean | "true" | "false" }>;
};

export type NavGroup<P extends string> = {
  id: string;
  label: string;
  items: NavGroupItem<P>[];
};

type Props<P extends string> = {
  groups: NavGroup<P>[];
  activePage: P;
  onNavigate: (page: P) => void;
  onPrefetch?: (page: P) => void;
};

// Hover opens a menu after a beat (so sweeping the pointer across the bar does
// not flash every menu) and closes it a little later (so the gap between the
// trigger and the panel can be crossed).
const HOVER_OPEN_MS = 90;
const HOVER_CLOSE_MS = 160;

/* The header's page menu: twelve pages as four groups, SaaS-style. A group of
   one page is a plain tab. The panel portals to <body> (see gotcha 17 — a
   transformed ancestor would otherwise become the containing block for
   `position: fixed`) and is placed under its trigger from the trigger's rect. */
export function NavGroups<P extends string>({ groups, activePage, onNavigate, onPrefetch }: Props<P>) {
  const [openId, setOpenId] = useState<string | null>(null);
  const [anchor, setAnchor] = useState<{ left: number; top: number } | null>(null);
  const triggerRefs = useRef(new Map<string, HTMLButtonElement>());
  const menuRef = useRef<HTMLDivElement | null>(null);
  const timerRef = useRef<number | null>(null);
  const focusFirstRef = useRef(false);
  // A click that lands just after hover opened the menu must not close it
  // again — the pointer got there first, the click means "yes, this one".
  const openedByHoverRef = useRef(false);

  const clearTimer = () => {
    if (timerRef.current !== null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  };

  const close = useCallback((returnFocusTo?: string) => {
    clearTimer();
    setOpenId(null);
    if (returnFocusTo) triggerRefs.current.get(returnFocusTo)?.focus();
  }, []);

  const open = (id: string, focusFirst = false) => {
    clearTimer();
    focusFirstRef.current = focusFirst;
    openedByHoverRef.current = false;
    setOpenId(id);
  };

  const scheduleOpen = (id: string) => {
    clearTimer();
    if (openId === id) return;
    // Moving between triggers while a menu is already open switches at once.
    if (openId !== null) {
      openedByHoverRef.current = true;
      setOpenId(id);
      return;
    }
    timerRef.current = window.setTimeout(() => {
      openedByHoverRef.current = true;
      setOpenId(id);
    }, HOVER_OPEN_MS);
  };

  const scheduleClose = () => {
    clearTimer();
    timerRef.current = window.setTimeout(() => setOpenId(null), HOVER_CLOSE_MS);
  };

  // Placed from the trigger's rect, and re-placed whenever anything scrolls or
  // resizes — closing instead would drop the menu every time a table or the
  // ticker scrolled underneath it.
  useLayoutEffect(() => {
    if (!openId) {
      setAnchor(null);
      return;
    }
    let frame = 0;
    const place = () => {
      frame = 0;
      const rect = triggerRefs.current.get(openId)?.getBoundingClientRect();
      if (!rect) return;
      setAnchor((prev) =>
        prev && prev.left === rect.left && prev.top === rect.bottom + 8 ? prev : { left: rect.left, top: rect.bottom + 8 },
      );
    };
    const schedule = () => {
      if (!frame) frame = window.requestAnimationFrame(place);
    };
    place();
    window.addEventListener("resize", schedule);
    window.addEventListener("scroll", schedule, true);
    return () => {
      if (frame) window.cancelAnimationFrame(frame);
      window.removeEventListener("resize", schedule);
      window.removeEventListener("scroll", schedule, true);
    };
  }, [openId]);

  useEffect(() => {
    if (!openId || !anchor || !focusFirstRef.current) return;
    focusFirstRef.current = false;
    menuRef.current?.querySelector<HTMLButtonElement>("[role='menuitem']")?.focus();
  }, [openId, anchor]);

  useEffect(() => {
    if (!openId) return;
    const onPointerDown = (event: PointerEvent) => {
      const target = event.target as Node;
      if (menuRef.current?.contains(target)) return;
      if (triggerRefs.current.get(openId)?.contains(target)) return;
      close();
    };
    const onKey = (event: globalThis.KeyboardEvent) => {
      if (event.key === "Escape") close(openId);
    };
    window.addEventListener("pointerdown", onPointerDown, true);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("pointerdown", onPointerDown, true);
      window.removeEventListener("keydown", onKey);
    };
  }, [openId, close]);

  useEffect(() => clearTimer, []);

  const choose = (page: P) => {
    close();
    onNavigate(page);
  };

  const onTriggerKeyDown = (event: KeyboardEvent<HTMLButtonElement>, id: string) => {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      open(id, true);
    }
  };

  const onMenuKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp" && event.key !== "Home" && event.key !== "End") return;
    event.preventDefault();
    const items = Array.from(menuRef.current?.querySelectorAll<HTMLButtonElement>("[role='menuitem']") ?? []);
    if (items.length === 0) return;
    const current = items.indexOf(document.activeElement as HTMLButtonElement);
    let next = 0;
    if (event.key === "End") next = items.length - 1;
    else if (event.key === "ArrowDown") next = current < 0 ? 0 : (current + 1) % items.length;
    else if (event.key === "ArrowUp") next = current <= 0 ? items.length - 1 : current - 1;
    items[next].focus();
  };

  const openGroup = groups.find((group) => group.id === openId) ?? null;

  return (
    <>
      {groups.map((group) => {
        const activeItem = group.items.find((item) => item.page === activePage) ?? null;
        const className = activeItem ? "nav-button primary" : "nav-button ghost";

        if (group.items.length === 1) {
          const item = group.items[0];
          return (
            <button
              key={group.id}
              type="button"
              className={className}
              aria-current={activeItem ? "page" : undefined}
              onClick={() => choose(item.page)}
              onMouseEnter={() => {
                onPrefetch?.(item.page);
                if (openId) close();
              }}
              onFocus={() => onPrefetch?.(item.page)}
            >
              {item.label}
            </button>
          );
        }

        const isOpen = openId === group.id;
        return (
          <button
            key={group.id}
            ref={(node) => {
              if (node) triggerRefs.current.set(group.id, node);
              else triggerRefs.current.delete(group.id);
            }}
            type="button"
            className={`${className} nav-group-trigger${isOpen ? " is-open" : ""}`}
            aria-haspopup="menu"
            aria-expanded={isOpen}
            aria-controls={isOpen ? `nav-menu-${group.id}` : undefined}
            onClick={() => {
              if (!isOpen) open(group.id);
              else if (openedByHoverRef.current) openedByHoverRef.current = false;
              else close();
            }}
            onKeyDown={(event) => onTriggerKeyDown(event, group.id)}
            onMouseEnter={() => scheduleOpen(group.id)}
            onMouseLeave={scheduleClose}
          >
            <span>{group.label}</span>
            {activeItem ? <span className="nav-group-current">{activeItem.label}</span> : null}
            <ChevronDown className="nav-group-chevron" size={13} strokeWidth={2.4} aria-hidden="true" />
          </button>
        );
      })}

      {openGroup && anchor
        ? createPortal(
            <div
              ref={menuRef}
              id={`nav-menu-${openGroup.id}`}
              className="nav-menu"
              role="menu"
              aria-label={openGroup.label}
              style={{ left: anchor.left, top: anchor.top }}
              onMouseEnter={clearTimer}
              onMouseLeave={scheduleClose}
              onKeyDown={onMenuKeyDown}
            >
              {openGroup.items.map(({ page, label, blurb, Icon }) => {
                const active = page === activePage;
                return (
                  <button
                    key={page}
                    type="button"
                    role="menuitem"
                    className={active ? "nav-menu-item is-active" : "nav-menu-item"}
                    aria-current={active ? "page" : undefined}
                    onClick={() => choose(page)}
                    onMouseEnter={() => onPrefetch?.(page)}
                    onFocus={() => onPrefetch?.(page)}
                  >
                    <span className="nav-menu-icon" aria-hidden="true">
                      <Icon size={16} strokeWidth={2} aria-hidden="true" />
                    </span>
                    <span className="nav-menu-text">
                      <span className="nav-menu-label">{label}</span>
                      <span className="nav-menu-blurb">{blurb}</span>
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
