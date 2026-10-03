import { useLayoutEffect, useRef } from "react";
import "./GlidePill.css";

/* Slides the active tab's pill from the old tab to the new one instead of
   letting it jump. Drop <GlidePill activeSelector=".active" watch={tab} /> in
   as a child of the tab container.

   It never restyles anything permanently. On a change it copies the new active
   tab's own background (so it matches Studio, Classic, light and dark without
   duplicating a colour), draws that as a ghost at the old tab's position,
   glides it across, then hands the background back to the tab. If anything is
   missing — no previous tab, reduced motion, a hidden container — the tabs
   simply switch the way they always did. */

const GLIDE_MS = 240;

type Box = { left: number; top: number; width: number; height: number };

function boxOf(el: HTMLElement, container: HTMLElement): Box {
  const r = el.getBoundingClientRect();
  const c = container.getBoundingClientRect();
  return {
    left: r.left - c.left + container.scrollLeft,
    top: r.top - c.top + container.scrollTop,
    width: r.width,
    height: r.height,
  };
}

export function GlidePill({ activeSelector, watch }: { activeSelector: string; watch: unknown }) {
  const ref = useRef<HTMLSpanElement | null>(null);
  const prevRef = useRef<{ el: HTMLElement; box: Box } | null>(null);
  const cleanupRef = useRef<(() => void) | null>(null);

  useLayoutEffect(() => {
    const pill = ref.current;
    const container = pill?.parentElement;
    if (!pill || !container) return;
    const active = container.querySelector<HTMLElement>(activeSelector);
    const prev = prevRef.current;
    prevRef.current = active ? { el: active, box: boxOf(active, container) } : null;

    cleanupRef.current?.();
    cleanupRef.current = null;
    if (!active || !prev || prev.el === active || !prev.el.isConnected) return;
    if (window.matchMedia?.("(prefers-reduced-motion: reduce)").matches) return;
    const next = prevRef.current!.box;
    if (next.width === 0 || prev.box.width === 0) return;

    // Read the tab's settled fill. Tabs animate their own background, so mid-
    // transition the computed value is still the old (transparent) one —
    // switching the transition off for the read returns where it is heading.
    const previousTransition = active.style.transition;
    active.style.transition = "none";
    const style = getComputedStyle(active);
    const bgColor = style.backgroundColor;
    const bgImage = style.backgroundImage;
    const radius = style.borderRadius;
    const shadow = style.boxShadow;
    active.style.transition = previousTransition;
    // Nothing to glide when the active state is not a filled pill.
    if ((bgColor === "rgba(0, 0, 0, 0)" || bgColor === "transparent") && (!bgImage || bgImage === "none")) return;

    if (getComputedStyle(container).position === "static") container.style.position = "relative";
    pill.style.backgroundColor = bgColor;
    pill.style.backgroundImage = bgImage;
    pill.style.borderRadius = radius;
    pill.style.boxShadow = shadow;
    pill.style.transition = "none";
    pill.style.width = `${prev.box.width}px`;
    pill.style.height = `${prev.box.height}px`;
    pill.style.transform = `translate(${prev.box.left}px, ${prev.box.top}px)`;
    pill.style.opacity = "1";
    // The tab's own fill steps aside while the ghost travels; inline
    // !important is the one thing that outranks the designs' !important rules.
    active.style.setProperty("background", "transparent", "important");
    active.style.setProperty("box-shadow", "none", "important");
    void pill.offsetWidth;
    pill.style.transition = `transform ${GLIDE_MS}ms cubic-bezier(0.23, 1, 0.32, 1), width ${GLIDE_MS}ms cubic-bezier(0.23, 1, 0.32, 1), height ${GLIDE_MS}ms cubic-bezier(0.23, 1, 0.32, 1)`;
    pill.style.width = `${next.width}px`;
    pill.style.height = `${next.height}px`;
    pill.style.transform = `translate(${next.left}px, ${next.top}px)`;

    const finish = () => {
      window.clearTimeout(timer);
      // Hand the fill back without the tab's own fade, or it would flash
      // transparent for a frame after the ghost disappears.
      const restore = active.style.transition;
      active.style.transition = "none";
      active.style.removeProperty("background");
      active.style.removeProperty("box-shadow");
      void active.offsetWidth;
      active.style.transition = restore;
      pill.style.opacity = "0";
      pill.style.transition = "none";
      cleanupRef.current = null;
    };
    const timer = window.setTimeout(finish, GLIDE_MS + 20);
    cleanupRef.current = finish;
  }, [watch, activeSelector]);

  useLayoutEffect(() => () => cleanupRef.current?.(), []);

  return <span ref={ref} className="glide-pill" aria-hidden="true" />;
}
