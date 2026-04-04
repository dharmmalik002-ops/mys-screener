import { useCallback, useEffect, useMemo, useRef, useState } from "react";

type ScrollState = {
  scrollTop: number;
  viewportHeight: number;
};

export type VirtualRow<T> = {
  index: number;
  key: string;
  item: T;
  top: number;
  height: number;
  bottom: number;
};

type UseVirtualRowsOptions<T> = {
  items: T[];
  getKey: (item: T) => string;
  getHeight: (item: T) => number;
  overscan?: number;
};

export function useMinWidth(minWidth: number) {
  const [matches, setMatches] = useState(() => {
    if (typeof window === "undefined") {
      return true;
    }
    return window.innerWidth > minWidth;
  });

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const update = () => setMatches(window.innerWidth > minWidth);
    update();
    window.addEventListener("resize", update);
    return () => window.removeEventListener("resize", update);
  }, [minWidth]);

  return matches;
}

export function useVirtualRows<T>({ items, getKey, getHeight, overscan = 420 }: UseVirtualRowsOptions<T>) {
  const containerNodeRef = useRef<HTMLDivElement | null>(null);
  const [containerNode, setContainerNode] = useState<HTMLDivElement | null>(null);
  const [scrollState, setScrollState] = useState<ScrollState>({ scrollTop: 0, viewportHeight: 0 });

  const rows = useMemo(() => {
    let top = 0;
    const nextRows: VirtualRow<T>[] = items.map((item, index) => {
      const height = getHeight(item);
      const row = {
        index,
        key: getKey(item),
        item,
        top,
        height,
        bottom: top + height,
      } satisfies VirtualRow<T>;
      top += height;
      return row;
    });

    return {
      rows: nextRows,
      totalHeight: top,
      rowByKey: new Map(nextRows.map((row) => [row.key, row] as const)),
    };
  }, [getHeight, getKey, items]);

  const syncScrollState = useCallback(() => {
    const node = containerNodeRef.current;
    if (!node) {
      return;
    }

    const nextState = {
      scrollTop: node.scrollTop,
      viewportHeight: node.clientHeight,
    } satisfies ScrollState;

    setScrollState((current) =>
      current.scrollTop === nextState.scrollTop && current.viewportHeight === nextState.viewportHeight
        ? current
        : nextState,
    );
  }, []);

  const containerRef = useCallback((node: HTMLDivElement | null) => {
    containerNodeRef.current = node;
    setContainerNode(node);
    if (node) {
      setScrollState({
        scrollTop: node.scrollTop,
        viewportHeight: node.clientHeight,
      });
    }
  }, []);

  useEffect(() => {
    if (!containerNode) {
      return;
    }

    const handleScroll = () => syncScrollState();
    containerNode.addEventListener("scroll", handleScroll, { passive: true });

    let resizeObserver: ResizeObserver | null = null;
    if (typeof ResizeObserver !== "undefined") {
      resizeObserver = new ResizeObserver(() => syncScrollState());
      resizeObserver.observe(containerNode);
    }

    syncScrollState();
    return () => {
      containerNode.removeEventListener("scroll", handleScroll);
      resizeObserver?.disconnect();
    };
  }, [containerNode, syncScrollState]);

  const visibleRows = useMemo(() => {
    const start = Math.max(0, scrollState.scrollTop - overscan);
    const end = scrollState.scrollTop + scrollState.viewportHeight + overscan;
    return rows.rows.filter((row) => row.bottom >= start && row.top <= end);
  }, [overscan, rows.rows, scrollState.scrollTop, scrollState.viewportHeight]);

  const scrollToKey = useCallback(
    (key: string) => {
      const node = containerNodeRef.current;
      const row = rows.rowByKey.get(key);
      if (!node || !row) {
        return;
      }

      const padding = 8;
      const viewportTop = node.scrollTop;
      const viewportBottom = viewportTop + node.clientHeight;
      const rowTop = Math.max(0, row.top - padding);
      const rowBottom = row.bottom + padding;

      if (rowTop < viewportTop) {
        node.scrollTop = rowTop;
        return;
      }

      if (rowBottom > viewportBottom) {
        node.scrollTop = Math.max(0, rowBottom - node.clientHeight);
      }
    },
    [rows.rowByKey],
  );

  return {
    containerRef,
    scrollToKey,
    totalHeight: rows.totalHeight,
    visibleRows,
  };
}