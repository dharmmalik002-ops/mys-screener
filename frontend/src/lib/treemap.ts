/**
 * Squarified treemap layout (Bruls, Huizing & van Wijk, 2000).
 *
 * Pure geometry — no React, no DOM, no colour. Given weighted items and a
 * rectangle, it returns one rect per item, chosen so tiles stay as close to
 * square as possible. Square-ish tiles matter here because the eye compares
 * *areas*, and it does that badly across long slivers.
 *
 * Area is proportional to `value`, always. Nothing in this file compresses or
 * rescales weights: a sector that is 20% of the market's cap gets 20% of the
 * pixels. Callers that want a different question answered (equal weight, say)
 * pass different values in.
 */

export type TreemapItem<T> = {
  value: number;
  datum: T;
};

export type TreemapRect<T> = {
  x: number;
  y: number;
  w: number;
  h: number;
  value: number;
  datum: T;
};

/**
 * Worst (largest) aspect ratio in a row, given the row's total area and the
 * length of the side it is laid along. Uses the closed form from the paper —
 * only the row's min and max item areas can be the worst — so this stays O(1)
 * per candidate instead of O(row).
 */
function worstAspect(rowArea: number, minArea: number, maxArea: number, side: number): number {
  if (rowArea <= 0 || side <= 0 || minArea <= 0) return Number.POSITIVE_INFINITY;
  const side2 = side * side;
  const rowArea2 = rowArea * rowArea;
  return Math.max((side2 * maxArea) / rowArea2, rowArea2 / (side2 * minArea));
}

export function squarify<T>(
  items: Array<TreemapItem<T>>,
  x: number,
  y: number,
  w: number,
  h: number,
): Array<TreemapRect<T>> {
  if (w <= 0 || h <= 0) return [];

  const usable = items.filter((item) => Number.isFinite(item.value) && item.value > 0);
  if (!usable.length) return [];

  const total = usable.reduce((sum, item) => sum + item.value, 0);
  if (total <= 0) return [];

  // Descending order is what makes the algorithm "squarified" — feeding it
  // unsorted values degrades it to a plain slice-and-dice.
  const scale = (w * h) / total;
  const queue = [...usable]
    .sort((a, b) => b.value - a.value)
    .map((item) => ({ item, area: item.value * scale }));

  const out: Array<TreemapRect<T>> = [];
  let rx = x;
  let ry = y;
  let rw = w;
  let rh = h;
  let cursor = 0;

  while (cursor < queue.length) {
    // Rows are laid along the shorter side of whatever space is left; that is
    // the choice that keeps aspect ratios near 1.
    const side = Math.min(rw, rh);
    const row: Array<{ item: TreemapItem<T>; area: number }> = [];
    let rowArea = 0;
    let rowMin = Number.POSITIVE_INFINITY;
    let rowMax = 0;
    let rowAspect = Number.POSITIVE_INFINITY;

    while (cursor < queue.length) {
      const next = queue[cursor];
      const nextArea = rowArea + next.area;
      const nextAspect = worstAspect(
        nextArea,
        Math.min(rowMin, next.area),
        Math.max(rowMax, next.area),
        side,
      );
      // Adding this item makes the row worse — close the row and place it.
      if (row.length && nextAspect > rowAspect) break;
      row.push(next);
      rowArea = nextArea;
      rowMin = Math.min(rowMin, next.area);
      rowMax = Math.max(rowMax, next.area);
      rowAspect = nextAspect;
      cursor += 1;
    }

    // Guard against a pathological zero-area row stalling the loop.
    if (!row.length) break;

    const horizontal = rw < rh;
    if (horizontal) {
      const rowHeight = Math.min(rh, rowArea / rw);
      let offset = rx;
      for (const entry of row) {
        const tileWidth = rowHeight > 0 ? entry.area / rowHeight : 0;
        out.push({
          x: offset,
          y: ry,
          w: tileWidth,
          h: rowHeight,
          value: entry.item.value,
          datum: entry.item.datum,
        });
        offset += tileWidth;
      }
      ry += rowHeight;
      rh -= rowHeight;
    } else {
      const rowWidth = Math.min(rw, rowArea / rh);
      let offset = ry;
      for (const entry of row) {
        const tileHeight = rowWidth > 0 ? entry.area / rowWidth : 0;
        out.push({
          x: rx,
          y: offset,
          w: rowWidth,
          h: tileHeight,
          value: entry.item.value,
          datum: entry.item.datum,
        });
        offset += tileHeight;
      }
      rx += rowWidth;
      rw -= rowWidth;
    }

    if (rw <= 0.5 || rh <= 0.5) break;
  }

  return out;
}

/** Shrinks a rect by `pad` on every side, never past zero. */
export function insetRect(rect: { x: number; y: number; w: number; h: number }, pad: number) {
  const w = Math.max(0, rect.w - pad * 2);
  const h = Math.max(0, rect.h - pad * 2);
  return { x: rect.x + pad, y: rect.y + pad, w, h };
}
