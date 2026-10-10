import { describe, expect, it } from "vitest";
import { VOLUME_MIN_PX, chartPaneLayout } from "./chartPaneLayout";

const close = (a: number, b: number) => expect(a).toBeCloseTo(b, 6);

describe("chartPaneLayout", () => {
  it("keeps the previous shares on a tall chart", () => {
    const withRs = chartPaneLayout(800, { hasRs: true, tight: false, priceTop: 0.22 });
    close(withRs.volume.top, 0.88);
    close(withRs.rs!.top, 0.72);
    close(withRs.rs!.bottom, 0.14);
    close(withRs.price.bottom, 0.32);
    const noRs = chartPaneLayout(800, { hasRs: false, tight: false, priceTop: 0.22 });
    close(noRs.volume.top, 0.82);
    close(noRs.price.bottom, 0.18);
    expect(noRs.rs).toBeNull();
  });

  it("never lets volume shrink below its pixel floor in a short full screen", () => {
    for (const hasRs of [true, false]) {
      const layout = chartPaneLayout(352, { hasRs, tight: true, priceTop: 0.04 });
      expect((1 - layout.volume.top) * 352).toBeGreaterThanOrEqual(VOLUME_MIN_PX - 0.001);
      // Price stays above the panes below it.
      expect(1 - layout.price.bottom).toBeGreaterThan(layout.price.top);
      if (layout.rs) {
        expect(layout.rs.bottom).toBeGreaterThanOrEqual(1 - layout.volume.top);
        expect(layout.price.bottom).toBeGreaterThanOrEqual(1 - layout.rs.top);
      }
    }
  });

  it("uses the fixed shares before the canvas has been measured", () => {
    close(chartPaneLayout(null, { hasRs: true, tight: true, priceTop: 0.04 }).volume.top, 0.92);
  });
});
