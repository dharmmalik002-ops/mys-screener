import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  build: {
    rollupOptions: {
      output: {
        manualChunks(id) {
          if (!id.includes("node_modules")) {
            return undefined;
          }
          // recharts + its d3 deps load only when fundamentals Charts/Valuation tab opens
          if (
            id.includes("recharts") ||
            id.includes("d3-scale") ||
            id.includes("d3-shape") ||
            id.includes("d3-path") ||
            id.includes("d3-color") ||
            id.includes("d3-format") ||
            id.includes("d3-interpolate") ||
            id.includes("d3-time") ||
            id.includes("d3-array") ||
            id.includes("decimal.js") ||
            id.includes("victory-vendor")
          ) {
            return "recharts-vendor";
          }
          // lightweight-charts loads with ChartPanel (on stock select)
          if (id.includes("lightweight-charts")) {
            return "lw-charts-vendor";
          }
          return "vendor";
        },
      },
    },
  },
  server: {
    port: 5173,
    host: "0.0.0.0",
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
});
