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
                    if (id.includes("react-dom") || id.includes("react/jsx-runtime") || id.includes("/react/")) {
                        return "react-vendor";
                    }
                    if (id.includes("lightweight-charts") || id.includes("recharts")) {
                        return "charts-vendor";
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
