import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5175,
    host: true,
    allowedHosts: [
      "nonrurally-unabsorbent-sook.ngrok-free.dev",
      process.env.VITE_ALLOWED_HOST || undefined,
    ].filter(Boolean) as string[],
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});