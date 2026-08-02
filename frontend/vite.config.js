import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const apiProxy = {
  '/api': { target: 'http://localhost:8080', changeOrigin: true },
  '/admin': { target: 'http://localhost:8080', changeOrigin: true },
  '/healthz': { target: 'http://localhost:8080', changeOrigin: true },
}

export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',
    port: 5000,
    allowedHosts: true,
    proxy: apiProxy,
  },
  preview: { host: '0.0.0.0', port: 5000, allowedHosts: true, proxy: apiProxy },
})
