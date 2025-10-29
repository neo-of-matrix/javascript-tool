import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 9000,
    proxy: {
      '/api': {
        target: 'http://localhost:3000', // 根据你的后端服务地址调整
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, '/api')
      }
    }
  }
})