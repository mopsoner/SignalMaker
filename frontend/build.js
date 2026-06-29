const fs = require('fs')
const path = require('path')

const root = __dirname
const dist = path.join(root, 'dist')
fs.rmSync(dist, { recursive: true, force: true })
fs.mkdirSync(dist, { recursive: true })
for (const file of fs.readdirSync(root)) {
  if (file.endsWith('.html') || file === 'styles.css' || file === 'app.js') {
    fs.copyFileSync(path.join(root, file), path.join(dist, file))
  }
}
const adminDir = path.join(root, 'admin')
if (fs.existsSync(adminDir)) {
  fs.cpSync(adminDir, path.join(dist, 'admin'), { recursive: true })
}
console.log('Static frontend copied to frontend/dist (no Vite/esbuild).')
