import { createSSE } from './sse/index'
import './style.css'

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `<div>javascript 工具库</div>`
createSSE()