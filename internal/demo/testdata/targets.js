const shown = (el) => {
  const r = el.getBoundingClientRect()
  return r.width > 0 && r.bottom > 0 && getComputedStyle(el).display !== 'inline'
}
const targets = Array.from(document.querySelectorAll('a[href], button, input, label')).filter(shown)
const name = (el) => `${el.tagName.toLowerCase()} "${el.textContent.trim().replace(/\s+/g, ' ')}"`
const gap = (x, y, r) => Math.hypot(Math.max(r.left - x, 0, x - r.right), Math.max(r.top - y, 0, y - r.bottom))

const close = []
for (const el of targets) {
  const r = el.getBoundingClientRect()
  if (r.width >= 24 && r.height >= 24) continue
  const x = (r.left + r.right) / 2
  const y = (r.top + r.bottom) / 2
  for (const other of targets) {
    if (other === el || other.contains(el) || el.contains(other)) continue
    const o = other.getBoundingClientRect()
    const small = o.width < 24 || o.height < 24
    const d = small ? Math.hypot(x - (o.left + o.right) / 2, y - (o.top + o.bottom) / 2) - 12 : gap(x, y, o)
    if (d < 12) close.push(`${name(el)} ${Math.round(r.width)}x${Math.round(r.height)} is ${d.toFixed(1)}px from ${name(other)}`)
  }
}
const report = window.top.document.documentElement.dataset
report.width = String(window.innerWidth)
report.close = close.join(' | ') || 'none'
