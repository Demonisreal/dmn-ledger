document.addEventListener('securitypolicyviolation', (e) => console.error(`Refused to ${e.effectiveDirective}: ${e.blockedURI}`))

let hidden = true
Object.defineProperty(document, 'hidden', { configurable: true, get: () => hidden })

const turn = (to) => {
  hidden = to
  document.dispatchEvent(new Event('visibilitychange'))
}
setTimeout(() => turn(false), 2500)
setTimeout(() => turn(true), 4000)
