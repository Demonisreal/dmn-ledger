import { prove } from '/static/proof.js'

document.addEventListener('securitypolicyviolation', (e) => console.error(`Refused to ${e.effectiveDirective}: ${e.blockedURI}`))

const importKey = crypto.subtle.importKey.bind(crypto.subtle)
crypto.subtle.importKey = (format, key, algorithm, ...rest) =>
  algorithm.name === 'Ed25519' ? Promise.reject(new DOMException('no Ed25519 in this test', 'NotSupportedError')) : importKey(format, key, algorithm, ...rest)

const section = document.querySelector('section.proof')
const shots = document.createElement('div')
shots.id = 'shots'
shots.hidden = true
document.body.append(shots)

const shot = (name) => {
  const copy = section.cloneNode(true)
  copy.dataset.shot = name
  shots.append(copy)
}

try {
  await prove()
  shot('agrees')
  await prove()
  shot('differs')
} catch (err) {
  shots.textContent = `error ${err}`
}
