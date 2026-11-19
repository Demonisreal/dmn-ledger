import { prove, tamper } from '/static/proof.js'

document.addEventListener('securitypolicyviolation', (e) => console.error(`Refused to ${e.effectiveDirective}: ${e.blockedURI}`))

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
  shot('fetched')
  await tamper()
  shot('tampered')
  await prove()
  shot('account')
} catch (err) {
  shots.textContent = `error ${err}`
}
