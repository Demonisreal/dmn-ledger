new MutationObserver((_, watcher) => {
  watcher.disconnect()
  const btn = document.querySelector('button[data-kind="kill"]')
  btn.focus()
  btn.click()
  document.body.dataset.focus = document.activeElement.id
}).observe(document.querySelector('main'), { attributeFilter: ['data-now'] })
