new MutationObserver((_, watcher) => {
  watcher.disconnect()
  document.querySelector('button[data-kind="kill"]').click()
}).observe(document.querySelector('main'), { attributeFilter: ['data-now'] })
