
function fileReportBlockMacro (context) {
  return function () {
    this.process((parent, target, attrs) => {
      const lines = [
        `Files in catalog: ${context.contentCatalog.getFiles().length}`,
        `URL of *current page*: ${context.file.pub.url}`,
      ]
      return this.createBlock(parent, 'paragraph', lines)
    })
  }
}

function register (registry, context) {
  registry.blockMacro('files', fileReportBlockMacro(context))
}

module.exports = fileReportBlockMacro
module.exports.register = register
