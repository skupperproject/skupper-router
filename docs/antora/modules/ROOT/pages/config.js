//config.js
module.exports = {
  from: /Unresolved directive .* include\:\:\{FragmentDir\}\//g,
  to: 'include::partial$',
};
