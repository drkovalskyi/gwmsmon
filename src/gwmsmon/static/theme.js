(function(){
  var m = document.cookie.match(/(?:^|;\s*)theme=(\w+)/);
  if (m && m[1] === 'dark') document.documentElement.dataset.theme = 'dark';
})();
