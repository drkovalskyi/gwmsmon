// Freshness indicator — updates every second
(function() {
  var el = document.querySelector('.freshness');
  if (!el) return;
  var updated = parseFloat(el.dataset.updated);
  if (!updated) return;

  function refresh() {
    var age = Date.now() / 1000 - updated;
    var text;
    if (age < 60) text = Math.floor(age) + 's ago';
    else if (age < 3600) text = Math.floor(age / 60) + 'm ago';
    else if (age < 86400) text = Math.floor(age / 3600) + 'h ago';
    else text = Math.floor(age / 86400) + 'd ago';
    el.textContent = text;
    el.classList.toggle('stale', age > 600);
  }
  refresh();
  setInterval(refresh, 1000);
})();

// Table sorting
document.querySelectorAll('.data-table.sortable th').forEach(function(th) {
  th.addEventListener('click', function() {
    var table = th.closest('table');
    var tbody = table.querySelector('tbody');
    var idx = Array.from(th.parentNode.children).indexOf(th);
    var isNum = th.dataset.sort === 'num';
    var asc = th.classList.contains('sort-asc');

    // Clear other sort indicators
    table.querySelectorAll('th').forEach(function(h) {
      h.classList.remove('sort-asc', 'sort-desc');
    });
    th.classList.add(asc ? 'sort-desc' : 'sort-asc');

    var rows = Array.from(tbody.querySelectorAll('tr'));
    rows.sort(function(a, b) {
      var av = a.children[idx].textContent.trim();
      var bv = b.children[idx].textContent.trim();
      if (isNum) {
        av = parseFloat(av.replace(/,/g, '')) || 0;
        bv = parseFloat(bv.replace(/,/g, '')) || 0;
      }
      var cmp = av < bv ? -1 : av > bv ? 1 : 0;
      return asc ? -cmp : cmp;
    });
    rows.forEach(function(row) { tbody.appendChild(row); });
  });
});

// Table filtering
document.querySelectorAll('.table-filter').forEach(function(input) {
  var tableId = input.dataset.table;
  var table = document.getElementById(tableId);
  if (!table) return;

  input.addEventListener('input', function() {
    var filter = input.value.toLowerCase();
    table.querySelectorAll('tbody tr').forEach(function(row) {
      var text = row.children[0].textContent.toLowerCase();
      row.style.display = text.indexOf(filter) !== -1 ? '' : 'none';
    });
  });
});
