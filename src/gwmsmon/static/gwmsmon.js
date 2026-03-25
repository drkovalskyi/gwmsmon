// Theme toggle
function toggleTheme() {
  var isDark = document.documentElement.dataset.theme === 'dark';
  if (isDark) {
    delete document.documentElement.dataset.theme;
    document.cookie = 'theme=light;path=/;max-age=31536000;SameSite=Lax';
  } else {
    document.documentElement.dataset.theme = 'dark';
    document.cookie = 'theme=dark;path=/;max-age=31536000;SameSite=Lax';
  }
  location.reload();
}
var _tb = document.getElementById('theme-toggle-btn');
if (_tb) _tb.addEventListener('click', toggleTheme);

// Details collapse state cookies (one per data-section key)
(function() {
  document.querySelectorAll('details.section[data-section]').forEach(function(details) {
    var key = 'sec_' + details.dataset.section;
    var m = document.cookie.match(new RegExp('(?:^|;\\s*)' + key + '=(\\d)'));
    if (m && m[1] === '0') details.removeAttribute('open');
    details.addEventListener('toggle', function() {
      var val = details.open ? '1' : '0';
      document.cookie = key + '=' + val + ';path=/;max-age=31536000;SameSite=Lax';
    });
  });
})();

// Tab switching with URL hash support
(function() {
  function activateTab(tabId) {
    var btn = document.querySelector('.tab-btn[data-tab="' + tabId + '"]');
    if (!btn) return false;
    var parent = btn.closest('main') || document;
    parent.querySelectorAll('.tab-btn').forEach(function(b) { b.classList.remove('active'); });
    parent.querySelectorAll('.tab-pane').forEach(function(p) { p.classList.remove('active'); });
    btn.classList.add('active');
    var pane = document.getElementById(tabId);
    if (pane) pane.classList.add('active');
    return true;
  }

  document.querySelectorAll('.tab-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      activateTab(btn.dataset.tab);
      history.replaceState(null, '', '#' + btn.dataset.tab);
    });
  });

  // Activate tab from URL hash on load
  if (location.hash) {
    activateTab(location.hash.slice(1));
  }
})();

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

// Re-sort a table by its currently active sort column
function resortTable(table) {
  if (!table) return;
  var th = table.querySelector('th.sort-asc') || table.querySelector('th.sort-desc');
  if (th) {
    var dir = th.classList.contains('sort-asc') ? 'asc' : 'desc';
    sortTable(th, dir);
  }
}

// Table sorting
function sortTable(th, forceDir) {
  var table = th.closest('table');
  var tbody = table.querySelector('tbody');
  var idx = Array.from(th.parentNode.children).indexOf(th);
  var isNum = th.dataset.sort === 'num';
  var asc;
  if (forceDir) {
    asc = forceDir === 'asc';
  } else {
    asc = !th.classList.contains('sort-asc');
  }

  table.querySelectorAll('th').forEach(function(h) {
    h.classList.remove('sort-asc', 'sort-desc');
  });
  th.classList.add(asc ? 'sort-asc' : 'sort-desc');

  var rows = Array.from(tbody.querySelectorAll('tr'));
  rows.sort(function(a, b) {
    var av = a.children[idx].textContent.trim();
    var bv = b.children[idx].textContent.trim();
    if (isNum) {
      av = parseFloat(av.replace(/,/g, '')) || 0;
      bv = parseFloat(bv.replace(/,/g, '')) || 0;
    }
    var cmp = av < bv ? -1 : av > bv ? 1 : 0;
    return asc ? cmp : -cmp;
  });
  rows.forEach(function(row) { tbody.appendChild(row); });
}

document.querySelectorAll('.data-table.sortable th').forEach(function(th) {
  th.addEventListener('click', function() { sortTable(th); });
});

// Apply default sort (data-sort-default="colIdx:dir", e.g. "2:desc")
document.querySelectorAll('.data-table.sortable[data-sort-default]').forEach(function(table) {
  var parts = table.dataset.sortDefault.split(':');
  var colIdx = parseInt(parts[0], 10);
  var dir = parts[1] || 'desc';
  var th = table.querySelectorAll('thead th')[colIdx];
  if (th) sortTable(th, dir);
});

// Cross-filter: workflows ↔ sites ↔ stats
(function() {
  var wfInput = document.querySelector('.table-filter[data-table="wf-table"]');
  var siteInput = document.querySelector('.table-filter[data-table="sites-table"]');
  var wfTable = document.getElementById('wf-table');
  var sitesTable = document.getElementById('sites-table');

  // Number formatter matching Jinja |fmt filter
  function fmt(n) {
    if (n >= 1e6) return (n/1e6).toFixed(1).replace(/\.0$/, '') + 'M';
    if (n >= 1e3) return n.toLocaleString('en-US');
    return String(n);
  }

  // Fallback: simple filter for tables without cross-filter
  document.querySelectorAll('.table-filter').forEach(function(input) {
    if (input === wfInput || input === siteInput) return;
    var tableId = input.dataset.table;
    var table = document.getElementById(tableId);
    if (!table) return;
    input.addEventListener('input', function() {
      var filter = input.value.toLowerCase();
      table.querySelectorAll('tbody tr').forEach(function(row) {
        var text = row.textContent.toLowerCase();
        row.style.display = text.indexOf(filter) !== -1 ? '' : 'none';
      });
    });
  });

  if (!wfTable) return;

  // Detect view from page URL
  var viewMatch = location.pathname.match(/^\/(\w+)\//);
  var view = viewMatch ? viewMatch[1] : '';

  // Load cross-reference data, then apply filters if inputs have values
  var crossRef = null;
  var completionXref = null;
  if (view) {
    Promise.all([
      fetch('/' + view + '/json/cross_reference.json')
        .then(function(r) { return r.ok ? r.json() : null; }),
      fetch('/' + view + '/json/completion_cross_reference.json')
        .then(function(r) { return r.ok ? r.json() : null; })
    ]).then(function(results) {
      crossRef = results[0];
      completionXref = results[1];
      if ((wfInput && wfInput.value) || (siteInput && siteInput.value)) {
        applyFilters();
      }
    }).catch(function() {});
  }

  // Save original site cell values (innerHTML to preserve links)
  if (sitesTable) {
    sitesTable.querySelectorAll('tbody tr').forEach(function(row) {
      row._origCells = Array.from(row.querySelectorAll('td')).map(function(td) {
        return td.innerHTML;
      });
    });
  }

  function applyFilters() {
    var wfFilter = wfInput ? wfInput.value.toLowerCase() : '';
    var siteFilter = siteInput ? siteInput.value.toLowerCase() : '';
    var anyFilter = wfFilter || siteFilter;

    // 1. Build set of site names matching siteFilter text
    var matchingSites = null;
    if (siteFilter && sitesTable) {
      matchingSites = new Set();
      sitesTable.querySelectorAll('tbody tr').forEach(function(row) {
        if (row.dataset.name && row.dataset.name.toLowerCase().indexOf(siteFilter) !== -1) {
          matchingSites.add(row.dataset.name);
        }
      });
    }

    // 2. Filter workflow rows: text match AND site match
    var visibleWfs = new Set();
    var totals = {running: 0, idle: 0, cpusUse: 0, cpusPend: 0, count: 0, done: 0, fail: 0, cpu: 0, wallCpus: 0, slotOk: 0, slotAll: 0};
    wfTable.querySelectorAll('tbody tr').forEach(function(row) {
      var textMatch = !wfFilter || row.textContent.toLowerCase().indexOf(wfFilter) !== -1;
      var siteMatch = true;
      var name = row.dataset.name;
      var wfCounts = null; // [R, I, C, P] per-site contribution
      var wfComp = null;   // [done, fail, cpu, wall_cpus, slot_ok, slot_all]
      if (matchingSites && crossRef) {
        var wfSites = name ? crossRef[name] : null;
        var cSites = completionXref && name && completionXref[name] ? completionXref[name] : {};
        if (wfSites || Object.keys(cSites).length) {
          siteMatch = false;
          wfCounts = [0, 0, 0, 0];
          wfComp = [0, 0, 0, 0, 0, 0];
          // Accumulate live job counts
          if (wfSites) {
            for (var s in wfSites) {
              if (matchingSites.has(s)) {
                siteMatch = true;
                var v = wfSites[s];
                wfCounts[0] += v[0]; wfCounts[1] += v[1];
                wfCounts[2] += v[2]; wfCounts[3] += v[3];
              }
            }
          }
          // Accumulate completion counts (may include sites not in crossRef)
          for (var s in cSites) {
            if (matchingSites.has(s)) {
              siteMatch = true;
              for (var ci = 0; ci < 6; ci++) wfComp[ci] += cSites[s][ci] || 0;
            }
          }
        } else {
          siteMatch = false;
        }
      }
      var visible = textMatch && siteMatch;
      row.style.display = visible ? '' : 'none';

      // Save original cell values once (innerHTML to preserve links)
      if (!row._origCells) {
        row._origCells = Array.from(row.querySelectorAll('td')).map(function(td) {
          return td.innerHTML;
        });
      }

      // Update workflow row cells when site filter is active
      var cells = row.querySelectorAll('td');
      var n = cells.length;
      // Detect if row has completion+efficiency columns (prodview: 11 cells with Prio)
      var hasComp = row.dataset.done !== undefined;
      if (matchingSites && wfCounts) {
        if (hasComp) {
          // prodview: Name Prio R I C P Done Fail Fail% CpuEff ProcEff
          cells[2].textContent = fmt(wfCounts[0]);
          cells[3].textContent = fmt(wfCounts[1]);
          cells[4].textContent = fmt(wfCounts[2]);
          cells[5].textContent = fmt(wfCounts[3]);
          cells[6].textContent = fmt(wfComp[0]);
          cells[7].textContent = fmt(wfComp[1]);
          cells[8].textContent = wfComp[0] ? (wfComp[1] / wfComp[0] * 100).toFixed(1) + '%' : '';
          cells[8].className = (wfComp[0] && wfComp[1] / wfComp[0] > 0.05) ? 'warn' : '';
          var cpuEff = wfComp[3] ? wfComp[2] / wfComp[3] : 0;
          var procEff = wfComp[5] ? wfComp[4] / wfComp[5] : 0;
          if (cells[9]) { cells[9].textContent = wfComp[0] ? (cpuEff * 100).toFixed(1) + '%' : ''; cells[9].className = cpuEff && cpuEff < 0.5 ? 'warn' : ''; }
          if (cells[10]) { cells[10].textContent = wfComp[0] ? (procEff * 100).toFixed(1) + '%' : ''; cells[10].className = procEff && procEff < 0.8 ? 'warn' : ''; }
        } else {
          cells[n - 4].textContent = fmt(wfCounts[0]);
          cells[n - 3].textContent = fmt(wfCounts[1]);
          cells[n - 2].textContent = fmt(wfCounts[2]);
          cells[n - 1].textContent = fmt(wfCounts[3]);
        }
      } else if (!matchingSites) {
        // Restore original values
        row._origCells.forEach(function(v, i) { if (cells[i]) cells[i].innerHTML = v; });
      }

      if (visible) {
        if (name) visibleWfs.add(name);
        totals.count++;
        if (wfCounts) {
          totals.running += wfCounts[0]; totals.idle += wfCounts[1];
          totals.cpusUse += wfCounts[2]; totals.cpusPend += wfCounts[3];
          if (wfComp) {
            totals.done += wfComp[0]; totals.fail += wfComp[1];
            totals.cpu += wfComp[2]; totals.wallCpus += wfComp[3];
            totals.slotOk += wfComp[4]; totals.slotAll += wfComp[5];
          }
        } else {
          totals.running += parseInt(row.dataset.running) || 0;
          totals.idle += parseInt(row.dataset.idle) || 0;
          totals.cpusUse += parseInt(row.dataset.cpusUse) || 0;
          totals.cpusPend += parseInt(row.dataset.cpusPend) || 0;
          totals.done += parseInt(row.dataset.done) || 0;
          totals.fail += parseInt(row.dataset.fail) || 0;
          totals.cpu += parseFloat(row.dataset.cpuEff || 0) * (parseInt(row.dataset.done) || 0);
          totals.wallCpus += parseInt(row.dataset.done) || 0;
        }
      }
    });

    // 3. Update stats row
    var el;
    if ((el = document.getElementById('stat-running'))) el.textContent = fmt(totals.running);
    if ((el = document.getElementById('stat-idle'))) el.textContent = fmt(totals.idle);
    if ((el = document.getElementById('stat-cpus-use'))) el.textContent = fmt(totals.cpusUse);
    if ((el = document.getElementById('stat-cpus-pend'))) el.textContent = fmt(totals.cpusPend);
    if ((el = document.getElementById('stat-count'))) el.textContent = fmt(totals.count);
    if ((el = document.getElementById('stat-done'))) el.textContent = fmt(totals.done);
    if ((el = document.getElementById('stat-fail-rate'))) {
      var rate = totals.done ? (totals.fail / totals.done * 100).toFixed(1) + '%' : '';
      el.textContent = rate;
      el.className = 'stat-value' + ((totals.done && totals.fail / totals.done > 0.05) ? ' warn' : '');
    }
    if ((el = document.getElementById('stat-cpu-eff'))) {
      var ce = totals.wallCpus ? (totals.cpu / totals.wallCpus * 100).toFixed(1) + '%' : '';
      el.textContent = ce;
      el.className = 'stat-value' + ((totals.wallCpus && totals.cpu / totals.wallCpus < 0.5) ? ' warn' : '');
    }
    if ((el = document.getElementById('stat-proc-eff'))) {
      var pe = totals.slotAll ? (totals.slotOk / totals.slotAll * 100).toFixed(1) + '%' : '';
      el.textContent = pe;
      el.className = 'stat-value' + ((totals.slotAll && totals.slotOk / totals.slotAll < 0.8) ? ' warn' : '');
    }

    // 4. Recompute sites from visible workflows
    if (sitesTable) {
      if (wfFilter && crossRef) {
        // Workflow filter active: recompute site values from visible workflows
        var siteTotals = {};
        var siteCompTotals = {};  // {site: [done, fail, cpu, wall_cpus, slot_ok, slot_all]}
        visibleWfs.forEach(function(wf) {
          var sites = crossRef[wf];
          if (!sites) return;
          for (var site in sites) {
            var v = sites[site];
            var s = siteTotals[site];
            if (!s) { s = [0,0,0,0]; siteTotals[site] = s; }
            s[0] += v[0]; s[1] += v[1]; s[2] += v[2]; s[3] += v[3];
          }
          var cSites = completionXref ? completionXref[wf] : null;
          if (cSites) {
            for (var site in cSites) {
              var cv = cSites[site];
              var sc = siteCompTotals[site];
              if (!sc) { sc = [0,0,0,0,0,0]; siteCompTotals[site] = sc; }
              for (var ci = 0; ci < 6; ci++) sc[ci] += cv[ci] || 0;
            }
          }
        });
        sitesTable.querySelectorAll('tbody tr').forEach(function(row) {
          var site = row.dataset.name;
          var counts = siteTotals[site] || [0,0,0,0];
          var comp = siteCompTotals[site] || [0,0,0,0,0,0];
          var cells = row.querySelectorAll('td');
          cells[1].textContent = fmt(counts[0]);
          cells[2].textContent = fmt(counts[1]);
          cells[3].textContent = fmt(counts[2]);
          cells[4].textContent = fmt(counts[3]);
          // Restore original UniquePressure
          if (cells[5] && row._origCells) cells[5].innerHTML = row._origCells[5];
          // Update completion columns
          if (cells[6]) cells[6].textContent = fmt(comp[0]);
          if (cells[7]) cells[7].textContent = fmt(comp[1]);
          if (cells[8]) {
            cells[8].textContent = comp[0] ? (comp[1] / comp[0] * 100).toFixed(1) + '%' : '';
            cells[8].className = (comp[0] && comp[1] / comp[0] > 0.05) ? 'warn' : '';
          }
          // Efficiency columns
          var sCpuEff = comp[3] ? comp[2] / comp[3] : 0;
          var sProcEff = comp[5] ? comp[4] / comp[5] : 0;
          if (cells[9]) { cells[9].textContent = comp[0] ? (sCpuEff * 100).toFixed(1) + '%' : ''; cells[9].className = sCpuEff && sCpuEff < 0.5 ? 'warn' : ''; }
          if (cells[10]) { cells[10].textContent = comp[0] ? (sProcEff * 100).toFixed(1) + '%' : ''; cells[10].className = sProcEff && sProcEff < 0.8 ? 'warn' : ''; }
          var siteTextMatch = !siteFilter || site.toLowerCase().indexOf(siteFilter) !== -1;
          var hasData = counts[0] || counts[1] || comp[0];
          row.style.display = (siteTextMatch && hasData) ? '' : 'none';
        });
      } else {
        // No workflow filter — restore original values, apply site text filter only
        sitesTable.querySelectorAll('tbody tr').forEach(function(row) {
          if (row._origCells) {
            var cells = row.querySelectorAll('td');
            row._origCells.forEach(function(v, i) { if (cells[i]) cells[i].innerHTML = v; });
          }
          var siteTextMatch = !siteFilter || (row.dataset.name || '').toLowerCase().indexOf(siteFilter) !== -1;
          row.style.display = siteTextMatch ? '' : 'none';
        });
      }
    }

    // Re-sort tables after cell values changed
    resortTable(wfTable);
    resortTable(sitesTable);
  }

  if (wfInput) wfInput.addEventListener('input', applyFilters);
  if (siteInput) siteInput.addEventListener('input', applyFilters);

  // Apply on load if browser restored non-empty filter values (no cross-ref needed)
  if (wfInput && wfInput.value && !siteInput?.value) applyFilters();
})();

// --- uPlot chart rendering ---
(function() {
  var INTERVALS = { hourly: 3*3600, daily: 24*3600, weekly: 7*24*3600 };
  var isDark = document.documentElement.dataset.theme === 'dark';
  var CPUS_COLOR = isDark ? '#64b5f6' : '#0055D4';
  var RATIO_COLOR = isDark ? '#FF9830' : '#222222';
  var AXIS_STROKE = isDark ? '#999' : '#888';
  var AXIS_LABEL_STROKE = isDark ? '#bbb' : '#444';
  var YLIM_PAD = 0.20;
  var CHART_W = 373;
  var CHART_H = 300;
  var LABEL_H = 18;
  var GAP_H = 8;
  var XAXIS_H = 24;
  // Top panel has no x-axis labels; bottom panel has XAXIS_H for labels
  var PANEL_TOP_H = Math.floor((CHART_H - LABEL_H - GAP_H - XAXIS_H) / 2);
  var PANEL_BOT_H = Math.floor((CHART_H - LABEL_H - GAP_H + XAXIS_H) / 2);
  var LEFT_PAD = 8;
  var RIGHT_AXIS_SIZE = 58;
  var RIGHT_LABEL_OFFSET = 48;

  var PANELS = [
    { label: 'Running', cpusKey: 'CpusInUse', jobsKey: 'Running' },
    { label: 'Pending', cpusKey: 'CpusPending', jobsKey: 'MatchingIdle' },
  ];

  function fmtCount(v) {
    if (v == null) return '';
    var a = Math.abs(v);
    if (a >= 1e6) return (v/1e6).toFixed(1).replace(/\.0$/, '') + 'M';
    if (a >= 1e3) return (v/1e3).toFixed(1).replace(/\.0$/, '') + 'K';
    return v.toFixed(0);
  }

  function fmtRatio(v) {
    if (v == null) return '';
    // {:g} style — no trailing zeros
    if (v === 0) return '0';
    if (v >= 100) return v.toFixed(0);
    if (v >= 10) return v.toFixed(1).replace(/\.0$/, '');
    return v.toFixed(2).replace(/0+$/, '').replace(/\.$/, '');
  }

  function fmtTimeSplits(self, splits) {
    return splits.map(function(ts) {
      var d = new Date(ts * 1000);
      return ('0' + d.getHours()).slice(-2) + ':' + ('0' + d.getMinutes()).slice(-2);
    });
  }

  function fmtDateSplits(self, splits) {
    return splits.map(function(ts) {
      var d = new Date(ts * 1000);
      return (d.getMonth()+1) + '/' + d.getDate();
    });
  }

  // Find the latest timestamp across all series keys
  function seriesMaxTime(series, keys) {
    var maxT = 0;
    (keys || Object.keys(series)).forEach(function(k) {
      var pts = series[k] || {t:[],v:[]};
      if (pts.t.length) {
        var t = pts.t[pts.t.length - 1];
        if (t > maxT) maxT = t;
      }
    });
    return maxT;
  }

  // Build aligned uPlot data arrays from parallel-array series.
  // Returns {data: [...], tMin: N, tMax: N} or null.
  function buildAligned(series, keys, cutoff) {
    // Collect all unique timestamps >= cutoff across all keys
    var tSet = {};
    keys.forEach(function(k) {
      var pts = series[k] || {t:[],v:[]};
      for (var i = 0; i < pts.t.length; i++) {
        if (pts.t[i] >= cutoff) tSet[pts.t[i]] = true;
      }
    });
    var timestamps = Object.keys(tSet).map(Number).sort(function(a,b){return a-b;});
    if (!timestamps.length) return null;

    // Build lookup per key
    var lookups = keys.map(function(k) {
      var m = {};
      var pts = series[k] || {t:[],v:[]};
      for (var i = 0; i < pts.t.length; i++) {
        if (pts.t[i] >= cutoff) m[pts.t[i]] = pts.v[i];
      }
      return m;
    });

    var data = [new Float64Array(timestamps)];
    lookups.forEach(function(m) {
      var arr = new Float64Array(timestamps.length);
      for (var i = 0; i < timestamps.length; i++) {
        var v = m[timestamps[i]];
        arr[i] = v != null ? v : NaN;
      }
      data.push(arr);
    });
    return {data: data, tMin: timestamps[0], tMax: timestamps[timestamps.length - 1]};
  }

  // Extend aligned data to fill [xMin, xMax] range
  function extendToEdges(aligned, xMin, xMax) {
    if (!aligned || !aligned.data.length) return aligned;
    var ts = aligned.data[0];
    var n = ts.length;
    var addLeft = ts[0] > xMin ? 1 : 0;
    var addRight = ts[n - 1] < xMax ? 1 : 0;
    if (!addLeft && !addRight) return aligned;
    var newLen = n + addLeft + addRight;
    var newData = [new Float64Array(newLen)];
    if (addLeft) newData[0][0] = xMin;
    for (var j = 0; j < n; j++) newData[0][addLeft + j] = ts[j];
    if (addRight) newData[0][newLen - 1] = xMax;
    for (var i = 1; i < aligned.data.length; i++) {
      var s = aligned.data[i];
      var ns = new Float64Array(newLen);
      if (addLeft) ns[0] = s[0]; // repeat first value
      for (var j = 0; j < n; j++) ns[addLeft + j] = s[j];
      if (addRight) ns[newLen - 1] = s[n - 1]; // repeat last value
      newData.push(ns);
    }
    aligned.data = newData;
    aligned.tMin = xMin;
    aligned.tMax = xMax;
    return aligned;
  }

  // Downsample aligned data arrays by averaging into fixed-size time buckets.
  // aligned = {data: [timestamps, series1, ...], tMin, tMax}
  // bucketSec = target bucket width in seconds
  function downsampleAligned(aligned, bucketSec) {
    var src = aligned.data;
    var tsArr = src[0];
    var nSeries = src.length - 1;
    if (tsArr.length <= 1) return aligned;

    // Group indices into buckets
    var buckets = [];
    var curBucket = Math.floor(tsArr[0] / bucketSec) * bucketSec;
    var curStart = 0;
    for (var i = 0; i < tsArr.length; i++) {
      var b = Math.floor(tsArr[i] / bucketSec) * bucketSec;
      if (b !== curBucket) {
        buckets.push({t: curBucket + bucketSec / 2, start: curStart, end: i});
        curBucket = b;
        curStart = i;
      }
    }
    buckets.push({t: curBucket + bucketSec / 2, start: curStart, end: tsArr.length});

    var outTs = new Float64Array(buckets.length);
    var outSeries = [];
    for (var s = 0; s < nSeries; s++) outSeries.push(new Float64Array(buckets.length));

    for (var bi = 0; bi < buckets.length; bi++) {
      var bk = buckets[bi];
      outTs[bi] = bk.t;
      for (var s = 0; s < nSeries; s++) {
        var sum = 0, cnt = 0;
        for (var j = bk.start; j < bk.end; j++) {
          var v = src[s + 1][j];
          if (!isNaN(v)) { sum += v; cnt++; }
        }
        outSeries[s][bi] = cnt > 0 ? sum / cnt : NaN;
      }
    }

    var out = [outTs];
    for (var s = 0; s < nSeries; s++) out.push(outSeries[s]);
    return {data: out, tMin: aligned.tMin, tMax: aligned.tMax};
  }

  // Compute cores/job ratio array from cpus and jobs arrays
  function computeRatio(cpus, jobs) {
    var out = new Float64Array(cpus.length);
    for (var i = 0; i < cpus.length; i++) {
      var c = cpus[i], j = jobs[i];
      out[i] = (j > 0 && !isNaN(c) && !isNaN(j)) ? c / j : NaN;
    }
    return out;
  }

  function arrMax(arr) {
    var m = 0;
    for (var i = 0; i < arr.length; i++) {
      if (!isNaN(arr[i]) && arr[i] > m) m = arr[i];
    }
    return m;
  }

  // Both y-axes share GRID_N intervals so grid lines show clean
  // values on both sides simultaneously.
  var GRID_N = 4;

  // Round up to 2 significant digits — tight ceiling, no big jumps
  function ceilStep(v) {
    if (v <= 0) return 1;
    var mag = Math.pow(10, Math.floor(Math.log10(v)) - 1);
    return Math.ceil(v / mag) * mag;
  }

  // CPUs axis max: ceil step × GRID_N
  function alignedCpusMax(dataMax) {
    var padded = Math.max(dataMax, 1) * (1 + YLIM_PAD);
    return ceilStep(padded / GRID_N) * GRID_N;
  }

  // Ratio axis max: integer step × GRID_N
  function alignedRatioMax(dataMax) {
    var padded = Math.max(dataMax, 1) * (1 + YLIM_PAD);
    var step = Math.max(1, Math.ceil(padded / GRID_N));
    return step * GRID_N;
  }

  // Force exactly GRID_N intervals on any y-axis
  function gridSplits(u, axisIdx, scaleMin, scaleMax) {
    var out = [];
    var step = scaleMax / GRID_N;
    for (var i = 0; i <= GRID_N; i++) out.push(step * i);
    return out;
  }


  // Tooltip plugin — shows values on cursor hover
  function tooltipPlugin() {
    var tip;
    return {
      hooks: {
        init: function(u) {
          tip = document.createElement('div');
          tip.className = 'chart-tooltip';
          u.over.appendChild(tip);
        },
        setCursor: function(u) {
          var idx = u.cursor.idx;
          if (idx == null) {
            tip.style.display = 'none';
            return;
          }
          var ts = u.data[0][idx];
          var d = new Date(ts * 1000);
          var time = ('0'+d.getHours()).slice(-2) + ':' + ('0'+d.getMinutes()).slice(-2);
          var html = '<b>' + d.toLocaleDateString() + ' ' + time + '</b>';
          for (var i = 1; i < u.series.length; i++) {
            if (!u.series[i].show) continue;
            var v = u.data[i][idx];
            if (v != null && !isNaN(v)) {
              var color = u.series[i]._color || u.series[i].stroke;
              if (typeof color === 'function') color = '#000';
              var sLabel = u.series[i].label;
              var formatted = (u.series[i].scale === 'ratio' || u.series[i].scale === 'pct') ? fmtRatio(v) : fmtCount(v);
              html += '<br><span style="color:' + color + '">\u25CF</span> ' + sLabel + ': ' + formatted;
            }
          }
          tip.innerHTML = html;
          tip.style.display = 'block';
          var left = u.cursor.left + 12;
          if (left + tip.offsetWidth > u.over.offsetWidth)
            left = u.cursor.left - tip.offsetWidth - 12;
          tip.style.left = left + 'px';
          tip.style.top = '4px';
        },
      }
    };
  }

  // Plugin to draw right axis label rotated same direction as left axis
  function rightLabelPlugin(text, color) {
    return {
      hooks: {
        drawAxes: function(u) {
          var ctx = u.ctx;
          var pxR = devicePixelRatio;
          // u.bbox is in canvas pixels
          var x = u.bbox.left + u.bbox.width + RIGHT_LABEL_OFFSET * pxR;
          var y = u.bbox.top + u.bbox.height / 2;
          ctx.save();
          ctx.font = (10 * pxR) + 'px sans-serif';
          ctx.fillStyle = color;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.translate(x, y);
          ctx.rotate(-Math.PI / 2);
          ctx.fillText(text, 0, 0);
          ctx.restore();
        }
      }
    };
  }

  // Compute global max ratio from ALL data (unfiltered, both panels)
  function globalMaxRatio(series) {
    var maxR = 0;
    PANELS.forEach(function(p) {
      var cpusPts = series[p.cpusKey] || {t:[],v:[]};
      var jobsPts = series[p.jobsKey] || {t:[],v:[]};
      var jobsMap = {};
      for (var i = 0; i < jobsPts.t.length; i++) {
        jobsMap[jobsPts.t[i]] = jobsPts.v[i];
      }
      for (var i = 0; i < cpusPts.t.length; i++) {
        var j = jobsMap[cpusPts.t[i]];
        if (j > 0 && cpusPts.v[i] > 0) {
          var r = cpusPts.v[i] / j;
          if (r > maxR) maxR = r;
        }
      }
    });
    return maxR;
  }

  // Simple 3-line chart colors (colorblind-safe: blue / orange / grey)
  var RUNNING_COLOR = isDark ? '#5794F2' : '#2166AC';
  var IDLE_COLOR = isDark ? '#FF9830' : '#E55400';
  var HELD_COLOR = isDark ? '#B0B0B0' : '#878787';
  var SIMPLE_COLORS = {
    Running: RUNNING_COLOR,
    Idle: IDLE_COLOR,
    Held: HELD_COLOR,
    TotalRunning: RUNNING_COLOR,
    TotalIdle: IDLE_COLOR,
    TotalHeld: HELD_COLOR,
    TotalRunningJobs: RUNNING_COLOR,
    TotalIdleJobs: IDLE_COLOR,
    TotalHeldJobs: HELD_COLOR,
  };
  var SIMPLE_CHART_H = 282;

  function renderSimpleChart(el, series, sharedSimpleYMax) {
    var interval = INTERVALS[el.dataset.interval] || INTERVALS.hourly;

    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var label = document.createElement('div');
    label.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:' + AXIS_LABEL_STROKE + ';padding:2px 0 0';
    label.textContent = {hourly:'3 Hours', daily:'24 Hours', weekly:'7 Days'}[el.dataset.interval] || el.dataset.interval;
    el.appendChild(label);

    // Detect series keys (first 3 non-timestamp keys)
    var seriesKeys = Object.keys(series).slice(0, 3);
    if (!seriesKeys.length) return;

    // Fixed x-range: right edge = now, left edge = now - interval
    var xMax = Math.floor(Date.now() / 1000);
    var xMin = xMax - interval;

    var aligned = buildAligned(series, seriesKeys, xMin);
    if (!aligned) return;
    if (interval >= 7*86400) aligned = downsampleAligned(aligned, 3600);
    extendToEdges(aligned, xMin, xMax);

    var xFmt = interval > 2*86400 ? fmtDateSplits : fmtTimeSplits;
    var yMax = sharedSimpleYMax || alignedCpusMax(1);

    var uSeries = [{}];
    var dataArrays = [aligned.data[0]];
    seriesKeys.forEach(function(k, i) {
      uSeries.push({
        scale: 'y',
        stroke: SIMPLE_COLORS[k] || [RUNNING_COLOR, IDLE_COLOR, HELD_COLOR][i],
        width: 2,
        label: k,
      });
      dataArrays.push(aligned.data[i + 1]);
    });

    var opts = {
      width: CHART_W,
      height: SIMPLE_CHART_H,
      cursor: { show: true },
      legend: { show: false },
      padding: [10, 8, 2, LEFT_PAD],
      plugins: [tooltipPlugin()],
      scales: {
        x: { min: xMin, max: xMax },
        y: { min: 0, max: yMax, auto: false },
      },
      axes: [
        { size: XAXIS_H, font: '10px sans-serif', values: xFmt, stroke: AXIS_STROKE },
        {
          scale: 'y',
          size: 50,
          font: '10px sans-serif',
          stroke: AXIS_LABEL_STROKE,
          ticks: { size: 0 },
          splits: gridSplits,
          values: function(self, ticks) { return ticks.map(fmtCount); },
        },
      ],
      series: uSeries,
    };

    new uPlot(opts, dataArrays, el);
  }

  // Histogram height matches dual-panel total: PANEL_TOP_H + GAP_H + PANEL_BOT_H
  var HISTOGRAM_H = PANEL_TOP_H + GAP_H + PANEL_BOT_H;
  var SUCCESS_COLOR = isDark ? '#5794F2' : '#3274D9';
  var FAILURE_COLOR = isDark ? '#FF9830' : '#E55400';

  function renderHistogramChart(el, data) {
    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var label = document.createElement('div');
    label.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:' + AXIS_LABEL_STROKE + ';padding:2px 0 0';
    label.textContent = 'Completions (7 Days)';
    el.appendChild(label);

    var ts = data.timestamps;
    var success = data.success;
    var failure = data.failure;
    if (!ts || !ts.length) return;

    // Aggregate 10-min buckets into 4-hour buckets
    var hourBucket = 4 * 3600;
    var hourMap = {};
    for (var i = 0; i < ts.length; i++) {
      var hk = Math.floor(ts[i] / hourBucket) * hourBucket;
      if (!hourMap[hk]) hourMap[hk] = {s: 0, f: 0};
      hourMap[hk].s += success[i] || 0;
      hourMap[hk].f += failure[i] || 0;
    }
    var hourKeys = Object.keys(hourMap).map(Number).sort(function(a,b){return a-b;});

    var timestamps = new Float64Array(hourKeys);
    var totalArr = new Float64Array(hourKeys.length);
    var failArr = new Float64Array(hourKeys.length);
    for (var i = 0; i < hourKeys.length; i++) {
      var h = hourMap[hourKeys[i]];
      totalArr[i] = h.s + h.f;
      failArr[i] = h.f;
    }

    var yMax = alignedCpusMax(arrMax(totalArr));
    var bucketSize = hourBucket;

    // Fixed 7-day x-range: right edge = last bucket + size, left edge = 168h before
    var xMax = ts[ts.length - 1] + bucketSize;
    var xMin = xMax - 7 * 86400;

    var barOpts = uPlot.paths.bars({size: [1, 100]});

    var opts = {
      width: CHART_W,
      height: HISTOGRAM_H,
      cursor: { show: true },
      legend: { show: false },
      padding: [10, 8, 2, LEFT_PAD],
      plugins: [tooltipPlugin()],
      scales: {
        x: { min: xMin, max: xMax },
        y: { min: 0, max: yMax, auto: false },
      },
      axes: [
        { size: XAXIS_H, font: '10px sans-serif', values: fmtDateSplits, stroke: AXIS_STROKE },
        {
          scale: 'y',
          size: 50,
          font: '10px sans-serif',
          stroke: AXIS_LABEL_STROKE,
          ticks: { size: 0 },
          splits: gridSplits,
          values: function(self, ticks) { return ticks.map(fmtCount); },
        },
      ],
      series: [
        {},
        {
          scale: 'y',
          stroke: SUCCESS_COLOR,
          fill: SUCCESS_COLOR,
          width: 0,
          label: 'Success',
          paths: barOpts,
        },
        {
          scale: 'y',
          stroke: FAILURE_COLOR,
          fill: FAILURE_COLOR,
          width: 0,
          label: 'Failure',
          paths: barOpts,
        },
      ],
    };

    new uPlot(opts, [timestamps, totalArr, failArr], el);
  }

  // Priority block colors — exact old service palette
  // Visual order bottom→top: B0(red), B1(orange), ..., B7(black)
  var BLOCK_COLORS = [
    '#ff0000', '#ff7f00', '#ffff00', '#00ff00',
    '#0000ff', '#6600ff', '#8800ff', isDark ? '#90A4AE' : '#000000'
  ];
  var BLOCKS = ['B0','B1','B2','B3','B4','B5','B6','B7'];

  function renderStackedChart(el, blockData, metric, label) {
    var interval = INTERVALS[el.dataset.interval] || INTERVALS.hourly;
    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var titleEl = document.createElement('div');
    titleEl.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:' + AXIS_LABEL_STROKE + ';padding:2px 0 0';
    titleEl.textContent = label + ' \u2014 ' + ({hourly:'3 Hours', daily:'24 Hours', weekly:'7 Days'}[el.dataset.interval] || '');
    el.appendChild(titleEl);

    var xMax = Math.floor(Date.now() / 1000);
    var xMin = xMax - interval;
    var xFmt = interval > 2*86400 ? fmtDateSplits : fmtTimeSplits;

    // Collect all timestamps across all blocks
    var tSet = {};
    BLOCKS.forEach(function(b) {
      var s = (blockData[b] || {})[metric] || {t:[],v:[]};
      for (var i = 0; i < s.t.length; i++) {
        if (s.t[i] >= xMin) tSet[s.t[i]] = true;
      }
    });
    var timestamps = Object.keys(tSet).map(Number).sort(function(a,b){return a-b;});
    if (!timestamps.length) return;
    // Extend to edges so lines fill the chart
    if (timestamps[0] > xMin) timestamps.unshift(xMin);
    if (timestamps[timestamps.length - 1] < xMax) timestamps.push(xMax);

    // Build per-block raw value arrays aligned to timestamps
    // blockArrays[0]=B0, blockArrays[7]=B7
    var blockArrays = BLOCKS.map(function(b) {
      var s = (blockData[b] || {})[metric] || {t:[],v:[]};
      var m = {};
      for (var i = 0; i < s.t.length; i++) m[s.t[i]] = s.v[i];
      var arr = new Float64Array(timestamps.length);
      for (var i = 0; i < timestamps.length; i++) {
        // For the extended point, repeat the last known value
        arr[i] = m[timestamps[i]] != null ? m[timestamps[i]] :
                 (i > 0 ? arr[i-1] : 0);
      }
      return arr;
    });

    // Cumulate from B0 upward: cum[0]=B0, cum[1]=B0+B1, ..., cum[7]=total
    var cum = [];
    for (var i = 0; i < BLOCKS.length; i++) {
      var prev = i > 0 ? cum[i - 1] : null;
      var arr = new Float64Array(timestamps.length);
      for (var j = 0; j < timestamps.length; j++) {
        arr[j] = blockArrays[i][j] + (prev ? prev[j] : 0);
      }
      cum.push(arr);
    }

    // uPlot data: series 1 = outermost (total), series 8 = innermost (B0)
    // data[1]=cum[7](total), data[2]=cum[6], ..., data[8]=cum[0](B0)
    var tsArr = new Float64Array(timestamps);
    var data = [tsArr];
    for (var i = BLOCKS.length - 1; i >= 0; i--) {
      data.push(cum[i]);
    }

    var yMax = alignedCpusMax(arrMax(cum[BLOCKS.length - 1]));

    // Series drawn outermost first (data[1]=total) to innermost (data[8]=B0).
    // Each fills to zero; later series paint over earlier ones, creating stack.
    // data[si] contribution = block BLOCKS.length - si
    var uSeries = [{}];
    for (var si = 1; si <= BLOCKS.length; si++) {
      var bi = BLOCKS.length - si;
      uSeries.push({
        scale: 'y',
        stroke: BLOCK_COLORS[bi],
        fill: BLOCK_COLORS[bi],
        width: 0,
        label: BLOCKS[bi],
      });
    }

    // Tooltip: show per-block raw values (not cumulative)
    function stackedTooltipPlugin() {
      var tip;
      return {
        hooks: {
          init: function(u) {
            tip = document.createElement('div');
            tip.className = 'chart-tooltip';
            u.over.appendChild(tip);
          },
          setCursor: function(u) {
            var idx = u.cursor.idx;
            if (idx == null) { tip.style.display = 'none'; return; }
            var ts = u.data[0][idx];
            var d = new Date(ts * 1000);
            var time = ('0'+d.getHours()).slice(-2) + ':' + ('0'+d.getMinutes()).slice(-2);
            var html = '<b>' + d.toLocaleDateString() + ' ' + time + '</b>';
            // Iterate blocks bottom→top (B0 first)
            for (var bi = 0; bi < BLOCKS.length; bi++) {
              var val = blockArrays[bi][idx];
              if (val > 0) {
                html += '<br><span style="color:' + BLOCK_COLORS[bi] + '">\u25CF</span> ' + BLOCKS[bi] + ': ' + fmtCount(val);
              }
            }
            tip.innerHTML = html;
            tip.style.display = 'block';
            var left = u.cursor.left + 12;
            if (left + tip.offsetWidth > u.over.offsetWidth)
              left = u.cursor.left - tip.offsetWidth - 12;
            tip.style.left = left + 'px';
            tip.style.top = '4px';
          },
        }
      };
    }

    var opts = {
      width: CHART_W,
      height: SIMPLE_CHART_H,
      cursor: { show: true },
      legend: { show: false },
      padding: [10, 8, 2, LEFT_PAD],
      plugins: [stackedTooltipPlugin()],
      scales: {
        x: { min: xMin, max: xMax },
        y: { min: 0, max: yMax, auto: false },
      },
      axes: [
        { size: XAXIS_H, font: '10px sans-serif', values: xFmt, stroke: AXIS_STROKE },
        {
          scale: 'y',
          size: 50,
          font: '10px sans-serif',
          stroke: AXIS_LABEL_STROKE,
          ticks: { size: 0 },
          splits: gridSplits,
          values: function(self, ticks) { return ticks.map(fmtCount); },
        },
      ],
      series: uSeries,
    };

    new uPlot(opts, data, el);
  }

  // Site monitor chart: stacked CPUs by category (left) + % metrics (right)
  function renderSiteMonitorChart(el, series) {
    var interval = 7 * 24 * 3600;
    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var xMax = Math.floor(Date.now() / 1000);
    var xMin = xMax - interval;

    var cpusKeys = ['CpusTier0', 'CpusProd', 'CpusAna', 'CpusOther'];
    var pctKeys = ['FailureRate', 'CPUEff', 'ProcEff'];
    var allKeys = cpusKeys.concat(pctKeys);

    var aligned = buildAligned(series, allKeys, xMin);
    if (!aligned) return;
    aligned = downsampleAligned(aligned, 3600);
    extendToEdges(aligned, xMin, xMax);

    // Stack: other(bottom) + analysis + production + tier0(top)
    // Data order: [ts, Tier0, Prod, Ana, Other, FailRate, CPUEff, ProcEff]
    var n = aligned.data[0].length;
    var tier0 = aligned.data[1];
    var prod = aligned.data[2];
    var ana = aligned.data[3];
    var other = aligned.data[4];
    var cumOther = new Float64Array(n);
    var cumAna = new Float64Array(n);
    var cumProd = new Float64Array(n);
    var cumTier0 = new Float64Array(n);
    var cpusMax = 1;
    for (var i = 0; i < n; i++) {
      cumOther[i] = (other[i] || 0);
      cumAna[i] = cumOther[i] + (ana[i] || 0);
      cumProd[i] = cumAna[i] + (prod[i] || 0);
      cumTier0[i] = cumProd[i] + (tier0[i] || 0);
      if (cumTier0[i] > cpusMax) cpusMax = cumTier0[i];
    }
    // Keep raw values for tooltip
    var rawData = [tier0, prod, ana, other];
    aligned.data[1] = cumTier0;  // tier0 on top
    aligned.data[2] = cumProd;   // production
    aligned.data[3] = cumAna;    // analysis
    aligned.data[4] = cumOther;  // other bottom

    var yMaxCpus = alignedCpusMax(cpusMax);

    var TIER0_COLOR = '#E6A586';
    var PROD_COLOR = '#9EB8E2';
    var ANA_COLOR = '#D9C68A';
    var OTHER_COLOR = '#A0BF9C';
    var FAIL_COLOR = isDark ? '#FF6B6B' : '#D32F2F';
    var CPUEFF_COLOR = isDark ? '#64B5F6' : '#1976D2';
    var PROCEFF_COLOR = isDark ? '#81C784' : '#388E3C';

    // Custom tooltip that shows raw (non-cumulative) values for stacked series
    function siteMonitorTooltip() {
      var tip;
      return {
        hooks: {
          init: [function(u) {
            tip = document.createElement('div');
            tip.className = 'chart-tooltip';
            tip.style.display = 'none';
            u.over.appendChild(tip);
          }],
          setCursor: [function(u) {
            var idx = u.cursor.idx;
            if (idx == null) { tip.style.display = 'none'; return; }
            var ts = u.data[0][idx];
            var d = new Date(ts * 1000);
            var time = ('0'+d.getHours()).slice(-2)+':'+('0'+d.getMinutes()).slice(-2);
            var html = '<b>' + d.toLocaleDateString() + ' ' + time + '</b>';
            for (var i = 1; i < u.series.length; i++) {
              if (!u.series[i].show) continue;
              // Use raw value for stacked CPU series (indices 1-4), data value for others
              var v = (i >= 1 && i <= 4) ? (rawData[i-1][idx] || 0) : u.data[i][idx];
              if (v != null && !isNaN(v)) {
                var color = u.series[i]._color || u.series[i].stroke;
                if (typeof color === 'function') color = '#000';
                var sLabel = u.series[i].label;
                var formatted = (u.series[i].scale === 'pct') ? fmtRatio(v) : fmtCount(v);
                html += '<br><span style="color:'+color+'">\u25CF</span> '+sLabel+': '+formatted;
              }
            }
            tip.innerHTML = html;
            tip.style.display = 'block';
            var left = u.cursor.left + 12;
            if (left + tip.offsetWidth > u.over.offsetWidth) left = u.cursor.left - tip.offsetWidth - 12;
            tip.style.left = left + 'px';
            tip.style.top = (u.cursor.top - 10) + 'px';
          }]
        }
      };
    }

    var opts = {
      width: CHART_W,
      height: SIMPLE_CHART_H,
      cursor: { show: true },
      legend: { show: false },
      padding: [10, 8, 2, LEFT_PAD],
      plugins: [siteMonitorTooltip()],
      scales: {
        x: { min: xMin, max: xMax },
        cpus: { min: 0, max: yMaxCpus, auto: false },
        pct: { min: 0, max: 100, auto: false },
      },
      axes: [
        { size: XAXIS_H, font: '10px sans-serif', values: fmtDateSplits, stroke: AXIS_STROKE },
        {
          scale: 'cpus',
          size: 50,
          font: '10px sans-serif',
          stroke: AXIS_LABEL_STROKE,
          ticks: { size: 0 },
          splits: gridSplits,
          values: function(self, ticks) { return ticks.map(fmtCount); },
        },
        {
          scale: 'pct',
          side: 1,
          size: 40,
          font: '10px sans-serif',
          stroke: AXIS_LABEL_STROKE,
          ticks: { size: 0 },
          splits: function() { return [0, 25, 50, 75, 100]; },
          values: function(self, ticks) { return ticks.map(function(v) { return v + '%'; }); },
          grid: { show: false },
        },
      ],
      series: [
        {},
        { scale: 'cpus', stroke: '#FA6400', fill: TIER0_COLOR + '99', width: 1.5, label: 'Tier0', _color: TIER0_COLOR },
        { scale: 'cpus', stroke: '#818AB8', fill: PROD_COLOR + '99', width: 1.5, label: 'Production', _color: PROD_COLOR },
        { scale: 'cpus', stroke: '#BFAC4D', fill: ANA_COLOR + '99', width: 1.5, label: 'Analysis', _color: ANA_COLOR },
        { scale: 'cpus', stroke: '#7BAA3A', fill: OTHER_COLOR + '99', width: 1.5, label: 'Other', _color: OTHER_COLOR },
        { scale: 'pct', stroke: FAIL_COLOR, width: 1.5, label: 'Failure %', _color: FAIL_COLOR },
        { scale: 'pct', stroke: CPUEFF_COLOR, width: 1.5, label: 'CPU Eff %', _color: CPUEFF_COLOR },
        { scale: 'pct', stroke: PROCEFF_COLOR, width: 1.5, label: 'Proc Eff %', _color: PROCEFF_COLOR },
      ],
    };

    new uPlot(opts, aligned.data, el);
  }

  function renderCharts() {
    var charts = document.querySelectorAll('.chart');
    if (!charts.length) return;

    // Separate chart types
    var simpleCharts = [];
    var dualCharts = [];
    var histogramCharts = [];
    var stackedCharts = [];
    charts.forEach(function(el) {
      el.innerHTML = '<div class="chart-loading">Loading...</div>';
      if (el.dataset.chartType === 'simple') {
        simpleCharts.push(el);
      } else if (el.dataset.chartType === 'histogram') {
        histogramCharts.push(el);
      } else if (el.dataset.chartType === 'stacked') {
        stackedCharts.push(el);
      } else if (el.dataset.chartType === 'site-monitor') {
        simpleCharts.push(el); // group for fetching, render differently
      } else {
        dualCharts.push(el);
      }
    });

    // Group by data-src for single fetch
    var groups = {};
    charts.forEach(function(el) {
      var src = el.dataset.src;
      if (!groups[src]) groups[src] = [];
      groups[src].push(el);
    });

    // Fetch all data, then render
    var pending = Object.keys(groups).length;
    var allData = {};
    var rawData = {};

    Object.keys(groups).forEach(function(src) {
      fetch(src)
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(data) {
          if (!data) return;
          if (data.series) {
            allData[src] = data.series;
          } else {
            rawData[src] = data;
          }
        })
        .catch(function() {})
        .finally(function() {
          pending--;
          if (pending === 0) {
            // Compute shared scales for dual-panel charts
            var pageMaxRatio = 0;
            var sharedCpusYMax = {};
            if (dualCharts.length) {
              Object.keys(allData).forEach(function(s) {
                var hasDual = groups[s].some(function(el) {
                  return !el.dataset.chartType;
                });
                if (!hasDual) return;
                var mr = globalMaxRatio(allData[s]);
                if (mr > pageMaxRatio) pageMaxRatio = mr;
              });
              PANELS.forEach(function(panel) {
                var maxV = 0;
                Object.keys(allData).forEach(function(s) {
                  var hasDual = groups[s].some(function(el) {
                    return !el.dataset.chartType;
                  });
                  if (!hasDual) return;
                  var _cp = allData[s][panel.cpusKey] || {t:[],v:[]};
                  for (var _i = 0; _i < _cp.v.length; _i++) {
                    if (_cp.v[_i] > maxV) maxV = _cp.v[_i];
                  }
                });
                sharedCpusYMax[panel.cpusKey] = alignedCpusMax(maxV);
              });
            }
            var sharedRatioYMax = alignedRatioMax(pageMaxRatio);

            // Compute shared y-max for simple charts
            var simpleMaxV = 0;
            if (simpleCharts.length) {
              Object.keys(allData).forEach(function(s) {
                var hasSimple = groups[s].some(function(el) {
                  return el.dataset.chartType === 'simple';
                });
                if (!hasSimple) return;
                var series = allData[s];
                Object.keys(series).forEach(function(k) {
                  var _sp = series[k] || {t:[],v:[]};
                  for (var _i = 0; _i < _sp.v.length; _i++) {
                    if (_sp.v[_i] > simpleMaxV) simpleMaxV = _sp.v[_i];
                  }
                });
              });
            }
            var sharedSimpleYMax = alignedCpusMax(simpleMaxV);

            // Render all charts
            Object.keys(groups).forEach(function(s) {
              groups[s].forEach(function(el) {
                if (el.dataset.chartType === 'histogram') {
                  if (rawData[s]) renderHistogramChart(el, rawData[s]);
                } else if (el.dataset.chartType === 'site-monitor') {
                  if (allData[s]) renderSiteMonitorChart(el, allData[s]);
                } else if (el.dataset.chartType === 'simple') {
                  if (allData[s]) renderSimpleChart(el, allData[s], sharedSimpleYMax);
                } else if (el.dataset.chartType === 'stacked') {
                  if (rawData[s]) renderStackedChart(el, rawData[s], el.dataset.stackedMetric, el.dataset.stackedLabel || '');
                } else {
                  if (allData[s]) renderChartWithSharedScale(el, allData[s], sharedRatioYMax, sharedCpusYMax);
                }
              });
            });
          }
        });
    });
  }

  function renderChartWithSharedScale(el, series, sharedRatioYMax, sharedCpusYMax) {
    var interval = INTERVALS[el.dataset.interval] || INTERVALS.hourly;

    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var label = document.createElement('div');
    label.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:' + AXIS_LABEL_STROKE + ';padding:2px 0 0';
    label.textContent = {hourly:'3 Hours', daily:'24 Hours', weekly:'7 Days'}[el.dataset.interval] || el.dataset.interval;
    el.appendChild(label);

    // Fixed x-range: right edge = now, left edge = now - interval
    var xMax = Math.floor(Date.now() / 1000);
    var xMin = xMax - interval;
    var xFmt = interval > 2*86400 ? fmtDateSplits : fmtTimeSplits;

    PANELS.forEach(function(panel, panelIdx) {
      var isBottom = (panelIdx === PANELS.length - 1);

      // Gap between top and bottom panels
      if (panelIdx > 0) {
        var spacer = document.createElement('div');
        spacer.style.height = GAP_H + 'px';
        el.appendChild(spacer);
      }

      var keys = [panel.cpusKey, panel.jobsKey];
      var aligned = buildAligned(series, keys, xMin);
      if (!aligned) return;
      if (interval >= 7*86400) aligned = downsampleAligned(aligned, 3600);
      extendToEdges(aligned, xMin, xMax);

      var timestamps = aligned.data[0];
      var cpusArr = aligned.data[1];
      var jobsArr = aligned.data[2];
      var ratioArr = computeRatio(cpusArr, jobsArr);

      var cpusYMax = sharedCpusYMax[panel.cpusKey];

      var opts = {
        width: CHART_W,
        height: isBottom ? PANEL_BOT_H : PANEL_TOP_H,
        cursor: { show: true },
        legend: { show: false },
        padding: [10, 4, isBottom ? 2 : 0, LEFT_PAD],
        plugins: [tooltipPlugin(), rightLabelPlugin('cores/job', RATIO_COLOR)],
        scales: {
          x: { min: xMin, max: xMax },
          cpus: { min: 0, max: cpusYMax, auto: false },
          ratio: { min: 0, max: sharedRatioYMax, auto: false },
        },
        axes: [
          isBottom
            ? { size: XAXIS_H, font: '10px sans-serif', values: xFmt, stroke: AXIS_STROKE }
            : { size: 2, values: function() { return []; }, ticks: { show: false } },
          {
            scale: 'cpus',
            size: 50,
            font: '10px sans-serif',
            stroke: CPUS_COLOR,
            ticks: { size: 0 },
            splits: gridSplits,
            values: function(self, ticks) { return ticks.map(fmtCount); },
            label: panel.label,
            labelSize: 14,
            labelFont: '10px sans-serif',
            labelGap: 4,
          },
          {
            scale: 'ratio',
            side: 1,
            size: RIGHT_AXIS_SIZE,
            font: '10px sans-serif',
            stroke: RATIO_COLOR,
            ticks: { size: 0 },
            splits: gridSplits,
            values: function(self, ticks) { return ticks.map(function(v) { return v == null ? '' : String(Math.round(v)); }); },
            grid: { show: false },
          },
        ],
        series: [
          {},
          {
            scale: 'cpus',
            stroke: CPUS_COLOR,
            width: 2,
            label: panel.cpusKey,
          },
          {
            scale: 'ratio',
            stroke: RATIO_COLOR,
            width: 1.5,
            dash: [6, 3],
            label: 'cores/job',
          },
        ],
      };

      new uPlot(opts, [timestamps, cpusArr, ratioArr], el);
    });
  }

  renderCharts();
})();
