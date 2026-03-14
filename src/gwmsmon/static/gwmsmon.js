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

// --- uPlot chart rendering ---
(function() {
  var INTERVALS = { hourly: 3*3600, daily: 24*3600, weekly: 7*86400 };
  var CPUS_COLOR = '#0055D4';
  var RATIO_COLOR = '#222222';
  var YLIM_PAD = 0.20;
  var CHART_W = 420;
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

  // Build aligned uPlot data arrays from {t,v} point arrays
  function buildAligned(series, keys, cutoff) {
    // Collect all unique timestamps >= cutoff across all keys
    var tSet = {};
    keys.forEach(function(k) {
      (series[k] || []).forEach(function(p) {
        if (p.t >= cutoff) tSet[p.t] = true;
      });
    });
    var timestamps = Object.keys(tSet).map(Number).sort(function(a,b){return a-b;});
    if (!timestamps.length) return null;

    // Build lookup per key
    var lookups = keys.map(function(k) {
      var m = {};
      (series[k] || []).forEach(function(p) { if (p.t >= cutoff) m[p.t] = p.v; });
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
    return data;
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
              var color = u.series[i].stroke;
              var sLabel = u.series[i].label;
              var formatted = (u.series[i].scale === 'ratio') ? fmtRatio(v) : fmtCount(v);
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
      var cpusPts = series[p.cpusKey] || [];
      var jobsMap = {};
      (series[p.jobsKey] || []).forEach(function(pt) { jobsMap[pt.t] = pt.v; });
      cpusPts.forEach(function(pt) {
        var j = jobsMap[pt.t];
        if (j > 0 && pt.v > 0) {
          var r = pt.v / j;
          if (r > maxR) maxR = r;
        }
      });
    });
    return maxR;
  }

  // Simple 3-line chart colors (colorblind-safe)
  var SIMPLE_COLORS = {
    Running: '#2166AC',
    Idle: '#D6604D',
    Held: '#878787',
    TotalRunning: '#2166AC',
    TotalIdle: '#D6604D',
    TotalHeld: '#878787',
    TotalRunningJobs: '#2166AC',
    TotalIdleJobs: '#D6604D',
    TotalHeldJobs: '#878787',
  };
  var SIMPLE_CHART_H = 240;

  function renderSimpleChart(el, series, sharedSimpleYMax) {
    var interval = INTERVALS[el.dataset.interval] || INTERVALS.hourly;

    var loader = el.querySelector('.chart-loading');
    if (loader) loader.remove();

    var label = document.createElement('div');
    label.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:#444;padding:2px 0 0';
    label.textContent = el.dataset.interval.charAt(0).toUpperCase() + el.dataset.interval.slice(1);
    el.appendChild(label);

    var now = Date.now() / 1000;
    var cutoff = now - interval;

    // Detect series keys (first 3 non-timestamp keys)
    var seriesKeys = Object.keys(series).slice(0, 3);
    if (!seriesKeys.length) return;

    var aligned = buildAligned(series, seriesKeys, cutoff);
    if (!aligned) return;

    var xFmt = interval > 2*86400 ? fmtDateSplits : fmtTimeSplits;
    var yMax = sharedSimpleYMax || alignedCpusMax(1);

    var uSeries = [{}];
    var dataArrays = [aligned[0]];
    seriesKeys.forEach(function(k, i) {
      uSeries.push({
        scale: 'y',
        stroke: SIMPLE_COLORS[k] || ['#2166AC','#D6604D','#878787'][i],
        width: 2,
        label: k,
      });
      dataArrays.push(aligned[i + 1]);
    });

    var opts = {
      width: CHART_W,
      height: SIMPLE_CHART_H,
      cursor: { show: true },
      legend: { show: false },
      padding: [4, 8, 2, LEFT_PAD],
      plugins: [tooltipPlugin()],
      scales: {
        x: { min: cutoff, max: now },
        y: { min: 0, max: yMax, auto: false },
      },
      axes: [
        { size: XAXIS_H, font: '10px sans-serif', values: xFmt, stroke: '#888' },
        {
          scale: 'y',
          size: 50,
          font: '10px sans-serif',
          stroke: '#444',
          splits: gridSplits,
          values: function(self, ticks) { return ticks.map(fmtCount); },
        },
      ],
      series: uSeries,
    };

    new uPlot(opts, dataArrays, el);
  }

  function renderCharts() {
    var charts = document.querySelectorAll('.chart');
    if (!charts.length) return;

    // Separate simple and dual-panel charts
    var simpleCharts = [];
    var dualCharts = [];
    charts.forEach(function(el) {
      el.innerHTML = '<div class="chart-loading">Loading...</div>';
      if (el.dataset.chartType === 'simple') {
        simpleCharts.push(el);
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

    Object.keys(groups).forEach(function(src) {
      fetch(src)
        .then(function(r) { return r.ok ? r.json() : null; })
        .then(function(data) {
          if (data && data.series) allData[src] = data.series;
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
                // Only include data from dual-panel chart sources
                var hasDual = groups[s].some(function(el) {
                  return el.dataset.chartType !== 'simple';
                });
                if (!hasDual) return;
                var mr = globalMaxRatio(allData[s]);
                if (mr > pageMaxRatio) pageMaxRatio = mr;
              });
              PANELS.forEach(function(panel) {
                var maxV = 0;
                Object.keys(allData).forEach(function(s) {
                  var hasDual = groups[s].some(function(el) {
                    return el.dataset.chartType !== 'simple';
                  });
                  if (!hasDual) return;
                  (allData[s][panel.cpusKey] || []).forEach(function(p) {
                    if (p.v > maxV) maxV = p.v;
                  });
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
                  (series[k] || []).forEach(function(p) {
                    if (p.v > simpleMaxV) simpleMaxV = p.v;
                  });
                });
              });
            }
            var sharedSimpleYMax = alignedCpusMax(simpleMaxV);

            // Render all charts
            Object.keys(groups).forEach(function(s) {
              var series = allData[s];
              if (!series) return;
              groups[s].forEach(function(el) {
                if (el.dataset.chartType === 'simple') {
                  renderSimpleChart(el, series, sharedSimpleYMax);
                } else {
                  renderChartWithSharedScale(el, series, sharedRatioYMax, sharedCpusYMax);
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
    label.style.cssText = 'text-align:center;font-size:11px;font-weight:600;color:#444;padding:2px 0 0';
    label.textContent = el.dataset.interval.charAt(0).toUpperCase() + el.dataset.interval.slice(1);
    el.appendChild(label);

    PANELS.forEach(function(panel, panelIdx) {
      var isBottom = (panelIdx === PANELS.length - 1);

      // Gap between top and bottom panels
      if (panelIdx > 0) {
        var spacer = document.createElement('div');
        spacer.style.height = GAP_H + 'px';
        el.appendChild(spacer);
      }

      var now = Date.now() / 1000;
      var cutoff = now - interval;
      var keys = [panel.cpusKey, panel.jobsKey];
      var aligned = buildAligned(series, keys, cutoff);
      if (!aligned) return;

      var timestamps = aligned[0];
      var cpusArr = aligned[1];
      var jobsArr = aligned[2];
      var ratioArr = computeRatio(cpusArr, jobsArr);

      var cpusYMax = sharedCpusYMax[panel.cpusKey];

      var xFmt = interval > 2*86400 ? fmtDateSplits : fmtTimeSplits;

      var opts = {
        width: CHART_W,
        height: isBottom ? PANEL_BOT_H : PANEL_TOP_H,
        cursor: { show: true },
        legend: { show: false },
        padding: [4, 4, isBottom ? 2 : 0, LEFT_PAD],
        plugins: [tooltipPlugin(), rightLabelPlugin('cores/job', RATIO_COLOR)],
        scales: {
          x: { min: cutoff, max: now },
          cpus: { min: 0, max: cpusYMax, auto: false },
          ratio: { min: 0, max: sharedRatioYMax, auto: false },
        },
        axes: [
          isBottom
            ? { size: XAXIS_H, font: '10px sans-serif', values: xFmt, stroke: '#888' }
            : { size: 2, values: function() { return []; }, ticks: { show: false } },
          {
            scale: 'cpus',
            size: 50,
            font: '10px sans-serif',
            stroke: CPUS_COLOR,
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
