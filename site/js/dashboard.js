const GREEN = '#00dc82';
const BLUE = '#3b82f6';
const RED = '#ef4444';
const CYAN = '#22d3ee';
const AMBER = '#f59e0b';
const GRID_COLOR = '#1e293b';
const TEXT_MUTED = '#64748b';

const TICKER_COLORS = {
  AAPL: GREEN, AMZN: BLUE, GOOG: CYAN, META: '#a855f7',
  MSFT: '#3b82f6', NFLX: RED, NVDA: GREEN, TSLA: AMBER,
  JPM: '#6366f1', DIS: '#ec4899'
};

Chart.defaults.color = TEXT_MUTED;
Chart.defaults.borderColor = GRID_COLOR;
Chart.defaults.font.family = "'JetBrains Mono', monospace";
Chart.defaults.font.size = 11;

async function loadJSON(file) {
  const res = await fetch(`data/${file}`);
  return res.json();
}

function formatMarketCap(val) {
  if (!val) return '—';
  if (val >= 1e12) return `$${(val / 1e12).toFixed(2)}T`;
  if (val >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
  return `$${(val / 1e6).toFixed(0)}M`;
}

function formatNum(val, decimals = 2) {
  if (val == null) return '—';
  return Number(val).toFixed(decimals);
}

// ── PRICE CHART ──
async function renderPriceChart() {
  const data = await loadJSON('price_summary.json');
  const tickers = [...new Set(data.map(d => d.ticker))];
  const datasets = tickers.map(ticker => {
    const points = data.filter(d => d.ticker === ticker);
    return {
      label: ticker,
      data: points.map(p => ({ x: p.date, y: p.close })),
      borderColor: TICKER_COLORS[ticker] || GREEN,
      borderWidth: 1.5,
      pointRadius: 0,
      tension: 0.3,
      fill: false,
    };
  });

  new Chart(document.getElementById('priceChart'), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { position: 'top', labels: { boxWidth: 12, padding: 16, usePointStyle: true } },
      },
      scales: {
        x: { type: 'time', time: { unit: 'week' }, grid: { display: false } },
        y: { grid: { color: GRID_COLOR }, ticks: { callback: v => '$' + v } },
      },
    },
  });
}

// ── FUNDAMENTALS TABLE ──
async function renderFundamentalsTable() {
  const data = await loadJSON('fundamentals.json');
  const tbody = document.getElementById('fundamentalsBody');
  tbody.innerHTML = data.map(d => `
    <tr>
      <td class="ticker-cell">${d.ticker}</td>
      <td>${formatMarketCap(d.market_cap)}</td>
      <td>${formatNum(d.trailing_pe)}</td>
      <td>${formatNum(d.forward_pe)}</td>
      <td>${d.dividend_yield ? (d.dividend_yield * 100).toFixed(2) + '%' : '—'}</td>
      <td>${formatNum(d.beta)}</td>
    </tr>
  `).join('');

  // Update metric cards
  const totalMcap = data.reduce((s, d) => s + (d.market_cap || 0), 0);
  document.getElementById('totalMarketCap').textContent = formatMarketCap(totalMcap);
  document.getElementById('tickerCount').textContent = data.length;
}

// ── EARNINGS CHART ──
async function renderEarningsChart() {
  const data = await loadJSON('earnings.json');
  const aapl = data.filter(d => d.ticker === 'AAPL').slice(0, 8).reverse();

  new Chart(document.getElementById('earningsChart'), {
    type: 'bar',
    data: {
      labels: aapl.map(d => d.report_date),
      datasets: [
        {
          label: 'EPS Estimate',
          data: aapl.map(d => d.eps_estimate),
          backgroundColor: BLUE + '88',
          borderColor: BLUE,
          borderWidth: 1,
        },
        {
          label: 'EPS Actual',
          data: aapl.map(d => d.eps_actual),
          backgroundColor: GREEN + '88',
          borderColor: GREEN,
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 16 } } },
      scales: {
        x: { grid: { display: false } },
        y: { grid: { color: GRID_COLOR }, ticks: { callback: v => '$' + v } },
      },
    },
  });
}

// ── MACRO CHART ──
async function renderMacroChart() {
  const data = await loadJSON('macro.json');
  const series = {};
  data.forEach(d => {
    if (!series[d.series_id]) series[d.series_id] = { name: d.name, points: [] };
    series[d.series_id].points.push({ x: d.date, y: d.value });
  });

  const colorMap = { FEDFUNDS: GREEN, CPIAUCSL: AMBER, UNRATE: RED, GDP: BLUE };
  const datasets = Object.entries(series).map(([id, s]) => ({
    label: s.name,
    data: s.points,
    borderColor: colorMap[id] || CYAN,
    borderWidth: 1.5,
    pointRadius: 0,
    tension: 0.3,
    yAxisID: id === 'GDP' || id === 'CPIAUCSL' ? 'y1' : 'y',
  }));

  new Chart(document.getElementById('macroChart'), {
    type: 'line',
    data: { datasets },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: 'index', intersect: false },
      plugins: { legend: { position: 'top', labels: { boxWidth: 12, padding: 16, usePointStyle: true } } },
      scales: {
        x: { type: 'time', time: { unit: 'month' }, grid: { display: false } },
        y: { position: 'left', grid: { color: GRID_COLOR }, title: { display: true, text: 'Rate / %', color: TEXT_MUTED } },
        y1: { position: 'right', grid: { display: false }, title: { display: true, text: 'Index / $B', color: TEXT_MUTED } },
      },
    },
  });
}

// ── INIT ──
document.addEventListener('DOMContentLoaded', async () => {
  await Promise.all([
    renderPriceChart(),
    renderFundamentalsTable(),
    renderEarningsChart(),
    renderMacroChart(),
  ]);

  // Fade in cards
  document.querySelectorAll('.fade-in').forEach((el, i) => {
    setTimeout(() => el.classList.add('visible'), i * 80);
  });
});
