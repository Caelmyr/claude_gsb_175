/* 结果导出 Results */
Components.init('results');
const C = Components;

let currentJob = '';
let pageOffset = 0;
const PAGE_SIZE = 100;

async function render() {
  if (!currentJob) return;
  let d;
  try {
    d = await API.get('/api/jobs/' + currentJob + '/results?limit=' + PAGE_SIZE + '&offset=' + pageOffset);
  } catch (e) { return; }

  // If the result set shrank under us (e.g. job switched), clamp to the last page.
  if (d.total > 0 && pageOffset >= d.total) {
    pageOffset = Math.floor((d.total - 1) / PAGE_SIZE) * PAGE_SIZE;
    return render();
  }

  document.getElementById('dl-json').href = '/api/jobs/' + currentJob + '/results/download?format=json';
  document.getElementById('dl-csv').href = '/api/jobs/' + currentJob + '/results/download?format=csv';

  // The header total and the partition sum come from the same server-side
  // snapshot; showing both makes their agreement verifiable at a glance.
  const consistent = d.total === d.partition_total;
  const from = d.returned ? d.offset + 1 : 0;
  const to = d.offset + d.returned;
  document.getElementById('stats').innerHTML = [
    { label: '结果记录 Total records', value: C.fmtNum(d.total) },
    { label: '分区合计 Partition sum', value: C.fmtNum(d.partition_total) },
    { label: '分区数 Partitions', value: d.partitions.length },
    { label: '作业状态 Status', value: d.status },
    {
      label: '一致性 Consistency',
      value: consistent
        ? '<span class="badge good">一致 Consistent</span>'
        : '<span class="badge bad">不一致 Mismatch</span>',
      raw: true,
    },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value">${s.raw ? s.value : C.esc(s.value)}</div></div>`).join('');

  // Partition rows carry each partition's [from–to] window in the global
  // ordered record stream, so the list cross-checks against the preview's
  // record numbers one to one.
  document.getElementById('partitions').innerHTML = d.partitions.length
    ? C.table([
        { key: 'partition_name', label: '分区 Partition', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
        { key: 'count', label: '记录数 Count', render: r => C.fmtNum(r.count), num: true },
        {
          key: 'range', label: '记录区间 Range', num: true,
          render: r => r.count ? `<span class="tabular">${C.fmtNum(r.offset + 1)}–${C.fmtNum(r.offset + r.count)}</span>` : '—',
        },
        { key: 'task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.task_id)}</span>` },
      ], d.partitions)
    : C.empty('暂无结果 No results — 作业可能尚未完成');

  const records = d.records || [];
  const pager = d.total
    ? `<div class="flex between small mb">
        <span class="muted">第 <b class="tabular">${C.fmtNum(from)}–${C.fmtNum(to)}</b> 条 / 共 <b class="tabular">${C.fmtNum(d.total)}</b> 条
          · Record ${from}–${to} of ${C.fmtNum(d.total)}${d.total > PAGE_SIZE ? ' · 完整结果请导出 Full result via export' : ''}</span>
        <span class="flex">
          <button class="btn small" id="pg-prev" ${d.offset <= 0 ? 'disabled' : ''}>上一页 Prev</button>
          <button class="btn small" id="pg-next" ${d.truncated ? '' : 'disabled'}>下一页 Next</button>
        </span>
      </div>`
    : '';
  document.getElementById('preview').innerHTML = pager + (records.length
    ? C.table([
        { key: 'n', label: '#', render: (r, i) => `<span class="tabular muted">${C.fmtNum(d.offset + i + 1)}</span>`, num: true },
        { key: 'key', label: 'Key', render: r => `<b>${C.esc(r.key)}</b>` },
        { key: 'value', label: 'Value', render: r => C.valueCell(r) },
      ], records)
    : C.empty('暂无结果 No results'));

  const prev = document.getElementById('pg-prev');
  const next = document.getElementById('pg-next');
  if (prev) prev.addEventListener('click', () => { pageOffset = Math.max(0, pageOffset - PAGE_SIZE); render(); });
  if (next) next.addEventListener('click', () => { pageOffset += PAGE_SIZE; render(); });
}

C.jobPicker('job-picker', (id) => { currentJob = id; pageOffset = 0; render(); });
C.poll(render, 3000).start();
