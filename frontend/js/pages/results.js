/* 结果导出 Results — one canonical snapshot drives totals, partitions and preview. */
Components.init('results');
const C = Components;

let currentJob = '';
let currentPage = 1;
let requestSeq = 0;
const PAGE_SIZE = 100;

function pagerHtml(d) {
  if (!d.total) return '';
  const prevDisabled = d.page <= 1 ? ' disabled' : '';
  const nextDisabled = d.page >= d.page_count ? ' disabled' : '';
  return `
    <div class="flex between pagination small mt">
      <span class="muted">第 ${C.fmtNum(d.preview_start)}–${C.fmtNum(d.preview_end)} 条 / 共 ${C.fmtNum(d.total)} 条</span>
      <span class="flex">
        <button class="btn small" id="page-prev"${prevDisabled}>上一页 Prev</button>
        <span class="pager-label">${C.fmtNum(d.page)} / ${C.fmtNum(d.page_count)}</span>
        <button class="btn small" id="page-next"${nextDisabled}>下一页 Next</button>
      </span>
    </div>`;
}

function partitionAt(d, globalIndex) {
  return d.partitions.find(p => p.count && globalIndex >= p.start && globalIndex <= p.end) || {};
}

function cleanRecord(rec) {
  const visible = { ...rec };
  delete visible._global_index;
  delete visible._partition_name;
  return visible;
}

function render(d) {
  document.getElementById('dl-json').href = '/api/jobs/' + currentJob + '/results/download?format=json';
  document.getElementById('dl-csv').href = '/api/jobs/' + currentJob + '/results/download?format=csv';

  const partitionSum = d.partitions.reduce((n, p) => n + Number(p.count || 0), 0);
  const consistencyOk = partitionSum === d.total;
  const completeText = d.complete
    ? '完整结果 Complete snapshot'
    : `已提交 ${C.fmtNum(d.committed_partition_count)} / ${C.fmtNum(d.expected_partition_count)} 个分区（当前为已完成部分）`;
  const consistencyText = consistencyOk
    ? `一致：总数 = 分区合计 = ${C.fmtNum(d.total)}`
    : `不一致：总数 ${C.fmtNum(d.total)}，分区合计 ${C.fmtNum(partitionSum)}`;

  document.getElementById('stats').innerHTML = [
    { label: '结果记录总数 Total records', value: C.fmtNum(d.total) },
    { label: '分区记录合计 Partition sum', value: C.fmtNum(partitionSum) },
    { label: '结果分区 Partitions', value: `${C.fmtNum(d.committed_partition_count)} / ${C.fmtNum(d.expected_partition_count)}` },
    { label: '一致性 / 状态 Consistency', value: consistencyText, cls: consistencyOk ? 'good' : 'bad' },
    { label: '快照 Snapshot', value: completeText },
  ].map(s => `<div class="stat"><div class="label">${s.label}</div><div class="value ${s.cls || ''}">${C.esc(s.value)}</div></div>`).join('');

  document.getElementById('partitions').innerHTML = d.partitions.length
    ? C.table([
        { key: 'partition_name', label: '分区 Partition（统一顺序）', render: r => `<span class="mono">${C.esc(r.partition_name)}</span>` },
        { key: 'count', label: '记录数 Count', render: r => C.fmtNum(r.count), num: true },
        { key: 'range', label: '全局序号 Global range', render: r => r.count ? `${C.fmtNum(r.start)}–${C.fmtNum(r.end)}` : '—', num: true },
        { key: 'task_id', label: 'Reduce 任务 Task', render: r => `<span class="mono">${C.esc(r.task_id || '未提交')}</span>` },
      ], d.partitions)
    : C.empty('暂无结果 No results — 作业可能尚未完成');

  const records = d.records || [];
  const previewRows = records.map((r, i) => {
    const index = d.offset + i + 1;
    const partition = partitionAt(d, index);
    return { ...r, _global_index: index, _partition_name: partition.partition_name || '—' };
  });
  const previewHeader = records.length
    ? `<div class="small muted mb">预览与分区、总数使用同一次有序快照，按分区序号和 Key 升序排列。</div>`
    : '';
  document.getElementById('preview').innerHTML = records.length
    ? previewHeader + C.table([
        { key: '_global_index', label: '#', render: r => C.fmtNum(r._global_index), num: true, width: '80px' },
        { key: '_partition_name', label: '分区', render: r => `<span class="mono">${C.esc(r._partition_name)}</span>` },
        { key: 'key', label: 'Key', render: r => `<b>${C.esc(r.key)}</b>` },
        { key: 'value', label: 'Value', render: r => C.valueCell(cleanRecord(r)) },
      ], previewRows) + pagerHtml(d)
    : C.empty('暂无结果 No results') + pagerHtml(d);

  const prev = document.getElementById('page-prev');
  const next = document.getElementById('page-next');
  if (prev) prev.addEventListener('click', () => { currentPage = Math.max(1, d.page - 1); loadResults(); });
  if (next) next.addEventListener('click', () => { currentPage = Math.min(d.page_count, d.page + 1); loadResults(); });
}

async function loadResults() {
  if (!currentJob) return;
  const seq = ++requestSeq;
  let d;
  try {
    d = await API.get(`/api/jobs/${encodeURIComponent(currentJob)}/results?page=${currentPage}&limit=${PAGE_SIZE}`);
  } catch (e) {
    return;
  }
  // Ignore a stale response from polling or a rapidly switched job/page.
  if (seq !== requestSeq || currentJob !== d.job_id) return;
  currentPage = d.page;
  render(d);
}

function selectJob(id) {
  currentJob = id;
  currentPage = 1;
  requestSeq++;
  loadResults();
}

C.jobPicker('job-picker', selectJob);
C.poll(loadResults, 3000).start();
