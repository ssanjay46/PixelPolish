/* ─────────────────────────────────────────────────────────────────
   PixelPolish — Interactive Client Application
   Multi-page flow (Upload -> Editor -> Result)
   Batch processing, Enhancer/Reducer detection, Google Drive & ZIP
   ───────────────────────────────────────────────────────────────── */

let ws = null;
let currentReduction = 50;
let activeBatchFiles = [];      // Array of { filename, size_bytes, is_video, url }
let batchResults     = [];      // Array of completed result payloads
let activeTaskIds    = new Set();

/* ── INITIALIZATION ────────────────────────────────────────────── */
document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  connectWebSocket();
  fetchInitialData();
  setupDragAndDrop();
  updateSliderFill(50);
  setInterval(fetchInitialData, 4000);
});

/* ── LIGHT / DARK THEME SYSTEM ─────────────────────────────────── */
function initTheme() {
  const current = document.documentElement.getAttribute('data-theme') || 
    localStorage.getItem('pixelpolish_theme') || 
    (window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light');
  applyTheme(current);
}

function toggleTheme() {
  const current = document.documentElement.getAttribute('data-theme') === 'dark' ? 'dark' : 'light';
  const next = current === 'dark' ? 'light' : 'dark';
  applyTheme(next);
  localStorage.setItem('pixelpolish_theme', next);
}

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  const textEl = document.getElementById('theme-text');
  if (textEl) {
    textEl.textContent = theme === 'dark' ? 'Dark' : 'Light';
  }
}

/* ── VIEW SWITCHING ────────────────────────────────────────────── */
function showView(viewId) {
  document.querySelectorAll('.view-section').forEach(el => el.classList.remove('active'));
  const target = document.getElementById(viewId);
  if (target) {
    target.classList.add('active');
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }
}

function goToUploadView() {
  showView('view-upload');
}

/* ── DRAG AND DROP (ROBUST) ────────────────────────────────────── */
function setupDragAndDrop() {
  // Prevent browser from opening dragged files when dropped outside dropzone
  window.addEventListener('dragover', e => { e.preventDefault(); e.stopPropagation(); }, false);
  window.addEventListener('drop',     e => { e.preventDefault(); e.stopPropagation(); }, false);

  const zone = document.getElementById('drop-zone');
  if (!zone) return;

  let dragCounter = 0;

  zone.addEventListener('dragenter', e => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter++;
    zone.classList.add('drag-over');
  });

  zone.addEventListener('dragover', e => {
    e.preventDefault();
    e.stopPropagation();
    zone.classList.add('drag-over');
  });

  zone.addEventListener('dragleave', e => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter--;
    if (dragCounter <= 0) {
      dragCounter = 0;
      zone.classList.remove('drag-over');
    }
  });

  zone.addEventListener('drop', e => {
    e.preventDefault();
    e.stopPropagation();
    dragCounter = 0;
    zone.classList.remove('drag-over');

    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
      handleFilesUpload(files);
    }
  });
}

function handleFileSelect(e) {
  const files = e.target.files;
  if (files && files.length > 0) {
    handleFilesUpload(files);
    e.target.value = ''; // Reset input so same file can be selected again
  }
}

/* ── MULTI-FILE UPLOAD ─────────────────────────────────────────── */
async function handleFilesUpload(fileList) {
  addLog('UPLOAD', `Uploading ${fileList.length} file(s)…`, 'info');
  const form = new FormData();
  for (let i = 0; i < fileList.length; i++) {
    form.append('files', fileList[i]);
  }

  try {
    const res = await fetch('/api/media/upload-multiple', {
      method: 'POST',
      body: form
    });
    const data = await res.json();
    if (data.files && data.files.length > 0) {
      data.files.forEach(f => {
        // Prevent duplicate filenames in batch
        if (!activeBatchFiles.some(b => b.filename === f.filename)) {
          activeBatchFiles.push(f);
        }
      });
      addLog('UPLOAD_OK', `Successfully uploaded ${data.files.length} file(s)`, 'ok');
      renderSelectedFilesQueue();
      showView('view-editor');
      fetchSchedulingEstimate();
    }
  } catch (err) {
    addLog('UPLOAD_ERR', `Upload failed: ${err.message}`, 'error');
    alert('Upload failed: ' + err.message);
  }
}

/* ── EDITOR QUEUE RENDERING ────────────────────────────────────── */
function renderSelectedFilesQueue() {
  const listEl  = document.getElementById('selected-files-list');
  const countEl = document.getElementById('batch-count-label');
  const sumEl   = document.getElementById('queue-size-summary');

  if (!listEl) return;

  const count = activeBatchFiles.length;
  if (countEl) countEl.textContent = `${count} File${count !== 1 ? 's' : ''} Selected`;

  const totalBytes = activeBatchFiles.reduce((acc, f) => acc + (f.size_bytes || 0), 0);
  if (sumEl) sumEl.textContent = `Total payload: ${fmtBytes(totalBytes)} · Ready for distributed nodes`;
  updateButtonLabel();

  if (count === 0) {
    listEl.innerHTML = `<div style="text-align:center;padding:24px;color:var(--text-muted);font-size:13px">No files selected. Click "Back to Upload" to select files.</div>`;
    return;
  }

  listEl.innerHTML = activeBatchFiles.map((file, idx) => `
    <div class="batch-file-item" id="bf-item-${idx}">
      <div class="bf-icon">${file.is_video ? '🎥' : '🖼️'}</div>
      <div class="bf-meta">
        <div class="bf-name" title="${file.filename}">${file.filename}</div>
        <div class="bf-size">Original: ${fmtBytes(file.size_bytes)} · ${file.is_video ? 'Video' : 'Image'}</div>
      </div>
      <span class="bf-status-badge" id="bf-badge-${idx}">Ready</span>
      <button class="bf-remove" title="Remove" type="button" onclick="removeBatchItem(${idx})">&times;</button>
    </div>
  `).join('');
}

function removeBatchItem(idx) {
  activeBatchFiles.splice(idx, 1);
  if (activeBatchFiles.length === 0) {
    goToUploadView();
  } else {
    renderSelectedFilesQueue();
    fetchSchedulingEstimate();
  }
}

/* ── SETTINGS & SCHEDULING ─────────────────────────────────────── */
function onSliderChange(val) {
  currentReduction = parseInt(val, 10);
  document.getElementById('slider-display').innerHTML = `${val}%<span>target</span>`;
  updateSliderFill(val);

  document.querySelectorAll('.pill').forEach(p => {
    const pVal = parseInt(p.textContent, 10);
    p.classList.toggle('active', pVal === currentReduction);
  });

  fetchSchedulingEstimate();
  updateButtonLabel();
}

function applyPreset(val) {
  currentReduction = val;
  const slider = document.getElementById('reduction-slider');
  if (slider) slider.value = val;
  document.getElementById('slider-display').innerHTML = `${val}%<span>target</span>`;
  updateSliderFill(val);

  document.querySelectorAll('.pill').forEach(p => {
    const text = p.textContent;
    p.classList.toggle('active', text.includes(`${val}%`));
  });

  fetchSchedulingEstimate();
  updateButtonLabel();
}

function updateSliderFill(val) {
  const slider = document.getElementById('reduction-slider');
  if (!slider) return;
  const pct = ((val - 10) / (85 - 10)) * 100;
  slider.style.background = `linear-gradient(to right, var(--accent) 0%, var(--accent) ${pct}%, #e4e7ec ${pct}%, #e4e7ec 100%)`;
}

function onParamChange() {
  fetchSchedulingEstimate();
}

async function fetchSchedulingEstimate() {
  // Scheduling panel removed; this function kept for API compatibility
  updateButtonLabel();
}

/* ── DYNAMIC BUTTON LABEL (based on slider value) ─────────────── */
function updateButtonLabel() {
  const btnText = document.getElementById('btn-start-text');
  if (!btnText) return;
  const count = activeBatchFiles.length;
  const fileWord = count > 1 ? `${count} Files` : (count === 1 ? '1 File' : 'Files');
  let label, icon;
  if (currentReduction <= 25) {
    label = `Enhance Quality  (${fileWord})`;
    icon = `<path stroke-linecap="round" stroke-linejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z"/>`;
  } else if (currentReduction <= 50) {
    label = `Optimize & Compress  (${fileWord})`;
    icon = `<path stroke-linecap="round" stroke-linejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M16.5 12l-4.5 4.5m0 0L7.5 12m4.5 4.5V3"/>`;
  } else {
    label = `Compress & Reduce  (${fileWord})`;
    icon = `<path stroke-linecap="round" stroke-linejoin="round" d="M20.25 7.5l-.625 10.632a2.25 2.25 0 01-2.247 2.118H6.622a2.25 2.25 0 01-2.247-2.118L3.75 7.5M10 11.25h4M3.375 7.5h17.25c.621 0 1.125-.504 1.125-1.125v-1.5c0-.621-.504-1.125-1.125-1.125H3.375c-.621 0-1.125.504-1.125 1.125v1.5c0 .621.504 1.125 1.125 1.125z"/>`;
  }
  btnText.textContent = label;
  const iconEl = document.getElementById('btn-start-icon');
  if (iconEl) iconEl.innerHTML = icon;
}

/* ── BATCH EXECUTION ───────────────────────────────────────────── */
async function startBatchProcessing() {
  if (activeBatchFiles.length === 0) {
    alert('Please select at least one image or video first.');
    return;
  }

  const btn      = document.getElementById('btn-start');
  const btnText  = document.getElementById('btn-start-text');
  const progArea = document.getElementById('progress-area');
  const progBar  = document.getElementById('progress-bar');
  const progLbl  = document.getElementById('progress-label');

  btn.disabled = true;
  const count = activeBatchFiles.length;
  btnText.innerHTML = `<span class="spinner-ring"></span> Optimizing ${count > 1 ? count + ' Files' : 'Media'}…`;
  if (progArea) progArea.style.display = 'block';
  if (progBar)  progBar.style.width   = '25%';
  if (progLbl)  progLbl.textContent   = `Optimizing ${count} file(s)…`;

  batchResults = [];
  activeTaskIds.clear();

  // Mark all queue items as processing
  activeBatchFiles.forEach((_, idx) => {
    const b = document.getElementById(`bf-badge-${idx}`);
    if (b) {
      b.className = 'bf-status-badge processing';
      b.textContent = 'Processing…';
    }
  });

  const payload = {
    filenames: activeBatchFiles.map(f => f.filename),
    reduction_percent: currentReduction,
    target_resolution: document.getElementById('target-resolution').value
  };

  try {
    const res = await fetch('/api/media/compress-batch', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });
    const data = await res.json();

    if (data.tasks && data.tasks.length > 0) {
      data.tasks.forEach(t => activeTaskIds.add(t.task_id));
      if (progBar) progBar.style.width = '50%';
      if (progLbl) progLbl.textContent = `Optimizing ${data.tasks.length} file(s)…`;
      addLog('PROCESS', `Optimizing ${data.tasks.length} file(s)`, 'ok');
      
      // Start active polling fallback so results are fetched instantly without delay
      startActiveTaskPolling();
    }
  } catch (err) {
    alert('Failed to start processing: ' + err.message);
    btn.disabled = false;
    updateButtonLabel();
    if (progArea) progArea.style.display = 'none';
  }
}

/* ── ACTIVE TASK POLLING (INSTANT FALLBACK & RELIABILITY) ────────── */
let taskPollTimer = null;
function startActiveTaskPolling() {
  if (taskPollTimer) clearInterval(taskPollTimer);
  const startTime = Date.now();
  taskPollTimer = setInterval(async () => {
    if (activeTaskIds.size === 0 || (Date.now() - startTime > 45000)) {
      clearInterval(taskPollTimer);
      return;
    }
    for (const taskId of Array.from(activeTaskIds)) {
      try {
        const res = await fetch(`/api/tasks/${taskId}`);
        if (!res.ok) continue;
        const task = await res.json();
        if (task.status === 'COMPLETED' && task.result) {
          activeTaskIds.delete(taskId);
          const already = batchResults.some(r => r.output_filename === task.result.output_filename);
          if (!already) {
            onTaskFinished(task.result, task.execution_time_seconds || 0.1, task.assigned_worker_id || 'Fast Engine');
          }
        } else if (task.status === 'FAILED' || task.status === 'DEAD_LETTER') {
          activeTaskIds.delete(taskId);
          addLog('ERR', `Task failed: ${task.error_message || 'Processing error'}`, 'error');
        }
      } catch (_) {}
    }
    if (activeTaskIds.size === 0) {
      clearInterval(taskPollTimer);
    }
  }, 300);
}

/* ── WEBSOCKET & TASK COMPLETION TRACKING ───────────────────────── */
function connectWebSocket() {
  const proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(`${proto}//${location.host}/ws`);

  ws.onopen = () => {
    setWsStatus(true);
    addLog('SYS', 'WebSocket connected to primary node', 'ok');
  };

  ws.onmessage = e => {
    try {
      const msg = JSON.parse(e.data);
      handleWsEvent(msg);
    } catch (_) {}
  };

  ws.onclose = () => {
    setWsStatus(false);
    setTimeout(connectWebSocket, 2500);
  };
}

function setWsStatus(online) {
  const dot    = document.getElementById('ws-indicator');
  const status = document.getElementById('ws-status');
  if (dot)    dot.style.background = online ? '#12b76a' : '#f04438';
  if (status) status.textContent   = online ? 'Live' : 'Reconnecting…';
}

function handleWsEvent(msg) {
  const t = msg.type, d = msg.data || {};

  if (t === 'TASK_COMPLETED') {
    if (d.task_type === 'media_size_reduction' && activeTaskIds.has(d.task_id)) {
      activeTaskIds.delete(d.task_id);
      onTaskFinished(d.result, d.execution_time_seconds, d.worker_id);
    }
  } else if (t === 'TASK_CLAIMED') {
    addLog('CLAIM', `[${d.task_type}] claimed by [${d.worker_id}]`, 'info');
  }

  fetchInitialData();
}

function onTaskFinished(result, duration, workerId) {
  if (!result) return;
  // Guard against duplicate result registration
  if (batchResults.some(r => r.output_filename === result.output_filename)) return;

  result._duration = duration;
  result._workerId = workerId;
  batchResults.push(result);

  // Update batch badge
  const idx = activeBatchFiles.findIndex(f => f.filename === result.input_filename);
  if (idx !== -1) {
    const b = document.getElementById(`bf-badge-${idx}`);
    if (b) {
      b.className = 'bf-status-badge';
      b.textContent = result.is_enhanced ? 'Enhanced ✓' : 'Reduced ✓';
    }
  }

  const progBar = document.getElementById('progress-bar');
  const progLbl = document.getElementById('progress-label');
  const pct = Math.min(100, Math.round((batchResults.length / activeBatchFiles.length) * 100));
  if (progBar) progBar.style.width = pct + '%';
  if (progLbl) progLbl.textContent = `Completed ${batchResults.length} / ${activeBatchFiles.length} file(s)…`;

  // Check if entire batch is complete
  if (batchResults.length >= activeBatchFiles.length) {
    if (taskPollTimer) clearInterval(taskPollTimer);
    setTimeout(showBatchResults, 300);
  }
}

/* ── PAGE 3: RESULT VIEW RENDERING (SMART ENHANCER DETECTION) ──── */
function showBatchResults() {
  const btn      = document.getElementById('btn-start');
  const progArea = document.getElementById('progress-area');

  btn.disabled = false;
  updateButtonLabel();
  if (progArea) progArea.style.display = 'none';

  showView('view-result');

  const count = batchResults.length;
  const isMulti = count > 1;

  // Single primary result (for Before/After and preview)
  const primary = batchResults[0];

  // Smart ENHANCER vs REDUCER Detection (Requirement 2)
  const isEnhanced = primary.is_enhanced || (primary.reduced_size_bytes > primary.original_size_bytes);

  const bannerEl     = document.getElementById('result-banner');
  const bannerIcon   = document.getElementById('result-banner-icon');
  const bannerText   = document.getElementById('result-banner-text');

  const outputBox    = document.getElementById('ba-box-output');
  const outputTag    = document.getElementById('ba-output-tag');
  const outputVal    = document.getElementById('ba-reduced');
  const outputSub    = document.getElementById('ba-reduced-sub');
  const downloadLbl  = document.getElementById('download-label');
  const downloadBtn  = document.getElementById('btn-download');

  // Configure styling based on ENHANCER vs REDUCER
  if (isEnhanced) {
    if (bannerEl) bannerEl.className = 'result-banner enhanced';
    if (bannerIcon) bannerIcon.textContent = '✨';
    if (bannerText) {
      bannerText.textContent = isMulti
        ? `Batch Processed: Enhanced & Optimized ${count} files!`
        : `Media Quality Enhanced! Added fidelity (+${primary.bytes_added_formatted || primary.change_percent + '%'})`;
    }

    if (outputBox) outputBox.className = 'ba-box enhanced';
    if (outputTag) outputTag.textContent = 'ENHANCED';
    if (outputSub) outputSub.textContent = `+${primary.change_percent}% higher fidelity`;

    if (downloadLbl) downloadLbl.textContent = `⬇ Download Enhanced File (${primary.reduced_size_formatted})`;
  } else {
    if (bannerEl) bannerEl.className = 'result-banner reduced';
    if (bannerIcon) bannerIcon.textContent = '🎉';
    if (bannerText) {
      bannerText.textContent = isMulti
        ? `Batch Processed: Successfully compressed ${count} files!`
        : `Size Reduced by ${primary.reduction_percent_achieved}%! (${primary.bytes_saved_formatted} saved)`;
    }

    if (outputBox) outputBox.className = 'ba-box reduced';
    if (outputTag) outputTag.textContent = 'REDUCED';
    if (outputSub) outputSub.textContent = `${primary.reduction_percent_achieved}% smaller`;

    if (downloadLbl) downloadLbl.textContent = `⬇ Download Reduced File (${primary.reduced_size_formatted})`;
  }

  // Original size
  document.getElementById('ba-orig').textContent     = primary.original_size_formatted;
  document.getElementById('ba-orig-sub').textContent = primary.input_filename;
  outputVal.textContent                             = primary.reduced_size_formatted;

  // Single metrics
  document.getElementById('metric-duration').textContent = `${primary._duration || primary.compute_time_seconds}s`;
  document.getElementById('metric-res').textContent      = primary.resolution_applied || '—';
  // worker label removed (py-worker hidden per user request)

  // Preview player
  const vidEl = document.getElementById('player-video');
  const imgEl = document.getElementById('player-image');
  const ph    = document.getElementById('preview-placeholder');
  if (ph) ph.style.display = 'none';

  if (primary.media_type === 'VIDEO') {
    if (imgEl) imgEl.style.display = 'none';
    if (vidEl && primary.output_url) {
      vidEl.style.display = 'block';
      vidEl.src = primary.output_url;
      vidEl.load();
      vidEl.play().catch(() => {});
    }
  } else {
    if (vidEl) vidEl.style.display = 'none';
    if (imgEl && primary.output_url) {
      imgEl.style.display = 'block';
      imgEl.src = primary.output_url;
    }
  }

  // Setup single download link
  if (downloadBtn && primary.output_filename) {
    downloadBtn.href = `/api/media/download/${primary.output_filename}`;
    downloadBtn.setAttribute('download', primary.output_filename);
  }

  // Multi-file batch table and ZIP button (Requirement 5)
  const batchWrap = document.getElementById('batch-results-wrap');
  const zipBtn    = document.getElementById('btn-download-zip');
  const zipLbl    = document.getElementById('zip-download-label');

  if (isMulti) {
    if (batchWrap) batchWrap.style.display = 'block';
    if (zipBtn)    zipBtn.style.display    = 'flex';
    if (zipLbl)    zipLbl.textContent     = `⬇ Download All as ZIP (${count} Files)`;

    const tbody = document.getElementById('batch-table-body');
    if (tbody) {
      tbody.innerHTML = batchResults.map(res => {
        const itemEnhanced = res.is_enhanced || (res.reduced_size_bytes > res.original_size_bytes);
        const tag = itemEnhanced
          ? `<span style="font-weight:700;color:#0284c7;background:#f0f9ff;padding:2px 8px;border-radius:4px">ENHANCED (+${res.change_percent}%)</span>`
          : `<span style="font-weight:700;color:#16a34a;background:#f0fdf4;padding:2px 8px;border-radius:4px">REDUCED (-${res.reduction_percent_achieved}%)</span>`;

        return `
          <tr>
            <td><strong>${res.input_filename}</strong></td>
            <td>${res.original_size_formatted}</td>
            <td><strong>${res.reduced_size_formatted}</strong></td>
            <td>${tag}</td>
            <td>
              <a href="/api/media/download/${res.output_filename}" download="${res.output_filename}"
                 style="color:var(--accent);font-weight:700;text-decoration:none">
                ⬇ Download
              </a>
            </td>
          </tr>
        `;
      }).join('');
    }
  } else {
    if (batchWrap) batchWrap.style.display = 'none';
    if (zipBtn)    zipBtn.style.display    = 'none';
  }

  addLog('COMPLETE', `Finished processing ${count} file(s) across worker mesh`, 'ok');
}

/* ── ZIP DOWNLOAD (Requirement 5) ──────────────────────────────── */
async function downloadAllAsZip() {
  const filenames = batchResults.map(r => r.output_filename).filter(Boolean);
  if (filenames.length === 0) {
    alert('No output files ready for ZIP packaging.');
    return;
  }

  const zipBtn = document.getElementById('btn-download-zip');
  const zipLbl = document.getElementById('zip-download-label');
  if (zipBtn) zipBtn.disabled = true;
  if (zipLbl) zipLbl.innerHTML = `<span class="spinner-ring"></span> Packaging ZIP Archive…`;

  try {
    const res = await fetch('/api/media/download-zip', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filenames: filenames })
    });

    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'ZIP creation failed');
    }

    const blob = await res.blob();
    const downloadUrl = window.URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = `PixelPolish_Batch_${new Date().toISOString().slice(0,10)}.zip`;
    document.body.appendChild(a);
    a.click();
    a.remove();
    window.URL.revokeObjectURL(downloadUrl);

    addLog('ZIP', `Downloaded ZIP package containing ${filenames.length} file(s)`, 'ok');
  } catch (err) {
    alert('ZIP download failed: ' + err.message);
  } finally {
    if (zipBtn) zipBtn.disabled = false;
    if (zipLbl) zipLbl.textContent = `⬇ Download All as ZIP (${filenames.length} Files)`;
  }
}

/* ── DISTRIBUTED SYSTEM PANEL & WORKERS ────────────────────────── */
function toggleSystemPanel() {
  const panel = document.getElementById('system-panel');
  const btn   = document.getElementById('sys-toggle-btn');
  const open  = panel.classList.toggle('open');
  btn.innerHTML = open
    ? `<svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M4.5 15.75l7.5-7.5 7.5 7.5"/></svg> Hide Distributed System Panel`
    : `<svg width="12" height="12" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2"><path stroke-linecap="round" stroke-linejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5"/></svg> Show Distributed System Panel (Worker Mesh · Task Queue · Telemetry)`;
}

async function fetchInitialData() {
  try {
    const [stats, workers, tasks] = await Promise.all([
      fetch('/api/stats').then(r => r.json()),
      fetch('/api/workers').then(r => r.json()),
      fetch('/api/tasks?limit=30').then(r => r.json())
    ]);

    const algoEl = document.getElementById('algorithm-select');
    if (algoEl && stats.scheduling_algorithm) algoEl.value = stats.scheduling_algorithm;

    renderWorkers(workers);
    renderTasks(tasks);
  } catch (e) { /* silent */ }
}

function renderWorkers(workers) {
  const container  = document.getElementById('workers-container');
  const countBadge = document.getElementById('worker-count-badge');
  if (!container) return;

  const active = (workers || []).filter(w => w.status !== 'DEAD').length;
  if (countBadge) countBadge.textContent = active;

  if (!workers || workers.length === 0) {
    container.innerHTML = `<div style="grid-column:1/-1;text-align:center;padding:16px;color:var(--text-muted);font-size:12px">Connecting to worker mesh…</div>`;
    return;
  }

  container.innerHTML = workers.map(w => {
    const dead = w.status === 'DEAD';
    return `
      <div class="worker-card">
        <div class="worker-top">
          <div class="worker-name">
            <span class="worker-status-dot ${dead ? 'dead' : ''}"></span>${w.worker_id}
          </div>
          <span class="worker-lang ${w.language}">${w.language}</span>
        </div>
        <div style="font-size:11px;color:var(--text-muted);display:flex;justify-content:space-between">
          <span>CPU: ${(w.cpu_percent||0).toFixed(1)}%</span>
          <span>RAM: ${(w.memory_percent||0).toFixed(1)}%</span>
          <span>Done: ${w.completed_tasks_count||0}</span>
        </div>
      </div>
    `;
  }).join('');
}

function renderTasks(tasks) {
  const tbody = document.getElementById('task-table-body');
  if (!tbody || !tasks) return;

  if (tasks.length === 0) {
    tbody.innerHTML = `<tr><td colspan="6" style="text-align:center;padding:16px;color:var(--text-muted)">No active tasks</td></tr>`;
    return;
  }

  tbody.innerHTML = tasks.slice(0, 15).map(t => {
    const dur = t.execution_time_seconds ? `${t.execution_time_seconds.toFixed(2)}s` : '—';
    const statusCol = t.status === 'COMPLETED' ? '#16a34a' : (t.status === 'RUNNING' ? '#2563eb' : '#64748b');
    return `
      <tr>
        <td><strong>${t.task_type}</strong> <span style="font-size:10px;color:var(--text-muted)">(${t.task_id.slice(0,6)})</span></td>
        <td>Level ${t.priority}</td>
        <td><span style="font-weight:700;color:${statusCol}">${t.status}</span></td>
        <td><code>${t.assigned_worker_id || '—'}</code></td>
        <td>${dur}</td>
        <td>${t.result ? '✓ Complete' : '…'}</td>
      </tr>
    `;
  }).join('');
}

async function switchAlgorithm() {
  const algo = document.getElementById('algorithm-select').value;
  try {
    await fetch(`/api/queue/algorithm?algorithm=${algo}`, { method: 'POST' });
    addLog('ALGO', `Scheduling policy updated to ${algo}`, 'info');
  } catch (_) {}
}




/* ── UTILITIES ─────────────────────────────────────────────────── */
function fmtBytes(b) {
  if (!b || b === 0) return '0 B';
  const u = ['B','KB','MB','GB'];
  const i = Math.floor(Math.log(b) / Math.log(1024));
  return (b / Math.pow(1024, i)).toFixed(2) + ' ' + u[i];
}

function addLog(tag, msg, type = 'info') {
  const c = document.getElementById('event-logs');
  if (!c) return;
  const t = new Date().toLocaleTimeString();
  const div = document.createElement('div');
  div.className = 'log-line';
  div.innerHTML = `<span class="log-time">${t}</span><span class="log-tag ${type}">[${tag}]</span><span class="log-msg">${msg}</span>`;
  c.appendChild(div);
  c.scrollTop = c.scrollHeight;
}

function clearLogs() {
  const c = document.getElementById('event-logs');
  if (c) c.innerHTML = '';
}
