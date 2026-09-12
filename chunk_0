/**
 * Cloudflare Worker 直链网盘（最终修复版）
 * 
 * 核心修复：
 * 1. 任务每任务独立一行，消除并发覆盖
 * 2. 分片直接写 GitHub，修复 7 分片后卡住
 * 3. 32 线程上传，70MB 分片
 * 4. 删除文件快速（直接删仓库 + 32 并发 + 结构先更新）
 * 5. 客户端 XHR 显示速度和进度
 * 6. 修复进度条跳回：进度只由本地控制，服务端 uploading 时绝不覆盖
 * 7. 进度计算：服务端完成字节 + 传输中字节×0.3 加权，单调递增
 */

const GITHUB_USER = 'ikecode26';
const GITHUB_API = 'https://api.github.com';
const ASSETS_REPO = 'netdisk-assets';
const KV_SIZE_LIMIT = -1;
const GITHUB_SINGLE_LIMIT = 0;
const SERIAL_THRESHOLD = 1024 * 1024 * 1024;
const CHUNK_SIZE = 70 * 1024 * 1024;
const CLIENT_CHUNK_SIZE = 70 * 1024 * 1024;
const GH_PROXY = 'https://v6.gh-proxy.com/';

function proxyUrl(url) {
  if (!url || url.startsWith(GH_PROXY)) return url;
  return GH_PROXY + url;
}

let d1Initialized = false;

async function fetchWithTimeout(url, options = {}, timeoutMs = 30000) {
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(id);
  }
}

function ssid() {
  const arr = new Uint8Array(16);
  crypto.getRandomValues(arr);
  return Array.from(arr, b => b.toString(36)).join('').replace(/[^a-z0-9]/g, '') + Date.now().toString(36) + Date.now().toString(36);
}

function getKV(sid, env) {
  const kvs = [env.FILE_KV_1, env.FILE_KV_2, env.FILE_KV_3, env.FILE_KV_4, env.FILE_KV_5];
  let sum = 0;
  for (let i = 0; i < sid.length; i++) sum += sid.charCodeAt(i);
  return kvs[sum % kvs.length];
}

function arrayBufferToBase64(buffer) {
  if (typeof Buffer !== 'undefined') return Buffer.from(buffer).toString('base64');
  const bytes = new Uint8Array(buffer);
  let binary = '';
  const cs = 0x8000;
  for (let i = 0; i < bytes.byteLength; i += cs) binary += String.fromCharCode.apply(null, bytes.subarray(i, i + cs));
  return btoa(binary);
}

function formatSize(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024, sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatSpeed(bps) {
  if (bps === 0) return '0 B/s';
  if (bps < 1024) return bps.toFixed(0) + ' B/s';
  if (bps < 1024 * 1024) return (bps / 1024).toFixed(1) + ' KB/s';
  return (bps / 1024 / 1024).toFixed(1) + ' MB/s';
}

function getMime(name) {
  const ext = name.split('.').pop().toLowerCase();
  const map = {
    mp4: 'video/mp4', webm: 'video/webm', mkv: 'video/x-matroska', a3v8: 'video/mp4',
    mp3: 'audio/mpeg', wav: 'audio/wav', ogg: 'audio/ogg', flac: 'audio/flac', m4a: 'audio/mp4',
    txt: 'text/plain', md: 'text/markdown', json: 'application/json', js: 'application/javascript',
    css: 'text/css', html: 'text/html', xml: 'application/xml',
    zip: 'application/zip', rar: 'application/vnd.rar', '7z': 'application/x-7z-compressed',
    tar: 'application/x-tar', gz: 'application/gzip',
    pdf: 'application/pdf', jpg: 'image/jpeg', jpeg: 'image/jpeg', png: 'image/png', gif: 'image/gif', webp: 'image/webp'
  };
  return map[ext] || 'application/octet-stream';
}

function jsonResponse(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { 'Content-Type': 'application/json' } });
}

function errorResponse(msg, status = 400) {
  return jsonResponse({ error: msg }, status);
}

function checkPassword(request, env) {
  const password = env.CLOUD_PASSWORD;
  if (!password) return true;
  const cookie = request.headers.get('Cookie') || '';
  const cm = cookie.match(/(?:^|;)\s*auth=([^;]+)/);
  if (cm && decodeURIComponent(cm[1]) === password) return true;
  const auth = request.headers.get('Authorization') || '';
  const bm = auth.match(/^Bearer\s+(.+)$/i);
  if (bm && bm[1] === password) return true;
  const xpw = request.headers.get('X-Password');
  if (xpw && xpw === password) return true;
  try {
    const url = new URL(request.url);
    const qp = url.searchParams.get('auth');
    if (qp && qp === password) return true;
  } catch (_) {}
  return false;
}

function requirePassword(request, env) {
  if (!checkPassword(request, env)) return errorResponse('需要密码访问', 401);
  return null;
}

function getD1(env) {
  if (!env.NDK) throw new Error('D1 数据库 NDK 未绑定');
  return env.NDK;
}

async function ensureD1(env) {
  if (d1Initialized) return;
  await getD1(env).exec('CREATE TABLE IF NOT EXISTS kv_store (key TEXT PRIMARY KEY, value TEXT NOT NULL);');
  d1Initialized = true;
}

async function d1Get(env, key, defaultValue = undefined) {
  await ensureD1(env);
  const row = await getD1(env).prepare('SELECT value FROM kv_store WHERE key = ?').bind(key).first();
  if (!row || !row.value) return defaultValue;
  try { return JSON.parse(row.value); } catch (e) { return defaultValue; }
}

async function d1Set(env, key, value) {
  await ensureD1(env);
  await getD1(env).prepare('INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)')
    .bind(key, JSON.stringify(value)).run();
}

async function getStructure(env) {
  return await d1Get(env, 'file_structure', { type: 'root', name: '', children: {}, createdAt: Date.now() });
}
async function saveStructure(env, structure) { await d1Set(env, 'file_structure', structure); }
async function getSettings(env) { return await d1Get(env, 'app_settings', {}); }
async function saveSettings(env, settings) { await d1Set(env, 'app_settings', settings); }

function getNode(structure, path) {
  const parts = path.split('/').filter(Boolean);
  if (parts.length === 0) return structure;
  let node = structure;
  for (const p of parts) {
    if (!node.children || !node.children[p]) return null;
    node = node.children[p];
  }
  return node;
}

function setNode(structure, path, node) {
  const parts = path.split('/').filter(Boolean);
  let parent = structure;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (!parent.children[p]) parent.children[p] = { type: 'folder', name: p, children: {}, createdAt: Date.now() };
    parent = parent.children[p];
  }
  parent.children[parts[parts.length - 1]] = node;
}

function deleteNode(structure, path) {
  const parts = path.split('/').filter(Boolean);
  let parent = structure;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (!parent.children[p]) return false;
    parent = parent.children[p];
  }
  if (parent.children[parts[parts.length - 1]]) { delete parent.children[parts[parts.length - 1]]; return true; }
  return false;
}

function renameNode(structure, path, newName) {
  const parts = path.split('/').filter(Boolean);
  let parent = structure;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (!parent.children[p]) return null;
    parent = parent.children[p];
  }
  const oldName = parts[parts.length - 1];
  const node = parent.children[oldName];
  if (!node) return null;
  node.name = newName;
  delete parent.children[oldName];
  parent.children[newName] = node;
  return node;
}

function collectPaths(node, base = '') {
  let list = [];
  if (!node.children) return list;
  for (const [name, child] of Object.entries(node.children)) {
    const p = base ? `${base}/${name}` : name;
    list.push(p);
    if (child.type === 'folder') list = list.concat(collectPaths(child, p));
  }
  return list;
}

function cloneNode(node) {
  if (node.type === 'file') return { ...node, createdAt: Date.now() };
  const copy = { ...node, children: {} };
  for (const [name, child] of Object.entries(node.children)) copy.children[name] = cloneNode(child);
  return copy;
}

function isDescendantOrSelf(parentPath, childPath) {
  const p = parentPath.replace(/^\/|\/$/g, '');
  const c = childPath.replace(/^\/|\/$/g, '');
  if (!p) return true;
  return c === p || c.startsWith(p + '/');
}

function uniqueName(children, name) {
  if (!children[name]) return name;
  const dot = name.lastIndexOf('.');
  const base = dot > 0 ? name.slice(0, dot) : name;
  const ext = dot > 0 ? name.slice(dot) : '';
  let i = 1;
  while (children[base + ' (' + i + ')' + ext]) i++;
  return base + ' (' + i + ')' + ext;
}

function removeChild(structure, childPath) {
  const parts = childPath.split('/').filter(Boolean);
  const name = parts.pop();
  const parentPath = parts.join('/');
  const parent = parentPath ? getNode(structure, parentPath) : structure;
  if (parent && parent.children && parent.children[name]) delete parent.children[name];
}

function copyMissingChildren(sourceNode, targetNode) {
  for (const [childName, child] of Object.entries(sourceNode.children || {})) {
    if (!targetNode.children[childName]) targetNode.children[childName] = cloneNode(child);
    else if (child.type === 'folder' && targetNode.children[childName].type === 'folder') copyMissingChildren(child, targetNode.children[childName]);
  }
  return { ok: true };
}

function moveNode(structure, path, targetPath, mode = 'overwrite') {
  const parts = path.split('/').filter(Boolean);
  const name = parts[parts.length - 1];
  const node = getNode(structure, path);
  if (!node) return { ok: false, error: '源文件不存在' };
  const targetNode = targetPath ? getNode(structure, targetPath) : structure;
  if (!targetNode || (targetNode.type !== 'folder' && targetNode.type !== 'root')) return { ok: false, error: '目标文件夹不存在' };
  if (isDescendantOrSelf(path, targetPath)) return { ok: false, error: '不能移动到自身或子文件夹内' };
  if (targetNode.children[name]) {
    if (mode === 'skip' || mode === 'newOnly') return { ok: true };
    if (mode === 'rename') {
      const newName = uniqueName(targetNode.children, name);
      deleteNode(structure, path);
      node.name = newName;
      targetNode.children[newName] = node;
      return { ok: true };
    }
    removeChild(structure, (targetPath ? targetPath + '/' : '') + name);
  }
  deleteNode(structure, path);
  targetNode.children[name] = node;
  return { ok: true };
}

function copyNode(structure, path, targetPath, mode = 'overwrite') {
  const parts = path.split('/').filter(Boolean);
  const name = parts[parts.length - 1];
  const node = getNode(structure, path);
  if (!node) return { ok: false, error: '源文件不存在' };
  const targetNode = targetPath ? getNode(structure, targetPath) : structure;
  if (!targetNode || (targetNode.type !== 'folder' && targetNode.type !== 'root')) return { ok: false, error: '目标文件夹不存在' };
  if (isDescendantOrSelf(path, targetPath)) return { ok: false, error: '不能复制到自身或子文件夹内' };
  if (targetNode.children[name]) {
    if (mode === 'skip') return { ok: true };
    if (mode === 'newOnly') {
      if (node.type === 'folder' && targetNode.children[name].type === 'folder') return copyMissingChildren(node, targetNode.children[name]);
      return { ok: true };
    }
    if (mode === 'rename') {
      const newName = uniqueName(targetNode.children, name);
      const cloned = cloneNode(node);
      cloned.name = newName;
      targetNode.children[newName] = cloned;
      return { ok: true };
    }
    removeChild(structure, (targetPath ? targetPath + '/' : '') + name);
  }
  targetNode.children[name] = cloneNode(node);
  return { ok: true };
}

// ==================== GitHub API ====================

async function githubCreateRepo(sid, env) {
  const headers = {
    'Authorization': `token ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'Content-Type': 'application/json',
    'User-Agent': 'netdisk-worker'
  };
  const getHeaders = {
    'Authorization': `token ${env.GITHUB_TOKEN}`,
    'Accept': 'application/vnd.github+json',
    'User-Agent': 'netdisk-worker'
  };
  const exist = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}`, { headers: getHeaders }, 20000).catch(() => null);
  if (exist && exist.ok) return { name: sid, reused: true };
  const resp = await fetchWithTimeout(`${GITHUB_API}/user/repos`, {
    method: 'POST', headers,
    body: JSON.stringify({ name: sid, private: false, auto_init: true, description: 'Netdisk storage' })
  }, 30000);
  if (!resp.ok) {
    const txt = await resp.text();
    if (resp.status === 422 && txt.includes('name already exists')) return { name: sid, reused: true };
    throw new Error(`创建仓库失败: ${resp.status} ${txt}`);
  }
  const data = await resp.json();
  for (let i = 0; i < 10; i++) {
    await new Promise(r => setTimeout(r, 300));
    const check = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}`, { headers: getHeaders }, 10000).catch(() => null);
    if (check && check.ok) return data;
  }
  return data;
}

async function githubGetFileSha(sid, path, env) {
  try {
    const resp = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}/contents/${encodeURIComponent(path)}`, {
      headers: { 'Authorization': `token ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'netdisk-worker' }
    }, 20000);
    if (!resp.ok) return null;
    const data = await resp.json();
    return Array.isArray(data) ? null : data.sha;
  } catch (e) { return null; }
}

async function githubUploadFile(sid, path, content, env, message = 'upload', skipVerify = false) {
  const base64 = arrayBufferToBase64(content);
  if (base64.length > 95 * 1024 * 1024) throw new Error(`分片 base64 后 ${(base64.length / 1024 / 1024).toFixed(1)}MB 超过 GitHub 限制`);
  let retries = 0;
  const maxRetries = 5;
  while (true) {
    const sha = skipVerify ? null : await githubGetFileSha(sid, path, env);
    const body = { message, content: base64 };
    if (sha) body.sha = sha;
    const resp = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}/contents/${encodeURIComponent(path)}`, {
      method: 'PUT',
      headers: {
        'Authorization': `token ${env.GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'netdisk-worker'
      },
      body: JSON.stringify(body)
    }, 180000);
    if (resp.ok) {
      let data = null;
      try { data = await resp.json(); } catch (e) {}
      return data || { sha: data && data.content && data.content.sha };
    } else {
      const txt = await resp.text();
      if ((resp.status === 409 || (resp.status === 422 && txt.includes('sha'))) && retries < maxRetries) {
        retries++; await new Promise(r => setTimeout(r, 1000)); continue;
      }
      throw new Error(`上传 GitHub 失败: ${resp.status} ${txt}`);
    }
  }
}

async function githubDeleteRepo(sid, env) {
  const resp = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}`, {
    method: 'DELETE',
    headers: { 'Authorization': `token ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'netdisk-worker' }
  }, 30000);
  if (resp.ok || resp.status === 404) return true;
  const txt = await resp.text();
  throw new Error(`删除仓库失败: ${resp.status} ${txt}`);
}

async function githubDeleteFile(sid, path, env) {
  try {
    const infoResp = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}/contents/${encodeURIComponent(path)}`, {
      headers: { 'Authorization': `token ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'netdisk-worker' }
    }, 20000);
    if (!infoResp.ok) return;
    const info = await infoResp.json();
    const sha = Array.isArray(info) ? null : info.sha;
    if (!sha) return;
    await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}/contents/${encodeURIComponent(path)}`, {
      method: 'DELETE',
      headers: {
        'Authorization': `token ${env.GITHUB_TOKEN}`,
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'netdisk-worker'
      },
      body: JSON.stringify({ message: 'delete', sha })
    }, 20000);
  } catch (e) {}
}

async function githubGetDownloadUrl(sid, path, env) {
  const resp = await fetchWithTimeout(`${GITHUB_API}/repos/${GITHUB_USER}/${sid}/contents/${encodeURIComponent(path)}`, {
    headers: { 'Authorization': `token ${env.GITHUB_TOKEN}`, 'Accept': 'application/vnd.github+json', 'User-Agent': 'netdisk-worker' }
  }, 20000);
  if (!resp.ok) throw new Error(`获取下载链接失败: ${resp.status}`);
  const data = await resp.json();
  return data.download_url;
}

async function githubFetchFile(sid, path, env) {
  const url = await githubGetDownloadUrl(sid, path, env);
  const proxiedUrl = proxyUrl(url);
  let lastErr = null;
  for (let i = 0; i < 5; i++) {
    const resp = await fetchWithTimeout(proxiedUrl, { headers: { 'User-Agent': 'netdisk-worker', 'Accept-Encoding': 'identity' } }, 60000);
    if (resp.ok) return resp;
    lastErr = `GitHub 下载失败: ${resp.status}`;
    if (resp.status === 404) { await new Promise(r => setTimeout(r, 800)); continue; }
    throw new Error(lastErr);
  }
  throw new Error(lastErr);
}

async function githubStreamChunks(fileNode, writable, env) {
  const writer = writable.getWriter();
  try {
    for (let i = 0; i < fileNode.chunks; i++) {
      const resp = await githubFetchFile(fileNode.ssid, `chunk_${i}`, env);
      const reader = resp.body.getReader();
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        await writer.write(value);
      }
    }
  } catch (e) { await writer.abort(e); throw e; }
  finally { try { await writer.close(); } catch (e) {} }
}

// ==================== 任务系统 ====================

async function getTasks(env) {
  await ensureD1(env);
  const rows = await getD1(env).prepare("SELECT value FROM kv_store WHERE key LIKE 'task_%'").all();
  const tasks = [];
  for (const r of (rows.results || [])) {
    try { tasks.push(JSON.parse(r.value)); } catch (e) {}
  }
  tasks.sort((a, b) => (b.updatedAt || b.createdAt || 0) - (a.updatedAt || a.createdAt || 0));
  return tasks.slice(0, 300);
}

async function addTask(env, task) {
  await ensureD1(env);
  task.startedAt = task.startedAt || Date.now();
  task.createdAt = task.createdAt || Date.now();
  task.updatedAt = Date.now();
  await getD1(env).prepare('INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)')
    .bind('task_' + task.id, JSON.stringify(task)).run();
  return task;
}

async function updateTask(env, id, updates) {
  await ensureD1(env);
  const row = await getD1(env).prepare('SELECT value FROM kv_store WHERE key = ?').bind('task_' + id).first();
  if (!row || !row.value) return;
  let task;
  try { task = JSON.parse(row.value); } catch (e) { return; }
  Object.assign(task, updates, { updatedAt: Date.now() });
  await getD1(env).prepare('INSERT OR REPLACE INTO kv_store (key, value) VALUES (?, ?)')
    .bind('task_' + id, JSON.stringify(task)).run();
}

async function cancelTask(env, id) { await updateTask(env, id, { status: 'cancelled', message: '已取消' }); }

async function deleteTask(env, id) {
  await ensureD1(env);
  await getD1(env).prepare('DELETE FROM kv_store WHERE key = ?').bind('task_' + id).run();
}

const _taskThrottleCache = new Map();
async function updateTaskThrottled(env, id, updates) {
  const now = Date.now();
  const last = _taskThrottleCache.get(id) || 0;
  const isCritical = updates.status && updates.status !== 'uploading';
  if (!isCritical && now - last < 600) return;
  _taskThrottleCache.set(id, now);
  return updateTask(env, id, updates);
}

async function uploadBackgroundImage(buffer, ext, env) {
  await githubCreateRepo(ASSETS_REPO, env);
  const fileName = 'bg_' + Date.now() + '.' + ext;
  await githubUploadFile(ASSETS_REPO, fileName, buffer, env, 'background image');
  return await githubGetDownloadUrl(ASSETS_REPO, fileName, env);
}

async function uploadFontFile(buffer, ext, env) {
  await githubCreateRepo(ASSETS_REPO, env);
  const fileName = 'font_' + Date.now() + '.' + ext;
  await githubUploadFile(ASSETS_REPO, fileName, buffer, env, 'custom font');
  return await githubGetDownloadUrl(ASSETS_REPO, fileName, env);
}

// ==================== 存储核心 ====================

async function deleteFileStorage(node, env) {
  if (node.storage === 'kv') {
    try { await getKV(node.ssid, env).delete(node.ssid); } catch (e) {}
    return;
  }
  if (node.storage === 'github') {
    try { await githubDeleteRepo(node.ssid, env); return; } catch (e) {}
    const tasks = [];
    if (node.chunks > 1) {
      for (let i = 0; i < node.chunks; i++) tasks.push(githubDeleteFile(node.ssid, `chunk_${i}`, env));
    } else if (node.chunks === 1) {
      tasks.push(githubDeleteFile(node.ssid, 'chunk_0', env));
    } else if (node.githubPath) {
      tasks.push(githubDeleteFile(node.ssid, node.githubPath, env));
    }
    await Promise.all(tasks);
  }
}

async function deleteFileStoragesConcurrently(nodes, env, concurrency = 32) {
  let cursor = 0;
  const arr = nodes.slice();
  async function worker() {
    while (true) {
      const idx = cursor++;
      if (idx >= arr.length) return;
      try { await deleteFileStorage(arr[idx], env); } catch (e) {}
    }
  }
  const runners = [];
  for (let i = 0; i < Math.min(concurrency, arr.length); i++) runners.push(worker());
  await Promise.all(runners);
}

async function buildDownloadResponse(node, filename, env, inline = false) {
  const disp = inline ? 'inline' : 'attachment';
  const headers = {
    'Content-Disposition': `${disp}; filename*=UTF-8''${encodeURIComponent(filename)}`,
    'Content-Type': getMime(filename)
  };
  try {
    if (node.storage === 'kv') {
      const data = await getKV(node.ssid, env).get(node.ssid, { type: 'arrayBuffer' });
      if (!data) throw new Error('KV 数据丢失');
      return new Response(data, { headers });
    }
    if (node.chunks > 1) {
      const { readable, writable } = new TransformStream();
      githubStreamChunks(node, writable, env);
      return new Response(readable, { headers });
    }
    if (node.chunks === 1) {
      const resp = await githubFetchFile(node.ssid, 'chunk_0', env);
      return new Response(resp.body, { headers });
    }
    const resp = await githubFetchFile(node.ssid, node.githubPath || node.name || filename, env);
    return new Response(resp.body, { headers });
  } catch (e) {
    return errorResponse('文件下载失败: ' + e.message, 500);
  }
}

// ==================== WebDAV ====================

function checkBasicAuth(request, env) {
  const password = env.CLOUD_PASSWORD;
  if (!password) return true;
  const auth = request.headers.get('Authorization') || '';
  const m = auth.match(/^Basic\s+(.+)$/i);
  if (!m) return false;
  try {
    const creds = atob(m[1]);
    return creds.split(':').slice(1).join(':') === password;
  } catch (e) { return false; }
}

function davUnauthorized() {
  return new Response('Unauthorized', { status: 401, headers: { 'WWW-Authenticate': 'Basic realm="netdisk"' } });
}
function davXmlResponse(xml, status = 207) {
  return new Response(xml, { status, headers: { 'Content-Type': 'text/xml; charset=utf-8', 'DAV': '1, 2' } });
}
function toDavDate(ts) { return new Date(ts).toUTCString(); }
function davHref(path) { return '/webdav' + encodeURI(path).replace(/%2F/g, '/'); }
function escapeXml(text) { return String(text).replace(/[<>&'"]/g, m => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;', '"': '&quot;', "'": '&apos;' }[m])); }

function davPropResponse(path, node) {
  const name = path ? path.split('/').filter(Boolean).pop() : '';
  const isFolder = !node || node.type === 'folder' || node.type === 'root';
  const href = davHref(path || '/');
  const lastMod = toDavDate(node && node.createdAt ? node.createdAt : Date.now());
  let props = `<D:displayname>${escapeXml(name || 'root')}</D:displayname>`;
  if (isFolder) props += `<D:resourcetype><D:collection/></D:resourcetype><D:getcontentlength>0</D:getcontentlength>`;
  else props += `<D:resourcetype/><D:getcontentlength>${node.size || 0}</D:getcontentlength><D:getcontenttype>${getMime(name)}</D:getcontenttype>`;
  props += `<D:getlastmodified>${lastMod}</D:getlastmodified>`;
  return `<D:response><D:href>${href}</D:href><D:propstat><D:prop>${props}</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`;
}

async function handleWebDAV(request, env, reqPath) {
  if (!checkBasicAuth(request, env)) return davUnauthorized();
  const davPath = decodeURIComponent(reqPath.slice('/webdav'.length) || '/');
  const method = request.method;
  if (method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: { 'DAV': '1, 2', 'Allow': 'OPTIONS, GET, HEAD, PUT, DELETE, MKCOL, PROPFIND, PROPPATCH, MOVE, COPY, LOCK, UNLOCK', 'MS-Author-Via': 'DAV' } });
  }
  if (method === 'PROPFIND') {
    const depth = request.headers.get('Depth') || 'infinity';
    const structure = await getStructure(env);
    const node = davPath === '/' ? structure : getNode(structure, davPath);
    if (!node) return new Response('Not Found', { status: 404 });
    let responses = [davPropResponse(davPath || '/', node)];
    if ((node.type === 'folder' || node.type === 'root') && depth !== '0') {
      for (const [name, child] of Object.entries(node.children || {})) {
        const childPath = davPath === '/' ? name : davPath + '/' + name;
        responses.push(davPropResponse(childPath, child));
        if ((depth === 'infinity' || depth === '-1') && child.type === 'folder') {
          for (const sp of collectPaths(child, childPath)) responses.push(davPropResponse(sp, getNode(structure, sp)));
        }
      }
    }
    return davXmlResponse(`<?xml version="1.0" encoding="utf-8"?><D:multistatus xmlns:D="DAV:">${responses.join('')}</D:multistatus>`);
  }
  if (method === 'GET' || method === 'HEAD') {
    const structure = await getStructure(env);
    const node = getNode(structure, davPath);
    if (!node || node.type !== 'file') return new Response('Not Found', { status: 404 });
    if (method === 'HEAD') {
      return new Response(null, { status: 200, headers: { 'Content-Type': getMime(node.name), 'Content-Length': String(node.size || 0), 'Last-Modified': toDavDate(node.createdAt) } });
    }
    return buildDownloadResponse(node, node.name, env, true);
  }
  if (method === 'PUT') {
    const filename = davPath.split('/').pop();
    if (!filename) return new Response('Conflict', { status: 409 });
    const buffer = await request.arrayBuffer();
    const size = buffer.byteLength;
    const id = ssid();
    await githubCreateRepo(id, env);
    const chunks = Math.ceil(size / CHUNK_SIZE);
    for (let i = 0; i < chunks; i++) {
      const start = i * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, size);
      await githubUploadFile(id, `chunk_${i}`, buffer.slice(start, end), env, `chunk ${i}`, true);
    }
    const structure = await getStructure(env);
    const oldNode = getNode(structure, davPath);
    if (oldNode && oldNode.type === 'file') { try { await deleteFileStorage(oldNode, env); } catch (e) {} }
    setNode(structure, davPath, { type: 'file', name: filename, ssid: id, storage: 'github', size, chunks, createdAt: Date.now() });
    await saveStructure(env, structure);
    return new Response(null, { status: oldNode ? 204 : 201 });
  }
  if (method === 'DELETE') {
    const structure = await getStructure(env);
    const node = getNode(structure, davPath);
    if (!node) return new Response('Not Found', { status: 404 });
    const filesToDelete = [];
    if (node.type === 'file') filesToDelete.push(node);
    else {
      for (const p of collectPaths(node, davPath)) {
        const child = getNode(structure, p);
        if (child && child.type === 'file') filesToDelete.push(child);
      }
    }
    deleteNode(structure, davPath);
    await saveStructure(env, structure);
    deleteFileStoragesConcurrently(filesToDelete, env, 32).catch(() => {});
    return new Response(null, { status: 204 });
  }
  if (method === 'MKCOL') {
    const structure = await getStructure(env);
    if (getNode(structure, davPath)) return new Response('Method Not Allowed', { status: 405 });
    const parts = davPath.split('/').filter(Boolean);
    let parent = structure;
    for (const p of parts) {
      if (!parent.children[p]) parent.children[p] = { type: 'folder', name: p, children: {}, createdAt: Date.now() };
      parent = parent.children[p];
    }
    await saveStructure(env, structure);
    return new Response(null, { status: 201 });
  }
  if (method === 'MOVE') {
    const dest = request.headers.get('Destination');
    if (!dest) return new Response('Bad Request', { status: 400 });
    let destPath = decodeURIComponent(new URL(dest).pathname);
    if (destPath.startsWith('/webdav')) destPath = destPath.slice('/webdav'.length) || '/';
    const structure = await getStructure(env);
    const node = getNode(structure, davPath);
    if (!node) return new Response('Not Found', { status: 404 });
    if (node.type === 'file') {
      setNode(structure, destPath, { ...node, name: destPath.split('/').pop(), createdAt: Date.now() });
    } else {
      for (const p of collectPaths(node, davPath)) {
        const child = getNode(structure, p);
        if (!child) continue;
        setNode(structure, destPath + p.slice(davPath.length), { ...child, createdAt: Date.now() });
      }
      setNode(structure, destPath, { ...node, name: destPath.split('/').pop(), createdAt: Date.now() });
    }
    deleteNode(structure, davPath);
    await saveStructure(env, structure);
    return new Response(null, { status: 204 });
  }
  if (method === 'COPY') return new Response('Not Implemented', { status: 501 });
  if (method === 'LOCK') {
    const token = 'opaquelocktoken:' + crypto.randomUUID();
    const xml = `<?xml version="1.0" encoding="utf-8"?><D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock><D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope><D:depth>infinity</D:depth><D:owner></D:owner><D:timeout>Second-3600</D:timeout><D:locktoken><D:href>${token}</D:href></D:locktoken></D:activelock></D:lockdiscovery></D:prop>`;
    return new Response(xml, { status: 200, headers: { 'Content-Type': 'text/xml; charset=utf-8', 'Lock-Token': '<' + token + '>' } });
  }
  if (method === 'UNLOCK') return new Response(null, { status: 204 });
  return new Response('Method Not Allowed', { status: 405 });
}

// ==================== HTML 模板 ====================

const COMMON_HEAD = `
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
<style>
:root { --primary:#1976d2; --surface:#fff; --bg:#f5f5f5; --divider:#e0e0e0; --text:#212121; --text-sec:#757575; --danger:#d32f2f; --success:#388e3c; }
* { box-sizing:border-box; }
body { margin:0; font-family:system-ui,sans-serif; background:var(--bg); color:var(--text); }
.appbar { position:fixed; top:0; left:0; right:0; height:48px; background:var(--primary); color:#fff; display:flex; align-items:center; padding:0 12px; z-index:20; box-shadow:0 2px 4px rgba(0,0,0,.2); }
.appbar h1 { margin:0; font-size:16px; font-weight:500; flex:1; }
.appbar .material-icons { cursor:pointer; padding:6px; font-size:20px; }
.container { padding:60px 12px 76px 12px; max-width:900px; margin:0 auto; }
.breadcrumbs { display:flex; align-items:center; gap:4px; margin-bottom:12px; flex-wrap:wrap; }
.breadcrumbs a { color:var(--primary); text-decoration:none; font-size:14px; }
.breadcrumbs span { color:var(--text-sec); font-size:14px; }
.card { background:var(--surface); border-radius:10px; box-shadow:0 1px 3px rgba(0,0,0,.1); padding:10px; margin-bottom:10px; }
.file-card { background:#fff; border-radius:8px; box-shadow:0 1px 2px rgba(0,0,0,.08); padding:6px 8px; margin-bottom:5px; }
.file-main { display:flex; align-items:center; gap:6px; cursor:pointer; }
.file-icon { width:28px; height:28px; border-radius:5px; background:#e3f2fd; display:flex; align-items:center; justify-content:center; color:var(--primary); flex-shrink:0; }
.file-icon .material-icons { font-size:18px; }
.file-name-wrap { flex:1; min-width:0; overflow:hidden; }
.file-name { display:inline-block; font-size:13px; font-weight:500; white-space:nowrap; }
.file-meta { font-size:10px; color:var(--text-sec); margin-top:0; }
.file-actions { display:flex; gap:3px; margin-top:4px; justify-content:flex-end; flex-wrap:wrap; }
.file-actions button { background:none; border:none; color:var(--text-sec); cursor:pointer; padding:6px; border-radius:50%; }
.file-actions button .material-icons { font-size:16px; pointer-events:none; }
.file-card.selected { background:#e3f2fd !important; }
.sel-check { width:18px; height:18px; accent-color:var(--primary); margin-right:4px; display:none; }
#file-list.selection-mode .sel-check { display:inline-block; }
.sort-select { flex:1; padding:5px 6px; border-radius:6px; border:1px solid var(--divider); background:var(--surface); font-size:13px; }
.selection-bar { display:none; align-items:center; gap:5px; margin-bottom:5px; padding:5px 8px; background:var(--surface); border-radius:8px; flex-wrap:wrap; }
.selection-bar.show { display:flex; }
.selection-bar button { padding:4px 6px; border:none; border-radius:5px; cursor:pointer; font-size:11px; background:#e0e0e0; }
#btn-select-mode.active { color:var(--primary); background:#e3f2fd; }
.empty { text-align:center; padding:32px 0; color:var(--text-sec); font-size:13px; }
.bottom-bar { position:fixed; bottom:0; left:0; right:0; height:52px; background:var(--surface); display:flex; box-shadow:0 -2px 6px rgba(0,0,0,.1); z-index:20; }
.bottom-bar button { flex:1; border:none; background:none; display:flex; flex-direction:column; align-items:center; justify-content:center; color:var(--text-sec); cursor:pointer; font-size:11px; }
.bottom-bar button .material-icons { font-size:20px; }
.fab-menu { position:fixed; bottom:64px; left:50%; transform:translateX(-50%); background:var(--surface); border-radius:10px; box-shadow:0 4px 12px rgba(0,0,0,.2); display:none; flex-direction:column; min-width:160px; z-index:30; }
.fab-menu.show { display:flex; }
.fab-menu button { padding:10px 14px; border:none; background:none; text-align:left; cursor:pointer; font-size:13px; display:flex; align-items:center; gap:8px; }
.drawer { position:fixed; top:0; right:-300px; width:300px; max-width:85vw; bottom:0; background:var(--surface); box-shadow:-2px 0 8px rgba(0,0,0,.2); z-index:40; transition:right .3s; display:flex; flex-direction:column; }
.drawer.show { right:0; }
.drawer-head { height:48px; background:var(--primary); color:#fff; display:flex; align-items:center; padding:0 12px; font-weight:500; font-size:15px; }
.drawer-body { flex:1; overflow-y:auto; padding:8px; }
.task-item { padding:8px 10px; border-bottom:1px solid var(--divider); }
.task-title { font-size:13px; font-weight:500; }
.task-msg { font-size:11px; color:var(--text-sec); margin-top:1px; }
.task-progress { height:3px; background:var(--divider); border-radius:1.5px; margin-top:5px; overflow:hidden; }
.task-progress>div { height:100%; background:var(--primary); transition:width .3s; }
.task-actions { display:flex; gap:6px; margin-top:5px; }
.task-actions button { font-size:11px; padding:3px 6px; border:1px solid var(--divider); background:#fff; border-radius:4px; cursor:pointer; }
.modal-overlay { position:fixed; inset:0; background:rgba(0,0,0,.5); z-index:50; display:none; align-items:center; justify-content:center; }
.modal-overlay.show { display:flex; }
.modal { background:var(--surface); border-radius:12px; width:90%; max-width:400px; padding:20px; }
.modal input, .modal textarea { width:100%; padding:10px; border:1px solid var(--divider); border-radius:8px; font-size:14px; margin-bottom:12px; }
.modal textarea { min-height:120px; resize:vertical; }
.modal-actions { display:flex; justify-content:flex-end; gap:8px; }
.modal-actions button { padding:8px 16px; border:none; border-radius:8px; cursor:pointer; font-size:14px; }
.btn-primary { background:var(--primary); color:#fff; }
.btn-secondary { background:#e0e0e0; color:var(--text); }
.snackbar { position:fixed; bottom:80px; left:50%; transform:translateX(-50%); background:#323232; color:#fff; padding:10px 16px; border-radius:8px; font-size:14px; z-index:60; display:none; }
.snackbar.show { display:block; }
.preview-box { max-width:100%; overflow:auto; }
.preview-box img, .preview-box video { max-width:100%; border-radius:8px; }
.login-box { max-width:360px; margin:80px auto; }
</style>
`;

function page(title, body, scripts = '', themeCss = '') {
  return new Response(`<!DOCTYPE html><html><head>${COMMON_HEAD}${themeCss}<title>${escapeHtml(title)}</title></head><body>${body}${scripts}</body></html>`, {
    headers: { 'Content-Type': 'text/html; charset=utf-8' }
  });
}

const HOME_BODY = `
<div class="appbar"><h1>我的网盘</h1><div style="display:flex;align-items:center;gap:8px;"><span class="material-icons" id="btn-refresh">refresh</span><span class="material-icons" id="btn-settings">settings</span><span class="material-icons" id="btn-logout">logout</span></div></div>
<div class="container">
  <div class="breadcrumbs" id="breadcrumbs"><a href="/?path=">首页</a></div>
  <div id="selection-bar" class="selection-bar">
    <span id="selection-count" style="font-size:14px;flex:1;">已选 0</span>
    <button id="sel-all">全选</button>
    <button id="sel-cancel">取消</button>
    <button id="sel-move">移动</button>
    <button id="sel-copy">复制</button>
    <button id="sel-delete" style="color:var(--danger);">删除</button>
  </div>
  <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px;">
    <select id="sort-select" class="sort-select">
      <option value="name-asc">名称正序</option>
      <option value="name-desc">名称倒序</option>
      <option value="time-desc">上传时间倒序</option>
      <option value="time-asc">上传时间正序</option>
      <option value="size-desc">大小倒序</option>
      <option value="size-asc">大小正序</option>
    </select>
    <span class="material-icons" id="btn-select-mode" style="cursor:pointer;padding:8px;background:var(--surface);border-radius:50%;">check_circle</span>
  </div>
  <div id="file-list"></div>
</div>
<div class="fab-menu" id="new-menu">
  <button onclick="newText()"><span class="material-icons">article</span> 新建文本</button>
  <button onclick="newFolder()"><span class="material-icons">create_new_folder</span> 新建文件夹</button>
</div>
<div class="fab-menu" id="upload-menu">
  <button onclick="selectFile()"><span class="material-icons">upload_file</span> 上传文件</button>
  <button onclick="selectFolder()"><span class="material-icons">drive_folder_upload</span> 上传文件夹</button>
  <button onclick="manualSelectFile()"><span class="material-icons">cloud_upload</span> 手动上传文件</button>
  <button onclick="manualSelectFolder()"><span class="material-icons">folder_open</span> 手动上传文件夹</button>
</div>
<div class="bottom-bar">
  <button id="btn-tasks"><span class="material-icons">assignment</span>任务</button>
  <button id="btn-new"><span class="material-icons">add_circle</span>新建</button>
  <button id="btn-upload"><span class="material-icons">cloud_upload</span>上传</button>
</div>
<div class="drawer" id="task-drawer">
  <div class="drawer-head"><span>任务列表</span><span class="material-icons" id="close-tasks" style="cursor:pointer;padding:6px;">close</span></div>
  <div class="drawer-body" id="task-list"></div>
  <div style="padding:12px;border-top:1px solid var(--divider);display:flex;gap:8px;">
    <button class="btn-secondary" id="btn-refresh-tasks" style="flex:1;padding:10px;border:none;border-radius:8px;cursor:pointer;">刷新</button>
    <button class="btn-secondary" id="btn-clear-done" style="flex:1;padding:10px;border:none;border-radius:8px;cursor:pointer;">清除已完成</button>
  </div>
</div>
<div class="modal-overlay" id="modal">
  <div class="modal">
    <h3 id="modal-title">标题</h3>
    <div id="modal-content"></div>
    <div class="modal-actions"><button class="btn-secondary" id="modal-cancel">取消</button><button class="btn-primary" id="modal-ok">确定</button></div>
  </div>
</div>
<div class="snackbar" id="snackbar"></div>
<input type="file" id="file-input" style="display:none" multiple>
<input type="file" id="folder-input" style="display:none" webkitdirectory directory multiple>
<input type="file" id="manual-file-input" style="display:none" multiple>
<input type="file" id="manual-folder-input" style="display:none" webkitdirectory directory multiple>
`;

const HOME_SCRIPT = `
<script>
const params = new URLSearchParams(location.search);
let currentPath = params.get('path') || '';
let currentSort = localStorage.getItem('netdisk-sort') || 'name-asc';
let selectionMode = false;
let selectedPaths = new Set();

const TASK_CREATE_CONCURRENCY = 64;
const FILE_UPLOAD_CONCURRENCY = 32;
const CHUNK_UPLOAD_CONCURRENCY = 32;
const CLIENT_CHUNK_SIZE = 70 * 1024 * 1024;

function showMsg(msg){ const s=document.getElementById('snackbar'); s.textContent=msg; s.classList.add('show'); setTimeout(()=>s.classList.remove('show'),2500); }
function escapeHtml(t){ return t.replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
function formatSize(b){ if(!b)return '0 B'; const k=1024, s=['B','KB','MB','GB']; const i=Math.floor(Math.log(b)/Math.log(k)); return (b/Math.pow(k,i)).toFixed(2)+' '+s[i]; }
async function copyText(text){
  try { if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return; } } catch (e) {}
  const ta = document.createElement('textarea');
  ta.value = text; ta.style.position = 'fixed'; ta.style.opacity = '0';
  document.body.appendChild(ta); ta.select();
  document.execCommand('copy'); document.body.removeChild(ta);
}
async function api(url, opts={}){
  const r=await fetch(url, opts);
  if(r.status===401){ location.href='/login?redirect='+encodeURIComponent(location.pathname+location.search); return null; }
  if(!r.ok){ const j=await r.json().catch(()=>({})); throw new Error(j.error||r.statusText); }
  return r.json().catch(()=>null);
}
function sortEntries(a,b){
  const [nameA,nodeA]=a; const [nameB,nodeB]=b;
  const timeA=nodeA.createdAt||0; const timeB=nodeB.createdAt||0;
  const sizeA=nodeA.size||0; const sizeB=nodeB.size||0;
  switch(currentSort){
    case 'name-asc': return nameA.localeCompare(nameB,'zh-CN');
    case 'name-desc': return nameB.localeCompare(nameA,'zh-CN');
    case 'time-asc': return timeA-timeB;
    case 'time-desc': return timeB-timeA;
    case 'size-asc': return sizeA-sizeB;
    case 'size-desc': return sizeB-sizeA;
    default: return nameA.localeCompare(nameB,'zh-CN');
  }
}
function formatTime(ts){ if(!ts) return ''; return new Date(ts).toLocaleString('zh-CN',{month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit'}); }
function getIcon(name){
  const ext=name.split('.').pop().toLowerCase();
  if(['mp4','webm','mkv','a3v8'].includes(ext)) return 'movie';
  if(['mp3','wav','ogg','flac','m4a'].includes(ext)) return 'audiotrack';
  if(['jpg','jpeg','png','gif','webp'].includes(ext)) return 'image';
  if(['zip','rar','7z','tar','gz'].includes(ext)) return 'folder_zip';
  if(['txt','md','json','js','css','html'].includes(ext)) return 'description';
  return 'insert_drive_file';
}

async function loadList(){
  const data=await api('/api/structure?path='+encodeURIComponent(currentPath));
  if(!data) return;
  renderBreadcrumbs();
  document.getElementById('sort-select').value=currentSort;
  const list=document.getElementById('file-list');
  const entries=data.children?Object.entries(data.children).sort(sortEntries):[];
  if(entries.length===0){ list.innerHTML='<div class="empty"><span class="material-icons" style="font-size:48px;color:#bdbdbd;">folder_open</span><p>空文件夹</p></div>'; updateSelectionUI(); return; }
  list.innerHTML=entries.map(([name,node])=>{
    const icon = node.type==='folder'?'folder':(getIcon(name));
    const sizeMeta = node.type==='folder'?'文件夹':(formatSize(node.size)||'');
    const timeMeta = formatTime(node.createdAt);
    const meta = timeMeta ? (sizeMeta + ' · ' + timeMeta) : sizeMeta;
    const path = currentPath?currentPath+'/'+name:name;
    const isSelected = selectedPaths.has(path);
    return \`<div class="file-card \${isSelected?'selected':''}" data-path="\${escapeHtml(path)}" data-type="\${node.type}" data-action="open">
      <div class="file-main">
        <input type="checkbox" class="sel-check" data-action="select" \${isSelected?'checked':''}>
        <div class="file-icon"><span class="material-icons">\${icon}</span></div>
        <div class="file-name-wrap"><div class="file-name" title="\${escapeHtml(name)}">\${escapeHtml(name)}</div></div>
      </div>
      <div class="file-meta">\${meta}</div>
      <div class="file-actions">
        \${node.type==='file'?\`<button data-action="download" title="下载"><span class="material-icons">download</span></button>
        <button data-action="link" title="复制直链"><span class="material-icons">link</span></button>
        <button data-action="share" title="分享"><span class="material-icons">share</span></button>\`:''}
        \${node.type==='folder'?\`<button data-action="downloadFolder" title="打包下载"><span class="material-icons">folder_zip</span></button>\`:''}
        <button data-action="move" title="移动"><span class="material-icons">drive_file_move</span></button>
        <button data-action="copy" title="复制"><span class="material-icons">content_copy</span></button>
        <button data-action="rename" title="重命名"><span class="material-icons">edit</span></button>
        <button data-action="delete" title="删除"><span class="material-icons">delete</span></button>
      </div>
    </div>\`;
  }).join('');
  updateSelectionUI();
}

document.getElementById('file-list').addEventListener('click', e=>{
  const btn = e.target.closest('[data-action]');
  if(!btn) return;
  const item = btn.closest('.file-card');
  if(!item) return;
  const p = item.dataset.path;
  const type = item.dataset.type;
  const action = btn.dataset.action;
  if(action==='select'){ toggleSelect(p); e.stopPropagation(); return; }
  if(selectionMode && action==='open'){ toggleSelect(p); return; }
  if(action==='open'){ type==='folder'?openFolder(p):openFile(p); }
  else if(action==='download') downloadFile(p);
  else if(action==='link') copyDirectLink(p);
  else if(action==='share') shareFile(p);
  else if(action==='downloadFolder') downloadFolder(p);
  else if(action==='move') moveItem(p);
  else if(action==='copy') copyItem(p);
  else if(action==='rename') renameItem(p);
  else if(action==='delete') deleteItem(p);
});

function toggleSelectionMode(){
  selectionMode = !selectionMode;
  selectedPaths.clear();
  document.getElementById('btn-select-mode').classList.toggle('active', selectionMode);
  document.getElementById('file-list').classList.toggle('selection-mode', selectionMode);
  updateSelectionUI();
}
function toggleSelect(p){ if(selectedPaths.has(p)) selectedPaths.delete(p); else selectedPaths.add(p); updateSelectionUI(); }
function updateSelectionUI(){
  document.getElementById('selection-bar').classList.toggle('show', selectionMode);
  document.getElementById('selection-count').textContent = '已选 ' + selectedPaths.size;
  document.querySelectorAll('.file-card').forEach(card=>{
    const p = card.dataset.path;
    card.classList.toggle('selected', selectedPaths.has(p));
    const chk = card.querySelector('.sel-check');
    if(chk) chk.checked = selectedPaths.has(p);
  });
}
function selectAll(){ document.querySelectorAll('.file-card').forEach(c=>selectedPaths.add(c.dataset.path)); updateSelectionUI(); }
function clearSelection(){ selectedPaths.clear(); selectionMode = false; document.getElementById('btn-select-mode').classList.remove('active'); document.getElementById('file-list').classList.remove('selection-mode'); updateSelectionUI(); }

let targetPickState = null;
function isInvalidTargetPath(path, sourcePaths){
  const p = path.replace(/^\/|\/$/g, '');
  return sourcePaths.some(src => { const s = src.split('/').filter(Boolean).join('/'); return p === s || p.startsWith(s + '/'); });
}
function buildTargetTreeRows(node, basePath, level){
  if(!targetPickState) return '';
  const path = basePath;
  const isRoot = path === '';
  const children = Object.entries(node.children || {}).filter(([,c]) => c.type === 'folder').sort((a,b) => a[0].localeCompare(b[0]));
  const invalid = isInvalidTargetPath(path, targetPickState.sourcePaths);
  const selected = targetPickState.selectedPath === path;
  const expanded = targetPickState.expanded.has(path);
  let html = '<div class="target-tree-row' + (selected ? ' selected' : '') + (invalid ? ' invalid' : '') + '" data-path="' + escapeHtml(path) + '" style="padding-left:' + (level * 16) + 'px;">';
  html += '<span class="target-tree-toggle" data-path="' + escapeHtml(path) + '">' + (children.length ? (expanded ? '▼' : '▶') : '<span style="visibility:hidden">▶</span>') + '</span>';
  html += '<span class="material-icons" style="font-size:18px;color:var(--primary);margin-right:4px;">folder</span>';
  html += '<span style="flex:1;">' + escapeHtml(isRoot ? 'root' : node.name) + '</span></div>';
  if(children.length && expanded){
    for(const [name, child] of children) html += buildTargetTreeRows(child, path ? path + '/' + name : name, level + 1);
  }
  return html;
}
function renderTargetTree(){
  const el = document.getElementById('target-tree');
  if(!el || !targetPickState) return;
  el.innerHTML = buildTargetTreeRows(targetPickState.structure, '', 0);
}
function selectTargetPath(path){ if(!targetPickState || isInvalidTargetPath(path, targetPickState.sourcePaths)) return; targetPickState.selectedPath = path; renderTargetTree(); }
function toggleTargetExpand(path){ if(!targetPickState) return; if(targetPickState.expanded.has(path)) targetPickState.expanded.delete(path); else targetPickState.expanded.add(path); renderTargetTree(); }
async function refreshTargetTree(){ if(!targetPickState) return; try { targetPickState.structure = await api('/api/structure?path='); } catch(e){} renderTargetTree(); }

async function pickTargetFolder(opts = {}){
  const sourcePaths = opts.sourcePaths || [];
  const structure = await api('/api/structure?path=');
  targetPickState = { structure, sourcePaths, selectedPath: '', expanded: new Set(['']) };
  const content = '<style>' +
    '.target-tree-wrap{max-height:260px;overflow:auto;border:1px solid var(--divider);border-radius:8px;padding:8px;}' +
    '.target-tree-row{display:flex;align-items:center;gap:4px;padding:6px 8px;border-radius:6px;cursor:pointer;user-select:none;}' +
    '.target-tree-row:hover{background:var(--divider);}' +
    '.target-tree-row.selected{background:var(--primary);color:#fff;}' +
    '.target-tree-row.invalid{opacity:.45;cursor:not-allowed;}' +
    '.target-tree-toggle{width:16px;text-align:center;color:var(--primary);font-size:12px;flex-shrink:0;}' +
    '.target-tree-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;}' +
    '.target-tree-header button{padding:6px 10px;border:none;border-radius:6px;background:var(--surface);color:var(--primary);cursor:pointer;font-size:13px;box-shadow:0 0 0 1px var(--divider);}' +
    '.target-tree-select{width:100%;padding:8px;border:1px solid var(--divider);border-radius:6px;font-size:14px;}' +
    '</style>' +
    '<div class="target-tree-header"><span style="font-weight:500;font-size:16px;">选择目标文件夹</span><button id="target-new-folder">新建文件夹</button></div>' +
    '<div id="target-tree" class="target-tree-wrap"></div>' +
    '<div style="margin-top:12px;"><label style="display:block;margin-bottom:6px;font-size:14px;color:var(--text-sec);">冲突处理</label>' +
    '<select id="target-conflict" class="target-tree-select"><option value="overwrite" selected>覆盖重复文件</option><option value="rename">保留重复文件并重命名</option><option value="skip">跳过重复文件</option></select></div>';
  return new Promise(resolve => {
    openModal('', content, () => {
      const target = targetPickState ? targetPickState.selectedPath : '';
      const sel = document.getElementById('target-conflict');
      const mode = sel ? sel.value : 'overwrite';
      closeModal(); targetPickState = null;
      resolve({ target, mode });
    });
    document.getElementById('modal-cancel').onclick = () => { closeModal(); targetPickState = null; resolve(null); };
    renderTargetTree();
    const treeEl = document.getElementById('target-tree');
    treeEl.onclick = (e) => {
      const toggle = e.target.closest('.target-tree-toggle');
      if(toggle){ toggleTargetExpand(toggle.dataset.path); e.stopPropagation(); return; }
      const row = e.target.closest('.target-tree-row');
      if(row) selectTargetPath(row.dataset.path);
    };
    document.getElementById('target-new-folder').onclick = async () => {
      const name = prompt('请输入新文件夹名称');
      if(!name) return;
      const base = targetPickState.selectedPath;
      const newPath = base ? base + '/' + name : name;
      try {
        await api('/api/folder', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ path: newPath }) });
        await refreshTargetTree();
        if(base) toggleTargetExpand(base);
        targetPickState.expanded.add(newPath);
        selectTargetPath(newPath);
        loadList();
      } catch(e){ showMsg('新建文件夹失败: ' + (e.message || e)); }
    };
  });
}

async function moveItem(p){
  try {
    const res = await pickTargetFolder({operation:'move', sourcePaths:[p]});
    if(!res) return;
    await api('/api/file/move', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({path:p, targetPath:res.target, mode:res.mode})});
    showMsg('移动成功'); loadList();
  } catch(e) { showMsg('移动失败: ' + (e.message || e)); }
}
async function copyItem(p){
  try {
    const res = await pickTargetFolder({operation:'copy', sourcePaths:[p]});
    if(!res) return;
    await api('/api/file/copy', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({path:p, targetPath:res.target, mode:res.mode})});
    showMsg('复制成功'); loadList();
  } catch(e) { showMsg('复制失败: ' + (e.message || e)); }
}
async function batchMove(){
  if(selectedPaths.size===0){ showMsg('请先选择文件'); return; }
  try {
    const res = await pickTargetFolder({operation:'move', sourcePaths:Array.from(selectedPaths)});
    if(!res) return;
    await api('/api/file/move', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({paths:Array.from(selectedPaths), targetPath:res.target, mode:res.mode})});
    showMsg('批量移动成功'); selectedPaths.clear(); loadList();
  } catch(e) { showMsg('批量移动失败: ' + (e.message || e)); }
}
async function batchCopy(){
  if(selectedPaths.size===0){ showMsg('请先选择文件'); return; }
  try {
    const res = await pickTargetFolder({operation:'copy', sourcePaths:Array.from(selectedPaths)});
    if(!res) return;
    await api('/api/file/copy', {method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify({paths:Array.from(selectedPaths), targetPath:res.target, mode:res.mode})});
    showMsg('批量复制成功'); selectedPaths.clear(); loadList();
  } catch(e) { showMsg('批量复制失败: ' + (e.message || e)); }
}
async function batchDelete(){
  if(selectedPaths.size===0){ showMsg('请先选择文件'); return; }
  if(!confirm('确定删除选中的 '+selectedPaths.size+' 项?')) return;
  const paths = Array.from(selectedPaths);
  document.querySelectorAll('.file-card').forEach(card=>{ if(selectedPaths.has(card.dataset.path)) card.remove(); });
  selectedPaths.clear();
  try {
    await api('/api/files/batch', {method:'DELETE', headers:{'Content-Type':'application/json'}, body:JSON.stringify({paths})});
    showMsg('批量删除成功');
  } catch(e) { showMsg('批量删除失败: ' + (e.message || e)); }
  loadList();
}

function renderBreadcrumbs(){
  const parts=currentPath.split('/').filter(Boolean);
  let html='<a href="/?path=">首页</a>';
  let acc='';
  parts.forEach(p=>{ acc=acc?acc+'/'+p:p; html+=\`<span>/</span><a href="/?path=\${encodeURIComponent(acc)}">\${escapeHtml(p)}</a>\`; });
  document.getElementById('breadcrumbs').innerHTML=html;
}
function openFolder(p){ currentPath=p; selectedPaths.clear(); selectionMode=false; document.getElementById('btn-select-mode').classList.remove('active'); document.getElementById('file-list').classList.remove('selection-mode'); history.pushState(null,'','/?path='+encodeURIComponent(p)); loadList(); }
function openFile(p){ location.href='/file?path='+encodeURIComponent(p); }
async function downloadFile(p){
  const node=await api('/api/file?path='+encodeURIComponent(p));
  if(!node) return;
  location.href='/download/'+encodeURIComponent(node.ssid)+'/'+encodeURIComponent(node.name);
}
async function shareFile(p){
  try {
    const node=await api('/api/file?path='+encodeURIComponent(p));
    if(!node) return;
    await copyText(location.origin+'/share/'+node.ssid);
    showMsg('分享链接已复制');
  } catch(e) { showMsg('复制失败: '+(e.message||e)); }
}
async function copyDirectLink(p){
  try {
    const node=await api('/api/file?path='+encodeURIComponent(p));
    if(!node) return;
    await copyText(location.origin+'/direct/'+node.ssid+'/'+encodeURIComponent(node.name));
    showMsg('直链已复制');
  } catch(e) { showMsg('复制失败: '+(e.message||e)); }
}
function downloadFolder(p){ location.href = '/zip?path=' + encodeURIComponent(p); }
async function renameItem(p){
  const name=p.split('/').pop();
  const newName=prompt('新名称',name);
  if(!newName||newName===name) return;
  await api('/api/file/rename',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({path:p,newName})});
  loadList();
}
async function deleteItem(p){
  if(!confirm('确定删除 "'+p.split('/').pop()+'"?')) return;
  document.querySelectorAll('.file-card').forEach(card=>{ if(card.dataset.path === p) card.remove(); });
  try { await api('/api/file?path='+encodeURIComponent(p),{method:'DELETE'}); }
  catch(e) { showMsg('删除失败: ' + e.message); }
  loadList();
}

function newText(){
  document.getElementById('new-menu').classList.remove('show');
  const content=\`<h3>新建文本文件</h3><input id="new-text-name" placeholder="文件名"><textarea id="new-text-body" placeholder="内容"></textarea>\`;
  openModal('新建文本文件',content,async()=>{
    const name=document.getElementById('new-text-name').value.trim();
    const body=document.getElementById('new-text-body').value;
    if(!name){ showMsg('请输入文件名'); return; }
    const path=currentPath?currentPath+'/'+name:name;
    await api('/api/text',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path,content:body})});
    closeModal(); loadList();
  });
}
function newFolder(){
  document.getElementById('new-menu').classList.remove('show');
  const content='<h3>新建文件夹</h3><input id="new-folder-name" placeholder="文件夹名">';
  openModal('新建文件夹',content,async()=>{
    const name=document.getElementById('new-folder-name').value.trim();
    if(!name){ showMsg('请输入文件夹名'); return; }
    const path=currentPath?currentPath+'/'+name:name;
    await api('/api/folder',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({path})});
    closeModal(); loadList();
  });
}

// ==================== 上传核心 ====================
function genTaskId(){ return 'task_'+Date.now()+'_'+Math.random().toString(36).slice(2,9); }
const cancelledManualUploads = new Set();
const abortControllers = new Map();
let localTasks = new Map();
function addLocalTask(t){ localTasks.set(t.id, t); }
function removeLocalTask(id){ localTasks.delete(id); }

async function runWithConcurrency(items, concurrency, worker){
  let cursor = 0;
  const total = items.length;
  const runners = [];
  const n = Math.min(concurrency, total);
  for(let i=0;i<n;i++){
    runners.push((async ()=>{
      while(true){
        const idx = cursor++;
        if(idx >= total) return;
        try { await worker(items[idx], idx); } catch(e){ console.error('worker error', e); }
      }
    })());
  }
  await Promise.all(runners);
}

function selectFile(){ document.getElementById('file-input').click(); }
function selectFolder(){ document.getElementById('folder-input').click(); }
document.getElementById('file-input').addEventListener('change', e=>uploadFiles(e.target.files));
document.getElementById('folder-input').addEventListener('change', e=>uploadFiles(e.target.files));

async function uploadFiles(files){
  const fileArr = Array.from(files);
  if(fileArr.length === 0) return;
  await runWithConcurrency(fileArr, TASK_CREATE_CONCURRENCY, async (file)=>{
    const taskId = genTaskId();
    file._netdiskTaskId = taskId;
    const folderPrefix = file.webkitRelativePath ? file.webkitRelativePath.slice(0, -file.name.length) : '';
    file._netdiskDir = folderPrefix
      ? (currentPath ? currentPath + '/' + folderPrefix.slice(0,-1) : folderPrefix.slice(0,-1))
      : currentPath;
    addLocalTask({ id: taskId, name: file.name, status: 'uploading', message: '排队等待...', progress: 0, size: file.size, createdAt: Date.now(), updatedAt: Date.now() });
  });
  loadTasks();
  await runWithConcurrency(fileArr, FILE_UPLOAD_CONCURRENCY, async (file)=>{
    try { await uploadOne(file, file._netdiskDir, file._netdiskTaskId); }
    catch(e){ console.error('上传失败', file.name, e); }
  });
  loadList();
}

async function uploadOne(file, dir, externalTaskId){
  const path = dir ? dir + '/' + file.name : file.name;
  const taskId = externalTaskId || genTaskId();
  cancelledManualUploads.delete(taskId);
  const abortCtrl = new AbortController();
  abortControllers.set(taskId, abortCtrl);
  const CLIENT_CHUNK = CLIENT_CHUNK_SIZE;

  const baseTask = { id: taskId, name: file.name, status: 'uploading', message: '准备上传...', progress: 1, size: file.size, createdAt: Date.now(), updatedAt: Date.now() };
  if(!localTasks.has(taskId)) addLocalTask(baseTask);
  else { const t = localTasks.get(taskId); t.status = 'uploading'; t.message = '准备上传...'; t.progress = 1; t.updatedAt = Date.now(); }
  debouncedLoadTasks();

  function fmtSpd(bps){
    if (bps >= 1024*1024) return (bps/1024/1024).toFixed(2)+' MB/s';
    if (bps >= 1024) return (bps/1024).toFixed(2)+' KB/s';
    return bps.toFixed(0)+' B/s';
  }
  function fmtProgress(done, total){ return formatSize(done)+'/'+formatSize(total); }

  try {
    // ============ 小文件：单请求 XHR ============
    if (file.size <= CLIENT_CHUNK) {
      const t = localTasks.get(taskId);
      if(t){ t.message = '上传 ' + fmtProgress(0, file.size); t.progress = 1; t.updatedAt = Date.now(); }
      debouncedLoadTasks();

      const form = new FormData();
      form.append('path', path);
      form.append('file', file);
      form.append('taskId', taskId);

      let lastLoaded = 0, lastTime = Date.now(), smoothSpeed = 0;

      await new Promise((resolve, reject) => {
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/upload');
        xhr.upload.onprogress = (e) => {
          if (!e.lengthComputable) return;
          const now = Date.now();
          const dt = (now - lastTime) / 1000;
          if (dt > 0.3) {
            const instant = (e.loaded - lastLoaded) / dt;
            smoothSpeed = smoothSpeed * 0.6 + instant * 0.4;
            lastLoaded = e.loaded; lastTime = now;
          }
          // 传输阶段 0-50%，剩余 50% 留给 Worker 写 GitHub
          const pct = Math.min(50, Math.max(1, Math.round((e.loaded / e.total) * 50)));
          const tt = localTasks.get(taskId);
          if(tt){
            tt.message = '上传 ' + fmtProgress(e.loaded, e.total) + ' · ' + fmtSpd(smoothSpeed);
            tt.progress = pct;
            tt.updatedAt = Date.now();
          }
          debouncedLoadTasks();
        };
        xhr.onload = () => {
          if (xhr.status >= 200 && xhr.status < 300) {
            const tt = localTasks.get(taskId);
            if(tt){ tt.status = 'done'; tt.message = '完成'; tt.progress = 100; tt.updatedAt = Date.now(); }
            debouncedLoadTasks();
            resolve();
          } else {
            try { const j = JSON.parse(xhr.responseText); reject(new Error(j.error || ('HTTP ' + xhr.status))); }
            catch (e) { reject(new Error('HTTP ' + xhr.status)); }
          }
        };
        xhr.onerror = () => reject(new Error('网络错误'));
        xhr.onabort = () => reject(new Error('已取消'));
        abortCtrl.signal.addEventListener('abort', () => xhr.abort());
        xhr.send(form);
      });

      setTimeout(() => removeLocalTask(taskId), 2000);
      showMsg('上传完成: ' + file.name);
      return;
    }

    // ============ 大文件：分片 32 线程 ============
    const start = await api('/api/upload/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, filename: file.name, size: file.size, taskId, clientChunkSize: CLIENT_CHUNK })
    });
    if (!start) throw new Error('初始化失败');

    const totalChunks = start.chunks;
    const uploadId = start.uploadId;
    let uploadError = null;

    // 两个数组分离：服务端已确认完成 / 客户端传输中的字节
    const chunkDone = new Array(totalChunks).fill(false);
    const chunkInFlight = new Array(totalChunks).fill(0);
    const chunkSizeArr = [];
    for (let i = 0; i < totalChunks; i++) {
      const begin = i * CLIENT_CHUNK;
      const end = Math.min(begin + CLIENT_CHUNK, file.size);
      chunkSizeArr.push(end - begin);
    }
    let serverDoneBytes = 0;
    let doneChunksCount = 0;
    let lastServerBytes = 0, lastTime = Date.now(), smoothSpeed = 0;

    const progressTimer = setInterval(() => {
      if (uploadError) return;
      // 进度 = 服务端已完成分片(100%) + 传输中分片(×0.3 加权)
      let total = 0;
      for (let i = 0; i < totalChunks; i++) {
        if (chunkDone[i]) total += chunkSizeArr[i];
        else if (chunkInFlight[i] > 0) total += Math.min(chunkInFlight[i], chunkSizeArr[i]) * 0.3;
      }
      const now = Date.now();
      const dt = (now - lastTime) / 1000;
      if (dt > 0.3) {
        const instant = (serverDoneBytes - lastServerBytes) / dt;
        if (instant >= 0) smoothSpeed = smoothSpeed * 0.6 + instant * 0.4;
        lastServerBytes = serverDoneBytes;
        lastTime = now;
      }
      const pct = Math.min(99, Math.floor((total / file.size) * 99));
      const tt = localTasks.get(taskId);
      if (tt) {
        tt.message = '上传 ' + fmtProgress(Math.floor(total), file.size) + ' · ' + fmtSpd(smoothSpeed) + ' · 分片 ' + doneChunksCount + '/' + totalChunks;
        tt.progress = pct;
        tt.updatedAt = Date.now();
      }
      debouncedLoadTasks();
    }, 500);

    async function uploadSingleChunk(idx) {
      if (abortCtrl.signal.aborted || cancelledManualUploads.has(taskId)) throw new Error('已取消');
      const begin = idx * CLIENT_CHUNK;
      const end = Math.min(begin + CLIENT_CHUNK, file.size);
      const blob = file.slice(begin, end);
      const form = new FormData();
      form.append('uploadId', uploadId);
      form.append('index', String(idx));
      form.append('total', String(totalChunks));
      form.append('storage', start.storage);
      form.append('path', path);
      form.append('filename', file.name);
      form.append('chunk', blob, file.name + '.part' + idx);
      form.append('taskId', taskId);

      let retries = 0;
      while (retries < 3) {
        try {
          await new Promise((resolve, reject) => {
            const xhr = new XMLHttpRequest();
            xhr.open('POST', '/api/upload/chunk');
            if (retries > 0) chunkInFlight[idx] = 0;
            xhr.upload.onprogress = (e) => {
              if (!e.lengthComputable) return;
              chunkInFlight[idx] = e.loaded;
            };
            xhr.onload = () => {
              if (xhr.status >= 200 && xhr.status < 300) {
                // XHR onload = Worker 已写完 GitHub
                chunkDone[idx] = true;
                chunkInFlight[idx] = 0;
                serverDoneBytes += blob.size;
                doneChunksCount++;
                resolve();
              } else {
                let errMsg = 'HTTP ' + xhr.status;
                try { const j = JSON.parse(xhr.responseText); errMsg = j.error || errMsg; } catch (_) {}
                reject(new Error(errMsg));
              }
            };
            xhr.onerror = () => reject(new Error('网络错误'));
            xhr.onabort = () => reject(new Error('已取消'));
            abortCtrl.signal.addEventListener('abort', () => xhr.abort());
            xhr.send(form);
          });
          return;
        } catch (e) {
          if (e.message === '已取消' || abortCtrl.signal.aborted) throw new Error('已取消');
          retries++;
          if (retries > 2) throw new Error('分片' + (idx + 1) + '失败: ' + (e.message || e));
          await new Promise(r => setTimeout(r, 1000));
        }
      }
    }

    const indexes = [];
    for (let i = 0; i < totalChunks; i++) indexes.push(i);
    await runWithConcurrency(indexes, CHUNK_UPLOAD_CONCURRENCY, async (idx) => {
      if (uploadError) return;
      try { await uploadSingleChunk(idx); }
      catch (e) { uploadError = e; }
    });

    clearInterval(progressTimer);
    if (uploadError) throw uploadError;

    { const tt = localTasks.get(taskId); if(tt){ tt.message = '写入目录...'; tt.progress = 99; tt.updatedAt = Date.now(); } }
    debouncedLoadTasks();

    await api('/api/upload/finish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uploadId, path, filename: file.name, storage: start.storage, size: file.size, chunks: totalChunks, taskId })
    });

    const tt = localTasks.get(taskId);
    if(tt){ tt.status = 'done'; tt.message = '完成'; tt.progress = 100; tt.updatedAt = Date.now(); }
    debouncedLoadTasks();
    setTimeout(() => removeLocalTask(taskId), 2000);
    showMsg('上传完成: '+file.name);
  } catch (e) {
    abortControllers.delete(taskId);
    cancelledManualUploads.delete(taskId);
    if (e.message === '已取消' || e.name === 'AbortError') {
      const tt = localTasks.get(taskId);
      if(tt){ tt.status = 'cancelled'; tt.message = '已取消'; tt.progress = 0; tt.updatedAt = Date.now(); }
      debouncedLoadTasks(); showMsg('上传已取消: '+file.name);
    } else {
      const tt = localTasks.get(taskId);
      if(tt){ tt.status = 'error'; tt.message = e.message || '上传失败'; tt.progress = 0; tt.updatedAt = Date.now(); }
      debouncedLoadTasks(); showMsg('上传失败: '+file.name+' '+e.message);
    }
    throw e;
  }
  abortControllers.delete(taskId);
}

async function manualSelectFile(){ document.getElementById('manual-file-input').click(); }
async function manualSelectFolder(){ document.getElementById('manual-folder-input').click(); }
document.getElementById('manual-file-input').addEventListener('change', e=>manualUploadFiles(e.target.files));
document.getElementById('manual-folder-input').addEventListener('change', e=>manualUploadFiles(e.target.files));

async function manualUploadFiles(files){
  const fileArr = Array.from(files);
  if(fileArr.length === 0) return;
  await runWithConcurrency(fileArr, TASK_CREATE_CONCURRENCY, async (file)=>{
    const taskId = genTaskId();
    file._netdiskTaskId = taskId;
    const folderPrefix = file.webkitRelativePath ? file.webkitRelativePath.slice(0, -file.name.length) : '';
    file._netdiskDir = folderPrefix
      ? (currentPath ? currentPath + '/' + folderPrefix.slice(0,-1) : folderPrefix.slice(0,-1))
      : currentPath;
    addLocalTask({ id: taskId, name: file.name, status: 'uploading', message: '排队等待...', progress: 0, size: file.size, createdAt: Date.now(), updatedAt: Date.now() });
  });
  loadTasks();
  await runWithConcurrency(fileArr, FILE_UPLOAD_CONCURRENCY, async (file)=>{
    try { await manualUploadOne(file, file._netdiskDir, file._netdiskTaskId); }
    catch(e){ console.error('手动上传失败', file.name, e); }
  });
  loadList();
}

function arrayBufferToBase64Browser(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  const cs = 0x8000;
  for (let i = 0; i < bytes.byteLength; i += cs) binary += String.fromCharCode.apply(null, bytes.subarray(i, i + cs));
  return btoa(binary);
}
function blobToBase64(blob){
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onloadend = () => { try { resolve(arrayBufferToBase64Browser(reader.result)); } catch (e) { reject(e); } };
    reader.onerror = () => reject(new Error('FileReader error'));
    reader.readAsArrayBuffer(blob);
  });
}

async function manualUploadOne(file, dir, externalTaskId){
  const path = dir ? dir + '/' + file.name : file.name;
  const taskId = externalTaskId || genTaskId();
  cancelledManualUploads.delete(taskId);
  const baseTask = { id: taskId, name: file.name, status: 'uploading', message: '初始化手动上传...', progress: 0, size: file.size, createdAt: Date.now(), updatedAt: Date.now() };
  if(!localTasks.has(taskId)) addLocalTask(baseTask);
  else { const t = localTasks.get(taskId); t.message = '初始化手动上传...'; t.updatedAt = Date.now(); }
  debouncedLoadTasks();

  try {
    const start = await api('/api/upload/manual/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ path, filename: file.name, size: file.size, taskId })
    });
    if (!start) throw new Error('初始化手动上传失败');

    const chunkSize = start.chunkSize;
    const total = start.chunks;
    const uploadId = start.uploadId;
    const token = start.token;
    const repo = start.repo;

    let uploadedBytes = 0, completedChunks = 0;
    let lastBytes = 0, lastTime = Date.now(), smoothSpeed = 0;

    function formatSpeed(bps){
      if (bps >= 1024 * 1024) return (bps / (1024 * 1024)).toFixed(2) + ' MB/s';
      if (bps >= 1024) return (bps / 1024).toFixed(2) + ' KB/s';
      return bps.toFixed(0) + ' B/s';
    }

    const speedTimer = setInterval(() => {
      const now = Date.now();
      const dt = (now - lastTime) / 1000;
      if (dt > 0.3) {
        const instant = (uploadedBytes - lastBytes) / dt;
        smoothSpeed = smoothSpeed * 0.6 + instant * 0.4;
        lastBytes = uploadedBytes; lastTime = now;
      }
    }, 500);

    const progressTimer = setInterval(() => {
      if (cancelledManualUploads.has(taskId)) return;
      const t = localTasks.get(taskId);
      if(t){
        t.message = '直传 ' + formatSize(uploadedBytes) + '/' + formatSize(file.size) + ' · ' + formatSpeed(smoothSpeed) + ' · 分片 ' + completedChunks + '/' + total;
        t.progress = Math.min(99, Math.floor((uploadedBytes / file.size) * 99));
        t.updatedAt = Date.now();
      }
      debouncedLoadTasks();
    }, 500);

    async function uploadManualChunk(i) {
      if (cancelledManualUploads.has(taskId)) throw new Error('已取消');
      const begin = i * chunkSize;
      const end = Math.min(begin + chunkSize, file.size);
      const blob = file.slice(begin, end);
      const base64 = await blobToBase64(blob);

      let retries = 0, sha = null, lastErr = null;
      while (retries < 5) {
        if (cancelledManualUploads.has(taskId)) throw new Error('已取消');
        try {
          const resp = await fetch('https://api.github.com/repos/' + start.githubUser + '/' + repo + '/contents/chunk_' + i, {
            method: 'PUT',
            headers: { 'Authorization': 'token ' + token, 'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: 'chunk ' + i, content: base64 })
          });
          if (resp.ok) { const data = await resp.json(); sha = data.content.sha; break; }
          const errText = await resp.text();
          lastErr = 'GitHub ' + resp.status + ': ' + errText;
          throw new Error(lastErr);
        } catch (e) {
          retries++;
          if (retries >= 5) throw new Error('分片 ' + (i + 1) + ' 上传失败: ' + (lastErr || e.message));
          await new Promise(r => setTimeout(r, 1500 * retries));
        }
      }

      api('/api/upload/manual/chunk', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ uploadId, index: i, sha, total, taskId })
      }).catch(e=>console.error('notify fail', e));

      uploadedBytes += blob.size;
      completedChunks++;
    }

    const chunkIndexes = [];
    for (let i = 0; i < total; i++) chunkIndexes.push(i);
    await runWithConcurrency(chunkIndexes, CHUNK_UPLOAD_CONCURRENCY, async (i)=>{
      if (cancelledManualUploads.has(taskId)) return;
      await uploadManualChunk(i);
    });

    clearInterval(speedTimer); clearInterval(progressTimer);
    if (cancelledManualUploads.has(taskId)) throw new Error('已取消');

    { const t = localTasks.get(taskId); if(t){ t.message = '上报服务器...'; t.progress = 99; t.updatedAt = Date.now(); } }
    debouncedLoadTasks();

    await api('/api/upload/manual/finish', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ uploadId, path, filename: file.name, size: file.size, chunks: total, taskId })
    });

    cancelledManualUploads.delete(taskId);
    const t = localTasks.get(taskId);
    if(t){ t.status = 'done'; t.message = '完成'; t.progress = 100; t.updatedAt = Date.now(); }
    debouncedLoadTasks();
    setTimeout(() => removeLocalTask(taskId), 2000);
    showMsg('手动上传完成: ' + file.name);
  } catch (e) {
    cancelledManualUploads.delete(taskId);
    const t = localTasks.get(taskId);
    if (e.message === '已取消') {
      if(t){ t.status = 'cancelled'; t.message = '已取消'; t.progress = 0; t.updatedAt = Date.now(); }
      debouncedLoadTasks(); showMsg('手动上传已取消: ' + file.name);
    } else {
      if(t){ t.status = 'error'; t.message = e.message || '手动上传失败'; t.progress = 0; t.updatedAt = Date.now(); }
      debouncedLoadTasks(); showMsg('手动上传失败: ' + file.name + ' ' + e.message);
    }
    throw e;
  }
}

// ==================== 任务列表 ====================
let taskTimer=null;
function debounce(fn, ms){
  let timer = null;
  return function(...args){
    if (timer) clearTimeout(timer);
    timer = setTimeout(() => { try { fn.apply(this, args); } catch(e){ console.error(e); } }, ms);
  };
}

// ★ 关键修复：服务端 uploading 时绝不覆盖本地，进度只增不减
async function loadTasks(){
  let serverTasks = [];
  try { serverTasks = await api('/api/tasks') || []; } catch (e) { console.error('获取任务失败', e); }

  for (const t of serverTasks) {
    const local = localTasks.get(t.id);
    if (local) {
      if (t.status === 'done' || t.status === 'error' || t.status === 'cancelled') {
        local.status = t.status;
        if (t.message) local.message = t.message;
        if (t.progress != null) local.progress = t.progress;
        local.updatedAt = t.updatedAt || Date.now();
      }
      // 服务端 uploading 时：什么都不做，保留本地消息/进度/速度
    }
  }

  const map = new Map();
  for (const t of localTasks.values()) map.set(t.id, t);
  for (const t of serverTasks) { if (!map.has(t.id)) map.set(t.id, t); }
  const tasks = [...map.values()].sort((a, b) => (b.updatedAt || b.createdAt) - (a.updatedAt || a.createdAt));
  const box = document.getElementById('task-list');
  if (!box) return;
  if (tasks.length === 0) { box.innerHTML = '<div class="empty">暂无任务</div>'; return; }
  box.innerHTML = tasks.slice(0, 200).map(t => {
    const statusColor = t.status === 'done' ? 'var(--success)' : (t.status === 'error' ? 'var(--danger)' : (t.status === 'cancelled' ? 'var(--text-sec)' : 'var(--primary)'));
    return \`<div class="task-item" data-task-id="\${escapeHtml(t.id)}">
      <div class="task-title">\${escapeHtml(t.name)} <span style="color:\${statusColor};font-size:12px;">\${t.status}</span></div>
      <div class="task-msg">\${escapeHtml(t.message || '')}</div>
      <div class="task-progress"><div style="width:\${t.progress || 0}%"></div></div>
      <div class="task-actions">
        \${t.status === 'uploading' || t.status === 'processing' ? \`<button onclick="cancelTask('\${t.id}', this)">取消</button>\` : ''}
        <button onclick="deleteTask('\${t.id}', this)">删除</button>
      </div>
    </div>\`;
  }).join('');
}
const debouncedLoadTasks = debounce(loadTasks, 400);

async function cancelTask(id, el){
  cancelledManualUploads.add(id);
  const ctrl = abortControllers.get(id);
  if (ctrl) { try { ctrl.abort(); } catch(e){} }
  const t = localTasks.get(id);
  if (t) { t.status = 'cancelled'; t.message = '已取消'; }
  removeLocalTask(id);
  if (el) { const item = el.closest('.task-item'); if (item) item.remove(); }
  else { loadTasks(); }
  try { await api('/api/tasks/' + id + '?cancel=1', { method: 'DELETE' }); }
  catch (e) { showMsg('取消失败: ' + e.message); }
}
async function deleteTask(id, el){
  cancelledManualUploads.add(id);
  const ctrl = abortControllers.get(id);
  if (ctrl) { try { ctrl.abort(); } catch(e){} }
  removeLocalTask(id);
  if (el) { const item = el.closest('.task-item'); if (item) item.remove(); }
  try { await api('/api/tasks/' + id, { method: 'DELETE' }); }
  catch (e) { showMsg('删除失败: ' + e.message); loadTasks(); }
}
async function clearDoneTasks(){
  const serverTasks = await api('/api/tasks') || [];
  const ids = [];
  for (const t of serverTasks) {
    if (t.status === 'done' || t.status === 'error' || t.status === 'cancelled') { ids.push(t.id); removeLocalTask(t.id); }
  }
  document.querySelectorAll('.task-item').forEach(el => el.remove());
  if (!ids.length) { showMsg('没有可清除的任务'); return; }
  try {
    await api('/api/tasks/batch', { method: 'DELETE', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ids }) });
    showMsg('已清除 ' + ids.length + ' 个任务');
  } catch (e) { showMsg('清除失败: ' + e.message); loadTasks(); }
}

document.getElementById('btn-tasks').onclick=()=>{ document.getElementById('task-drawer').classList.add('show'); loadTasks(); if(taskTimer)clearInterval(taskTimer); taskTimer=setInterval(loadTasks, 2500); };
document.getElementById('close-tasks').onclick=()=>{ document.getElementById('task-drawer').classList.remove('show'); if(taskTimer)clearInterval(taskTimer); };
document.getElementById('btn-refresh-tasks').onclick=()=>{ loadTasks(); };
document.getElementById('btn-clear-done').onclick=clearDoneTasks;
document.getElementById('btn-refresh').onclick=loadList;
document.getElementById('btn-settings').onclick=()=>{ location.href='/settings'; };
document.getElementById('btn-select-mode').onclick=toggleSelectionMode;
document.getElementById('sel-all').onclick=selectAll;
document.getElementById('sel-cancel').onclick=clearSelection;
document.getElementById('sel-move').onclick=batchMove;
document.getElementById('sel-copy').onclick=batchCopy;
document.getElementById('sel-delete').onclick=batchDelete;
document.getElementById('sort-select').onchange=(e)=>{ currentSort=e.target.value; localStorage.setItem('netdisk-sort', currentSort); loadList(); };
document.getElementById('btn-new').onclick=()=>{ document.getElementById('upload-menu').classList.remove('show'); document.getElementById('new-menu').classList.toggle('show'); };
document.getElementById('btn-upload').onclick=()=>{ document.getElementById('new-menu').classList.remove('show'); document.getElementById('upload-menu').classList.toggle('show'); };
document.addEventListener('click',e=>{ if(!e.target.closest('#btn-new')&&!e.target.closest('#new-menu')) document.getElementById('new-menu').classList.remove('show'); if(!e.target.closest('#btn-upload')&&!e.target.closest('#upload-menu')) document.getElementById('upload-menu').classList.remove('show'); });
document.getElementById('btn-logout').onclick=async()=>{ await api('/api/logout'); location.href='/login'; };

function openModal(title,content,onOk){
  const titleEl=document.getElementById('modal-title');
  titleEl.textContent=title;
  titleEl.style.display=title?'':'none';
  document.getElementById('modal-content').innerHTML=content;
  document.getElementById('modal-ok').onclick=onOk;
  document.getElementById('modal-cancel').onclick = closeModal;
  document.getElementById('modal').classList.add('show');
}
function closeModal(){ document.getElementById('modal').classList.remove('show'); }

loadList();
</script>
`;

function loginPage() {
  return page('登录', `
<div class="container login-box">
  <div class="card">
    <h2 style="margin-top:0;color:var(--primary);">网盘登录</h2>
    <p style="color:var(--text-sec);">请输入访问密码</p>
    <input type="password" id="pwd" placeholder="密码" onkeydown="if(event.key==='Enter')login()">
    <button class="btn-primary" style="width:100%;padding:10px;border:none;border-radius:8px;cursor:pointer;" onclick="login()">进入</button>
    <p id="err" style="color:var(--danger);font-size:14px;"></p>
  </div>
</div>
<script>
async function login(){
  const pwd=document.getElementById('pwd').value;
  const r=await fetch('/api/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pwd})});
  if(r.ok){ const params=new URLSearchParams(location.search); location.href=params.get('redirect')||'/'; }
  else { document.getElementById('err').textContent='密码错误'; }
}
</script>
`);
}

function fileBody(node, filePath) {
  const meta = formatSize(node.size) + ' · ' + new Date(node.createdAt).toLocaleString();
  const downloadUrl = '/download/' + node.ssid + '/' + encodeURIComponent(node.name);
  return `
<div class="appbar"><span class="material-icons" onclick="history.back()">arrow_back</span><h1 id="title">${escapeHtml(node.name)}</h1></div>
<div class="container">
  <div class="card">
    <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">
      <div class="file-icon" style="width:56px;height:56px;"><span class="material-icons" id="file-icon" style="font-size:28px;">insert_drive_file</span></div>
      <div>
        <div id="file-name" style="font-size:18px;font-weight:500;">${escapeHtml(node.name)}</div>
        <div id="file-meta" style="color:var(--text-sec);font-size:14px;">${meta}</div>
      </div>
    </div>
    <div class="preview-box" id="preview"><div class="empty">正在加载预览…</div></div>
  </div>
  <div class="card" style="display:flex;gap:8px;flex-wrap:wrap;">
    <a class="btn-primary" href="${downloadUrl}" style="display:inline-flex;align-items:center;gap:4px;padding:10px 16px;border:none;border-radius:8px;cursor:pointer;text-decoration:none;"><span class="material-icons">download</span> 下载</a>
    <button class="btn-secondary" onclick="shareFile()" style="padding:10px 16px;border:none;border-radius:8px;cursor:pointer;">分享</button>
    <button class="btn-secondary" onclick="copyDirectLink()" style="padding:10px 16px;border:none;border-radius:8px;cursor:pointer;">复制直链</button>
    <button class="btn-secondary" onclick="renameFile()" style="padding:10px 16px;border:none;border-radius:8px;cursor:pointer;">重命名</button>
    <button class="btn-secondary" onclick="deleteFile()" style="padding:10px 16px;border:none;border-radius:8px;cursor:pointer;color:var(--danger);">删除</button>
    <button class="btn-secondary" id="btn-save" onclick="saveText()" style="display:none;padding:10px 16px;border:none;border-radius:8px;cursor:pointer;">保存</button>
  </div>
</div>
<div class="snackbar" id="snackbar"></div>
`;
}

const FILE_SCRIPT = `
<script>
const params=new URLSearchParams(location.search);
const path=params.get('path')||'';
let fileNode=null;
function showMsg(msg){ const s=document.getElementById('snackbar'); s.textContent=msg; s.classList.add('show'); setTimeout(()=>s.classList.remove('show'),2500); }
function escapeHtml(t){ return t.replace(/[&<>"']/g,m=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[m])); }
function formatSize(b){ if(!b)return '0 B'; const k=1024, s=['B','KB','MB','GB']; const i=Math.floor(Math.log(b)/Math.log(k)); return (b/Math.pow(k,i)).toFixed(2)+' '+s[i]; }
async function copyText(text){ try { if (navigator.clipboard && window.isSecureContext) { await navigator.clipboard.writeText(text); return; } } catch(e){} }
async function api(url, opts={}){
  const r=await fetch(url, opts);
  if(r.status===401){ location.href='/login'; return null; }
  if(!r.ok){ const j=await r.json().catch(()=>({})); throw new Error(j.error||r.statusText); }
  return r.json().catch(()=>null);
}
function getMime(name){
  const ext=name.split('.').pop().toLowerCase();
  const map={mp4:'video/mp4',webm:'video/webm',mkv:'video/x-matroska',a3v8:'video/mp4',mp3:'audio/mpeg',wav:'audio/wav',ogg:'audio/ogg',flac:'audio/flac',m4a:'audio/mp4',txt:'text/plain',md:'text/markdown',json:'application/json',js:'application/javascript',css:'text/css',html:'text/html',xml:'application/xml',zip:'application/zip',rar:'application/vnd.rar','7z':'application/x-7z-compressed',tar:'application/x-tar',gz:'application/gzip',pdf:'application/pdf',jpg:'image/jpeg',jpeg:'image/jpeg',png:'image/png',gif:'image/gif',webp:'image/webp'};
  return map[ext]||'application/octet-stream';
}
async function load(){
  const preview=document.getElementById('preview');
  try{
    fileNode=await api('/api/file?path='+encodeURIComponent(path));
    if(!fileNode){ preview.innerHTML='<div class="empty">文件不存在或无权访问</div>'; return; }
    document.getElementById('file-name').textContent=fileNode.name;
    document.getElementById('file-meta').textContent=formatSize(fileNode.size)+' · '+new Date(fileNode.createdAt).toLocaleString();
    document.getElementById('title').textContent=fileNode.name;
    await renderPreview();
  }catch(e){ preview.innerHTML='<div class="empty">加载失败: '+escapeHtml(e.message)+'</div>'; }
}
async function renderPreview(){
  const ext=fileNode.name.split('.').pop().toLowerCase();
  const mime=getMime(fileNode.name);
  const preview=document.getElementById('preview');
  const url='/direct/'+fileNode.ssid+'/'+encodeURIComponent(fileNode.name);
  const downloadUrl='/download/'+fileNode.ssid+'/'+encodeURIComponent(fileNode.name);
  if(mime.startsWith('video/')){
    preview.innerHTML='<video controls playsinline style="width:100%;max-height:70vh;"><source src="'+url+'" type="'+mime+'"></video>';
  } else if(mime.startsWith('audio/')){
    preview.innerHTML='<audio controls src="'+url+'" style="width:100%;"></audio>';
  } else if(['txt','md','json','js','css','html','xml'].includes(ext)){
    const r=await fetch(url);
    const text=await r.text();
    preview.innerHTML='<textarea id="fallback-editor" style="width:100%;min-height:400px;font-family:monospace;padding:12px;border:1px solid #e0e0e0;border-radius:8px;background:#fff;">'+escapeHtml(text)+'</textarea>';
    document.getElementById('btn-save').style.display='inline-flex';
  } else if(mime.startsWith('image/')){
    preview.innerHTML='<img src="'+url+'" style="max-width:100%;max-height:70vh;display:block;margin:0 auto;border-radius:8px;">';
  } else {
    preview.innerHTML='<div class="empty">无法预览<br><a href="'+downloadUrl+'">下载文件</a></div>';
  }
}
async function shareFile(){ if(!fileNode) return; await copyText(location.origin+'/share/'+fileNode.ssid); showMsg('分享链接已复制'); }
async function copyDirectLink(){ if(!fileNode) return; await copyText(location.origin+'/direct/'+fileNode.ssid+'/'+encodeURIComponent(fileNode.name)); showMsg('直链已复制'); }
async function renameFile(){ if(!fileNode) return; const n=prompt('新名称',fileNode.name); if(!n||n===fileNode.name) return; await api('/api/file/rename',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({path,newName:n})}); location.reload(); }
async function deleteFile(){ if(!fileNode) return; if(!confirm('确定删除?')) return; await api('/api/file?path='+encodeURIComponent(path),{method:'DELETE'}); location.href='/?path='+encodeURIComponent(path.split('/').slice(0,-1).join('/')); }
async function saveText(){
  const ta=document.getElementById('fallback-editor');
  if(!ta){ showMsg('编辑器未加载'); return; }
  try{ await api('/api/file/content',{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify({path,content:ta.value})}); showMsg('已保存'); }
  catch(e){ showMsg('保存失败: '+e.message); }
}
load();
</script>
`;

function settingsPage(settings = {}) {
  const primary = settings.primary || '#1976d2';
  const bg = settings.bg || '';
  const cardOpacity = settings.cardOpacity != null ? settings.cardOpacity : 1;
  const fontFamily = settings.fontFamily || '';
  const fontCss = settings.fontCss || '';
  const fontCssFamily = settings.fontCssFamily || '';
  const SOURCE_HAN_SERIF = 'SourceHanSerifSC, serif';
  const useCustomFontFile = fontFamily && (fontFamily.startsWith('http') || fontFamily.startsWith('/'));
  const useSourceHan = fontFamily === SOURCE_HAN_SERIF;
  const useCustomCss = Boolean(fontCss && fontCssFamily);
  let fontMode = 'system';
  if (useCustomFontFile) fontMode = 'customfile';
  else if (useSourceHan) fontMode = 'sourcehan';
  else if (useCustomCss) fontMode = 'customcss';
  const presetColors = ['#1976d2', '#d32f2f', '#388e3c', '#f9a825', '#7b1fa2', '#00796b', '#e64a19', '#5d4037', '#303f9f', '#c2185b'];
  const colorSwatches = presetColors.map(c => `<span class="color-swatch" data-color="${c}" style="width:24px;height:24px;border-radius:50%;background:${c};cursor:pointer;border:2px solid ${c===primary?'#fff':'transparent'};box-shadow:0 0 0 1px ${c===primary?c:'var(--divider)'};"></span>`).join('');
  return page('设置', `
<div class="appbar"><span class="material-icons" onclick="history.back()">arrow_back</span><h1>设置</h1></div>
<div class="container">
  <div class="card">
    <h3 style="margin-top:0;">外观</h3>
    <label style="display:block;margin-bottom:8px;font-size:14px;color:var(--text-sec);">主题颜色</label>
    <div style="display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin-bottom:8px;">${colorSwatches}</div>
    <div style="display:flex;gap:8px;align-items:center;margin-bottom:16px;">
      <input type="color" id="primary-color" value="${primary}" style="width:48px;height:32px;padding:0;border:none;background:none;cursor:pointer;">
      <input type="text" id="primary-hex" value="${primary}" maxlength="7" style="width:80px;padding:5px 6px;border-radius:6px;border:1px solid var(--divider);font-size:13px;text-transform:uppercase;">
    </div>
    <label style="display:block;margin-bottom:8px;font-size:14px;color:var(--text-sec);">全局字体</label>
    <div style="display:flex;flex-direction:column;gap:8px;margin-bottom:8px;">
      <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer;"><input type="radio" name="font-mode" value="system" ${fontMode === 'system' ? 'checked' : ''}> 手机默认字体</label>
      <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer;"><input type="radio" name="font-mode" value="sourcehan" ${fontMode === 'sourcehan' ? 'checked' : ''}> 思源宋体</label>
      <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer;"><input type="radio" name="font-mode" value="customcss" ${fontMode === 'customcss' ? 'checked' : ''}> 自定义字体 CSS</label>
      <label style="display:flex;align-items:center;gap:8px;font-size:14px;cursor:pointer;"><input type="radio" name="font-mode" value="customfile" ${fontMode === 'customfile' ? 'checked' : ''}> 上传字体文件</label>
    </div>
    <div id="font-css-wrap" style="display:${useCustomCss ? 'flex' : 'none'};flex-direction:column;gap:6px;margin-bottom:12px;">
      <input type="text" id="font-css-url" placeholder="CSS 链接" value="${escapeHtml(fontCss)}" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--divider);font-size:13px;">
      <input type="text" id="font-css-family" placeholder="font-family 名称" value="${escapeHtml(fontCssFamily)}" style="width:100%;padding:6px 8px;border-radius:6px;border:1px solid var(--divider);font-size:13px;">
    </div>
    <div id="font-upload-wrap" style="display:${useCustomFontFile ? 'flex' : 'none'};gap:8px;margin-bottom:16px;align-items:center;">
      <input type="text" id="font-value" placeholder="字体文件 URL" value="${escapeHtml(useCustomFontFile ? fontFamily : '')}" style="flex:1;">
      <input type="file" id="font-file" accept=".ttf,.otf,.woff,.woff2" style="display:none">
      <button class="btn-secondary" onclick="document.getElementById('font-file').click()" style="padding:6px;border:none;border-radius:6px;cursor:pointer;font-size:12px;">上传</button>
    </div>
    <label style="display:block;margin-bottom:8px;font-size:14px;color:var(--text-sec);">文件列表背景</label>
    <div style="display:flex;gap:8px;margin-bottom:8px;">
      <input type="text" id="bg-value" placeholder="颜色/图片URL" value="${escapeHtml(bg)}" style="flex:1;">
      <input type="color" id="bg-color" value="#f5f5f5" style="width:48px;padding:0;border:none;background:none;cursor:pointer;">
      <input type="file" id="bg-file" accept="image/*" style="display:none">
      <button class="btn-secondary" onclick="document.getElementById('bg-file').click()" style="padding:8px;border:none;border-radius:8px;cursor:pointer;">上传</button>
    </div>
    <label style="display:block;margin-bottom:8px;font-size:14px;color:var(--text-sec);">卡片透明度: <span id="opacity-label">${Math.round(cardOpacity * 100)}%</span></label>
    <input type="range" id="card-opacity" min="0.2" max="1" step="0.05" value="${cardOpacity}" style="width:100%;margin-bottom:16px;">
  </div>
  <div class="card">
    <h3 style="margin-top:0;">危险操作</h3>
    <button class="btn-secondary" onclick="clearAllData()" style="width:100%;padding:12px;border-radius:8px;border:none;cursor:pointer;color:var(--danger);">一键清空网盘</button>
  </div>
  <div class="card" style="text-align:center;">
    <button class="btn-primary" onclick="saveSettings()" style="padding:12px 24px;border:none;border-radius:8px;cursor:pointer;">保存设置</button>
  </div>
</div>
<div class="snackbar" id="snackbar"></div>
`, `
<script>
const SOURCE_HAN_SERIF = 'SourceHanSerifSC, serif';
function getFontSettings(){
  const mode = document.querySelector('input[name="font-mode"]:checked').value;
  if (mode === 'sourcehan') return { fontFamily: SOURCE_HAN_SERIF, fontCss: '', fontCssFamily: '' };
  if (mode === 'customfile') return { fontFamily: document.getElementById('font-value').value.trim(), fontCss: '', fontCssFamily: '' };
  if (mode === 'customcss') return { fontFamily: '', fontCss: document.getElementById('font-css-url').value.trim(), fontCssFamily: document.getElementById('font-css-family').value.trim() };
  return { fontFamily: '', fontCss: '', fontCssFamily: '' };
}
function updateFontUI(){
  const mode = document.querySelector('input[name="font-mode"]:checked').value;
  document.getElementById('font-css-wrap').style.display = mode === 'customcss' ? 'flex' : 'none';
  document.getElementById('font-upload-wrap').style.display = mode === 'customfile' ? 'flex' : 'none';
}
function setPrimaryColor(c){
  const hex = c.toLowerCase();
  document.getElementById('primary-color').value = hex;
  document.getElementById('primary-hex').value = hex;
  document.querySelectorAll('.color-swatch').forEach(s => {
    const active = s.dataset.color === hex;
    s.style.borderColor = active ? '#fff' : 'transparent';
    s.style.boxShadow = active ? '0 0 0 1px ' + hex : '0 0 0 1px var(--divider)';
  });
}
function showMsg(msg){ const s=document.getElementById('snackbar'); s.textContent=msg; s.classList.add('show'); setTimeout(()=>s.classList.remove('show'),2500); }
async function api(url, opts={}){
  const r=await fetch(url, opts);
  if(!r.ok){ const j=await r.json().catch(()=>({})); throw new Error(j.error||r.statusText); }
  return r.json().catch(()=>null);
}
document.getElementById('primary-color').oninput = function(){ setPrimaryColor(this.value); };
document.getElementById('primary-hex').oninput = function(){ const v=this.value.trim(); if (/^#[0-9a-fA-F]{6}$/.test(v)) setPrimaryColor(v); };
document.querySelectorAll('.color-swatch').forEach(s => { s.onclick = function() { setPrimaryColor(this.dataset.color); }; });
document.getElementById('bg-color').oninput = function(){ document.getElementById('bg-value').value = this.value; };
document.getElementById('card-opacity').oninput = function(){ document.getElementById('opacity-label').textContent = Math.round(parseFloat(this.value) * 100) + '%'; };
document.getElementById('bg-file').onchange = async function(){
  const file = this.files[0]; if (!file) return;
  const form = new FormData(); form.append('file', file);
  try {
    const r = await fetch('/api/upload/background', { method: 'POST', body: form });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || '上传失败');
    document.getElementById('bg-value').value = j.url;
    showMsg('背景图已上传');
  } catch (e) { showMsg('背景图上传失败: ' + e.message); }
};
document.querySelectorAll('input[name="font-mode"]').forEach(radio => { radio.onchange = updateFontUI; });
document.getElementById('font-file').onchange = async function(){
  const file = this.files[0]; if (!file) return;
  const form = new FormData(); form.append('file', file);
  try {
    const r = await fetch('/api/upload/font', { method: 'POST', body: form });
    const j = await r.json();
    if (!r.ok) throw new Error(j.error || '上传失败');
    document.getElementById('font-value').value = j.url;
    showMsg('字体已上传');
  } catch (e) { showMsg('字体上传失败: ' + e.message); }
};
async function saveSettings(){
  const font = getFontSettings();
  const settings = {
    primary: document.getElementById('primary-color').value,
    bg: document.getElementById('bg-value').value.trim(),
    cardOpacity: parseFloat(document.getElementById('card-opacity').value),
    fontFamily: font.fontFamily, fontCss: font.fontCss, fontCssFamily: font.fontCssFamily
  };
  await api('/api/settings', { method: 'PUT', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(settings) });
  showMsg('设置已保存'); setTimeout(() => location.reload(), 600);
}
async function clearAllData(){
  const pwd = prompt('警告：这将清空所有文件、任务和设置。请输入网盘密码确认：');
  if (!pwd) return;
  await api('/api/clear', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ password: pwd }) });
  showMsg('网盘已清空'); setTimeout(() => location.href = '/', 800);
}
</script>
`, settings.themeCss || '');
}

function generateThemeCss(settings = {}) {
  const primary = settings.primary || '#1976d2';
  const bg = (settings.bg || '').replace(/["'`<>]/g, '');
  const cardOpacity = settings.cardOpacity != null ? settings.cardOpacity : 1;
  const fontFamily = (settings.fontFamily || '').replace(/["'`<>]/g, '');
  const fontCss = (settings.fontCss || '').replace(/["'`<>]/g, '');
  let fontCssFamily = (settings.fontCssFamily || '').replace(/["'`]/g, '').trim();
  if (fontCssFamily && !/^[a-zA-Z0-9_-]+$/.test(fontCssFamily)) fontCssFamily = '"' + fontCssFamily + '"';
  const SOURCE_HAN_SERIF_CSS = 'https://v6.gh-proxy.com/github.com/ike-lee-820/font/raw/main/siyuansongti/Font_Source_Han_Serif.css';
  const isCustomFontFile = fontFamily && (fontFamily.startsWith('http') || fontFamily.startsWith('/'));
  const isSourceHan = fontFamily === 'SourceHanSerifSC, serif';
  const isCustomCss = Boolean(fontCss && fontCssFamily);
  let link = '';
  if (isSourceHan) link = '<link rel="stylesheet" href="' + SOURCE_HAN_SERIF_CSS + '">';
  else if (isCustomCss) link = '<link rel="stylesheet" href="' + fontCss + '">';
  let css = '<style id="theme-style">';
  css += ':root { --primary:' + primary + '; }';
  if (isSourceHan) css += 'body, input, select, button, textarea { font-family: "SourceHanSerifSC", system-ui, sans-serif !important; }';
  else if (isCustomCss) css += 'body, input, select, button, textarea { font-family: ' + fontCssFamily + ', system-ui, sans-serif !important; }';
  else if (isCustomFontFile) {
    css += '@font-face { font-family: "CustomNetdiskFont"; src: url(' + fontFamily + '); }';
    css += 'body, input, select, button, textarea { font-family: "CustomNetdiskFont", system-ui, sans-serif !important; }';
  } else css += 'body, input, select, button, textarea { font-family: system-ui, sans-serif !important; }';
  if (bg) {
    if (bg.startsWith('http') || bg.startsWith('data:') || bg.startsWith('/')) {
      css += 'html, body { background: transparent !important; }';
      css += 'body::before { content:""; position:fixed; inset:0; z-index:-1; background-image: url(' + bg + '); background-size: cover; background-attachment: fixed; background-position: center; }';
    } else css += 'html, body { background: ' + bg + ' !important; }';
  }
  const alpha = Math.round(cardOpacity * 255).toString(16).padStart(2, '0');
  const cardBg = cardOpacity < 1 ? 'ffffff' + alpha : 'ffffff';
  css += '.file-card, .sort-select, #btn-select-mode, .selection-bar, .selection-bar button { background-color: #' + cardBg + ' !important; }';
  css += '</style>';
  return link + css;
}

function sharePage(node) {
  return page('分享', `
<div class="appbar"><h1>文件分享</h1></div>
<div class="container">
  <div class="card" style="text-align:center;">
    <div class="file-icon" style="width:72px;height:72px;margin:0 auto;"><span class="material-icons" style="font-size:36px;">insert_drive_file</span></div>
    <h2 style="margin:12px 0 4px;">${escapeHtml(node.name)}</h2>
    <p style="color:var(--text-sec);">${formatSize(node.size)}</p>
    <div style="margin-top:20px;">
      <button class="btn-primary" onclick="location.href='/direct/${node.ssid}/${encodeURIComponent(node.name)}'" style="padding:10px 20px;border:none;border-radius:8px;cursor:pointer;">下载</button>
    </div>
  </div>
</div>
`);
}

function zipPage(folderName, files) {
  const filesJson = JSON.stringify(files).replace(/</g, '\u003c');
  const fileListHtml = files.map(f => 
    `<div style="padding:8px 0;border-bottom:1px solid var(--divider);display:flex;align-items:center;gap:8px;">
      <span style="flex:1;font-size:13px;word-break:break-all;">${escapeHtml(f.path)}</span>
      <span style="font-size:12px;color:var(--text-sec);">${formatSize(f.size)}</span>
    </div>`
  ).join('');
  return page('打包下载: ' + folderName, `
<div class="appbar"><span class="material-icons" onclick="history.back()">arrow_back</span><h1>打包下载</h1></div>
<div class="container">
  <div class="card">
    <h3 style="margin-top:0;">${escapeHtml(folderName)}</h3>
    <p style="color:var(--text-sec);">共 ${files.length} 个文件</p>
    <div style="margin:16px 0;">
      <div style="height:4px;background:var(--divider);border-radius:2px;overflow:hidden;">
        <div id="zip-bar" style="height:100%;width:0%;background:var(--primary);transition:width .3s;"></div>
      </div>
      <div id="zip-status" style="font-size:13px;color:var(--text-sec);margin-top:8px;">准备中...</div>
    </div>
    <button class="btn-primary" id="btn-start" onclick="startZip()" style="padding:10px 20px;border:none;border-radius:8px;cursor:pointer;">开始打包</button>
  </div>
  <div class="card" style="max-height:50vh;overflow:auto;">${fileListHtml}</div>
</div>
<script src="https://cdn.jsdelivr.net/npm/jszip@3.10.1/dist/jszip.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/file-saver@2.0.5/dist/FileSaver.min.js"></script>
<script>
const files = ${filesJson};
const folderName = '${escapeHtml(folderName).replace(/'/g, "\\'")}';
async function startZip(){
  document.getElementById('btn-start').disabled = true;
  const zip = new JSZip();
  const folder = zip.folder(folderName);
  for (let i = 0; i < files.length; i++) {
    const f = files[i];
    document.getElementById('zip-status').textContent = '下载 ' + (i+1) + '/' + files.length;
    const resp = await fetch('/direct/' + f.ssid + '/' + encodeURIComponent(f.name));
    const blob = await resp.blob();
    folder.file(f.path, blob);
    document.getElementById('zip-bar').style.width = Math.floor((i/files.length)*50) + '%';
  }
  document.getElementById('zip-status').textContent = '正在生成 ZIP...';
  const content = await zip.generateAsync({type: 'blob', streamFiles: true}, function(meta){
    document.getElementById('zip-bar').style.width = (75 + Math.floor(meta.percent * 0.2)) + '%';
  });
  saveAs(content, folderName + '.zip');
  document.getElementById('zip-status').textContent = '完成';
  document.getElementById('zip-bar').style.width = '100%';
  document.getElementById('btn-start').disabled = false;
}
</script>
`, '');
}

function escapeHtml(text) {
  return text.replace(/[&<>"']/g, m => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[m]));
}

// ==================== 路由处理 ====================

async function handleRequest(request, env, ctx = null) {
  const url = new URL(request.url);
  const path = decodeURIComponent(url.pathname);

  if (path === '/api/login') {
    const body = await request.json();
    if (body.password === env.CLOUD_PASSWORD) {
      return new Response(JSON.stringify({ ok: true }), {
        headers: { 'Content-Type': 'application/json', 'Set-Cookie': `auth=${encodeURIComponent(env.CLOUD_PASSWORD)}; Path=/; HttpOnly; SameSite=Lax; Max-Age=86400` }
      });
    }
    return errorResponse('密码错误', 401);
  }

  if (path === '/api/logout') {
    return new Response(JSON.stringify({ ok: true }), {
      headers: { 'Content-Type': 'application/json', 'Set-Cookie': `auth=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0` }
    });
  }

  if (path === '/api/structure') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const p = url.searchParams.get('path') || '';
    const structure = await getStructure(env);
    const node = p ? getNode(structure, p) : structure;
    if (!node) return errorResponse('路径不存在', 404);
    return jsonResponse(node);
  }

  if (path === '/api/folder' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const structure = await getStructure(env);
    const parts = body.path.split('/').filter(Boolean);
    let parent = structure;
    for (const p of parts) {
      if (!parent.children[p]) parent.children[p] = { type: 'folder', name: p, children: {}, createdAt: Date.now() };
      parent = parent.children[p];
    }
    await saveStructure(env, structure);
    return jsonResponse({ ok: true });
  }

  if (path === '/api/text' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const content = new TextEncoder().encode(body.content || '');
    const name = body.path.split('/').pop() || 'untitled.txt';
    const taskId = ssid();
    await addTask(env, { id: taskId, name, status: 'uploading', message: '保存文本...', progress: 0, size: content.byteLength, createdAt: Date.now(), updatedAt: Date.now() });
    try {
      const id = ssid();
      await githubCreateRepo(id, env);
      await githubUploadFile(id, 'chunk_0', content.buffer, env, 'text', true);
      const structure = await getStructure(env);
      setNode(structure, body.path, { type: 'file', name, ssid: id, storage: 'github', size: content.byteLength, chunks: 1, createdAt: Date.now() });
      await saveStructure(env, structure);
      await updateTask(env, taskId, { status: 'done', message: '完成', progress: 100 });
    } catch (e) {
      await updateTask(env, taskId, { status: 'error', message: e.message || '保存失败', progress: 0 });
      return errorResponse(e.message || '保存失败', 500);
    }
    return jsonResponse({ ok: true, taskId });
  }

  if (path === '/api/upload' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const form = await request.formData();
    const filePath = form.get('path') || '';
    const file = form.get('file');
    if (!file || !filePath) return errorResponse('缺少文件或路径');
    const arrayBuffer = await file.arrayBuffer();
    const taskId = form.get('taskId') || ssid();
    const size = arrayBuffer.byteLength;
    const id = ssid();
    const chunks = Math.ceil(size / CHUNK_SIZE);
    await addTask(env, { id: taskId, name: file.name, status: 'uploading', message: '上传中...', progress: 5, size, createdAt: Date.now(), updatedAt: Date.now() });
    try {
      await githubCreateRepo(id, env);
      for (let i = 0; i < chunks; i++) {
        const start = i * CHUNK_SIZE;
        const end = Math.min(start + CHUNK_SIZE, size);
        await githubUploadFile(id, `chunk_${i}`, arrayBuffer.slice(start, end), env, `chunk ${i}`, true);
        await updateTaskThrottled(env, taskId, {
          message: '写入 ' + (i + 1) + '/' + chunks + ' 分片 · ' + formatSize(Math.min(size, end)) + '/' + formatSize(size),
          progress: 5 + Math.floor(((i + 1) / chunks) * 90)
        });
      }
      const structure = await getStructure(env);
      const oldNode = getNode(structure, filePath);
      if (oldNode && oldNode.type === 'file') await deleteFileStorage(oldNode, env);
      setNode(structure, filePath, { type: 'file', name: file.name, ssid: id, storage: 'github', size, chunks, createdAt: Date.now() });
      await saveStructure(env, structure);
      await updateTask(env, taskId, { status: 'done', message: '完成', progress: 100 });
    } catch (e) {
      await updateTask(env, taskId, { status: 'error', message: e.message || '上传失败', progress: 0 });
      return errorResponse(e.message || '上传失败', 500);
    }
    return jsonResponse({ ok: true, taskId });
  }

  if (path === '/api/upload/start' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const filePath = body.path, filename = body.filename, size = body.size;
    const taskId = body.taskId || ssid();
    if (!filePath || !filename || size == null) return errorResponse('缺少参数');
    const uploadId = ssid();
    const storage = size <= KV_SIZE_LIMIT ? 'kv' : 'github';
    const chunkSize = Math.min(body.clientChunkSize || CLIENT_CHUNK_SIZE, CHUNK_SIZE);
    const chunks = Math.ceil(size / chunkSize);
    if (storage === 'github') await githubCreateRepo(uploadId, env);
    await addTask(env, { id: taskId, name: filename, status: 'uploading', message: '准备分片上传...', progress: 0, size, createdAt: Date.now(), updatedAt: Date.now() });
    return jsonResponse({ uploadId, storage, chunks, chunkSize, taskId });
  }

  if (path === '/api/upload/chunk' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const form = await request.formData();
    const uploadId = form.get('uploadId');
    const index = parseInt(form.get('index'), 10);
    const total = parseInt(form.get('total'), 10);
    const taskId = form.get('taskId');
    const storage = form.get('storage') || 'github';
    const chunkFile = form.get('chunk');
    if (!uploadId || isNaN(index) || !chunkFile) return errorResponse('缺少分片参数');
    const chunkBuf = await chunkFile.arrayBuffer();
    try {
      if (storage === 'kv') {
        await getKV(uploadId, env).put(uploadId + '_chunk_' + index, chunkBuf);
      } else {
        await githubUploadFile(uploadId, 'chunk_' + index, chunkBuf, env, 'chunk ' + index, true);
      }
      if (!isNaN(total) && taskId) {
        await ensureD1(env);
        const row = await getD1(env).prepare('SELECT value FROM kv_store WHERE key = ?').bind('task_' + taskId).first();
        let msg = '已写入 ' + (index + 1) + '/' + total + ' 分片';
        let prog = Math.min(90, Math.floor(((index + 1) / total) * 90));
        if (row && row.value) {
          try {
            const task = JSON.parse(row.value);
            const size = task.size || 0;
            const elapsed = (Date.now() - (task.startedAt || task.createdAt || Date.now())) / 1000;
            const doneBytes = size > 0 ? Math.min(size, Math.floor(((index + 1) / total) * size)) : 0;
            const speed = elapsed > 0 ? doneBytes / elapsed : 0;
            msg = '写入 ' + (index + 1) + '/' + total + ' 分片 · ' + formatSize(doneBytes) + '/' + formatSize(size) + ' · ' + formatSpeed(speed);
          } catch (e) {}
        }
        await updateTaskThrottled(env, taskId, { message: msg, progress: prog });
      }
      return jsonResponse({ ok: true, index, total });
    } catch (e) {
      try { await updateTask(env, taskId, { status: 'error', message: e.message || '分片上传失败', progress: 0 }); } catch (_) {}
      return errorResponse(e.message || '分片上传失败', 500);
    }
  }

  if (path === '/api/upload/finish' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const filePath = body.path, uploadId = body.uploadId, filename = body.filename;
    const storage = body.storage, size = body.size, chunks = body.chunks, taskId = body.taskId;
    if (!filePath || !uploadId || !filename || !storage) return errorResponse('缺少参数');
    try {
      const structure = await getStructure(env);
      const oldNode = getNode(structure, filePath);
      if (oldNode && oldNode.type === 'file') { try { await deleteFileStorage(oldNode, env); } catch (e) {} }
      let finalStorage = storage, finalSsid = uploadId, finalChunks = chunks;
      if (storage === 'kv' && chunks > 1) {
        await updateTask(env, taskId, { message: '合并分片...', progress: 95 });
        let totalLen = 0; const bufs = [];
        for (let i = 0; i < chunks; i++) {
          const b = await getKV(uploadId, env).get(uploadId + '_chunk_' + i, { type: 'arrayBuffer' });
          if (!b) throw new Error('分片 ' + (i + 1) + ' 丢失');
          bufs.push(new Uint8Array(b)); totalLen += b.byteLength;
        }
        const combined = new Uint8Array(totalLen);
        let off = 0;
        for (const b of bufs) { combined.set(b, off); off += b.byteLength; }
        await getKV(uploadId, env).put(uploadId, combined.buffer);
        for (let i = 0; i < chunks; i++) await getKV(uploadId, env).delete(uploadId + '_chunk_' + i);
        finalStorage = 'kv'; finalChunks = 1;
      }
      setNode(structure, filePath, { type: 'file', name: filename, ssid: finalSsid, storage: finalStorage, size, chunks: finalChunks, createdAt: Date.now() });
      await saveStructure(env, structure);
      await updateTask(env, taskId, { status: 'done', message: '完成', progress: 100 });
      return jsonResponse({ ok: true });
    } catch (e) {
      await updateTask(env, taskId, { status: 'error', message: e.message || '完成失败', progress: 0 });
      return errorResponse(e.message || '完成失败', 500);
    }
  }

  if (path === '/api/upload/manual/start' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const filename = body.filename, size = body.size, filePath = body.path;
    if (!filename || size == null || !filePath) return errorResponse('缺少参数');
    const uploadId = ssid();
    const taskId = body.taskId || ssid();
    const chunks = Math.ceil(size / CHUNK_SIZE);
    await githubCreateRepo(uploadId, env);
    await addTask(env, { id: taskId, name: filename, status: 'uploading', message: '等待客户端直传...', progress: 0, size, createdAt: Date.now(), updatedAt: Date.now() });
    return jsonResponse({ uploadId, taskId, chunks, chunkSize: CHUNK_SIZE, githubUser: GITHUB_USER, repo: uploadId, token: env.GITHUB_TOKEN });
  }

  if (path === '/api/upload/manual/chunk' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const uploadId = body.uploadId, index = body.index, sha = body.sha, taskId = body.taskId;
    if (!uploadId || index == null || !sha) return errorResponse('缺少参数');
    await ensureD1(env);
    const row = await getD1(env).prepare('SELECT value FROM kv_store WHERE key = ?').bind('task_' + taskId).first();
    if (row && row.value) {
      try {
        const task = JSON.parse(row.value);
        const total = body.total || 1;
        const size = task.size || 0;
        const elapsed = (Date.now() - (task.startedAt || task.createdAt || Date.now())) / 1000;
        const doneChunks = index + 1;
        const doneBytes = size > 0 ? Math.min(size, Math.floor((doneChunks / total) * size)) : 0;
        const speed = elapsed > 0 ? doneBytes / elapsed : 0;
        await updateTask(env, taskId, {
          message: '直传 ' + doneChunks + '/' + total + ' 分片 · ' + formatSize(doneBytes) + '/' + formatSize(size) + ' · ' + formatSpeed(speed),
          progress: Math.min(90, Math.floor((doneChunks / total) * 90))
        });
      } catch (e) {
        await updateTaskThrottled(env, taskId, { message: '直传 ' + (index + 1) + '/' + body.total, progress: Math.min(90, Math.floor((index + 1) / body.total * 90)) });
      }
    }
    return jsonResponse({ ok: true, index, sha });
  }

  if (path === '/api/upload/manual/finish' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const filePath = body.path, uploadId = body.uploadId, filename = body.filename, size = body.size, chunks = body.chunks, taskId = body.taskId;
    if (!filePath || !uploadId || !filename || !size || !chunks) return errorResponse('缺少参数');
    const structure = await getStructure(env);
    const oldNode = getNode(structure, filePath);
    if (oldNode && oldNode.type === 'file') { try { await deleteFileStorage(oldNode, env); } catch (e) {} }
    setNode(structure, filePath, { type: 'file', name: filename, ssid: uploadId, storage: 'github', size, chunks, createdAt: Date.now() });
    await saveStructure(env, structure);
    await updateTask(env, taskId, { status: 'done', message: '完成', progress: 100 });
    return jsonResponse({ ok: true });
  }

  if (path === '/api/file' && request.method === 'GET') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const p = url.searchParams.get('path') || '';
    const structure = await getStructure(env);
    const node = getNode(structure, p);
    if (!node || node.type !== 'file') return errorResponse('文件不存在', 404);
    return jsonResponse(node);
  }

  if (path === '/api/file' && request.method === 'DELETE') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const p = url.searchParams.get('path') || '';
    const structure = await getStructure(env);
    const node = getNode(structure, p);
    if (!node) return errorResponse('不存在', 404);
    const filesToDelete = [];
    if (node.type === 'file') filesToDelete.push(node);
    else {
      for (const cp of collectPaths(node, p)) {
        const child = getNode(structure, cp);
        if (child && child.type === 'file') filesToDelete.push(child);
      }
    }
    deleteNode(structure, p);
    await saveStructure(env, structure);
    const bgTask = (async () => { try { await deleteFileStoragesConcurrently(filesToDelete, env, 32); } catch (e) {} })();
    if (ctx && ctx.waitUntil) ctx.waitUntil(bgTask); else bgTask.catch(() => {});
    return jsonResponse({ ok: true });
  }

  if (path === '/api/file/rename' && request.method === 'PUT') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const structure = await getStructure(env);
    renameNode(structure, body.path, body.newName);
    await saveStructure(env, structure);
    return jsonResponse({ ok: true });
  }

  if (path === '/api/file/move' && request.method === 'PUT') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const structure = await getStructure(env);
    const paths = Array.isArray(body.paths) ? body.paths : [body.path];
    const errors = [];
    for (const p of paths) {
      const res = moveNode(structure, p, body.targetPath || '', body.mode || 'overwrite');
      if (!res.ok) errors.push(`${p}: ${res.error}`);
    }
    await saveStructure(env, structure);
    if (errors.length) return errorResponse(errors.join('; '), 400);
    return jsonResponse({ ok: true });
  }

  if (path === '/api/file/copy' && request.method === 'PUT') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const structure = await getStructure(env);
    const paths = Array.isArray(body.paths) ? body.paths : [body.path];
    const errors = [];
    for (const p of paths) {
      const res = copyNode(structure, p, body.targetPath || '', body.mode || 'overwrite');
      if (!res.ok) errors.push(`${p}: ${res.error}`);
    }
    await saveStructure(env, structure);
    if (errors.length) return errorResponse(errors.join('; '), 400);
    return jsonResponse({ ok: true });
  }

  if (path === '/api/files/batch' && request.method === 'DELETE') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const paths = body.paths || [];
    const structure = await getStructure(env);
    const filesToDelete = [];
    for (const p of paths) {
      const node = getNode(structure, p);
      if (!node) continue;
      if (node.type === 'file') filesToDelete.push(node);
      else {
        for (const cp of collectPaths(node, p)) {
          const child = getNode(structure, cp);
          if (child && child.type === 'file') filesToDelete.push(child);
        }
      }
      deleteNode(structure, p);
    }
    await saveStructure(env, structure);
    const bgTask = (async () => { try { await deleteFileStoragesConcurrently(filesToDelete, env, 32); } catch (e) {} })();
    if (ctx && ctx.waitUntil) ctx.waitUntil(bgTask); else bgTask.catch(() => {});
    return jsonResponse({ ok: true });
  }

  if (path === '/api/file/content' && request.method === 'PUT') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    const structure = await getStructure(env);
    const node = getNode(structure, body.path);
    if (!node || node.type !== 'file') return errorResponse('文件不存在', 404);
    const content = new TextEncoder().encode(body.content || '');
    if (node.storage === 'kv') {
      await getKV(node.ssid, env).put(node.ssid, content);
    } else {
      const githubPath = node.chunks === 1 ? 'chunk_0' : (node.githubPath || node.name);
      await githubUploadFile(node.ssid, githubPath, content.buffer, env, 'edit text', true);
    }
    node.size = content.byteLength;
    await saveStructure(env, structure);
    return jsonResponse({ ok: true });
  }

  if (path === '/api/tasks' && request.method === 'GET') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    return jsonResponse(await getTasks(env));
  }

  if (path === '/api/tasks/batch' && request.method === 'DELETE') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json().catch(() => ({}));
    const ids = Array.isArray(body.ids) ? body.ids : [];
    for (const id of ids) { try { await deleteTask(env, id); } catch (e) {} }
    return jsonResponse({ ok: true, deleted: ids.length });
  }

  if (path.startsWith('/api/tasks/') && request.method === 'DELETE') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const id = path.slice('/api/tasks/'.length);
    if (url.searchParams.get('cancel') === '1') await cancelTask(env, id);
    else await deleteTask(env, id);
    return jsonResponse({ ok: true });
  }

  if (path.startsWith('/api/share/')) {
    const id = path.slice('/api/share/'.length);
    const structure = await getStructure(env);
    const allPaths = collectPaths(structure);
    let found = null;
    for (const p of allPaths) {
      const n = getNode(structure, p);
      if (n && n.type === 'file' && n.ssid === id) { found = { path: p, node: n }; break; }
    }
    if (!found) return errorResponse('分享文件不存在', 404);
    return jsonResponse({
      name: found.node.name, size: found.node.size, ssid: found.node.ssid,
      downloadUrl: '/download/' + found.node.ssid + '/' + encodeURIComponent(found.node.name),
      previewUrl: '/direct/' + found.node.ssid + '/' + encodeURIComponent(found.node.name)
    });
  }

  if (path === '/api/settings' && request.method === 'PUT') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    await saveSettings(env, {
      primary: body.primary || '#1976d2',
      bg: body.bg || '',
      cardOpacity: body.cardOpacity != null ? body.cardOpacity : 1,
      fontFamily: body.fontFamily || '',
      fontCss: body.fontCss || '',
      fontCssFamily: body.fontCssFamily || ''
    });
    return jsonResponse({ ok: true });
  }

  if (path === '/api/settings' && request.method === 'GET') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    return jsonResponse(await getSettings(env));
  }

  if (path === '/api/upload/background' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const form = await request.formData();
    const file = form.get('file');
    if (!file) return errorResponse('缺少文件');
    const buffer = await file.arrayBuffer();
    const ext = (file.name.split('.').pop() || 'png').toLowerCase();
    try {
      const u = await uploadBackgroundImage(buffer, ext, env);
      return jsonResponse({ ok: true, url: u });
    } catch (e) { return errorResponse(e.message || '上传失败', 500); }
  }

  if (path === '/api/upload/font' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const form = await request.formData();
    const file = form.get('file');
    if (!file) return errorResponse('缺少文件');
    const buffer = await file.arrayBuffer();
    const ext = (file.name.split('.').pop() || 'ttf').toLowerCase();
    if (!['ttf', 'otf', 'woff', 'woff2'].includes(ext)) return errorResponse('仅支持 ttf/otf/woff/woff2', 400);
    try {
      const u = await uploadFontFile(buffer, ext, env);
      return jsonResponse({ ok: true, url: u });
    } catch (e) { return errorResponse(e.message || '上传失败', 500); }
  }

  if (path === '/api/clear' && request.method === 'POST') {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const body = await request.json();
    if (body.password !== env.CLOUD_PASSWORD) return errorResponse('密码错误', 403);
    const structure = await getStructure(env);
    const allPaths = collectPaths(structure);
    const filesToDelete = [];
    for (const p of allPaths) {
      const node = getNode(structure, p);
      if (node && node.type === 'file') filesToDelete.push(node);
    }
    await d1Set(env, 'file_structure', { type: 'root', name: '', children: {}, createdAt: Date.now() });
    const bgTask = (async () => {
      try { await deleteFileStoragesConcurrently(filesToDelete, env, 32); } catch (e) {}
      for (const kv of [env.FILE_KV_1, env.FILE_KV_2, env.FILE_KV_3, env.FILE_KV_4, env.FILE_KV_5]) {
        try {
          const list = await kv.list();
          for (const key of list.keys) { try { await kv.delete(key.name); } catch (e) {} }
        } catch (e) {}
      }
      await getD1(env).prepare("DELETE FROM kv_store WHERE key LIKE 'task_%'").run();
      await d1Set(env, 'app_settings', {});
    })();
    if (ctx && ctx.waitUntil) ctx.waitUntil(bgTask);
    return jsonResponse({ ok: true });
  }

  // 页面路由
  if (path === '/login') return loginPage();
  if (path === '/') {
    if (!checkPassword(request, env)) return loginPage();
    const settings = await getSettings(env);
    return page('我的网盘', HOME_BODY, HOME_SCRIPT, generateThemeCss(settings));
  }
  if (path === '/file') {
    if (!checkPassword(request, env)) return loginPage();
    const filePath = url.searchParams.get('path') || '';
    const structure = await getStructure(env);
    const node = getNode(structure, filePath);
    if (!node || node.type !== 'file') return errorResponse('文件不存在或已删除', 404);
    const settings = await getSettings(env);
    return page(node.name, fileBody(node, filePath), FILE_SCRIPT, generateThemeCss(settings));
  }
  if (path === '/settings') {
    if (!checkPassword(request, env)) return loginPage();
    const settings = await getSettings(env);
    settings.themeCss = generateThemeCss(settings);
    return settingsPage(settings);
  }
  if (path === '/zip') {
    if (!checkPassword(request, env)) return loginPage();
    const zipPath = url.searchParams.get('path') || '';
    const structure = await getStructure(env);
    const folder = getNode(structure, zipPath);
    if (!folder || folder.type !== 'folder') return errorResponse('文件夹不存在', 404);
    const prefix = zipPath ? zipPath + '/' : '';
    const paths = collectPaths(folder, zipPath);
    const files = [];
    for (const p of paths) {
      const node = getNode(structure, p);
      if (!node || node.type !== 'file') continue;
      files.push({ path: p.slice(prefix.length).replace(/\\/g, '/'), ssid: node.ssid, name: node.name, size: node.size });
    }
    const folderName = zipPath ? zipPath.split('/').pop() : 'root';
    return zipPage(folderName, files);
  }

  if (path === '/webdav' || path.startsWith('/webdav/')) return handleWebDAV(request, env, path);

  if (path.startsWith('/download/')) {
    const forbid = requirePassword(request, env);
    if (forbid) return forbid;
    const parts = path.slice('/download/'.length).split('/');
    const id = parts[0];
    const filename = decodeURIComponent(parts.slice(1).join('/'));
    const structure = await getStructure(env);
    const allPaths = collectPaths(structure);
    let node = null;
    for (const p of allPaths) {
      const n = getNode(structure, p);
      if (n && n.type === 'file' && n.ssid === id) { node = n; break; }
    }
    if (!node) return errorResponse('文件不存在', 404);
    return buildDownloadResponse(node, filename || node.name, env, false);
  }

  if (path.startsWith('/direct/')) {
    const parts = path.slice('/direct/'.length).split('/');
    const id = parts[0];
    const filename = decodeURIComponent(parts.slice(1).join('/'));
    const structure = await getStructure(env);
    const allPaths = collectPaths(structure);
    let node = null;
    for (const p of allPaths) {
      const n = getNode(structure, p);
      if (n && n.type === 'file' && n.ssid === id) { node = n; break; }
    }
    if (!node) return errorResponse('文件不存在', 404);
    return buildDownloadResponse(node, filename || node.name, env, true);
  }

  if (path.startsWith('/share/')) {
    const id = path.slice('/share/'.length);
    const structure = await getStructure(env);
    const allPaths = collectPaths(structure);
    let found = null;
    for (const p of allPaths) {
      const n = getNode(structure, p);
      if (n && n.type === 'file' && n.ssid === id) { found = { path: p, node: n }; break; }
    }
    if (!found) return errorResponse('分享文件不存在', 404);
    return sharePage(found.node);
  }

  return errorResponse('Not Found', 404);
}

// ==================== 入口 ====================

const CORS_HEADERS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-Password, X-Requested-With'
};

function withCors(response) {
  const headers = new Headers(response.headers);
  for (const [k, v] of Object.entries(CORS_HEADERS)) headers.set(k, v);
  return new Response(response.body, { status: response.status, statusText: response.statusText, headers });
}

export default {
  async fetch(request, env, ctx) {
    if (request.method === 'OPTIONS') return new Response(null, { status: 204, headers: CORS_HEADERS });
    try {
      return withCors(await handleRequest(request, env, ctx));
    } catch (e) {
      console.error(e);
      return withCors(errorResponse(e.message || 'Internal Error', 500));
    }
  }
};