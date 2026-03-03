// api/apiIndex.js
import { handleSearch } from './search.js';
import { handleProjects, handleProject } from './projects.js';
import { handleBuckets } from './buckets.js';
import { handleConfig } from './config.js';
import { handleTask } from './task.js';
import { handleQueueStatus } from '../queue.js';
import { handleVerify } from './verify.js';

export async function handleAPI(request, env) {
  const url = new URL(request.url);
  const method = request.method;
  const path = url.pathname.slice(5); // 去掉 '/api/'

  if (path === 'search' && method === 'GET') {
    return handleSearch(request, env);
  }
  if (path === 'projects/github' && method === 'GET') {
    return handleProjects('github', env);
  }
  if (path === 'projects/docker' && method === 'GET') {
    return handleProjects('docker', env);
  }
  if (path === 'project' && method === 'POST') {   // <-- 这里必须是 project，不是 projects
    return handleProject(request, env);
  }
  if (path === 'buckets' && method === 'GET') {    // <-- 处理 GET /api/buckets
    return handleBuckets(request, env);
  }
  if (path === 'buckets' && method === 'POST') {   // <-- 处理 POST /api/buckets（保存桶）
    return handleBuckets(request, env);
  }
  if (path === 'config' && (method === 'GET' || method === 'POST')) {
    return handleConfig(request, env);
  }
  if (path.startsWith('task/') && method === 'GET') {
    return handleTask(request, env);
  }
  if (path === 'queue/status' && method === 'GET') {
    return handleQueueStatus(request, env);
  }
  if (path === 'verify-bucket' && method === 'POST') {
    return handleVerify(request, env);
  }
  return new Response('API not found', { status: 404 });
}