// queue.js
import { processBatch } from './lib/batchProcessor.js';
import { getRepoFileTree } from './lib/githubDownloader.js';
import { createMasterTask } from './lib/taskManager.js';

export async function queueHandler(batch, env, ctx) {
  for (const message of batch.messages) {
    const task = JSON.parse(message.body);
    
    if (task.type === 'master') {
      try {
        const { taskId, owner, repo, bucketId } = task;
        const filePaths = await getRepoFileTree(owner, repo);
        await createMasterTask(env, taskId, owner, repo, bucketId, filePaths);
        message.ack();
      } catch (error) {
        console.error('Master task failed', error);
        await env.B2_KV.put(`master:${task.taskId}`, JSON.stringify({
          status: 'failed',
          error: error.message,
          failedAt: Date.now()
        }));
        message.ack();
      }
    } else if (task.type === 'batch') {
      try {
        await processBatch(task, env);
        message.ack();
      } catch (error) {
        console.error('Batch task failed', error);
        message.retry();
      }
    } else {
      message.ack();
    }
  }
}

export async function handleQueueStatus(request, env) {
  const list = await env.B2_KV.list({ prefix: "master:" });
  const tasks = [];
  for (const key of list.keys) {
    const task = await env.B2_KV.get(key.name, 'json');
    if (task && (task.status === 'processing' || task.status === 'queued')) {
      tasks.push({
        name: `${task.owner}/${task.repo}`,
        progress: task.completedBatches ? Math.round((task.completedBatches.length / task.totalBatches) * 100) : 0,
        totalFiles: task.totalFiles,
        processedFiles: task.processedFiles || 0,
        currentFile: task.currentFile || null
      });
    }
  }
  return Response.json({ tasks });
}