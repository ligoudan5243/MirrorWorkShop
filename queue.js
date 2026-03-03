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

// 队列状态 API - 从 active_tasks 读取
export async function handleQueueStatus(request, env) {
  const activeTasks = await env.B2_KV.get('active_tasks', 'json') || [];
  return Response.json({ tasks: activeTasks });
}