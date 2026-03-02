// lib/taskManager.js
const BATCH_SIZE = 50; // 每批文件数量

export async function createMasterTask(env, taskId, owner, repo, bucketId, filePaths) {
  const batches = [];
  for (let i = 0; i < filePaths.length; i += BATCH_SIZE) {
    batches.push(filePaths.slice(i, i + BATCH_SIZE));
  }

  const masterTask = {
    taskId,
    owner,
    repo,
    bucketId,
    totalFiles: filePaths.length,
    totalBatches: batches.length,
    completedBatches: [],
    processedFiles: 0,
    failedFiles: [],
    status: 'processing',
    createdAt: Date.now(),
  };
  await env.B2_KV.put(`master:${taskId}`, JSON.stringify(masterTask));

  // 为每个批次发送子任务到队列
  for (let i = 0; i < batches.length; i++) {
    const batchTask = {
      type: 'batch',
      masterTaskId: taskId,
      bucketId,
      owner,
      repo,
      files: batches[i],
      batchIndex: i,
      totalBatches: batches.length,
    };
    await env.TASKS_QUEUE.send(JSON.stringify(batchTask));
  }
  return masterTask;
}

export async function getMasterTask(env, taskId) {
  return await env.B2_KV.get(`master:${taskId}`, 'json');
}
