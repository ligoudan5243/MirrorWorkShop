// lib/batchProcessor.js
import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';
import { updateMasterTaskProgress, completeMasterTask } from './taskManager.js'; // 新增导入

// ... 其他函数保持不变 ...

export async function processBatch(batchTask, env) {
  const { masterTaskId, bucketId, owner, repo, files, batchIndex, totalBatches } = batchTask;
  const date = new Date().toISOString().split('T')[0];
  const { client, bucket } = await getB2Client(bucketId, env);

  let successCount = 0;
  const failedFiles = [];

  for (const filePath of files) {
    try {
      // 更新当前文件
      await updateMasterTaskProgress(env, masterTaskId, {
        currentFile: filePath,
      });

      const rawUrl = `https://raw.githubusercontent.com/${owner}/${repo}/HEAD/${filePath}`;
      const rawRes = await fetch(rawUrl, {
        headers: { 'User-Agent': 'B2-Mirror-Worker' },
      });
      if (!rawRes.ok) throw new Error(`Download failed: ${rawRes.status}`);
      
      let body, length;
      const contentLength = rawRes.headers.get('content-length');
      if (contentLength) {
        body = rawRes.body;
        length = parseInt(contentLength, 10);
      } else {
        const buffer = await rawRes.arrayBuffer();
        body = buffer;
        length = buffer.byteLength;
      }

      const b2Path = `${owner}/${repo}/${date}/${filePath}`;
      await uploadFile(client, bucket, b2Path, body, length);
      successCount++;
    } catch (err) {
      failedFiles.push({ path: filePath, error: err.message });
    }
  }

  // 获取当前主任务以更新 completedBatches
  const masterKey = `master:${masterTaskId}`;
  const master = await env.B2_KV.get(masterKey, 'json') || {};
  
  const completedBatches = master.completedBatches || [];
  if (!completedBatches.includes(batchIndex)) {
    completedBatches.push(batchIndex);
  }
  
  const newProcessedFiles = (master.processedFiles || 0) + successCount;
  const newFailedFiles = (master.failedFiles || []).concat(failedFiles);
  
  await updateMasterTaskProgress(env, masterTaskId, {
    completedBatches,
    processedFiles: newProcessedFiles,
    failedFiles: newFailedFiles,
    currentFile: null,
  });

  // 检查是否所有批次完成
  const updatedMaster = await env.B2_KV.get(masterKey, 'json');
  if (updatedMaster && updatedMaster.completedBatches.length === updatedMaster.totalBatches) {
    await completeMasterTask(env, masterTaskId, 'completed');
  }
}