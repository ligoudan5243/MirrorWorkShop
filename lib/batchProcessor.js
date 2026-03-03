// lib/batchProcessor.js
// 处理单个批次任务：下载一个批次的所有文件并流式上传到 B2

import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';

async function getB2Client(bucketId, env) {
  const buckets = await getJSON(env.B2_KV, 'buckets');
  const bucket = buckets.find(b => b.id === bucketId);
  if (!bucket) throw new Error('Bucket not found');
  const { keyID, applicationKey, endpoint } = bucket;
  const client = new AwsClient({
    accesskeyID: keyID,
    secretAccessKey: applicationKey,
    service: 's3',
    region: extractRegionFromEndpoint(endpoint)
  });
  return { client, bucket };
}

async function uploadFile(b2Client, bucket, key, body, contentLength) {
  // 正确使用 bucket.bucketName 构建 URL
  const url = `https://${bucket.bucketName}.${bucket.endpoint}/${key}`;
  const signed = await b2Client.sign(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Length': contentLength,
      'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
    },
    body: body,
  });
  const res = await fetch(signed.url, {
    method: 'PUT',
    headers: signed.headers,
    body: body,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
}

// 更新主任务进度
async function updateMasterProgress(env, masterTaskId, update) {
  const key = `master:${masterTaskId}`;
  const master = await env.B2_KV.get(key, 'json') || {};
  
  const completed = master.completedBatches || [];
  if (!completed.includes(update.batchIndex)) {
    completed.push(update.batchIndex);
    master.completedBatches = completed;
    master.processedFiles = (master.processedFiles || 0) + update.successCount;
    master.failedFiles = (master.failedFiles || []).concat(update.failedFiles);
    master.updatedAt = Date.now();

    if (completed.length === update.totalBatches) {
      master.status = 'completed';
      master.completedAt = Date.now();
    }
    await env.B2_KV.put(key, JSON.stringify(master));
  }
}

// 处理一个批次
export async function processBatch(batchTask, env) {
  const { masterTaskId, bucketId, owner, repo, files, batchIndex, totalBatches } = batchTask;
  const date = new Date().toISOString().split('T')[0];
  const { client, bucket } = await getB2Client(bucketId, env);

  let successCount = 0;
  const failedFiles = [];

  for (const filePath of files) {
    try {
      const rawUrl = `https://raw.githubusercontent.com/${owner}/${repo}/HEAD/${filePath}`;
      const rawRes = await fetch(rawUrl, {
        headers: { 'User-Agent': 'B2-Mirror-Worker' },
      });
      if (!rawRes.ok) throw new Error(`Download failed: ${rawRes.status}`);
      
      let body, length;
      const contentLength = rawRes.headers.get('content-length');
      if (contentLength) {
        body = rawRes.body; // 流式
        length = parseInt(contentLength, 10);
      } else {
        // 如果缺少 content-length，则读取整个文件到 buffer
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

  await updateMasterProgress(env, masterTaskId, {
    successCount,
    failedFiles,
    batchIndex,
    totalBatches
  });
}
