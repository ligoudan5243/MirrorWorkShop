// lib/batchProcessor.js
import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';

async function getB2Client(bucketId, env) {
  const keyID = env[`B2_KEY_ID_${bucketId}`];
  const appKey = env[`B2_APP_KEY_${bucketId}`];
  const buckets = await getJSON(env.B2_KV, 'buckets');
  const bucket = buckets.find(b => b.id === bucketId);
  if (!bucket) throw new Error('Bucket not found');
  const client = new AwsClient({
    accesskeyID: keyID,
    secretAccessKey: appKey,
    service: 's3',
    region: extractRegionFromEndpoint(bucket.endpoint)
  });
  return { client, bucket };
}

async function uploadFileStream(b2Client, bucket, key, stream, contentLength) {
  const url = `https://${bucket.id}.${bucket.endpoint}/${key}`;
  const signed = await b2Client.sign(url, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      'Content-Length': contentLength,
      'X-Amz-Content-Sha256': 'UNSIGNED-PAYLOAD',
    },
    body: stream,
  });
  const res = await fetch(signed.url, {
    method: 'PUT',
    headers: signed.headers,
    body: stream,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
}

async function updateMasterProgress(env, masterTaskId, update) {
  const key = `master:${masterTaskId}`;
  const master = await env.B2_KV.get(key, 'json') || {};
  
  const completed = master.completedBatches || [];
  if (!completed.includes(update.batchIndex)) {
    completed.push(update.batchIndex);
    master.completedBatches = completed;
    master.processedFiles = (master.processedFiles || 0) + update.successCount;
    master.failedFiles = (master.failedFiles || []).concat(update.failedFiles);
    if (update.currentFile) master.currentFile = update.currentFile;
    master.updatedAt = Date.now();

    if (completed.length === update.totalBatches) {
      master.status = 'completed';
      master.completedAt = Date.now();
    }
    await env.B2_KV.put(key, JSON.stringify(master));
  }
}

export async function processBatch(batchTask, env) {
  const { masterTaskId, bucketId, owner, repo, files, batchIndex, totalBatches } = batchTask;
  const date = new Date().toISOString().split('T')[0];
  const { client, bucket } = await getB2Client(bucketId, env);

  let successCount = 0;
  const failedFiles = [];

  for (const filePath of files) {
    try {
      // 更新当前正在处理的文件
      await updateMasterProgress(env, masterTaskId, {
        currentFile: filePath,
        batchIndex,
        successCount: 0,
        failedFiles: [],
        totalBatches
      });

      const rawUrl = `https://raw.githubusercontent.com/${owner}/${repo}/HEAD/${filePath}`;
      const rawRes = await fetch(rawUrl, {
        headers: { 'User-Agent': 'B2-Mirror-Worker' },
      });
      if (!rawRes.ok) throw new Error(`Download failed: ${rawRes.status}`);
      
      const contentLength = rawRes.headers.get('content-length');
      if (!contentLength) throw new Error('Missing content-length header');

      const b2Path = `${owner}/${repo}/${date}/${filePath}`;
      await uploadFileStream(client, bucket, b2Path, rawRes.body, parseInt(contentLength, 10));
      successCount++;
    } catch (err) {
      failedFiles.push({ path: filePath, error: err.message });
    }
  }

  await updateMasterProgress(env, masterTaskId, {
    successCount,
    failedFiles,
    batchIndex,
    totalBatches,
    currentFile: null // 批次结束，清除当前文件
  });
}