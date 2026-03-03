// lib/batchProcessor.js
import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';
import { updateMasterProgress } from './taskManager.js';

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

export async function processBatch(batchTask, env) {
  const { masterTaskId, bucketId, owner, repo, files, batchIndex, totalBatches } = batchTask;
  const date = new Date().toISOString().split('T')[0];
  const { client, bucket } = await getB2Client(bucketId, env);

  let successCount = 0;
  const failedFiles = [];

  for (const filePath of files) {
    try {
      await updateMasterProgress(env, masterTaskId, { currentFile: filePath });

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

  const masterKey = `master:${masterTaskId}`;
  const master = await env.B2_KV.get(masterKey, 'json') || {};
  const completedBatches = master.completedBatches || [];
  if (!completedBatches.includes(batchIndex)) {
    completedBatches.push(batchIndex);
  }
  const processedFiles = (master.processedFiles || 0) + successCount;
  const failedFilesList = (master.failedFiles || []).concat(failedFiles);

  await updateMasterProgress(env, masterTaskId, {
    completedBatches,
    processedFiles,
    failedFiles: failedFilesList,
    currentFile: null
  });
}
