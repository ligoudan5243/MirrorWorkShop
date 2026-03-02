// lib/batchProcessor.js
// 处理单个批次任务：下载一个批次的所有文件并流式上传到 B2

import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';
import { updateMasterTaskProgress, completeMasterTask } from './taskManager.js';

async function getB2Client(bucketId, env) {
    const keyID = env[`B2_KEY_ID_${bucketId}`];
    const appKey = env[`B2_APP_KEY_${bucketId}`];
    const buckets = await getJSON(env.B2_KV, 'buckets');
    const bucket = buckets.find(b => b.id === bucketId);
    if (!bucket) throw new Error('Bucket not found');
    const client = new AwsClient({
        accesskeyID: keyID || bucket.keyID,               // 优先使用环境变量，否则使用 bucket 中的
        secretAccessKey: appKey || bucket.applicationKey,
        service: 's3',
        region: extractRegionFromEndpoint(bucket.endpoint)
    });
    return { client, bucket };
}

async function uploadFileStream(b2Client, bucket, key, stream, contentLength) {
    // 修复：使用 bucket.bucketName 而不是 bucket.id
    const url = `https://${bucket.bucketName}.${bucket.endpoint}/${key}`;
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

// 处理一个批次
export async function processBatch(batchTask, env) {
    const { masterTaskId, bucketId, owner, repo, files, batchIndex, totalBatches } = batchTask;
    const date = new Date().toISOString().split('T')[0];
    const { client, bucket } = await getB2Client(bucketId, env);

    let successCount = 0;
    const failedFiles = [];

    for (const filePath of files) {
        try {
            // 更新当前正在处理的文件
            await updateMasterTaskProgress(env, masterTaskId, {
                currentFile: filePath,
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

    // 获取当前主任务，更新批次完成状态
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
        currentFile: null, // 批次结束，清除当前文件
    });

    // 如果所有批次完成，标记主任务为完成
    const updatedMaster = await env.B2_KV.get(masterKey, 'json');
    if (updatedMaster && updatedMaster.completedBatches.length === updatedMaster.totalBatches) {
        await completeMasterTask(env, masterTaskId, 'completed');
    }
}
