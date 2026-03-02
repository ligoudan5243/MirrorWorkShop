// lib/batchProcessor.js
import { AwsClient } from '../aws4fetch.js';
import { extractRegionFromEndpoint } from './b2.js';
import { getJSON } from './kv.js';
import { updateMasterTaskProgress } from './taskManager.js';

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
                // 注意：这里不传 batchIndex，因为批次还未完成
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

    // 获取当前主任务，更新 completedBatches 和 processedFiles
    // 这里我们通过 KV 直接读取主任务来获取当前值，或者可以用内存变量，但简单起见重新读取
    const masterKey = `master:${masterTaskId}`;
    const master = await env.B2_KV.get(masterKey, 'json') || {};
    
    // 更新批次完成状态
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

    // 如果所有批次完成，则标记主任务为完成（由调用者处理？）
    // 注意：这里不直接调用 completeMasterTask，因为批次任务可能并行，由队列消费者检查？
    // 但主任务的完成状态应该在所有批次完成后由某个逻辑触发。可以在 updateMasterTaskProgress 中检查并自动完成。
    // 或者让队列消费者在收到最后一个批次消息后触发。为了简化，我们可以在 updateMasterTaskProgress 中检查
    // 但这里我们保持与之前一致，让主任务的完成由其他机制处理（比如在轮询中检测）。
    // 实际上，可以在 updateMasterTaskProgress 中检查并调用 completeMasterTask。
    // 我们在这里加入自动完成逻辑。
    const updatedMaster = await env.B2_KV.get(masterKey, 'json');
    if (updatedMaster && updatedMaster.completedBatches.length === updatedMaster.totalBatches) {
        // 所有批次完成
        await completeMasterTask(env, masterTaskId, 'completed');
    }
}

// 注意：需要从 taskManager.js 导入 completeMasterTask
import { completeMasterTask } from './taskManager.js';
