// lib/taskManager.js
const BATCH_SIZE = 50;

async function updateActiveTasks(env, taskId, action, taskData = null) {
    const key = 'active_tasks';
    let tasks = await env.B2_KV.get(key, 'json') || [];
    
    if (action === 'add') {
        if (!tasks.find(t => t.taskId === taskId)) {
            tasks.push(taskData);
        }
    } else if (action === 'remove') {
        tasks = tasks.filter(t => t.taskId !== taskId);
    } else if (action === 'update') {
        const index = tasks.findIndex(t => t.taskId === taskId);
        if (index !== -1 && taskData) {
            tasks[index] = { ...tasks[index], ...taskData };
        }
    }
    
    await env.B2_KV.put(key, JSON.stringify(tasks));
}

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
        currentFile: null,
        status: 'processing',
        createdAt: Date.now(),
    };
    await env.B2_KV.put(`master:${taskId}`, JSON.stringify(masterTask));

    await updateActiveTasks(env, taskId, 'add', {
        taskId,
        name: `${owner}/${repo}`,
        totalFiles: filePaths.length,
        processedFiles: 0,
        progress: 0,
        status: 'processing'
    });

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

export async function updateMasterProgress(env, masterTaskId, update) {
    const key = `master:${masterTaskId}`;
    const master = await env.B2_KV.get(key, 'json') || {};
    
    if (update.completedBatches !== undefined) master.completedBatches = update.completedBatches;
    if (update.processedFiles !== undefined) master.processedFiles = update.processedFiles;
    if (update.failedFiles !== undefined) master.failedFiles = update.failedFiles;
    if (update.currentFile !== undefined) master.currentFile = update.currentFile;
    if (update.status !== undefined) master.status = update.status;
    master.updatedAt = Date.now();

    if (master.completedBatches && master.completedBatches.length === master.totalBatches) {
        master.status = 'completed';
        master.completedAt = Date.now();
    }

    await env.B2_KV.put(key, JSON.stringify(master));

    // 更新活动任务列表中的进度
    const progress = master.completedBatches ? Math.round((master.completedBatches.length / master.totalBatches) * 100) : 0;
    await updateActiveTasks(env, masterTaskId, 'update', {
        processedFiles: master.processedFiles,
        progress: progress,
        currentFile: master.currentFile,
        status: master.status
    });

    // 如果任务已完成或失败，从活动列表中移除
    if (master.status === 'completed' || master.status === 'failed') {
        await updateActiveTasks(env, masterTaskId, 'remove');
    }
}

export async function getMasterTask(env, taskId) {
    return await env.B2_KV.get(`master:${taskId}`, 'json');
}
