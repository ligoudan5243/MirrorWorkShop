// api/buckets.js
import { getJSON, putJSON, defaultBuckets } from '../lib/kv.js';

export async function handleBuckets(request, env) {
  const method = request.method;
  if (method === 'GET') {
    const buckets = await getJSON(env.B2_KV, 'buckets', defaultBuckets);
    return Response.json(buckets);
  }
  if (method === 'POST') {
    try {
      const newBuckets = await request.json();
      await putJSON(env.B2_KV, 'buckets', newBuckets);
      return Response.json({ success: true });
    } catch (error) {
      console.error('保存桶失败:', error);
      return new Response(JSON.stringify({ error: error.message }), { status: 500 });
    }
  }
  return new Response('Method not allowed', { status: 405 });
}