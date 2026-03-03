// lib/utils.js
import { AwsClient } from '../aws4fetch.js';
import { getJSON, defaultBuckets } from './kv.js';
import { extractRegionFromEndpoint } from './b2.js';

// 从 B2 列出文件并解析版本
export async function fetchVersionsFromB2(bucketId, projectName, env) {
  const keyID = env[`B2_KEY_ID_${bucketId}`];
  const appKey = env[`B2_APP_KEY_${bucketId}`];
  if (!keyID || !appKey) {
    return [
      { date: new Date().toISOString().slice(0,10), files: ['file1.txt', 'file2.txt'] }
    ];
  }
  const buckets = await getJSON(env.B2_KV, 'buckets', defaultBuckets);
  const bucket = buckets.find(b => b.id === bucketId);
  if (!bucket) throw new Error('Bucket not found');
  const client = new AwsClient({
    accesskeyID: keyID,
    secretAccessKey: appKey,
    service: 's3',
    region: extractRegionFromEndpoint(bucket.endpoint)
  });
  const listUrl = `https://${bucket.id}.${bucket.endpoint}/?prefix=${encodeURIComponent(projectName)}/&delimiter=/`;
  const signed = await client.sign(listUrl, { method: 'GET' });
  const res = await fetch(signed.url, { headers: signed.headers });
  const xml = await res.text();
  const versionMatches = [...xml.matchAll(/<CommonPrefixes><Prefix>(.*?)<\/Prefix><\/CommonPrefixes>/g)];
  const versions = [];
  for (const match of versionMatches) {
    const prefix = match[1];
    const parts = prefix.split('/');
    if (parts.length >= 2) {
      const date = parts[1];
      const listFilesUrl = `https://${bucket.id}.${bucket.endpoint}/?prefix=${encodeURIComponent(prefix)}`;
      const signedFiles = await client.sign(listFilesUrl, { method: 'GET' });
      const filesRes = await fetch(signedFiles.url, { headers: signedFiles.headers });
      const filesXml = await filesRes.text();
      const fileMatches = [...filesXml.matchAll(/<Key>(.*?)<\/Key>/g)];
      const files = fileMatches.map(m => m[1].replace(prefix, '')).filter(f => f);
      versions.push({ date, files });
    }
  }
  return versions;
}
