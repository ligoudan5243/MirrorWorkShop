// lib/githubDownloader.js
// 获取 GitHub 仓库完整文件树（递归列出所有文件路径）

export async function getRepoFileTree(owner, repo, branch = 'HEAD') {
  const treeUrl = `https://api.github.com/repos/${owner}/${repo}/git/trees/${branch}?recursive=1`;
  const res = await fetch(treeUrl, {
    headers: { 'User-Agent': 'B2-Mirror-Worker' },
  });
  if (!res.ok) throw new Error(`GitHub API error: ${res.status}`);
  const data = await res.json();
  // 过滤出文件（type === 'blob'），返回路径数组
  return data.tree.filter(item => item.type === 'blob').map(item => item.path);
}
