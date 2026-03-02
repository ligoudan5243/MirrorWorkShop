// templates/admin/hostname.js
export const hostnameHTML = `
<div class="card">
    <div class="card-header"><h2>自定义主机名</h2></div>
    <div class="hostname-row">
        <div class="hostname-input">
            <label>官网自定义主机名:</label>
            <input type="text" id="officialHostname" placeholder="https://mirror.example.com" value="https://gh-mirror.example.com">
        </div>
        <div class="hostname-input">
            <label>存储桶自定义主机名:</label>
            <input type="text" id="bucketHostname" placeholder="https://b2-mirror.example.com" value="https://b2-mirror.example.com">
        </div>
        <p style="color:#475569; font-size:0.9rem;">设置后，相应卡片中的下载链接会替换为自定义主机名</p>
    </div>
</div>
`;