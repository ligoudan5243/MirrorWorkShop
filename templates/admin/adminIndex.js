// templates/admin/adminIndex.js
import { adminHeaderHTML } from './adminHeader.js';
import { addProjectHTML } from './addProject.js';
import { autoMonitorHTML } from './autoMonitor.js';
import { hostnameHTML } from './hostname.js';
import { s3ConfigHTML } from './s3Config.js';

export const adminHTML = `
<div id="adminPanel" class="admin-panel">
    ${adminHeaderHTML}
    ${addProjectHTML}
    ${autoMonitorHTML}
    ${hostnameHTML}
    ${s3ConfigHTML}
</div>
`;