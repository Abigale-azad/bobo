/**
 * jrsy-sync.js — Cloud Sync Module for jrsy (v3: 按 store 增量同步)
 *
 * 设计：
 * - 每个 IndexedDB store 独立追踪 hash
 * - 只有变化的 store 才重新分片上传
 * - gzip 压缩 + 800KB 分片 + 1.5s 间隔防限流
 *
 * 云端结构：
 *   users/{userId}/stores/{storeName}/meta → {hash, recordCount, size, chunkCount, updatedAt}
 *   users/{userId}/stores/{storeName}/chunks/{0,1,2...} → {data: Blob, index, hash}
 *
 * 依赖：firebase-app-compat, firebase-firestore-compat, pako
 */

const CloudSync = {
    DEFAULT_CONFIG: {
        apiKey: "AIzaSyDrQIUrNuvnjTwn03-rg2TOf2ODXM3BMo8",
        authDomain: "jrsy-sync.firebaseapp.com",
        projectId: "jrsy-sync",
        storageBucket: "jrsy-sync.firebasestorage.app",
        messagingSenderId: "544446110834",
        appId: "1:544446110834:web:24341084a3805d715f76eb"
    },

    CHUNK_SIZE: 800 * 1024,       // 800KB per chunk
    CHUNK_DELAY: 3000,            // 3s between chunks (Spark plan write limit)
    MAX_RETRIES: 3,

    initialized: false,
    enabled: false,
    userId: null,
    firebaseApp: null,
    firestore: null,
    isUploading: false,
    isDownloading: false,
    lastSyncTime: 0,
    uploadTimer: null,

    SYNC_STORES: [
        'friends', 'chatHistories', 'diaries', 'worldBooks', 'worldBookFolders',
        'favorites', 'moments', 'playlist', 'appSettings', 'apiSettings',
        'customEmojis', 'memories', 'openingStatements', 'writingStyles', 'skits',
        'forumPosts', 'forumRules', 'forumLikes', 'bubbleCssPresets', 'stickerGroups',
        'interfaceCssPresets', 'apiPresets', 'cloneApiSettings',
        'offlineContentPresets', 'voiceAudioCache', 'fontPresets',
        'offlineCssPresets', 'gameApiSettings', 'summaryPrompts', 'babies', 'babyChats'
    ],

    // 路径辅助
    _storeDocPath(storeName) {
        // 4 段 = 偶数 = 文档引用
        return 'users/' + this.userId + '/stores/' + storeName;
    },
    _storeChunkPath(storeName, index) {
        // 6 段 = 偶数 = 文档引用
        return 'users/' + this.userId + '/stores/' + storeName + '/chunks/' + index;
    },

    // ==================== 初始化 ====================
    async init(config) {
        if (typeof firebase === 'undefined') {
            console.warn('[CloudSync] Firebase SDK 未加载');
            return false;
        }
        if (this.initialized) {
            console.log('[CloudSync] 已初始化');
            return true;
        }
        try {
            this.firebaseApp = firebase.initializeApp(config, 'jrsy-cloud-sync');
            this.firestore = firebase.firestore(this.firebaseApp);
            try { await this.firestore.disableNetwork(); await this.firestore.enableNetwork(); } catch(e) {}

            this.userId = this._resolveUserId();
            await this._waitForReady();
            this._hookSaveData();
            this._injectSyncUI();

            this.initialized = true;
            this.enabled = true;
            console.log('[CloudSync] 初始化完成 userId=' + this.userId);

            await this._checkAndRestore();
            return true;
        } catch (e) {
            console.error('[CloudSync] 初始化失败:', e);
            return false;
        }
    },

    _resolveUserId() {
        const code = localStorage.getItem('jrsy_activation_record');
        if (code) return 'u_' + this._simpleHash(code);
        let uid = localStorage.getItem('jrsy_cloud_uid');
        if (uid) return uid;
        uid = 'u_' + Date.now().toString(36) + '_' + Math.random().toString(36).substr(2, 6);
        localStorage.setItem('jrsy_cloud_uid', uid);
        return uid;
    },

    _simpleHash(str) {
        let h = 0;
        for (let i = 0; i < str.length; i++) h = ((h << 5) - h + str.charCodeAt(i)) | 0;
        return Math.abs(h).toString(36);
    },

    _delay(ms) { return new Promise(r => setTimeout(r, ms)); },

    _waitForReady() {
        return new Promise((resolve) => {
            const check = () => {
                if (typeof dbManager !== 'undefined' && typeof saveData === 'function' && typeof loadData === 'function') resolve();
                else setTimeout(check, 200);
            };
            check();
        });
    },

    // ==================== Hook ====================
    _hookSaveData() {
        const self = this;
        const _orig = saveData;
        saveData = async function() {
            const r = await _orig.apply(this, arguments);
            self._scheduleUpload().catch(e => console.warn('[CloudSync] 上传失败:', e.message));
            return r;
        };
    },

    async _scheduleUpload() {
        if (!this.enabled || this.isUploading) return;
        if (this.uploadTimer) clearTimeout(this.uploadTimer);
        this.uploadTimer = setTimeout(() => this._doUpload(), 3000);
    },

    // ==================== 增量上传（核心） ====================
    async _doUpload() {
        if (this.isUploading) return;
        this.isUploading = true;
        this._updateSyncStatus('uploading');

        try {
            const startTime = Date.now();
            let changedCount = 0;
            let totalRecords = 0;

            // 逐个 store 检查
            for (const storeName of this.SYNC_STORES) {
                try {
                    const records = await dbManager.getAll(storeName);
                    if (!records || records.length === 0) continue;

                    const dataStr = JSON.stringify(records);
                    const newHash = this._simpleHash(dataStr);

                    // 检查云端是否已有相同 hash
                    const metaPath = this._storeDocPath(storeName);
                    const metaDoc = await this.firestore.doc(metaPath).get();
                    const oldHash = metaDoc.exists ? metaDoc.data().hash : null;

                    if (newHash === oldHash) {
                        totalRecords += records.length;
                        continue; // 没变，跳过
                    }

                    // 变化了，上传
                    const compressed = pako.gzip(dataStr);
                    const chunks = this._splitBuffer(compressed);
                    changedCount++;

                    if (changedCount === 1) {
                        console.log('[CloudSync] 检测到变化的 store，开始增量上传...');
                    }
                    console.log('[CloudSync] ' + storeName + ': ' + records.length + '条 → ' + (compressed.length/1024).toFixed(0) + 'KB (' + chunks.length + '片)');

                    // 上传分片
                    for (let i = 0; i < chunks.length; i++) {
                        const chunkPath = this._storeChunkPath(storeName, i);
                        const blob = firebase.firestore.Blob.fromUint8Array(chunks[i]);

                        let ok = false;
                        for (let a = 1; a <= this.MAX_RETRIES; a++) {
                            try {
                                await this.firestore.doc(chunkPath).set({ data: blob, idx: i, hash: newHash });
                                ok = true; break;
                            } catch (e) {
                                if (a < this.MAX_RETRIES) {
                                    console.warn('[CloudSync] ' + storeName + ' 分片' + (i+1) + ' 重试 ' + a + '/' + this.MAX_RETRIES);
                                    await this._delay(this.CHUNK_DELAY * a);
                                } else throw e;
                            }
                        }
                        if (i < chunks.length - 1) await this._delay(this.CHUNK_DELAY);
                    }

                    // 清理旧的多余分片
                    const oldChunkCount = metaDoc.exists ? (metaDoc.data().chunkCount || 0) : 0;
                    for (let i = chunks.length; i < oldChunkCount; i++) {
                        try { await this.firestore.doc(this._storeChunkPath(storeName, i)).delete(); } catch(e) {}
                    }

                    // 写元数据
                    await this.firestore.doc(metaPath).set({
                        hash: newHash,
                        recordCount: records.length,
                        size: dataStr.length,
                        compressedSize: compressed.length,
                        chunkCount: chunks.length,
                        updatedAt: firebase.firestore.FieldValue.serverTimestamp()
                    });

                    // 等待写入队列清空，防止下个 store 触发限流
                    await this.firestore.waitForPendingWrites();

                    totalRecords += records.length;
                } catch (e) {
                    console.warn('[CloudSync] ' + storeName + ' 上传失败:', e.message);
                }
            }

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            if (changedCount > 0) {
                console.log('[CloudSync] 增量上传完成: ' + changedCount + ' 个 store (' + totalRecords + ' 条, ' + elapsed + 's)');
            }
            this.lastSyncTime = Date.now();
            this._updateSyncStatus('synced');

        } catch (e) {
            console.error('[CloudSync] 上传失败:', e);
            this._updateSyncStatus('error');
            throw e;
        } finally {
            this.isUploading = false;
        }
    },

    _splitBuffer(buf) {
        const chunks = [];
        for (let i = 0; i < buf.length; i += this.CHUNK_SIZE) chunks.push(buf.slice(i, i + this.CHUNK_SIZE));
        return chunks;
    },

    // ==================== 下载 ====================
    async _checkAndRestore() {
        if (!this.enabled) return;
        try {
            const localHasData = await this._localHasData();

            // 检查云端是否有任意 meta
            let hasCloud = false;
            for (const storeName of this.SYNC_STORES) {
                const doc = await this.firestore.doc(this._storeDocPath(storeName)).get();
                if (doc.exists) { hasCloud = true; break; }
            }

            if (!hasCloud) {
                console.log('[CloudSync] 云端无数据');
                this._updateSyncStatus(localHasData ? 'local_only' : 'empty');
                return;
            }

            if (!localHasData) {
                console.log('[CloudSync] 本地无数据，从云端恢复...');
                await this._doDownload();
            } else {
                // 检查云端是否更新
                const localMeta = await this._getLocalMeta();
                let cloudNewer = false;
                for (const storeName of this.SYNC_STORES) {
                    const doc = await this.firestore.doc(this._storeDocPath(storeName)).get();
                    if (doc.exists) {
                        const t = doc.data().updatedAt;
                        const ct = t && t.toMillis ? t.toMillis() : 0;
                        if (ct > (localMeta.lastSyncTime || 0)) { cloudNewer = true; break; }
                    }
                }
                if (cloudNewer) {
                    console.log('[CloudSync] 云端数据更新，同步...');
                    await this._doDownload();
                } else {
                    console.log('[CloudSync] 本地已是最新');
                }
            }
            this._updateSyncStatus('synced');
        } catch (e) {
            console.error('[CloudSync] 恢复检查失败:', e);
            this._updateSyncStatus('error');
        }
    },

    async _doDownload() {
        if (this.isDownloading) return;
        this.isDownloading = true;
        this._updateSyncStatus('downloading');

        try {
            const startTime = Date.now();
            let storeCount = 0;

            for (const storeName of this.SYNC_STORES) {
                const metaPath = this._storeDocPath(storeName);
                const metaDoc = await this.firestore.doc(metaPath).get();
                if (!metaDoc.exists) continue;

                const meta = metaDoc.data();
                const chunkCount = meta.chunkCount || 0;
                if (chunkCount === 0) continue;

                // 分批并行下载分片
                const chunks = [];
                const BATCH = 3;
                for (let i = 0; i < chunkCount; i += BATCH) {
                    const end = Math.min(i + BATCH, chunkCount);
                    const batchP = [];
                    for (let j = i; j < end; j++) {
                        batchP.push(this._fetchStoreChunk(storeName, j));
                    }
                    const results = await Promise.all(batchP);
                    chunks.push(...results);
                    if (end < chunkCount) await this._delay(500);
                }

                // 合并
                let totalLen = 0;
                for (const c of chunks) totalLen += c.length;
                const merged = new Uint8Array(totalLen);
                let off = 0;
                for (const c of chunks) { merged.set(c, off); off += c.length; }

                // 解压 + 写入
                const decompressed = pako.ungzip(merged, { to: 'string' });
                const records = JSON.parse(decompressed);

                await dbManager.clear(storeName);
                const writes = records.map(r => dbManager.set(storeName, r));
                await Promise.all(writes);

                storeCount++;
                console.log('[CloudSync] ' + storeName + ': ' + records.length + ' 条已恢复');
            }

            if (typeof loadData === 'function') await loadData();
            await this._updateLocalMeta();

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log('[CloudSync] 下载完成: ' + storeCount + ' 个 store (' + elapsed + 's)');

            if (typeof updateFriendList === 'function') updateFriendList();
            if (typeof updateHomeWidget === 'function') updateHomeWidget();
            if (typeof updateProfileDisplay === 'function') updateProfileDisplay();

            showToast('云端数据已恢复 (' + storeCount + '个store)');

        } catch (e) {
            console.error('[CloudSync] 下载失败:', e);
            throw e;
        } finally {
            this.isDownloading = false;
        }
    },

    async _fetchStoreChunk(storeName, index) {
        const doc = await this.firestore.doc(this._storeChunkPath(storeName, index)).get();
        if (!doc.exists) throw new Error(storeName + '/chunks/' + index + ' 不存在');
        return doc.data().data.toUint8Array();
    },

    // ==================== 手动上传备份文件 ====================
    async uploadBackupFile(file) {
        if (!this.enabled) {
            showToast('请先启用云同步');
            return;
        }
        if (this.isUploading) {
            showToast('上传进行中，请稍候');
            return;
        }
        this.isUploading = true;
        this._updateSyncStatus('uploading');
        showToast('正在处理备份文件...');

        try {
            const startTime = Date.now();

            // 读取文件内容
            let data;
            if (file.name && file.name.endsWith('.gz')) {
                // .json.gz 或 .gz 文件
                const buffer = await file.arrayBuffer();
                const uint8 = new Uint8Array(buffer);
                const decompressed = pako.ungzip(uint8, { to: 'string' });
                data = JSON.parse(decompressed);
            } else if (file.name && file.name.endsWith('.json')) {
                const text = await file.text();
                data = JSON.parse(text);
            } else {
                // 尝试当作 gz 处理
                try {
                    const buffer = await file.arrayBuffer();
                    const uint8 = new Uint8Array(buffer);
                    const decompressed = pako.ungzip(uint8, { to: 'string' });
                    data = JSON.parse(decompressed);
                } catch (e) {
                    throw new Error('无法识别的文件格式，请使用 .json.gz 或 .json 文件');
                }
            }

            console.log('[CloudSync] 备份文件已解析: ' + Object.keys(data).length + ' 个 store');

            let storeCount = 0;
            let totalRecords = 0;

            for (const storeName of this.SYNC_STORES) {
                const records = data[storeName];
                if (!records || records.length === 0) continue;

                const dataStr = JSON.stringify(records);
                const newHash = this._simpleHash(dataStr);
                const compressed = pako.gzip(dataStr);
                const chunks = this._splitBuffer(compressed);

                console.log('[CloudSync] 手动上传 ' + storeName + ': ' + records.length + '条 → ' + (compressed.length/1024).toFixed(0) + 'KB (' + chunks.length + '片)');

                for (let i = 0; i < chunks.length; i++) {
                    const blob = firebase.firestore.Blob.fromUint8Array(chunks[i]);
                    let ok = false;
                    for (let a = 1; a <= this.MAX_RETRIES; a++) {
                        try {
                            await this.firestore.doc(this._storeChunkPath(storeName, i)).set({ data: blob, idx: i, hash: newHash });
                            ok = true; break;
                        } catch (e) {
                            if (a < this.MAX_RETRIES) {
                                console.warn('[CloudSync] ' + storeName + ' 分片' + (i+1) + ' 重试 ' + a);
                                await this._delay(this.CHUNK_DELAY * a);
                            } else throw e;
                        }
                    }
                    if (i < chunks.length - 1) await this._delay(this.CHUNK_DELAY);
                }

                await this.firestore.doc(this._storeDocPath(storeName)).set({
                    hash: newHash,
                    recordCount: records.length,
                    size: dataStr.length,
                    compressedSize: compressed.length,
                    chunkCount: chunks.length,
                    updatedAt: firebase.firestore.FieldValue.serverTimestamp()
                });

                // 等待写入队列清空，防止下个 store 触发限流
                await this.firestore.waitForPendingWrites();

                storeCount++;
                totalRecords += records.length;
            }

            const elapsed = ((Date.now() - startTime) / 1000).toFixed(1);
            console.log('[CloudSync] 备份上传完成: ' + storeCount + ' 个 store, ' + totalRecords + ' 条, ' + elapsed + 's');
            showToast('备份上传完成 (' + storeCount + '个store, ' + totalRecords + '条)');
            this._updateSyncStatus('synced');

        } catch (e) {
            console.error('[CloudSync] 备份上传失败:', e);
            showToast('备份上传失败: ' + e.message);
            this._updateSyncStatus('error');
        } finally {
            this.isUploading = false;
        }
    },

    // ==================== 手动同步 ====================
    async manualSync() {
        if (!this.enabled) { showToast('云同步未启用'); return; }
        if (this.isUploading || this.isDownloading) { showToast('同步进行中'); return; }
        showToast('正在同步...');
        try {
            await this._doUpload();
            await this._checkAndRestore();
            showToast('同步完成');
        } catch (e) { showToast('同步失败: ' + e.message); }
    },

    // ==================== 本地操作 ====================
    async _localHasData() {
        try {
            const f = await dbManager.getAll('friends');
            const c = await dbManager.getAll('chatHistories');
            return (f && f.length > 0) || (c && c.length > 0);
        } catch(e) { return false; }
    },
    async _getLocalMeta() {
        try { const r = localStorage.getItem('jrsy_sync_meta'); return r ? JSON.parse(r) : { lastSyncTime: 0 }; }
        catch(e) { return { lastSyncTime: 0 }; }
    },
    async _updateLocalMeta() {
        localStorage.setItem('jrsy_sync_meta', JSON.stringify({ lastSyncTime: Date.now() }));
    },

    // ==================== 清空云端 ====================
    async clearCloudData() {
        if (!this.enabled) return;
        try {
            for (const storeName of this.SYNC_STORES) {
                // 读取 meta 获取分片数
                const metaDoc = await this.firestore.doc(this._storeDocPath(storeName)).get();
                if (metaDoc.exists) {
                    const cc = metaDoc.data().chunkCount || 0;
                    for (let i = 0; i < cc; i++) {
                        try { await this.firestore.doc(this._storeChunkPath(storeName, i)).delete(); } catch(e) {}
                    }
                    await this.firestore.doc(this._storeDocPath(storeName)).delete();
                }
            }
            console.log('[CloudSync] 云端已清空');
        } catch(e) { console.error('[CloudSync] 清空失败:', e); }
    },

    // ==================== UI ====================
    _injectSyncUI() {
        if (document.getElementById('cloudsync-style')) return;
        const s = document.createElement('style');
        s.id = 'cloudsync-style';
        s.textContent = `
            #cloudsync-indicator{position:fixed;top:36px;right:8px;z-index:9999;display:flex;align-items:center;gap:4px;font-size:10px;padding:4px 8px;border-radius:10px;background:rgba(0,0,0,0.6);color:#fff;pointer-events:none;opacity:0;transition:opacity 0.3s;white-space:nowrap}
            #cloudsync-indicator.visible{opacity:1}
            #cloudsync-indicator.synced{background:rgba(52,199,89,0.8)}
            #cloudsync-indicator.syncing{background:rgba(0,122,255,0.8)}
            #cloudsync-indicator.error{background:rgba(255,59,48,0.8)}
            #cloudsync-indicator .dot{width:6px;height:6px;border-radius:50%;background:#fff;display:inline-block}
            #cloudsync-indicator.syncing .dot{animation:cs-pulse 0.8s infinite}
            @keyframes cs-pulse{0%,100%{opacity:1}50%{opacity:.3}}
        `;
        document.head.appendChild(s);
        const d = document.createElement('div');
        d.id = 'cloudsync-indicator';
        d.innerHTML = '<span class="dot"></span> <span class="txt">云同步</span>';
        document.body.appendChild(d);
    },

    _updateSyncStatus(status) {
        const el = document.getElementById('cloudsync-indicator');
        if (!el) return;
        const txt = el.querySelector('.txt');
        const statusText = {
            synced: '已同步', uploading: '上传中...', downloading: '下载中...',
            error: '同步失败', local_only: '仅本地', empty: ''
        };
        el.className = 'visible ' + status;
        if (txt) txt.textContent = statusText[status] || '';
        if (status === 'synced' || status === 'local_only') setTimeout(() => el.classList.remove('visible'), 3000);
        if (status === 'error') setTimeout(() => el.classList.remove('visible'), 5000);
    }
};

window.CloudSync = CloudSync;
console.log('[jrsy-sync.js] 云同步模块已加载 (v3: 按 store 增量同步)');
