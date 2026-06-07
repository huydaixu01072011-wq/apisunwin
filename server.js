const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');

const app = express();
app.use(cors());
app.set('etag', false);
app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    next();
});

const PORT = 5000;

// --- CẤU HÌNH WEBSOCKET ---
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win"
};

const RECONNECT_DELAY = 1500;
const PING_INTERVAL = 10000;
const MAX_HISTORY = 200; // Lưu đủ dữ liệu cho AI học

// --- BIẾN TRẠNG THÁI ---
let currentSession = null;       // Phiên vừa quay (object)
let sessionHistory = [];         // Mảng các phiên đã hoàn thành (không bao gồm current)
let predictData = null;
let pendingPrediction = null;

let predictionStats = { total: 0, correct: 0, wrong: 0, history: [] };

// AI tự học: lưu tần suất pattern 4 kết quả -> kết quả tiếp theo
const aiModel = {};             // key: "T,X,T,X" -> { T: count, X: count }
const AI_PATTERN_LENGTH = 4;    // Độ dài mẫu để học

let ws = null;
let pingInterval = null;
let heartbeatTimeout = null;

// Tin nhắn khởi tạo (giữ nguyên)
const initialMessages = [
    [1, "MiniGame", "GM_fbbdbebndbbc", "123123p", {
        "info": "{\"ipAddress\":\"2402:800:62cd:cb7c:1a7:7a52:9c3e:c290\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJuZG5lYmViYnMiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMTIxMDczMTUsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJ0aW1lc3RhbXAiOjE3NTQ5MjYxMDI1MjcsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjpmYWxzZSwiaXBBZGRyZXNzIjoiMjQwMjo4MDA6NjJjZDpjYjdjOjFhNzo3YTUyOjljM2U6YzI5MCIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMDEucG5nIiwicGxhdGZvcm1JZCI6NSwidXNlcklkIjoiN2RhNDlhNDQtMjlhYS00ZmRiLWJkNGMtNjU5OTQ5YzU3NDdkIiwicmVnVGltZSI6MTc1NDkyNjAyMjUxNSwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJHTV9mYmJkYmVibmRiYmMifQ.DAyEeoAnz8we-Qd0xS0tnqOZ8idkUJkxksBjr_Gei8A\",\"locale\":\"vi\",\"userId\":\"7da49a44-29aa-4fdb-bd4c-659949c5747d\",\"username\":\"GM_fbbdbebndbbc\",\"timestamp\":1754926102527,\"refreshToken\":\"7cc4ad191f4348849f69427a366ea0fd.a68ece9aa85842c7ba523170d0a4ae3e\"}",
        "signature": "53D9E12F910044B140A2EC659167512E2329502FE84A6744F1CD5CBA9B6EC04915673F2CBAE043C4EDB94DDF88F3D3E839A931100845B8F179106E1F44ECBB4253EC536610CCBD0CE90BD8495DAC3E8A9DBDB46FE49B51E88569A6F117F8336AC7ADC226B4F213ECE2F8E0996F2DD5515476C8275F0B2406CDF2987F38A6DA24"
    }],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

// ==================== AI TỰ HỌC ====================
function learnFromResult(patternKey, nextResult) {
    if (!aiModel[patternKey]) {
        aiModel[patternKey] = { 'Tài': 0, 'Xỉu': 0 };
    }
    aiModel[patternKey][nextResult]++;
}

function getAIPrediction(patternKey) {
    const stats = aiModel[patternKey];
    if (!stats) return null;
    const total = stats['Tài'] + stats['Xỉu'];
    if (total < 5) return null; // cần ít nhất 5 mẫu để tin cậy
    const probTai = stats['Tài'] / total;
    const probXiu = stats['Xỉu'] / total;
    const do_tin_cay = Math.round(Math.max(probTai, probXiu) * 100);
    return {
        du_doan: probTai >= probXiu ? 'Tài' : 'Xỉu',
        do_tin_cay: do_tin_cay
    };
}

// Cập nhật model AI khi có phiên mới
function updateAIModel() {
    if (sessionHistory.length >= AI_PATTERN_LENGTH + 1) {
        // Lấy pattern từ (độ dài pattern) phiên cuối cùng của history và kết quả tiếp theo (current)
        const patternSlice = sessionHistory.slice(-AI_PATTERN_LENGTH);
        const patternKey = patternSlice.map(s => s.ket_qua).join(',');
        // currentSession là phiên vừa xảy ra (kết quả đã biết)
        if (currentSession) {
            learnFromResult(patternKey, currentSession.ket_qua);
        }
    }
}

// ==================== DỰ ĐOÁN VIP PRO (AI + Rule‑based) ====================
function predictNextVIPPro() {
    // Cần ít nhất 2 phiên lịch sử để phân tích
    if (sessionHistory.length < 2) {
        return { du_doan: "Chờ dữ liệu", do_tin_cay: 50 };
    }

    // 1. Lấy pattern 4 phiên gần nhất (từ history + current nếu cần)
    let recentResults = [];
    // Dùng sessionHistory + currentSession để có bức tranh đầy đủ
    const allSessions = [...sessionHistory];
    if (currentSession) {
        allSessions.push(currentSession);
    }
    if (allSessions.length >= AI_PATTERN_LENGTH) {
        const patternSlice = allSessions.slice(-AI_PATTERN_LENGTH);
        const patternKey = patternSlice.map(s => s.ket_qua).join(',');
        const aiPred = getAIPrediction(patternKey);
        if (aiPred) {
            return { du_doan: aiPred.du_doan, do_tin_cay: aiPred.do_tin_cay, source: 'AI' };
        }
    }

    // 2. Fallback: Rule‑based engine (phiên bản nâng cao)
    const results = allSessions.map(s => s.ket_qua);
    const len = results.length;
    const last1 = results[len - 1];
    const last2 = results[len - 2];
    const last3 = len >= 3 ? results[len - 3] : null;
    const last4 = len >= 4 ? results[len - 4] : null;

    // a) Cầu bệt (>= 3 cùng loại)
    if (last1 === last2 && last2 === last3 && last1 === last4) {
        return { du_doan: last1, do_tin_cay: 82, source: 'Bệt dài' };
    }
    if (last1 === last2 && last2 === last3) {
        return { du_doan: last1, do_tin_cay: 75, source: 'Bệt 3' };
    }

    // b) Cầu 1-1 (so le)
    if (last1 !== last2 && last2 !== last3 && last3 !== last4 && last1 === last3) {
        return { du_doan: last1 === 'Tài' ? 'Xỉu' : 'Tài', do_tin_cay: 72, source: 'Cầu 1-1' };
    }

    // c) Pattern matching 3 gần nhất (tìm trong quá khứ)
    if (len >= 6) {
        const recentPattern = results.slice(-3).join(',');
        let countT = 0, countX = 0;
        for (let i = 0; i < len - 3; i++) {
            if (results.slice(i, i + 3).join(',') === recentPattern) {
                const next = results[i + 3];
                if (next === 'Tài') countT++;
                else countX++;
            }
        }
        const total = countT + countX;
        if (total >= 3) {
            const du_doan = countT > countX ? 'Tài' : 'Xỉu';
            const do_tin_cay = Math.round((Math.max(countT, countX) / total) * 100);
            return { du_doan, do_tin_cay: Math.min(do_tin_cay, 85), source: 'Pattern 3' };
        }
    }

    // d) Phân tích xu hướng 10 phiên gần nhất
    const recent10 = results.slice(-10);
    const taiCount = recent10.filter(r => r === 'Tài').length;
    return {
        du_doan: taiCount >= 5 ? 'Tài' : 'Xỉu',
        do_tin_cay: 55 + Math.abs(taiCount - 5) * 2,
        source: 'Xu hướng'
    };
}

// ==================== CẬP NHẬT SAU MỖI PHIÊN MỚI ====================
function processNewSession(session) {
    // Tránh trùng lặp tuyệt đối
    if (currentSession && currentSession.phien === session.phien) return;

    // Đẩy currentSession cũ vào history nếu có
    if (currentSession) {
        // Chỉ thêm nếu chưa có trong history
        if (!sessionHistory.some(s => s.phien === currentSession.phien)) {
            sessionHistory.push(currentSession);
        }
    }

    // Cập nhật currentSession mới
    currentSession = session;

    // Giới hạn lịch sử
    if (sessionHistory.length > MAX_HISTORY) {
        sessionHistory = sessionHistory.slice(-MAX_HISTORY);
    }

    // Sắp xếp lại lịch sử theo phien tăng dần (phòng dữ liệu đến lộn xộn)
    sessionHistory.sort((a, b) => a.phien - b.phien);

    // === KIỂM TRA DỰ ĐOÁN TRƯỚC ĐÓ ===
    if (pendingPrediction && pendingPrediction.phien === session.phien) {
        predictionStats.total++;
        const isCorrect = pendingPrediction.du_doan === session.ket_qua;
        if (isCorrect) predictionStats.correct++;
        else predictionStats.wrong++;

        predictionStats.history.unshift({
            phien: session.phien,
            du_doan: pendingPrediction.du_doan,
            thuc_te: session.ket_qua,
            trang_thai: isCorrect ? '✅ ĐÚNG' : '❌ SAI'
        });
        if (predictionStats.history.length > 50) predictionStats.history.pop();
    }

    // === HỌC AI từ dữ liệu mới ===
    updateAIModel();

    // === TẠO DỰ ĐOÁN MỚI ===
    // Dự đoán cho phiên tiếp theo: max phien trong (history + current) + 1
    const maxPhien = sessionHistory.length > 0
        ? Math.max(sessionHistory[sessionHistory.length - 1].phien, currentSession.phien)
        : currentSession.phien;
    const nextPhien = maxPhien + 1;

    const prediction = predictNextVIPPro();
    pendingPrediction = { phien: nextPhien, du_doan: prediction.du_doan };

    // Cập nhật dữ liệu API
    predictData = {
        phien: currentSession.phien,
        xuc_xac_1: currentSession.xuc_xac_1,
        xuc_xac_2: currentSession.xuc_xac_2,
        xuc_xac_3: currentSession.xuc_xac_3,
        tong: currentSession.tong,
        ket_qua: currentSession.ket_qua,
        phien_hien_tai: nextPhien,
        du_doan: prediction.du_doan,
        do_tin_cay: `${prediction.do_tin_cay}%`,
        nguon: prediction.source || 'Rule',
        ai_stats: aiModelStats() // thêm thông tin AI
    };

    console.log(`[🎲] Phiên ${currentSession.phien}: ${currentSession.ket_qua} | AI học: ${Object.keys(aiModel).length} mẫu | Dự đoán #${nextPhien}: ${prediction.du_doan} (${prediction.do_tin_cay}%) [${prediction.source}]`);
}

// Thống kê nhanh model AI
function aiModelStats() {
    const patterns = Object.keys(aiModel);
    const totalSamples = patterns.reduce((sum, p) => sum + aiModel[p]['Tài'] + aiModel[p]['Xỉu'], 0);
    return { patterns_loaded: patterns.length, total_observations: totalSamples };
}

// ==================== WEBSOCKET HANDLING ====================
function heartbeat() {
    clearTimeout(heartbeatTimeout);
    heartbeatTimeout = setTimeout(() => {
        console.log('[⚠️] Không nhận được pong – khởi động lại kết nối...');
        if (ws) ws.terminate();
    }, PING_INTERVAL + 5000);
}

function connectWebSocket() {
    if (ws) {
        ws.removeAllListeners();
        ws.terminate();
    }

    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        console.log('[✅] WebSocket đã kết nối.');
        heartbeat();
        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
            }, i * 600);
        });
        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) ws.ping();
        }, PING_INTERVAL);
    });

    ws.on('ping', heartbeat);
    ws.on('pong', heartbeat);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;

            const { cmd, d1, d2, d3 } = data[1];

            if (cmd === 1003 && d1 && d2 && d3) {
                const total = d1 + d2 + d3;
                const ketQua = total > 10 ? 'Tài' : 'Xỉu';

                // Lấy sid nếu có, nếu không thì tự sinh dựa trên session hiện tại
                let phien = data[1].sid;
                if (!phien) {
                    // fallback: nếu không có sid, dùng max hiện tại + 1
                    phien = currentSession ? currentSession.phien + 1 : Date.now();
                }

                const sessionData = {
                    phien: Number(phien),
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: ketQua
                };

                processNewSession(sessionData);
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý message:', e.message);
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`[🔌] Mất kết nối (code: ${code}). Tự động kết nối lại sau ${RECONNECT_DELAY}ms...`);
        clearTimeout(heartbeatTimeout);
        clearInterval(pingInterval);
        setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        console.error('[🚫] Lỗi WebSocket:', err.message);
        ws.terminate(); // sẽ kích hoạt close -> reconnect
    });
}

// ==================== REST API ====================
app.get('/sunlon', (req, res) => {
    res.json({
        phien_hien_tai: currentSession,
        lich_su_phien: sessionHistory
    });
});

app.get('/predict', (req, res) => {
    if (predictData) res.json(predictData);
    else res.json({ error: "Chưa có dữ liệu" });
});

// ==================== GIAO DIỆN /status NÂNG CAO ====================
app.get('/status', (req, res) => {
    const winRate = predictionStats.total > 0
        ? Math.round((predictionStats.correct / predictionStats.total) * 100)
        : 0;

    let tableRows = predictionStats.history.map(item => `
        <tr>
            <td>#${item.phien}</td>
            <td class="${item.du_doan === 'Tài' ? 'tai' : 'xiu'}">${item.du_doan}</td>
            <td class="${item.thuc_te === 'Tài' ? 'tai' : 'xiu'}">${item.thuc_te}</td>
            <td class="status ${item.trang_thai.includes('ĐÚNG') ? 'correct' : 'wrong'}">${item.trang_thai}</td>
        </tr>
    `).join('') || `<tr><td colspan="4" style="text-align:center; padding:20px;">Đang thu thập dữ liệu...</td></tr>`;

    // Hiển thị thông tin AI
    const aiInfo = aiModelStats();
    const aiPatterns = Object.entries(aiModel).slice(0, 10).map(([pattern, counts]) => {
        const total = counts['Tài'] + counts['Xỉu'];
        const pctTai = total ? Math.round(counts['Tài'] / total * 100) : 0;
        const pctXiu = total ? Math.round(counts['Xỉu'] / total * 100) : 0;
        return `<tr><td>${pattern}</td><td>${total}</td><td>${pctTai}%</td><td>${pctXiu}%</td></tr>`;
    }).join('');

    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>SunWin VIP PRO - AI Engine</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
            body { background: #0d1117; color: #c9d1d9; padding: 20px; }
            .container { max-width: 1000px; margin: 0 auto; }
            h1 { text-align: center; color: #58a6ff; margin-bottom: 20px; }
            .flex-row { display: flex; gap: 20px; flex-wrap: wrap; }
            .card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 20px; margin-bottom: 20px; }
            .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 15px; }
            .stat-box { background: #0d1117; border: 1px solid #30363d; border-radius: 8px; padding: 15px; text-align: center; }
            .stat-box .label { font-size: 13px; color: #8b949e; }
            .stat-box .value { font-size: 24px; font-weight: bold; }
            .green { color: #3fb950; }
            .red { color: #f85149; }
            .blue { color: #58a6ff; }
            .gold { color: #d2991d; }
            table { width: 100%; border-collapse: collapse; background: #0d1117; border: 1px solid #30363d; border-radius: 8px; overflow: hidden; }
            th, td { padding: 12px; text-align: center; border-bottom: 1px solid #21262d; }
            th { background: #161b22; color: #58a6ff; font-weight: 600; }
            tr:hover { background: #1c2128; }
            .tai { color: #58a6ff; font-weight: bold; }
            .xiu { color: #f85149; font-weight: bold; }
            .status.correct { color: #3fb950; background: rgba(63,185,80,0.1); border-radius: 4px; padding: 2px 6px; }
            .status.wrong { color: #f85149; background: rgba(248,81,73,0.1); border-radius: 4px; padding: 2px 6px; }
            .auto-refresh { text-align: center; margin-top: 15px; font-size: 13px; color: #8b949e; }
            @media (max-width: 600px) { .stats { grid-template-columns: 1fr 1fr; } }
        </style>
        <script>
            setInterval(() => window.location.reload(), 10000);
        </script>
    </head>
    <body>
        <div class="container">
            <h1>🎰 SUNWIN TÀI XỈU – AI PRO</h1>
            <div class="card">
                <div class="stats">
                    <div class="stat-box"><div class="label">Tổng dự đoán</div><div class="value blue">${predictionStats.total}</div></div>
                    <div class="stat-box"><div class="label">Đúng</div><div class="value green">${predictionStats.correct}</div></div>
                    <div class="stat-box"><div class="label">Sai</div><div class="value red">${predictionStats.wrong}</div></div>
                    <div class="stat-box"><div class="label">Tỉ lệ thắng</div><div class="value gold">${winRate}%</div></div>
                    <div class="stat-box"><div class="label">AI Patterns</div><div class="value blue">${aiInfo.patterns_loaded}</div></div>
                    <div class="stat-box"><div class="label">Mẫu đã học</div><div class="value">${aiInfo.total_observations}</div></div>
                </div>
            </div>

            <div class="flex-row">
                <div class="card" style="flex:2;">
                    <h3 style="margin-bottom: 15px;">📋 Lịch sử dự đoán gần đây</h3>
                    <table>
                        <thead><tr><th>Phiên</th><th>Dự đoán</th><th>Thực tế</th><th>Kết quả</th></tr></thead>
                        <tbody>${tableRows}</tbody>
                    </table>
                </div>
                <div class="card" style="flex:1;">
                    <h3 style="margin-bottom: 15px;">🧠 Mô hình AI (top 10)</h3>
                    <table>
                        <thead><tr><th>Pattern</th><th>Mẫu</th><th>Tài%</th><th>Xỉu%</th></tr></thead>
                        <tbody>${aiPatterns || '<tr><td colspan="4">Đang xây dựng...</td></tr>'}</tbody>
                    </table>
                </div>
            </div>

            <div class="auto-refresh">🔄 Tự động làm mới mỗi 10 giây</div>
        </div>
    </body>
    </html>
    `;
    res.send(html);
});

app.get('/', (req, res) => res.send(`<h2>SunWin AI Engine</h2><p><a href="/status">/status</a> | <a href="/predict">/predict</a> | <a href="/sunlon">/sunlon</a></p>`));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server VIP PRO đang chạy tại http://localhost:${PORT}`);
    connectWebSocket();
});