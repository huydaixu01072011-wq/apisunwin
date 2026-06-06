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
const MAX_HISTORY = 150;

// --- BIẾN TRẠNG THÁI ---
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;
const sessionHistory = [];
let lastProcessedSid = null; // Chống trùng lặp

let predictionStats = { total: 0, correct: 0, wrong: 0, history: [] };
let pendingPrediction = null; // { duDoan, cau, chienLuoc }

// --- HIỆU SUẤT TỪNG CHIẾN LƯỢC (AI ONLINE LEARNING) ---
const strategyPerformance = {
    'Bệt':      { correct: 0, total: 0 },
    '1-1':      { correct: 0, total: 0 },
    'Markov2':  { correct: 0, total: 0 },
    'Markov3':  { correct: 0, total: 0 },
    'Xu hướng':{ correct: 0, total: 0 }
};
let nextStrategyPredictions = {}; // Lưu dự đoán tạm của từng chiến lược cho phiên vừa kết thúc

let ws = null;
let pingInterval = null;
let heartbeatTimeout = null;

const initialMessages = [
    [1, "MiniGame", "GM_fbbdbebndbbc", "123123p", {
        "info": "{\"ipAddress\":\"2402:800:62cd:cb7c:1a7:7a52:9c3e:c290\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJuZG5lYmViYnMiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMTIxMDczMTUsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJ0aW1lc3RhbXAiOjE3NTQ5MjYxMDI1MjcsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjpmYWxzZSwiaXBBZGRyZXNzIjoiMjQwMjo4MDA6NjJjZDpjYjdjOjFhNzo3YTUyOjljM2U6YzI5MCIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMDEucG5nIiwicGxhdGZvcm1JZCI6NSwidXNlcklkIjoiN2RhNDlhNDQtMjlhYS00ZmRiLWJkNGMtNjU5OTQ5YzU3NDdkIiwicmVnVGltZSI6MTc1NDkyNjAyMjUxNSwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJHTV9mYmJkYmVibmRiYmMifQ.DAyEeoAnz8we-Qd0xS0tnqOZ8idkUJkxksBjr_Gei8A\",\"locale\":\"vi\",\"userId\":\"7da49a44-29aa-4fdb-bd4c-659949c5747d\",\"username\":\"GM_fbbdbebndbbc\",\"timestamp\":1754926102527,\"refreshToken\":\"7cc4ad191f4348849f69427a366ea0fd.a68ece9aa85842c7ba523170d0a4ae3e\"}",
        "signature": "53D9E12F910044B140A2EC659167512E2329502FE84A6744F1CD5CBA9B6EC04915673F2CBAE043C4EDB94DDF88F3D3E839A931100845B8F179106E1F44ECBB4253EC536610CCBD0CE90BD8495DAC3E8A9DBDB46FE49B51E88569A6F117F8336AC7ADC226B4F213ECE2F8E0996F2DD5515476C8275F0B2406CDF2987F38A6DA24"
    }],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

// ============= THUẬT TOÁN NHẬN DIỆN CẦU =============
function detectPattern(history) {
    if (history.length < 4) return 'Đang thu thập...';
    const res = history.map(x => x.ket_qua);
    const last4 = res.slice(-4);
    if (last4.every(v => v === last4[0])) return `🔥 Bệt ${last4[0]} (dài ≥4)`;
    if (last4[0] !== last4[1] && last4[1] !== last4[2] && last4[2] !== last4[3] && last4[0] === last4[2] && last4[1] === last4[3])
        return `🔁 Cầu 1-1 (${last4[3]} → ${last4[3]==='Tài'?'Xỉu':'Tài'})`;
    if (last4[0] === last4[1] && last4[2] === last4[3] && last4[0] !== last4[2])
        return `🔹 Cầu 2-2 (hiện ${last4[3]})`;
    return 'Không rõ cầu đặc biệt';
}

// ============= CÁC CHIẾN LƯỢC DỰ ĐOÁN =============
function predictBet(history) {
    if (history.length < 4) return null;
    const last4 = history.slice(-4).map(x => x.ket_qua);
    return last4.every(v => v === last4[0]) ? last4[0] : null;
}

function predictOneOne(history) {
    if (history.length < 4) return null;
    const last4 = history.slice(-4).map(x => x.ket_qua);
    if (last4[0] !== last4[1] && last4[1] !== last4[2] && last4[2] !== last4[3] && last4[0] === last4[2] && last4[1] === last4[3])
        return last4[3] === 'Tài' ? 'Xỉu' : 'Tài';
    return null;
}

function predictMarkov(history, order = 3) {
    if (history.length < order) return null;
    const results = history.map(x => x.ket_qua);
    const recentState = results.slice(-order).join(',');
    let taiCount = 0, xiuCount = 0;
    for (let i = 0; i < history.length - order; i++) {
        if (results.slice(i, i + order).join(',') === recentState) {
            const next = results[i + order];
            next === 'Tài' ? taiCount++ : xiuCount++;
        }
    }
    if (taiCount + xiuCount === 0) return null;
    return taiCount >= xiuCount ? 'Tài' : 'Xỉu';
}

function predictTrend(history) {
    const recent20 = history.slice(-20).map(x => x.ket_qua);
    if (recent20.length === 0) return null;
    const tai = recent20.filter(r => r === 'Tài').length;
    return tai > recent20.length / 2 ? 'Tài' : 'Xỉu';
}

// ============= TỔNG HỢP AI (BỎ PHIẾU CÓ TRỌNG SỐ) =============
function predictSuperAI(history) {
    const strategies = {
        'Bệt': predictBet(history),
        '1-1': predictOneOne(history),
        'Markov2': predictMarkov(history, 2),
        'Markov3': predictMarkov(history, 3),
        'Xu hướng': predictTrend(history)
    };

    // Lưu dự đoán của từng chiến lược cho phiên này để học sau
    nextStrategyPredictions = {};
    for (const [name, pred] of Object.entries(strategies)) {
        if (pred) nextStrategyPredictions[name] = pred;
    }

    // Tính trọng số (tỉ lệ đúng), mặc định 0.5 nếu chưa có dữ liệu
    const weights = {};
    let totalWeight = 0;
    for (const [name, perf] of Object.entries(strategyPerformance)) {
        weights[name] = perf.total > 0 ? perf.correct / perf.total : 0.5;
        totalWeight += weights[name];
    }

    let taiScore = 0, xiuScore = 0;
    for (const [name, pred] of Object.entries(strategies)) {
        if (pred) {
            const w = weights[name] || 0.5;
            pred === 'Tài' ? taiScore += w : xiuScore += w;
        }
    }

    const maxScore = Math.max(taiScore, xiuScore);
    const doTinCay = maxScore > 0 ? Math.round((maxScore / totalWeight) * 100) : 50;
    const duDoan = taiScore >= xiuScore ? 'Tài' : 'Xỉu';

    // Xác định cầu hiện tại
    const cau = detectPattern(history);

    // Xác định chiến lược quyết định (có trọng số cao nhất trong các chiến lược dự đoán đúng)
    const bestStrategy = Object.entries(strategies)
        .filter(([n, p]) => p === duDoan && weights[n])
        .sort((a, b) => weights[b[0]] - weights[a[0]])[0];
    const chienLuoc = bestStrategy ? bestStrategy[0] : 'Default';

    return { duDoan, doTinCay, cau, chienLuoc };
}

// ============= WEBSOCKET CONNECTION =============
function heartbeat() {
    clearTimeout(heartbeatTimeout);
    heartbeatTimeout = setTimeout(() => {
        console.log('[⚠️] Không nhận được phản hồi (Treo), khởi động lại...');
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
        console.log('[✅] WebSocket đã kết nối thành công.');
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

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;

                // Chống trùng lặp phiên
                if (sid && sid === lastProcessedSid) return;

                // Xác định số phiên (ưu tiên sid từ server)
                let phien;
                if (sid !== undefined && sid !== null) {
                    phien = typeof sid === 'string' ? parseInt(sid) : sid;
                    if (isNaN(phien)) phien = sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien + 1 : Date.now();
                } else {
                    phien = sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien + 1 : Date.now();
                }
                lastProcessedSid = sid; // Lưu để chống trùng

                const total = d1 + d2 + d3;
                const resultText = (total > 10) ? "Tài" : "Xỉu";

                const sessionData = { phien, xuc_xac_1: d1, xuc_xac_2: d2, xuc_xac_3: d3, tong: total, ket_qua: resultText };

                // Lưu vào lịch sử (tránh trùng nếu lỗi)
                if (sessionHistory.length === 0 || sessionHistory[sessionHistory.length - 1].phien !== phien) {
                    sessionHistory.push(sessionData);
                    if (sessionHistory.length > MAX_HISTORY) sessionHistory.shift();
                }

                // --- KIỂM TRA DỰ ĐOÁN TRƯỚC ĐÓ ---
                if (pendingPrediction) {
                    predictionStats.total++;
                    const isCorrect = pendingPrediction.duDoan === resultText;
                    if (isCorrect) predictionStats.correct++;
                    else predictionStats.wrong++;

                    predictionStats.history.unshift({
                        phien: phien,
                        du_doan: pendingPrediction.duDoan,
                        thuc_te: resultText,
                        trang_thai: isCorrect ? "✅ ĐÚNG" : "❌ SAI",
                        cau: pendingPrediction.cau || detectPattern(sessionHistory.slice(0, -1)), // cầu trước đó
                        chien_luoc: pendingPrediction.chienLuoc || '--'
                    });
                    if (predictionStats.history.length > 50) predictionStats.history.pop();

                    // Cập nhật hiệu suất chiến lược
                    for (const [strategyName, pred] of Object.entries(nextStrategyPredictions)) {
                        if (strategyPerformance[strategyName]) {
                            strategyPerformance[strategyName].total++;
                            if (pred === resultText) strategyPerformance[strategyName].correct++;
                        }
                    }
                    nextStrategyPredictions = {};
                }

                // --- DỰ ĐOÁN MỚI ---
                const aiPrediction = predictSuperAI(sessionHistory);
                pendingPrediction = {
                    duDoan: aiPrediction.duDoan,
                    cau: aiPrediction.cau,
                    chienLuoc: aiPrediction.chienLuoc
                };

                // Cập nhật dữ liệu JSON API
                apiResponseData = {
                    ...sessionData,
                    lich_su_phien: sessionHistory
                };

                predictData = {
                    phien: phien,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: resultText,
                    phien_tiep_theo: phien + 1, // Dự đoán cho phiên kế tiếp (chỉ mang tính tương đối)
                    du_doan: aiPrediction.duDoan,
                    do_tin_cay: `${aiPrediction.doTinCay}%`,
                    cau_hien_tai: aiPrediction.cau,
                    chien_luoc_ai: aiPrediction.chienLuoc
                };

                console.log(`[🎲] Phiên ${phien}: ${total} (${resultText}) | AI dự đoán: ${aiPrediction.duDoan} (${aiPrediction.doTinCay}%) | Cầu: ${aiPrediction.cau} | Chiến lược: ${aiPrediction.chienLuoc}`);
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý:', e.message);
        }
    });

    ws.on('close', () => {
        console.log(`[🔌] Mất kết nối. Đang nối lại...`);
        clearTimeout(heartbeatTimeout);
        clearInterval(pingInterval);
        setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', () => {
        ws.terminate();
    });
}

// --- REST API ROUTES ---
app.get('/sunlon', (req, res) => res.json(apiResponseData));
app.get('/predict', (req, res) => predictData ? res.json(predictData) : res.json({ error: "Chưa đủ dữ liệu" }));
app.get('/ai-stats', (req, res) => res.json({ performance: strategyPerformance, pending: pendingPrediction }));

// --- GIAO DIỆN /STATUS SIÊU VIP ---
app.get('/status', (req, res) => {
    const winRate = predictionStats.total > 0 ? Math.round((predictionStats.correct / predictionStats.total) * 100) : 0;

    let perfRows = '';
    for (const [name, perf] of Object.entries(strategyPerformance)) {
        const acc = perf.total > 0 ? Math.round((perf.correct / perf.total) * 100) : '--';
        perfRows += `<tr><td>${name}</td><td>${perf.total}</td><td>${perf.correct}</td><td>${acc}%</td></tr>`;
    }

    let tableRows = predictionStats.history.map(item => `
        <tr>
            <td>#${item.phien}</td>
            <td class="${item.du_doan === 'Tài' ? 'tai' : 'xiu'}">${item.du_doan}</td>
            <td class="${item.thuc_te === 'Tài' ? 'tai' : 'xiu'}">${item.thuc_te}</td>
            <td class="status ${item.trang_thai.includes('ĐÚNG') ? 'correct' : 'wrong'}">${item.trang_thai}</td>
            <td>${item.cau || '--'}</td>
            <td>${item.chien_luoc || '--'}</td>
        </tr>
    `).join('');

    if (predictionStats.history.length === 0) {
        tableRows = `<tr><td colspan="6" style="text-align:center; padding: 20px;">Hệ thống đang thu thập dữ liệu... Vui lòng chờ vài phiên.</td></tr>`;
    }

    const currentCau = predictData ? predictData.cau_hien_tai : '...';
    const nextPred = predictData ? predictData.du_doan : '...';
    const confidence = predictData ? predictData.do_tin_cay : '...';
    const strategyUsed = predictData ? predictData.chien_luoc_ai : '...';

    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>AI Siêu VIP - Sun.Win</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
            body { background-color: #0d1117; color: #e6edf3; padding: 20px; }
            .container { max-width: 1200px; margin: 0 auto; }
            h1 { text-align: center; color: #f78166; margin-bottom: 10px; }
            .subtitle { text-align: center; color: #8b949e; margin-bottom: 30px; font-size: 14px; }
            .ai-panel { background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 20px; margin-bottom: 25px; display: flex; flex-wrap: wrap; gap: 20px; justify-content: space-around; align-items: center; }
            .ai-item { text-align: center; }
            .ai-label { font-size: 13px; color: #8b949e; margin-bottom: 5px; }
            .ai-value { font-size: 24px; font-weight: bold; }
            .tai { color: #58a6ff; }
            .xiu { color: #f85149; }
            .stats-cards { display: flex; gap: 15px; margin-bottom: 30px; }
            .card { background: #161b22; padding: 20px; border-radius: 10px; flex: 1; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.2); border: 1px solid #30363d; }
            .card h3 { font-size: 14px; color: #8b949e; margin-bottom: 10px; }
            .card .value { font-size: 28px; font-weight: bold; }
            .text-green { color: #3fb950; }
            .text-red { color: #f85149; }
            .text-blue { color: #58a6ff; }
            .text-gold { color: #d2991d; }
            table { width: 100%; border-collapse: collapse; background: #161b22; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.2); border: 1px solid #30363d; }
            thead { background: #1c2128; }
            th, td { padding: 15px; text-align: center; border-bottom: 1px solid #30363d; font-size: 14px; }
            th { color: #f78166; text-transform: uppercase; }
            tr:hover { background: #1c2128; }
            .status.correct { color: #3fb950; font-weight: bold; background: rgba(63, 185, 80, 0.1); border-radius: 5px; padding: 5px; }
            .status.wrong { color: #f85149; font-weight: bold; background: rgba(248, 81, 73, 0.1); border-radius: 5px; padding: 5px; }
            .auto-refresh { text-align: center; margin-top: 20px; font-size: 12px; color: #8b949e; }
            .perf-table { margin-top: 30px; }
        </style>
        <script>
            setInterval(() => window.location.reload(), 10000);
        </script>
    </head>
    <body>
        <div class="container">
            <h1>🧠 AI SIÊU VIP DỰ ĐOÁN TÀI XỈU</h1>
            <div class="subtitle">Tự học online – Tỉ lệ thắng tối ưu</div>

            <div class="ai-panel">
                <div class="ai-item">
                    <div class="ai-label">Cầu hiện tại</div>
                    <div class="ai-value" style="font-size:20px;">${currentCau}</div>
                </div>
                <div class="ai-item">
                    <div class="ai-label">Dự đoán phiên tiếp</div>
                    <div class="ai-value ${nextPred === 'Tài' ? 'tai' : 'xiu'}">${nextPred}</div>
                </div>
                <div class="ai-item">
                    <div class="ai-label">Độ tin cậy</div>
                    <div class="ai-value text-gold">${confidence}</div>
                </div>
                <div class="ai-item">
                    <div class="ai-label">Chiến lược AI</div>
                    <div class="ai-value" style="font-size:18px; color:#c9d1d9;">${strategyUsed}</div>
                </div>
            </div>

            <div class="stats-cards">
                <div class="card">
                    <h3>Tổng Dự Đoán</h3>
                    <div class="value text-blue">${predictionStats.total}</div>
                </div>
                <div class="card">
                    <h3>Đúng</h3>
                    <div class="value text-green">${predictionStats.correct}</div>
                </div>
                <div class="card">
                    <h3>Sai</h3>
                    <div class="value text-red">${predictionStats.wrong}</div>
                </div>
                <div class="card">
                    <h3>Tỉ lệ Thắng</h3>
                    <div class="value text-gold">${winRate}%</div>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Phiên</th>
                        <th>Dự Đoán</th>
                        <th>Kết Quả</th>
                        <th>Trạng Thái</th>
                        <th>Cầu</th>
                        <th>Chiến lược</th>
                    </tr>
                </thead>
                <tbody>${tableRows}</tbody>
            </table>

            <div class="perf-table">
                <h2 style="color:#f78166; margin: 30px 0 15px;">📊 Hiệu suất từng chiến lược</h2>
                <table>
                    <thead>
                        <tr><th>Chiến lược</th><th>Số lần dùng</th><th>Đúng</th><th>Độ chính xác</th></tr>
                    </thead>
                    <tbody>${perfRows}</tbody>
                </table>
            </div>

            <div class="auto-refresh">🔄 Tự động cập nhật mỗi 10 giây</div>
        </div>
    </body>
    </html>
    `;
    res.send(html);
});

app.get('/', (req, res) => res.send(`<p>Vào <a href="/status">/status</a> để xem giao diện AI.</p>`));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server chạy tại cổng ${PORT}`);
    connectWebSocket();
});