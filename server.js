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
const MAX_HISTORY = 1000; // Đã nâng lên thu thập 1000 phiên cho AI tự học

// --- BIẾN TRẠNG THÁI ---
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;
let latestSid = null;
const sessionHistory = []; // Lưu tối đa 1000 phiên

let predictionStats = { total: 0, correct: 0, wrong: 0, history: [] };
let pendingPrediction = null; 

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

// --- THUẬT TOÁN DỰ ĐOÁN SIÊU VIP AI (TỰ HỌC TỪ DATA) ---
function predictSuperVipAI(history) {
    // Chỉ cần 10 phiên là bắt đầu dự đoán
    if (history.length < 10) return { du_doan: `Chờ Data (${history.length}/10)`, do_tin_cay: 50 };

    const results = history.map(x => x.ket_qua);
    const last1 = results[results.length - 1];
    const last2 = results[results.length - 2];
    const last3 = results[results.length - 3];
    const last4 = results[results.length - 4];

    let scoreTai = 0;
    let scoreXiu = 0;

    // 1. TỰ HỌC TỪ QUÁ KHỨ (DYNAMIC MARKOV CHAIN TRÊN TOÀN BỘ DATA MỚI NHẤT)
    // Hệ thống sẽ quét toàn bộ mảng (lên tới 1000 phiên) để xem pattern 3 ván gần nhất thường dẫn đến kết quả gì
    const recentPattern = `${last3}-${last2}-${last1}`;
    let matchTai = 0;
    let matchXiu = 0;

    for (let i = 0; i < results.length - 3; i++) {
        const pattern = `${results[i]}-${results[i+1]}-${results[i+2]}`;
        if (pattern === recentPattern) {
            if (results[i+3] === 'Tài') matchTai++;
            if (results[i+3] === 'Xỉu') matchXiu++;
        }
    }

    // Tự động điều chỉnh trọng số dựa trên số lần thuật toán bắt gặp pattern trong 1000 phiên
    if (matchTai > matchXiu) {
        scoreTai += 30 + (matchTai * 2); // Càng lặp lại nhiều trong quá khứ, trọng số càng cao
    } else if (matchXiu > matchTai) {
        scoreXiu += 30 + (matchXiu * 2);
    }

    // 2. NHẬN DIỆN CẦU KINH ĐIỂN
    // Cầu bệt
    if (last1 === last2 && last2 === last3 && last3 === last4) {
        if (last1 === 'Tài') scoreTai += 35; 
        else scoreXiu += 35;
    }
    // Cầu 1-1
    if (last1 !== last2 && last2 !== last3 && last3 !== last4) {
        const nextExpected = last1 === 'Tài' ? 'Xỉu' : 'Tài';
        if (nextExpected === 'Tài') scoreTai += 30; 
        else scoreXiu += 30;
    }

    // 3. THUẬT TOÁN HỒI QUY VỀ MỨC TRUNG BÌNH (TỰ ĐIỀU CHỈNH THEO DATA)
    // Quét 30 ván gần nhất để tìm sự mất cân bằng
    const analysisRange = Math.min(results.length, 30);
    const historyToAnalyze = results.slice(-analysisRange);
    const countTai = historyToAnalyze.filter(r => r === 'Tài').length;
    const countXiu = historyToAnalyze.length - countTai;

    // Nếu một bên ra quá mức (hơn 65%), AI học được rằng nhịp gãy sắp đến
    const threshold = Math.floor(analysisRange * 0.65);
    if (countTai >= threshold) {
        scoreXiu += 20; 
    } else if (countXiu >= threshold) {
        scoreTai += 20; 
    }

    // --- TỔNG HỢP ---
    const totalScore = scoreTai + scoreXiu;
    
    // Nếu chưa có data pattern rõ ràng, bám theo xu hướng ván cuối
    if (totalScore === 0) {
        return { du_doan: last1, do_tin_cay: 51 };
    }

    const predicted = scoreTai > scoreXiu ? "Tài" : "Xỉu";
    
    // Tính toán tỷ lệ tự tin (Confidence Rate)
    const winRatio = Math.max(scoreTai, scoreXiu) / totalScore;
    let confidence = Math.round(50 + (winRatio * 45));
    
    // Chuẩn hóa độ tin cậy để nhìn thực tế (55% - 92%)
    if (confidence > 92) confidence = 92; 
    if (confidence < 55) confidence = 55;

    return {
        du_doan: predicted,
        do_tin_cay: confidence
    };
}

// --- QUẢN LÝ WEBSOCKET ---
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

            if (sid) {
                latestSid = sid; 
            }

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;

                let activeSession = latestSid || (sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien + 1 : Date.now());

                const total = d1 + d2 + d3;
                const resultText = (total > 10) ? "Tài" : "Xỉu";

                const sessionData = {
                    phien: activeSession,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: resultText
                };

                // Chống lưu trùng & Đẩy vào bộ nhớ tự học (Tối đa 1000 phiên)
                if (sessionHistory.length === 0 || sessionHistory[sessionHistory.length - 1].phien !== activeSession) {
                    sessionHistory.push(sessionData);
                    if (sessionHistory.length > MAX_HISTORY) sessionHistory.shift();
                }

                // KIỂM TRA DỰ ĐOÁN CŨ
                if (pendingPrediction && pendingPrediction.phien === activeSession) {
                    predictionStats.total++;
                    const isCorrect = pendingPrediction.du_doan === resultText;
                    if (isCorrect) predictionStats.correct++;
                    else predictionStats.wrong++;

                    predictionStats.history.unshift({
                        phien: activeSession,
                        du_doan: pendingPrediction.du_doan,
                        thuc_te: resultText,
                        trang_thai: isCorrect ? "✅ ĐÚNG" : "❌ SAI"
                    });

                    // Chỉ hiển thị 50 kết quả gần nhất trên UI để tránh lag
                    if (predictionStats.history.length > 50) predictionStats.history.pop(); 
                }

                // TẠO DỰ ĐOÁN MỚI BẰNG AI
                const nextPhien = activeSession + 1;
                const prediction = predictSuperVipAI(sessionHistory);
                
                // Tránh lưu prediction ảo nếu chưa đủ 10 phiên
                if (sessionHistory.length >= 10) {
                    pendingPrediction = { phien: nextPhien, du_doan: prediction.du_doan };
                }

                // CẬP NHẬT TRẠNG THÁI SERVER
                apiResponseData = {
                    ...sessionData,
                    lich_su_phien: sessionHistory
                };

                predictData = {
                    phien: activeSession,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: resultText,
                    phien_tiep_theo: nextPhien,
                    du_doan_ai: prediction.du_doan,
                    do_tin_cay: typeof prediction.do_tin_cay === 'number' ? `${prediction.do_tin_cay}%` : prediction.do_tin_cay,
                    tong_data_da_hoc: sessionHistory.length
                };
                
                console.log(`[🎲] Phiên ${activeSession}: ${total} (${resultText}) | AI Dự đoán tiếp: ${prediction.du_doan} (${predictData.do_tin_cay}) | Đã học: ${sessionHistory.length}/1000`);
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
app.get('/api/data', (req, res) => res.json(apiResponseData));
app.get('/predict', (req, res) => predictData ? res.json(predictData) : res.json({ error: "Chưa đủ dữ liệu khởi tạo" }));

// --- GIAO DIỆN HTML /STATUS ---
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
    `).join('');

    if(predictionStats.history.length === 0) {
        tableRows = `<tr><td colspan="4" style="text-align:center; padding: 20px;">AI đang thu thập dữ liệu tự học... Đã có (${sessionHistory.length}/10) phiên.</td></tr>`;
    }

    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Hệ Thống AI Tự Học - 1000 Phiên</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; }
            body { background-color: #121212; color: #ffffff; padding: 20px; }
            .container { max-width: 900px; margin: 0 auto; }
            h1 { text-align: center; color: #00ff88; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 1px; }
            .subtitle { text-align: center; color: #aaaaaa; margin-bottom: 30px; font-size: 14px; }
            .stats-cards { display: flex; gap: 15px; margin-bottom: 30px; flex-wrap: wrap; }
            .card { background: #1e1e1e; padding: 20px; border-radius: 10px; flex: 1; min-width: 150px; text-align: center; box-shadow: 0 4px 6px rgba(0,0,0,0.3); border: 1px solid #333; }
            .card h3 { font-size: 13px; color: #aaaaaa; margin-bottom: 10px; text-transform: uppercase; }
            .card .value { font-size: 26px; font-weight: bold; }
            .text-green { color: #00ff88; }
            .text-red { color: #ff4757; }
            .text-blue { color: #1e90ff; }
            .text-gold { color: #ffa502; }
            .text-purple { color: #9b59b6; }
            table { width: 100%; border-collapse: collapse; background: #1e1e1e; border-radius: 10px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.3); }
            thead { background: #2f3542; }
            th, td { padding: 12px 15px; text-align: center; border-bottom: 1px solid #333; }
            th { color: #00ff88; text-transform: uppercase; font-size: 13px; }
            tr:hover { background: #2a2a2a; }
            .tai { color: #1e90ff; font-weight: bold; }
            .xiu { color: #ff4757; font-weight: bold; }
            .status.correct { color: #00ff88; font-weight: bold; background: rgba(0, 255, 136, 0.1); border-radius: 5px; padding: 5px; }
            .status.wrong { color: #ff4757; font-weight: bold; background: rgba(255, 71, 87, 0.1); border-radius: 5px; padding: 5px; }
            .auto-refresh { text-align: center; margin-top: 20px; font-size: 12px; color: #888; }
        </style>
        <script>
            setInterval(() => window.location.reload(), 10000);
        </script>
    </head>
    <body>
        <div class="container">
            <h1>🧠 Bảng Điều Khiển AI Tự Học</h1>
            <p class="subtitle">Đang học từ Big Data (Tối đa 1000 phiên gần nhất)</p>
            
            <div class="stats-cards">
                <div class="card">
                    <h3>Data Đã Học</h3>
                    <div class="value text-purple">${sessionHistory.length} / 1000</div>
                </div>
                <div class="card">
                    <h3>Tổng Dự Đoán</h3>
                    <div class="value text-blue">${predictionStats.total}</div>
                </div>
                <div class="card">
                    <h3>Dự Đoán Đúng</h3>
                    <div class="value text-green">${predictionStats.correct}</div>
                </div>
                <div class="card">
                    <h3>Dự Đoán Sai</h3>
                    <div class="value text-red">${predictionStats.wrong}</div>
                </div>
                <div class="card">
                    <h3>Tỉ Lệ Thắng (Win Rate)</h3>
                    <div class="value text-gold">${winRate}%</div>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Phiên Giao Dịch</th>
                        <th>AI Dự Đoán</th>
                        <th>Kết Quả Thực Tế</th>
                        <th>Trạng Thái</th>
                    </tr>
                </thead>
                <tbody>
                    ${tableRows}
                </tbody>
            </table>
            
            <div class="auto-refresh">🔄 Giao diện tự động cập nhật sau mỗi 10 giây</div>
        </div>
    </body>
    </html>
    `;

    res.send(html);
});

app.get('/', (req, res) => res.send(`<p>Vào <a href="/status">/status</a> để xem UI thống kê AI.</p>`));

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server đang chạy tại cổng ${PORT}`);
    connectWebSocket();
});
