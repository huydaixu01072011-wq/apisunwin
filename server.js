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
const MAX_HISTORY = 1000;

// --- BIẾN TRẠNG THÁI & AI TRAIN MEMORY ---
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;
let latestSid = null;
const sessionHistory = [];

let predictionStats = { total: 0, correct: 0, wrong: 0, history: [] };
let pendingPrediction = null; 

// 🧠 HỆ THỐNG TRỌNG SỐ TỰ HỌC (AI WEIGHTS)
// Các chỉ số này sẽ tự động TĂNG nếu dự đoán ĐÚNG và GIẢM nếu dự đoán SAI
let aiWeights = {
    markovChain: 30,  // Nhận diện thói quen server
    patternMatch: 35, // Nhận diện cầu kinh điển (bệt, 1-1)
    reversionMean: 20 // Phân tích xu hướng bù trừ
};

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

// --- THUẬT TOÁN DỰ ĐOÁN AI TRAIN (DYNAMIC WEIGHTS) ---
function predictAiTrain(history) {
    if (history.length < 10) return { du_doan: `Train Data (${history.length}/10)`, do_tin_cay: 50, models: {} };

    const results = history.map(x => x.ket_qua);
    const last1 = results[results.length - 1];
    const last2 = results[results.length - 2];
    const last3 = results[results.length - 3];
    const last4 = results[results.length - 4];

    let scoreTai = 0;
    let scoreXiu = 0;
    
    // Lưu lại dự đoán của từng Model để xíu nữa đối chiếu phạt/thưởng
    let modelPredictions = { markov: null, pattern: null, mean: null };

    // 1. MODEL MARKOV CHAIN (Quét thói quen quá khứ)
    const recentPattern = `${last3}-${last2}-${last1}`;
    let matchTai = 0; let matchXiu = 0;
    for (let i = 0; i < results.length - 3; i++) {
        if (`${results[i]}-${results[i+1]}-${results[i+2]}` === recentPattern) {
            if (results[i+3] === 'Tài') matchTai++;
            if (results[i+3] === 'Xỉu') matchXiu++;
        }
    }
    if (matchTai > matchXiu) {
        scoreTai += aiWeights.markovChain;
        modelPredictions.markov = 'Tài';
    } else if (matchXiu > matchTai) {
        scoreXiu += aiWeights.markovChain;
        modelPredictions.markov = 'Xỉu';
    }

    // 2. MODEL PATTERN (Bám Cầu)
    if (last1 === last2 && last2 === last3) { // Đang bệt
        if (last1 === 'Tài') { scoreTai += aiWeights.patternMatch; modelPredictions.pattern = 'Tài'; }
        else { scoreXiu += aiWeights.patternMatch; modelPredictions.pattern = 'Xỉu'; }
    } else if (last1 !== last2 && last2 !== last3) { // Đang 1-1
        const nextExpected = last1 === 'Tài' ? 'Xỉu' : 'Tài';
        if (nextExpected === 'Tài') { scoreTai += aiWeights.patternMatch; modelPredictions.pattern = 'Tài'; }
        else { scoreXiu += aiWeights.patternMatch; modelPredictions.pattern = 'Xỉu'; }
    }

    // 3. MODEL REVERSION (Hồi quy xu hướng)
    const range = Math.min(results.length, 30);
    const recentHistory = results.slice(-range);
    const countTai = recentHistory.filter(r => r === 'Tài').length;
    const threshold = Math.floor(range * 0.65); // Mức gãy 65%
    
    if (countTai >= threshold) {
        scoreXiu += aiWeights.reversionMean;
        modelPredictions.mean = 'Xỉu';
    } else if (recentHistory.length - countTai >= threshold) {
        scoreTai += aiWeights.reversionMean;
        modelPredictions.mean = 'Tài';
    }

    // TỔNG HỢP KẾT QUẢ TỪ 3 MODEL
    const totalScore = scoreTai + scoreXiu;
    if (totalScore === 0) return { du_doan: last1, do_tin_cay: 51, models: modelPredictions }; // Fallback

    const predicted = scoreTai > scoreXiu ? "Tài" : "Xỉu";
    const winRatio = Math.max(scoreTai, scoreXiu) / totalScore;
    
    let confidence = Math.round(50 + (winRatio * 45));
    if (confidence > 95) confidence = 95; 
    if (confidence < 55) confidence = 55;

    return { du_doan: predicted, do_tin_cay: confidence, models: modelPredictions };
}

// 🧠 HÀM PHẠT/THƯỞNG ĐỂ AI TỰ HỌC TỪ KẾT QUẢ THỰC TẾ
function trainAiWeights(actualResult, modelPredictions) {
    const LEARNING_RATE = 2; // Tốc độ học (Điểm cộng/trừ mỗi ván)
    
    const adjustWeight = (weightName, modelVoted) => {
        if (!modelVoted) return; // Model không đưa ra ý kiến thì bỏ qua
        if (modelVoted === actualResult) {
            aiWeights[weightName] = Math.min(60, aiWeights[weightName] + LEARNING_RATE); // Thưởng (Max 60)
        } else {
            aiWeights[weightName] = Math.max(10, aiWeights[weightName] - LEARNING_RATE); // Phạt (Min 10)
        }
    };

    adjustWeight('markovChain', modelPredictions.markov);
    adjustWeight('patternMatch', modelPredictions.pattern);
    adjustWeight('reversionMean', modelPredictions.mean);
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
        console.log('[✅] WebSocket kết nối thành công.');
        heartbeat();
        initialMessages.forEach((msg, i) => {
            setTimeout(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); }, i * 600);
        });
        clearInterval(pingInterval);
        pingInterval = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.ping(); }, PING_INTERVAL);
    });

    ws.on('ping', heartbeat);
    ws.on('pong', heartbeat);

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;
            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (sid) latestSid = sid; 

            if (cmd === 1003 && gBB && d1 && d2 && d3) {
                let activeSession = latestSid || (sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien + 1 : Date.now());

                // 🚨 KIỂM TRA MẤT KẾT NỐI (GAP DETECTION)
                const lastSavedPhien = sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien : null;
                if (lastSavedPhien && activeSession > lastSavedPhien + 1) {
                    console.log(`[⚠️ CẢNH BÁO] Phát hiện mất dữ liệu! Phiên bị lọt: từ ${lastSavedPhien + 1} đến ${activeSession - 1}`);
                }

                const total = d1 + d2 + d3;
                const resultText = (total > 10) ? "Tài" : "Xỉu";
                const sessionData = { phien: activeSession, xuc_xac_1: d1, xuc_xac_2: d2, xuc_xac_3: d3, tong: total, ket_qua: resultText };

                // Lưu lịch sử (Chống lưu trùng phiên)
                if (!lastSavedPhien || lastSavedPhien !== activeSession) {
                    sessionHistory.push(sessionData);
                    if (sessionHistory.length > MAX_HISTORY) sessionHistory.shift();
                }

                // 🧠 TRAIN AI VÀ ĐÁNH GIÁ DỰ ĐOÁN
                if (pendingPrediction && pendingPrediction.phien === activeSession) {
                    predictionStats.total++;
                    const isCorrect = pendingPrediction.du_doan === resultText;
                    
                    if (isCorrect) predictionStats.correct++;
                    else predictionStats.wrong++;

                    // Chạy hàm Training để AI rút kinh nghiệm cho ván sau
                    trainAiWeights(resultText, pendingPrediction.models);

                    predictionStats.history.unshift({
                        phien: activeSession,
                        du_doan: pendingPrediction.du_doan,
                        thuc_te: resultText,
                        trang_thai: isCorrect ? "✅ ĐÚNG" : "❌ SAI"
                    });
                    if (predictionStats.history.length > 50) predictionStats.history.pop(); 
                }

                // TẠO DỰ ĐOÁN MỚI
                const nextPhien = activeSession + 1;
                const prediction = predictAiTrain(sessionHistory);
                
                if (sessionHistory.length >= 10) {
                    pendingPrediction = { phien: nextPhien, du_doan: prediction.du_doan, models: prediction.models };
                }

                apiResponseData = { ...sessionData, lich_su_phien: sessionHistory };
                predictData = {
                    phien: activeSession,
                    tong: total,
                    ket_qua: resultText,
                    phien_tiep_theo: nextPhien,
                    du_doan_ai: prediction.du_doan,
                    do_tin_cay: typeof prediction.do_tin_cay === 'number' ? `${prediction.do_tin_cay}%` : prediction.do_tin_cay,
                    ai_weights_hien_tai: aiWeights
                };
                
                console.log(`[🎲] Phiên ${activeSession} | Kết quả: ${resultText} | AI Đoán Tiếp: ${prediction.du_doan} (${predictData.do_tin_cay})`);
                console.log(`[🧠] Trạng thái não bộ AI: Markov(${aiWeights.markovChain}) - Pattern(${aiWeights.patternMatch}) - Trend(${aiWeights.reversionMean})`);
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý:', e.message);
        }
    });

    ws.on('close', () => {
        console.log(`[🔌] Mất kết nối. Đang nối lại...`);
        clearTimeout(heartbeatTimeout); clearInterval(pingInterval);
        setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', () => ws.terminate());
}

// --- REST API ROUTES ---
app.get('/api/data', (req, res) => res.json(apiResponseData));
app.get('/predict', (req, res) => predictData ? res.json(predictData) : res.json({ error: "Đang thu thập Data..." }));

// --- GIAO DIỆN HTML /STATUS ---
app.get('/status', (req, res) => {
    const winRate = predictionStats.total > 0 ? Math.round((predictionStats.correct / predictionStats.total) * 100) : 0;
    let tableRows = predictionStats.history.map(item => `
        <tr>
            <td>#${item.phien}</td>
            <td class="${item.du_doan === 'Tài' ? 'tai' : 'xiu'}">${item.du_doan}</td>
            <td class="${item.thuc_te === 'Tài' ? 'tai' : 'xiu'}">${item.thuc_te}</td>
            <td class="status ${item.trang_thai.includes('ĐÚNG') ? 'correct' : 'wrong'}">${item.trang_thai}</td>
        </tr>
    `).join('');

    if(predictionStats.history.length === 0) tableRows = `<tr><td colspan="4" style="text-align:center; padding: 20px;">AI đang thu thập dữ liệu... (${sessionHistory.length}/10) phiên.</td></tr>`;

    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Hệ Thống AI TRAIN Siêu VIP</title>
        <style>
            * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Tahoma, sans-serif; }
            body { background-color: #0d0d0d; color: #fff; padding: 20px; }
            .container { max-width: 900px; margin: 0 auto; }
            h1 { text-align: center; color: #00ffcc; margin-bottom: 10px; text-transform: uppercase; }
            .subtitle { text-align: center; color: #888; margin-bottom: 20px; font-size: 14px; }
            .weights-box { background: #1a1a1a; padding: 15px; border-radius: 8px; margin-bottom: 20px; border: 1px solid #333; text-align: center; }
            .weights-box span { margin: 0 15px; font-weight: bold; color: #ffcc00; }
            .stats-cards { display: flex; gap: 15px; margin-bottom: 30px; flex-wrap: wrap; }
            .card { background: #1a1a1a; padding: 20px; border-radius: 10px; flex: 1; min-width: 150px; text-align: center; border: 1px solid #333; }
            .card h3 { font-size: 13px; color: #aaa; margin-bottom: 10px; }
            .card .value { font-size: 26px; font-weight: bold; }
            .text-green { color: #00ffcc; } .text-red { color: #ff3366; } .text-gold { color: #ffcc00; }
            table { width: 100%; border-collapse: collapse; background: #1a1a1a; border-radius: 10px; overflow: hidden; }
            th, td { padding: 12px; text-align: center; border-bottom: 1px solid #333; }
            th { background: #262626; color: #00ffcc; font-size: 13px; }
            .tai { color: #3399ff; font-weight: bold; } .xiu { color: #ff3366; font-weight: bold; }
            .status.correct { color: #00ffcc; background: rgba(0, 255, 204, 0.1); padding: 4px 8px; border-radius: 4px; }
            .status.wrong { color: #ff3366; background: rgba(255, 51, 102, 0.1); padding: 4px 8px; border-radius: 4px; }
        </style>
        <script>setInterval(() => window.location.reload(), 10000);</script>
    </head>
    <body>
        <div class="container">
            <h1>🧠 Bảng Điều Khiển AI TRAIN</h1>
            <p class="subtitle">Trọng số (Weights) sẽ tự động TĂNG nếu dự đoán đúng và GIẢM nếu sai</p>
            
            <div class="weights-box">
                Chỉ Số Não Bộ AI: 
                <span>Markov: ${aiWeights.markovChain}</span> | 
                <span>Pattern: ${aiWeights.patternMatch}</span> | 
                <span>Trend: ${aiWeights.reversionMean}</span>
            </div>

            <div class="stats-cards">
                <div class="card"><h3>Data Đã Học</h3><div class="value">${sessionHistory.length}/1000</div></div>
                <div class="card"><h3>Dự Đoán Đúng</h3><div class="value text-green">${predictionStats.correct}</div></div>
                <div class="card"><h3>Dự Đoán Sai</h3><div class="value text-red">${predictionStats.wrong}</div></div>
                <div class="card"><h3>Tỉ Lệ Thắng (Win Rate)</h3><div class="value text-gold">${winRate}%</div></div>
            </div>

            <table>
                <thead><tr><th>Phiên</th><th>AI Dự Đoán</th><th>Kết Quả Thực Tế</th><th>Trạng Thái</th></tr></thead>
                <tbody>${tableRows}</tbody>
            </table>
        </div>
    </body>
    </html>
    `;
    res.send(html);
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server đang chạy tại cổng ${PORT}`);
    connectWebSocket();
});
