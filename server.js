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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win"
};

const RECONNECT_DELAY = 500;
const MAX_HISTORY = 1000;

// --- BIẾN TRẠNG THÁI ---
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;
let latestSid = null;
const sessionHistory = [];
let predictionStats = { total: 0, correct: 0, wrong: 0, history: [] };
let pendingPrediction = null; 

// 🧠 HỆ THỐNG TRỌNG SỐ TỰ HỌC V5 (5 MODELS)
let aiWeights = {
    markov: 20,       // Thói quen Server
    pattern: 20,      // Bám Cầu
    trend: 15,        // Hồi quy (Chống gãy)
    momentum: 15,     // Nhịp 1-2, 2-1
    diceShadow: 30    // Bóng Âm/Dương Xúc Xắc (SUPER VIP)
};

let ws = null;
let pingInterval = null;
let rollWatchdog = null; 

const initialMessages = [
    [1, "MiniGame", "GM_fbbdbebndbbc", "123123p", {
        "info": "{\"ipAddress\":\"2402:800:62cd:cb7c:1a7:7a52:9c3e:c290\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJuZG5lYmViYnMiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMTIxMDczMTUsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJ0aW1lc3RhbXAiOjE3NTQ5MjYxMDI1MjcsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjpmYWxzZSwiaXBBZGRyZXNzIjoiMjQwMjo4MDA6NjJjZDpjYjdjOjFhNzo3YTUyOjljM2U6YzI5MCIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMDEucG5nIiwicGxhdGZvcm1JZCI6NSwidXNlcklkIjoiN2RhNDlhNDQtMjlhYS00ZmRiLWJkNGMtNjU5OTQ5YzU3NDdkIiwicmVnVGltZSI6MTc1NDkyNjAyMjUxNSwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJHTV9mYmJkYmVibmRiYmMifQ.DAyEeoAnz8we-Qd0xS0tnqOZ8idkUJkxksBjr_Gei8A\",\"locale\":\"vi\",\"userId\":\"7da49a44-29aa-4fdb-bd4c-659949c5747d\",\"username\":\"GM_fbbdbebndbbc\",\"timestamp\":1754926102527,\"refreshToken\":\"7cc4ad191f4348849f69427a366ea0fd.a68ece9aa85842c7ba523170d0a4ae3e\"}",
        "signature": "53D9E12F910044B140A2EC659167512E2329502FE84A6744F1CD5CBA9B6EC04915673F2CBAE043C4EDB94DDF88F3D3E839A931100845B8F179106E1F44ECBB4253EC536610CCBD0CE90BD8495DAC3E8A9DBDB46FE49B51E88569A6F117F8336AC7ADC226B4F213ECE2F8E0996F2DD5515476C8275F0B2406CDF2987F38A6DA24"
    }],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }]
];

// --- THUẬT TOÁN V5 (BÓNG NGŨ HÀNH & DEEP PATTERN) ---
function predictAiTrain(history) {
    if (history.length < 15) return { du_doan: `Train Data (${history.length}/15)`, do_tin_cay: 50, models: {} };

    const results = history.map(x => x.ket_qua);
    const last1 = results[results.length - 1];
    const last2 = results[results.length - 2];
    const last3 = results[results.length - 3];
    
    // Lấy thông tin xúc xắc của ván gần nhất
    const lastGame = history[history.length - 1];
    
    let scoreTai = 0; let scoreXiu = 0;
    let models = { markov: null, pattern: null, trend: null, momentum: null, diceShadow: null };

    // 1. MARKOV CHAIN
    const recentPattern = `${last3}-${last2}-${last1}`;
    let matchTai = 0; let matchXiu = 0;
    for (let i = 0; i < results.length - 3; i++) {
        if (`${results[i]}-${results[i+1]}-${results[i+2]}` === recentPattern) {
            if (results[i+3] === 'Tài') matchTai++;
            else matchXiu++;
        }
    }
    if (matchTai > matchXiu) { scoreTai += aiWeights.markov; models.markov = 'Tài'; } 
    else if (matchXiu > matchTai) { scoreXiu += aiWeights.markov; models.markov = 'Xỉu'; }

    // 2. PATTERN
    if (last1 === last2 && last2 === last3) { 
        if (last1 === 'Tài') { scoreTai += aiWeights.pattern; models.pattern = 'Tài'; }
        else { scoreXiu += aiWeights.pattern; models.pattern = 'Xỉu'; }
    } else if (last1 !== last2 && last2 !== last3) { 
        const nextExpected = last1 === 'Tài' ? 'Xỉu' : 'Tài';
        if (nextExpected === 'Tài') { scoreTai += aiWeights.pattern; models.pattern = 'Tài'; }
        else { scoreXiu += aiWeights.pattern; models.pattern = 'Xỉu'; }
    }

    // 3. DICE SHADOW (BÓNG ÂM DƯƠNG XÚC XẮC - SIÊU VIP)
    // Quy tắc Bóng Dương: 1-6, 2-7, 3-8, 4-9, 5-0
    const tongDiem = lastGame.tong;
    const chuSoCuoi = tongDiem % 10;
    let bongDuong = (chuSoCuoi + 5) % 10; 
    
    // Thuật toán kinh nghiệm: Nếu Bóng dương ra số chẵn -> Thường bẻ Xỉu, Lẻ -> Thường bệt Tài
    if (bongDuong % 2 === 0) {
        scoreXiu += aiWeights.diceShadow;
        models.diceShadow = 'Xỉu';
    } else {
        scoreTai += aiWeights.diceShadow;
        models.diceShadow = 'Tài';
    }

    // 4. MOMENTUM & TREND (Bù trừ)
    const countTai = results.slice(-15).filter(r => r === 'Tài').length;
    if (countTai >= 10) { scoreXiu += aiWeights.trend; models.trend = 'Xỉu'; } 
    else if (countTai <= 5) { scoreTai += aiWeights.trend; models.trend = 'Tài'; }

    // TỔNG HỢP & ĐÁNH GIÁ
    const totalScore = scoreTai + scoreXiu;
    if (totalScore === 0) return { du_doan: last1, do_tin_cay: 51, models };

    const predicted = scoreTai > scoreXiu ? "Tài" : "Xỉu";
    const winRatio = Math.max(scoreTai, scoreXiu) / totalScore;
    
    let confidence = Math.round(50 + (winRatio * 48));
    if (confidence > 98) confidence = 98; 
    if (confidence < 55) confidence = 55;

    return { du_doan: predicted, do_tin_cay: confidence, models };
}

// 🧠 HÀM PHẠT/THƯỞNG AI V5
function trainAiWeights(actualResult, models) {
    const LEARNING_RATE = 2.0; 
    const adjustWeight = (weightName, modelVoted) => {
        if (!modelVoted) return; 
        if (modelVoted === actualResult) {
            aiWeights[weightName] = Math.min(50, aiWeights[weightName] + LEARNING_RATE); 
        } else {
            aiWeights[weightName] = Math.max(5, aiWeights[weightName] - LEARNING_RATE); 
        }
    };
    for (const key in models) {
        adjustWeight(key, models[key]);
    }
}

// --- QUẢN LÝ WEBSOCKET (SĂN PHIÊN GẮT GAO V5) ---
function hardReconnect() {
    console.log('[🔄] ÉP KHỞI ĐỘNG LẠI KẾT NỐI (Hard Reset)...');
    if (ws) {
        ws.terminate(); 
        ws = null;
    }
    clearTimeout(rollWatchdog);
    clearInterval(pingInterval);
    setTimeout(connectWebSocket, RECONNECT_DELAY);
}

function resetRollWatchdog() {
    clearTimeout(rollWatchdog);
    // Ván game 60s, cho độ trễ 2s. Hết 62s không có kết quả -> Server Anti-Bot -> Hard Reset ngay!
    rollWatchdog = setTimeout(() => {
        console.log('[⚠️] QUÁ 62S KHÔNG NHẬN KẾT QUẢ. BỊ SERVER DROP KẾT NỐI!');
        hardReconnect();
    }, 62000); 
}

function connectWebSocket() {
    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        console.log('[✅] Bắt đầu luồng WebSocket mới (V5)');
        resetRollWatchdog(); 

        initialMessages.forEach((msg, i) => {
            setTimeout(() => { if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg)); }, i * 300);
        });

        // Bơm Ping liên tục mỗi 3s để Server không đưa vào trạng thái rảnh
        pingInterval = setInterval(() => { 
            if (ws && ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify([6, "MiniGame", "taixiuPlugin", { cmd: 1005 }]));
            }
        }, 3000);
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;
            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (sid) latestSid = sid; 

            if (cmd === 1003 && gBB && d1 && d2 && d3) {
                resetRollWatchdog(); // Có dữ liệu xúc xắc -> Reset chó canh gác 62s

                let activeSession = latestSid || (sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien + 1 : Date.now());

                const lastSavedPhien = sessionHistory.length > 0 ? sessionHistory[sessionHistory.length - 1].phien : null;
                if (lastSavedPhien && activeSession > lastSavedPhien + 1) {
                    console.log(`[🚨 DATA GAP] Lọt phiên từ ${lastSavedPhien + 1} -> ${activeSession - 1}. Kết nối đã được sửa lỗi cho các ván sau!`);
                }

                const total = d1 + d2 + d3;
                const resultText = (total > 10) ? "Tài" : "Xỉu";
                const sessionData = { phien: activeSession, xuc_xac_1: d1, xuc_xac_2: d2, xuc_xac_3: d3, tong: total, ket_qua: resultText };

                if (!lastSavedPhien || lastSavedPhien !== activeSession) {
                    sessionHistory.push(sessionData);
                    if (sessionHistory.length > MAX_HISTORY) sessionHistory.shift();
                }

                if (pendingPrediction && pendingPrediction.phien === activeSession) {
                    predictionStats.total++;
                    const isCorrect = pendingPrediction.du_doan === resultText;
                    if (isCorrect) predictionStats.correct++; else predictionStats.wrong++;

                    trainAiWeights(resultText, pendingPrediction.models);

                    predictionStats.history.unshift({
                        phien: activeSession,
                        du_doan: pendingPrediction.du_doan,
                        thuc_te: resultText,
                        trang_thai: isCorrect ? "✅ ĐÚNG" : "❌ SAI"
                    });
                    if (predictionStats.history.length > 60) predictionStats.history.pop(); 
                }

                const nextPhien = activeSession + 1;
                const prediction = predictAiTrain(sessionHistory);
                
                if (sessionHistory.length >= 15) {
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
                
                console.log(`[🎲] P${activeSession}: [${d1}-${d2}-${d3}] = ${total} (${resultText}) | AI Mới: ${prediction.du_doan} (${predictData.do_tin_cay})`);
            }
        } catch (e) { }
    });

    ws.on('close', () => hardReconnect());
    ws.on('error', () => hardReconnect());
}

// --- API & TRANG STATUS ---
app.get('/api/data', (req, res) => res.json(apiResponseData));
app.get('/predict', (req, res) => predictData ? res.json(predictData) : res.json({ error: "Đang thu thập Data..." }));

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

    const html = `
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <title>AI V5 - Bóng Xúc Xắc Ngũ Hành</title>
        <style>
            * { font-family: sans-serif; box-sizing: border-box; }
            body { background: #0a0a0a; color: #fff; padding: 20px; }
            .card { background: #1a1a1a; padding: 15px; border-radius: 8px; margin-bottom: 10px; border: 1px solid #333; text-align: center; }
            .tai { color: #3399ff; font-weight: bold; } .xiu { color: #ff3366; font-weight: bold; }
            table { width: 100%; border-collapse: collapse; margin-top: 20px; }
            th, td { padding: 10px; border-bottom: 1px solid #333; text-align: center; }
            th { background: #222; color: #ffcc00; }
            .correct { color: #00ffcc; } .wrong { color: #ff3366; }
        </style>
        <script>setInterval(() => window.location.reload(), 10000);</script>
    </head>
    <body>
        <h2 style="text-align:center; color: #ffcc00;">🧠 AI V5 - BÓNG XÚC XẮC NGŨ HÀNH</h2>
        <div style="display:flex; gap: 10px;">
            <div class="card" style="flex:1">Win Rate<br><strong style="font-size:24px; color:#00ffcc">${winRate}%</strong></div>
            <div class="card" style="flex:1">Đúng/Sai<br><strong style="font-size:24px;">${predictionStats.correct}/${predictionStats.wrong}</strong></div>
        </div>
        <div class="card" style="font-size: 13px; color: #aaa;">
            Trọng số AI: Shadow(${Math.round(aiWeights.diceShadow)}) - Markov(${Math.round(aiWeights.markov)}) - Pattern(${Math.round(aiWeights.pattern)})
        </div>
        <table>
            <tr><th>Phiên</th><th>Dự Đoán</th><th>Thực Tế</th><th>Kết Quả</th></tr>
            ${tableRows || '<tr><td colspan="4">Đang thu thập dữ liệu...</td></tr>'}
        </table>
    </body>
    </html>
    `;
    res.send(html);
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server V5 chạy tại cổng ${PORT}`);
    connectWebSocket();
});
