const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const fs = require('fs');

const app = express();
app.use(cors());
app.set('etag', false);

const PORT = 5000;
const HISTORY_FILE = './session_history.json';

// --- CẤU HÌNH HỆ THỐNG ---
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win",
    "Cache-Control": "no-cache"
};

// ================ BỘ NHỚ CHÍNH ================
let sessionHistory = [];            // tối đa 2000 phiên
let predictionHistory = [];        // lịch sử dự đoán (100 mục gần nhất)
let currentPrediction = null;      // { du_doan, do_tin_cay, phien_du_doan, votes }
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;

// ---- TRỌNG SỐ KHỞI ĐẦU CỦA CÁC CHUYÊN GIA ----
const expertWeights = {
    markov2: 1.0,
    markov3: 1.0,
    markov4: 1.0,
    markov5: 1.0,
    nguHanh: 1.0,
    betCau: 1.0,
    nhipCau: 1.0,
    canBang: 1.0,
    avgTotal: 1.0,
    xuHuongTong: 1.0,   // mới thêm
    worm: 1.0
};

// ---- BỘ NHỚ MẪU CHO WORM GPT ----
const patternMemory = {
    map3: new Map(),   // key: pattern 3 phiên -> { countTai, countXiu }
    map4: new Map(),
    order3: [],
    order4: []
};
const MAX_MAP_SIZE = 5000;

// ==================== KHỞI TẠO / PHỤC HỒI DỮ LIỆU ====================
function loadHistoryFromFile() {
    try {
        if (fs.existsSync(HISTORY_FILE)) {
            const raw = fs.readFileSync(HISTORY_FILE);
            const data = JSON.parse(raw);
            if (Array.isArray(data)) {
                sessionHistory = data.slice(-2000);
                console.log(`[📂] Đã tải ${sessionHistory.length} phiên từ file.`);
                return;
            }
        }
    } catch (e) {}
    sessionHistory = [];
}
function saveHistoryToFile() {
    try {
        fs.writeFileSync(HISTORY_FILE, JSON.stringify(sessionHistory.slice(-2000)));
    } catch (e) {}
}
loadHistoryFromFile();

// ==================== CÁC CHUYÊN GIA ====================
function getExpertVotes(history) {
    const results = history.map(x => x.ket_qua);
    const last = history[history.length - 1];

    // --- Markov bậc k ---
    const markovVote = (k) => {
        if (results.length < k + 1) return null;
        const pattern = results.slice(-k).join('-');
        let countTai = 0, countXiu = 0;
        for (let i = 0; i < results.length - k; i++) {
            const test = results.slice(i, i + k).join('-');
            if (test === pattern) {
                if (results[i + k] === 'Tài') countTai++;
                else countXiu++;
            }
        }
        if (countTai + countXiu < 2) return null;
        return countTai > countXiu ? 'Tài' : (countXiu > countTai ? 'Xỉu' : null);
    };

    // --- Ngũ hành (bóng âm dương của tổng) ---
    const nguHanhVote = () => {
        const bongDuong = ((last.tong % 10) + 5) % 10;
        return bongDuong % 2 === 0 ? 'Xỉu' : 'Tài';
    };

    // --- Bệt cầu ---
    const betCauVote = () => {
        const lastResult = results[results.length - 1];
        let count = 1;
        for (let i = results.length - 2; i >= 0; i--) {
            if (results[i] === lastResult) count++;
            else break;
        }
        if (count >= 4) return lastResult === 'Tài' ? 'Xỉu' : 'Tài'; // đảo chiều
        if (count === 1 || count === 2) return lastResult;           // tiếp tục nhịp
        return null; // count = 3 -> không rõ
    };

    // --- Nhịp cầu luân phiên ---
    const nhipCauVote = () => {
        if (results.length < 4) return null;
        const last4 = results.slice(-4);
        if (last4[0] !== last4[1] && last4[1] !== last4[2] && last4[2] !== last4[3]) {
            return last4[3] === 'Tài' ? 'Xỉu' : 'Tài';
        }
        if (results.length >= 6) {
            const last6 = results.slice(-6);
            if (last6[0] === last6[1] && last6[2] === last6[3] &&
                last6[0] !== last6[2] && last6[4] === last6[5] && last6[4] === last6[0]) {
                return last6[5] === 'Tài' ? 'Xỉu' : 'Tài';
            }
        }
        return null;
    };

    // --- Cân bằng tỉ lệ (cửa sổ 50, 100, 200) ---
    const canBangVote = () => {
        const windows = [50, 100, 200];
        for (const win of windows) {
            const sample = history.slice(-win);
            if (sample.length < win * 0.8) continue;
            const taiCount = sample.filter(x => x.ket_qua === 'Tài').length;
            const ratio = taiCount / sample.length;
            if (ratio > 0.58) return 'Xỉu';
            if (ratio < 0.42) return 'Tài';
        }
        return null;
    };

    // --- Trung bình tổng (hồi quy về 10.5) ---
    const avgTotalVote = () => {
        const sample = history.slice(-50);
        if (sample.length < 30) return null;
        const avg = sample.reduce((s, x) => s + x.tong, 0) / sample.length;
        if (avg > 10.8) return 'Xỉu';
        if (avg < 10.2) return 'Tài';
        return null;
    };

    // --- Xu hướng tổng 5 phiên gần nhất (tăng/giảm) ---
    const xuHuongTongVote = () => {
        if (history.length < 6) return null;
        const last5 = history.slice(-5).map(x => x.tong);
        let up = 0, down = 0;
        for (let i = 1; i < last5.length; i++) {
            if (last5[i] > last5[i - 1]) up++;
            else if (last5[i] < last5[i - 1]) down++;
        }
        if (up >= 4) return 'Tài';
        if (down >= 4) return 'Xỉu';
        return null;
    };

    // --- WORM GPT (tự tìm mẫu ẩn) ---
    const wormVote = () => {
        const tryWorm = (k, map) => {
            if (results.length < k) return null;
            const pattern = results.slice(-k).join('-');
            const stats = map.get(pattern);
            if (stats && (stats.countTai + stats.countXiu) >= 5) {
                const total = stats.countTai + stats.countXiu;
                const ratioTai = stats.countTai / total;
                if (ratioTai > 0.6) return 'Tài';
                if (ratioTai < 0.4) return 'Xỉu';
            }
            return null;
        };
        let vote = tryWorm(4, patternMemory.map4);
        if (vote) return vote;
        vote = tryWorm(3, patternMemory.map3);
        return vote;
    };

    return {
        markov2: markovVote(2),
        markov3: markovVote(3),
        markov4: markovVote(4),
        markov5: markovVote(5),
        nguHanh: nguHanhVote(),
        betCau: betCauVote(),
        nhipCau: nhipCauVote(),
        canBang: canBangVote(),
        avgTotal: avgTotalVote(),
        xuHuongTong: xuHuongTongVote(),
        worm: wormVote()
    };
}

// ==================== TỔNG HỢP DỰ ĐOÁN ====================
function ensemblePredict(votes) {
    let weightTai = 0, weightXiu = 0;
    for (const [expert, vote] of Object.entries(votes)) {
        if (vote === 'Tài') weightTai += expertWeights[expert];
        else if (vote === 'Xỉu') weightXiu += expertWeights[expert];
    }
    const totalWeight = weightTai + weightXiu;
    if (totalWeight === 0) return { du_doan: 'Không xác định', do_tin_cay: 50 };
    const doTinCay = Math.round((Math.max(weightTai, weightXiu) / totalWeight) * 100);
    return {
        du_doan: weightTai > weightXiu ? 'Tài' : 'Xỉu',
        do_tin_cay: Math.min(99, doTinCay)
    };
}

// ==================== CẬP NHẬT TRỌNG SỐ (HEDGE) ====================
function updateWeights(votes, actualResult) {
    const eta = 0.5; // tốc độ học
    for (const [expert, vote] of Object.entries(votes)) {
        if (vote === null) continue;
        const reward = (vote === actualResult) ? 1 : 0;
        const factor = Math.exp(eta * reward); // đúng -> *~1.65, sai -> *1
        expertWeights[expert] *= factor;
        // Giới hạn trọng số
        if (expertWeights[expert] > 10) expertWeights[expert] = 10;
        if (expertWeights[expert] < 0.1) expertWeights[expert] = 0.1;
    }
    // Chuẩn hóa để tổng trọng số không bùng nổ
    const total = Object.values(expertWeights).reduce((a, b) => a + b, 0);
    const targetAvg = 1.0;
    const ratio = (targetAvg * Object.keys(expertWeights).length) / total;
    for (const key of Object.keys(expertWeights)) {
        expertWeights[key] *= ratio;
    }
}

// ==================== CẬP NHẬT BỘ NHỚ WORM ====================
function updateWormMemory(history) {
    const results = history.map(x => x.ket_qua);
    if (results.length < 5) return;

    const updateMap = (k, map, order) => {
        const pattern = results.slice(-k - 1, -1).join('-'); // mẫu trước phiên mới nhất
        const nextResult = results[results.length - 1];      // kết quả phiên mới nhất
        const stats = map.get(pattern) || { countTai: 0, countXiu: 0 };
        if (nextResult === 'Tài') stats.countTai++;
        else stats.countXiu++;
        map.set(pattern, stats);
        order.push(pattern);
        if (order.length > MAX_MAP_SIZE) {
            const oldest = order.shift();
            map.delete(oldest);
        }
    };

    updateMap(3, patternMemory.map3, patternMemory.order3);
    updateMap(4, patternMemory.map4, patternMemory.order4);
}

// ==================== TÍNH ĐỘ CHÍNH XÁC ====================
function calcAccuracy(lastN) {
    const subset = predictionHistory.slice(-lastN);
    if (subset.length === 0) return null;
    const correct = subset.filter(p => p.dung).length;
    return ((correct / subset.length) * 100).toFixed(2) + '%';
}

// ==================== WEBSOCKET ====================
let ws = null;
let overclockInterval = null;
let reconnecting = false;

function initSocket() {
    if (reconnecting) return;
    reconnecting = true;

    if (ws) {
        ws.removeAllListeners();
        ws.terminate();
    }

    console.log('[🔄] Đang thiết lập đường truyền 24/7...');
    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        reconnecting = false;
        console.log('[✅] Kết nối thành công. Hội đồng chuyên gia đã sẵn sàng!');

        const initMsg = [1, "MiniGame", "GM_fbbdbebndbbc", "123123p", {"info":"{\"ipAddress\":\"2402:800:62cd:cb7c:1a7:7a52:9c3e:c290\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJuZG5lYmViYnMiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMTIxMDczMTUsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJ0aW1lc3RhbXAiOjE3NTQ5MjYxMDI1MjcsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjpmYWxzZSwiaXBBZGRyZXNzIjoiMjQwMjo4MDA6NjJjZDpjYjdjOjFhNzo3YTUyOjljM2U6YzI5MCIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMDEucG5nIiwicGxhdGZvcm1JZCI6NSwidXNlcklkIjoiN2RhNDlhNDQtMjlhYS00ZmRiLWJkNGMtNjU5OTQ5YzU3NDdkIiwicmVnVGltZSI6MTc1NDkyNjAyMjUxNSwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJHTV9mYmJkYmVibmRiYmMifQ.DAyEeoAnz8we-Qd0xS0tnqOZ8idkUJkxksBjr_Gei8A\",\"locale\":\"vi\",\"userId\":\"7da49a44-29aa-4fdb-bd4c-659949c5747d\",\"username\":\"GM_fbbdbebndbbc\",\"timestamp\":1754926102527,\"refreshToken\":\"7cc4ad191f4348849f69427a366ea0fd.a68ece9aa85842c7ba523170d0a4ae3e\"}","signature":"53D9E12F910044B140A2EC659167512E2329502FE84A6744F1CD5CBA9B6EC04915673F2CBAE043C4EDB94DDF88F3D3E839A931100845B8F179106E1F44ECBB4253EC536610CCBD0CE90BD8495DAC3E8A9DBDB46FE49B51E88569A6F117F8336AC7ADC226B4F213ECE2F8E0996F2DD5515476C8275F0B2406CDF2987F38A6DA24"}];
        ws.send(JSON.stringify(initMsg));

        clearInterval(overclockInterval);
        overclockInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.send(JSON.stringify([6, "MiniGame", "taixiuPlugin", { cmd: 1005 }]));
                ws.ping();
            } else {
                console.log('[⚠️] Luồng kẹt. Reconnect cưỡng chế!');
                initSocket();
            }
        }, 10000);
    });

    ws.on('pong', () => {});

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;
            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1003 && gBB && d1 !== undefined && d2 !== undefined && d3 !== undefined) {
                const phien = sid;
                if (!phien) return;
                // Chống trùng phiên
                if (sessionHistory.length > 0 && sessionHistory[sessionHistory.length - 1].phien === phien) return;

                const total = d1 + d2 + d3;
                const ketQua = total > 10 ? 'Tài' : 'Xỉu';

                // --- Đối chiếu dự đoán cũ (nếu có) ---
                if (currentPrediction && currentPrediction.phien_du_doan === phien) {
                    const dung = (currentPrediction.du_doan === ketQua);
                    // Cập nhật trọng số từ votes đã lưu
                    if (currentPrediction.votes) {
                        updateWeights(currentPrediction.votes, ketQua);
                    }
                    // Lưu vào lịch sử dự đoán
                    predictionHistory.push({
                        phien: phien,
                        du_doan: currentPrediction.du_doan,
                        ket_qua: ketQua,
                        dung: dung,
                        do_tin_cay: currentPrediction.do_tin_cay,
                        timestamp: new Date().toISOString()
                    });
                    if (predictionHistory.length > 100) predictionHistory.shift();
                }

                // --- Thêm phiên mới vào lịch sử ---
                const sessionData = {
                    phien,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: ketQua
                };
                sessionHistory.push(sessionData);
                if (sessionHistory.length > 2000) sessionHistory.shift();
                saveHistoryToFile();

                // Cập nhật bộ nhớ Worm
                updateWormMemory(sessionHistory);

                // --- Dự đoán phiên tiếp theo ---
                const votes = getExpertVotes(sessionHistory);
                const prediction = ensemblePredict(votes);
                currentPrediction = {
                    du_doan: prediction.du_doan,
                    do_tin_cay: prediction.do_tin_cay,
                    phien_du_doan: phien + 1,
                    votes: votes
                };

                // Chuẩn bị dữ liệu cho API
                apiResponseData = {
                    phien_hien_tai: phien,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: ketQua,
                    lich_su_phien: sessionHistory
                };

                predictData = {
                    phien_hien_tai: phien,
                    ket_qua_vua_ra: `${ketQua} (${total})`,
                    du_doan_phien_tiep: phien + 1,
                    ai_chot: prediction.du_doan,
                    do_tin_cay: `${prediction.do_tin_cay}%`,
                    loi_khuyen: prediction.do_tin_cay >= 85 ? 'Vào mạnh tay (3x)' :
                               (prediction.do_tin_cay < 65 ? 'Đánh nhỏ / Bỏ qua' : 'Đánh đều tay (1x)'),
                    expert_votes: votes,
                    expert_weights: { ...expertWeights }
                };

                console.log(`\n=================================================`);
                console.log(`[🎲] PHIÊN ${phien} | ${ketQua} (${total})`);
                console.log(`🤖 DỰ ĐOÁN PHIÊN ${phien + 1}: ${prediction.du_doan} (${prediction.do_tin_cay}%)`);
                console.log(`   Trọng số:`, expertWeights);
                console.log(`=================================================`);
            }
        } catch (e) {
            // bỏ qua lỗi parse
        }
    });

    ws.on('close', () => {
        reconnecting = false;
        setTimeout(initSocket, 1000);
    });

    ws.on('error', () => {
        ws.terminate();
    });
}

// ==================== ENDPOINTS ====================
app.get('/api/data', (req, res) => res.json(apiResponseData));

app.get('/predict', (req, res) => {
    if (predictData) res.json(predictData);
    else res.json({ error: "Chưa đủ dữ liệu AI..." });
});

// Dữ liệu JSON cho Dashboard
app.get('/api/status-data', (req, res) => {
    const recentPredictions = predictionHistory.slice(-20).reverse(); // 20 mới nhất
    const acc50 = calcAccuracy(50);
    const acc100 = calcAccuracy(100);
    res.json({
        current_prediction: currentPrediction ? {
            phien_du_doan: currentPrediction.phien_du_doan,
            du_doan: currentPrediction.du_doan,
            do_tin_cay: currentPrediction.do_tin_cay
        } : null,
        prediction_history: recentPredictions,
        expert_weights: expertWeights,
        overall_accuracy_50: acc50,
        overall_accuracy_100: acc100,
        total_sessions: sessionHistory.length,
        last_updated: new Date().toISOString()
    });
});

// Giao diện Dashboard (/status)
app.get('/status', (req, res) => {
    res.send(`
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>AI SIÊU VIP - Dashboard</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { background: #0a0f1e; color: #e0e0e0; font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; }
    .container { max-width: 1000px; margin: auto; }
    h1 { text-align: center; color: #00e676; margin-bottom: 20px; }
    .card { background: #131a2e; border-radius: 12px; padding: 20px; margin-bottom: 20px; box-shadow: 0 4px 12px rgba(0,0,0,0.5); }
    .flex { display: flex; flex-wrap: wrap; gap: 20px; }
    .flex > div { flex: 1; min-width: 200px; }
    .label { font-size: 0.9rem; color: #9aa0b0; }
    .value { font-size: 1.8rem; font-weight: bold; color: #ffd740; }
    .prediction { font-size: 2rem; font-weight: bold; color: #00e5ff; }
    .accuracy { font-size: 1.2rem; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { padding: 8px 12px; text-align: center; border-bottom: 1px solid #2a2f45; }
    th { background: #1e2640; color: #b0bec5; }
    .correct { color: #00e676; } .wrong { color: #ff5252; }
    .expert-weights { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 8px; }
    .weight-item { display: flex; justify-content: space-between; background: #1e2640; padding: 6px 10px; border-radius: 6px; }
    .bar { height: 6px; background: #2a2f45; border-radius: 3px; margin-top: 4px; }
    .bar-fill { height: 100%; border-radius: 3px; background: #00e5ff; }
    .loading { text-align: center; padding: 20px; color: #9aa0b0; }
  </style>
</head>
<body>
  <div class="container">
    <h1>🧠 AI SIÊU VIP - DASHBOARD</h1>

    <div class="card">
      <div class="flex">
        <div>
          <div class="label">🌐 Phiên dự đoán tiếp theo</div>
          <div class="prediction" id="pred-phien">...</div>
        </div>
        <div>
          <div class="label">🎯 Dự đoán</div>
          <div class="prediction" id="pred-value">...</div>
        </div>
        <div>
          <div class="label">📊 Độ tin cậy</div>
          <div class="value" id="pred-confidence">...%</div>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="flex">
        <div>
          <div class="label">✅ Chính xác (50 phiên gần nhất)</div>
          <div class="accuracy" id="acc50">...</div>
        </div>
        <div>
          <div class="label">✅ Chính xác (100 phiên gần nhất)</div>
          <div class="accuracy" id="acc100">...</div>
        </div>
        <div>
          <div class="label">📚 Tổng số phiên đã thu thập</div>
          <div class="value" id="total-sessions">0</div>
        </div>
      </div>
    </div>

    <div class="card">
      <h3>🏋️ Trọng số chuyên gia (Hội đồng AI)</h3>
      <div class="expert-weights" id="weights-container">
        <div class="loading">Đang tải...</div>
      </div>
    </div>

    <div class="card">
      <h3>📜 Lịch sử dự đoán gần đây</h3>
      <table id="history-table">
        <thead>
          <tr>
            <th>Phiên</th>
            <th>Dự đoán</th>
            <th>Kết quả</th>
            <th>Độ tin cậy</th>
            <th>Đúng?</th>
          </tr>
        </thead>
        <tbody>
          <tr><td colspan="5" class="loading">Chưa có dữ liệu</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <script>
    async function fetchData() {
      try {
        const res = await fetch('/api/status-data');
        const data = await res.json();

        // Dự đoán hiện tại
        if (data.current_prediction) {
          document.getElementById('pred-phien').textContent = '#' + data.current_prediction.phien_du_doan;
          document.getElementById('pred-value').textContent = data.current_prediction.du_doan;
          document.getElementById('pred-confidence').textContent = data.current_prediction.do_tin_cay + '%';
        } else {
          document.getElementById('pred-phien').textContent = '...';
          document.getElementById('pred-value').textContent = '...';
          document.getElementById('pred-confidence').textContent = '...%';
        }

        // Độ chính xác
        document.getElementById('acc50').textContent = data.overall_accuracy_50 || 'N/A';
        document.getElementById('acc100').textContent = data.overall_accuracy_100 || 'N/A';
        document.getElementById('total-sessions').textContent = data.total_sessions || 0;

        // Trọng số chuyên gia
        const weightsDiv = document.getElementById('weights-container');
        weightsDiv.innerHTML = '';
        if (data.expert_weights) {
          const entries = Object.entries(data.expert_weights);
          const maxWeight = Math.max(...entries.map(e => e[1]), 1);
          for (const [name, weight] of entries) {
            const percent = (weight / maxWeight * 100).toFixed(0);
            weightsDiv.innerHTML += \`
              <div class="weight-item">
                <span>🤖 \${name}</span>
                <span>\${weight.toFixed(2)}</span>
              </div>
              <div class="bar"><div class="bar-fill" style="width:\${percent}%"></div></div>
            \`;
          }
        }

        // Lịch sử dự đoán
        const tbody = document.querySelector('#history-table tbody');
        tbody.innerHTML = '';
        if (data.prediction_history && data.prediction_history.length > 0) {
          for (const item of data.prediction_history) {
            const row = document.createElement('tr');
            row.innerHTML = \`
              <td>\${item.phien}</td>
              <td>\${item.du_doan}</td>
              <td>\${item.ket_qua}</td>
              <td>\${item.do_tin_cay}%</td>
              <td class="\${item.dung ? 'correct' : 'wrong'}">\${item.dung ? '✔️' : '❌'}</td>
            \`;
            tbody.appendChild(row);
          }
        } else {
          tbody.innerHTML = '<tr><td colspan="5">Chưa có lịch sử dự đoán</td></tr>';
        }
      } catch (err) {
        console.error(err);
      }
    }

    fetchData();
    setInterval(fetchData, 3000);
  </script>
</body>
</html>
    `);
});

// ==================== KHỞI ĐỘNG ====================
app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🚀] SERVER AI SIÊU VIP 24/7 (PORT ${PORT})`);
    console.log(`[📊] Dashboard: http://localhost:${PORT}/status`);
    initSocket();
});