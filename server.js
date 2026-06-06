const WebSocket = require('ws');
const express = require('express');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.set('etag', false);
app.use((req, res, next) => {
    res.set('Cache-Control', 'no-store');
    next();
});

const PORT = 5000;
const HISTORY_FILE = path.join(__dirname, 'predict_history.json');

// Cấu hình dữ liệu gốc
let apiResponseData = {
    id: "HuyDaiXuVN",
    phien_hien_tai: null,
    xuc_xac_1: null,
    xuc_xac_2: null,
    xuc_xac_3: null,
    tong: null,
    ket_qua: "",
    lich_su_phien: []
};

let currentSessionId = null;
let sessionHistory = [];
const MAX_HISTORY = 20;

let predictHistory = [];
const MAX_PREDICT_HISTORY = 20;
let stats = {
    tong_du_doan: 0,
    dung: 0,
    sai: 0
};

// Khôi phục dữ liệu từ file JSON chống sập/restart server
function loadPredictData() {
    try {
        if (fs.existsSync(HISTORY_FILE)) {
            const data = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf8'));
            predictHistory = data.predictHistory || [];
            stats = data.stats || { tong_du_doan: 0, dung: 0, sai: 0 };
            console.log('[💾] Hệ thống đã khôi phục lịch sử từ file JSON.');
        }
    } catch (e) {
        console.error('[❌] Lỗi đọc file dự phòng:', e.message);
    }
}

function savePredictData() {
    try {
        const dataToSave = { predictHistory, stats };
        fs.writeFileSync(HISTORY_FILE, JSON.stringify(dataToSave, null, 2), 'utf8');
    } catch (e) {
        console.error('[❌] Không thể ghi file sao lưu dữ liệu:', e.message);
    }
}

loadPredictData();

// THUẬT TOÁN ĐỊNH HƯỚNG SIÊU VIP (100% ĐỊNH LƯỢNG - KHÔNG RANDOM)
function analyzeAndPredictVIP(history) {
    if (!history || history.length < 5) {
        return {
            du_doan: "Tài",
            do_tin_cay: 60,
            phan_tich: "Hệ thống đang tích lũy dữ liệu nền tảng (Cần tối thiểu 5 phiên thực tế)."
        };
    }

    let scoreTai = 0;
    let scoreXiu = 0;
    let logs = [];

    const results = history.map(h => h.ket_qua); 
    const sums = history.map(h => h.tong);
    const len = results.length;
    const latestResult = results[len - 1];
    const latestSum = sums[len - 1];

    // CƠ CHẾ 1: Đối sánh chuỗi mẫu hình quá khứ (Pattern Matching Markov)
    if (len >= 4) {
        const samplePattern = results.slice(-2).join(','); // Lấy 2 phiên gần nhất làm mẫu
        let matchTai = 0;
        let matchXiu = 0;

        for (let i = 0; i < len - 3; i++) {
            const currentSub = results.slice(i, i + 2).join(',');
            if (currentSub === samplePattern) {
                const nextResult = results[i + 2];
                if (nextResult === 'Tài') matchTai++;
                if (nextResult === 'Xỉu') matchXiu++;
            }
        }
        if (matchTai > matchXiu) {
            scoreTai += 4.5;
            logs.push(`Mẫu chuỗi [${samplePattern}] trong quá khứ thiên về Tài`);
        } else if (matchXiu > matchTai) {
            scoreXiu += 4.5;
            logs.push(`Mẫu chuỗi [${samplePattern}] trong quá khứ thiên về Xỉu`);
        }
    }

    // CƠ CHẾ 2: Phân tích bệt nâng cao & Điểm gãy xu hướng
    let betStreak = 1;
    for (let i = len - 2; i >= 0; i--) {
        if (results[i] === latestResult) betStreak++;
        else break;
    }
    if (betStreak >= 3) {
        if (betStreak >= 5) { // Bệt quá dài -> Tỷ lệ bẻ cầu cực cao
            if (latestResult === 'Tài') { scoreXiu += 6; logs.push(`Cầu bệt Tài chạm đỉnh (${betStreak} tay) -> Ưu tiên bẻ cầu Xỉu`); }
            else { scoreTai += 6; logs.push(`Cầu bệt Xỉu chạm đỉnh (${betStreak} tay) -> Ưu tiên bẻ cầu Tài`); }
        } else { // Bệt ngắn -> Thuận thế đẩy theo dòng cầu
            if (latestResult === 'Tài') { scoreTai += 4; logs.push(`Cầu bệt Tài đang thuận (${betStreak} tay) -> Nuôi tiếp Tài`); }
            else { scoreXiu += 4; logs.push(`Cầu bệt Xỉu đang thuận (${betStreak} tay) -> Nuôi tiếp Xỉu`); }
        }
    }

    // CƠ CHẾ 3: Phân tích nhịp sóng đối ứng 1-1 và 2-2
    // Xét nhịp 1-1
    let is11 = true;
    for (let i = 0; i < Math.min(len - 1, 4); i++) {
        if (results[len - 1 - i] === results[len - 2 - i]) { is11 = false; break; }
    }
    if (is11 && len >= 4) {
        if (latestResult === 'Tài') { scoreXiu += 5; logs.push("Sóng đối ứng nhịp 1-1 đang chạy ổn định -> Đánh Xỉu"); }
        else { scoreTai += 5; logs.push("Sóng đối ứng nhịp 1-1 đang chạy ổn định -> Đánh Tài"); }
    }

    // CƠ CHẾ 4: Lực nén Hồi quy biên độ Tổng Điểm xúc xắc
    if (latestSum >= 15) {
        scoreXiu += 5.5;
        logs.push(`Tổng điểm đột biến lớn (${latestSum}) -> Lực hút nén kéo về Xỉu cực mạnh`);
    } else if (latestSum <= 6) {
        scoreTai += 5.5;
        logs.push(`Tổng điểm đột biến nhỏ (${latestSum}) -> Lực đẩy hồi phục về Tài cực mạnh`);
    } else {
        // Hồi quy tiệm cận trung vị 10.5
        if (latestSum > 10) { scoreXiu += 1.5; } 
        else { scoreTai += 1.5; }
    }

    // CƠ CHẾ 5: Trọng số tiệm cận thời gian (Phiên càng gần điểm cộng càng cao)
    for (let i = 0; i < Math.min(len, 3); i++) {
        const weight = (3 - i) * 1.5;
        if (results[len - 1 - i] === 'Tài') scoreTai += weight;
        else scoreXiu += weight;
    }

    // TỔNG HỢP VÀ ĐƯA RA ĐIỂM SỐ CUỐI CÙNG
    let finalPrediction = scoreTai >= scoreXiu ? "Tài" : "Xỉu";
    let diff = Math.abs(scoreTai - scoreXiu);
    let maxScore = Math.max(scoreTai, scoreXiu);

    // Tính toán độ tin cậy khoa học (Khống chế từ 58% - 96%)
    let confidence = 55 + Math.floor((diff / (maxScore || 1)) * 41);
    if (confidence > 96) confidence = 96;
    if (confidence < 58) confidence = 58;

    return {
        du_doan: finalPrediction,
        do_tin_cay: confidence,
        phan_tich: logs.length > 0 ? logs.join(' ✦ ') : "Dữ liệu cân bằng, phân tích dựa trên biến thiên trọng số dao động xúc xắc."
    };
}

// Cấu hình WebSocket kết nối Sunwin
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win"
};
const RECONNECT_DELAY = 2500;
const PING_INTERVAL = 15000;

const initialMessages = [
    [1, "MiniGame", "GM_fbbdbebndbbc", "123123p", {
        "info": "{\"ipAddress\":\"2402:800:62cd:cb7c:1a7:7a52:9c3e:c290\",\"wsToken\":\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJnZW5kZXIiOjAsImNhblZpZXdTdGF0IjpmYWxzZSwiZGlzcGxheU5hbWUiOiJuZG5lYmViYnMiLCJib3QiOjAsImlzTWVyY2hhbnQiOmZhbHNlLCJ2ZXJpZmllZEJhbmtBY2NvdW50IjpmYWxzZSwicGxheUV2ZW50TG9iYnkiOmZhbHNlLCJjdXN0b21lcklkIjozMTIxMDczMTUsImFmZklkIjoiR0VNV0lOIiwiYmFubmVkIjpmYWxzZSwiYnJhbmQiOiJnZW0iLCJ0aW1lc3RhbXAiOjE3NTQ5MjYxMDI1MjcsImxvY2tHYW1lcyI6W10sImFtb3VudCI6MCwibG9ja0NoYXQiOmZhbHNlLCJwaG9uZVZlcmlmaWVkIjpmYWxzZSwiaXBBZGRyZXNzIjoiMjQwMjo4MDA6NjJjZDpjYjdjOjFhNzo3YTUyOjljM2U6YzI5MCIsIm11dGUiOmZhbHNlLCJhdmF0YXIiOiJodHRwczovL2ltYWdlcy5zd2luc2hvcC5uZXQvaW1hZ2VzL2F2YXRhci9hdmF0YXJfMDEucG5nIiwicGxhdGZvcm1JZCI6NSwidXNlcklkIjoiN7RhNDlhNDQtMjlhYS00ZmRiLWJkNGMtNjU5OTQ5YzU3NDdkIiwicmVnVGltZSI6MTc1NDkyNjAyMjUxNSwicGhvbmUiOiIiLCJkZXBvc2l0IjpmYWxzZSwidXNlcm5hbWUiOiJHTV_mYmJkYmVibmRiYmMifQ.DAyEeoAnz8we-Qd0xS0tnqOZ8idkUJkxksBjr_Gei8A\", \"locale\":\"vi\",\"userId\":\"7da49a44-29aa-4fdb-bd4c-659949c5747d\",\"username\":\"GM_fbbdbebndbbc\",\"timestamp\":1754926102527,\"refreshToken\":\"7cc4ad191f4348849f69427a366ea0fd.a68ece9aa85842c7ba523170d0a4ae3e\"}",
        "signature": "53D9E12F910044B140A2EC659167512E2329502FE84A6744F1CD5CBA9B6EC04915673F2CBAE043C4EDB94DDF88F3D3E839A931100845B8F179106E1F44ECBB4253EC536610CCBD0CE90BD8495DAC3E8A9DBDB46FE49B51E88569A6F117F8336AC7ADC226B4F213ECE2F8E0996F2DD5515476C8275F0B2406CDF2987F38A6DA24"
    }],
    [6, "MiniGame", "taixiuPlugin", { cmd: 1005 }],
    [6, "MiniGame", "lobbyPlugin", { cmd: 10001 }]
];

let ws = null;
let pingInterval = null;
let reconnectTimeout = null;

function connectWebSocket() {
    if (ws) {
        ws.removeAllListeners();
        ws.close();
    }

    ws = new WebSocket(WEBSOCKET_URL, { headers: WS_HEADERS });

    ws.on('open', () => {
        console.log('[✅] Kết nối thành công máy chủ Sunwin.');
        initialMessages.forEach((msg, i) => {
            setTimeout(() => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(JSON.stringify(msg));
                }
            }, i * 600);
        });

        clearInterval(pingInterval);
        pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
                ws.ping();
            }
        }, PING_INTERVAL);
    });

    ws.on('pong', () => { /* Ping giữ luồng ổn định */ });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);
            if (!Array.isArray(data) || typeof data[1] !== 'object') return;

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
                apiResponseData.phien_hien_tai = sid;
            }

            if (cmd === 1003 && gBB) {
                if (d1 === undefined || d2 === undefined || d3 === undefined) return;

                const targetSid = sid || currentSessionId;
                if (!targetSid) return;

                const total = d1 + d2 + d3;
                const resultText = (total > 10) ? 'Tài' : 'Xỉu';

                const sessionData = {
                    phien: targetSid,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: resultText
                };

                const existingIndex = sessionHistory.findIndex(s => s.phien === targetSid);
                if (existingIndex !== -1) {
                    sessionHistory[existingIndex] = sessionData;
                } else {
                    sessionHistory.push(sessionData);
                }

                sessionHistory.sort((a, b) => a.phien - b.phien);
                while (sessionHistory.length > MAX_HISTORY) {
                    sessionHistory.shift();
                }

                apiResponseData = {
                    ...apiResponseData,
                    phien_hien_tai: targetSid,
                    xuc_xac_1: d1,
                    xuc_xac_2: d2,
                    xuc_xac_3: d3,
                    tong: total,
                    ket_qua: resultText,
                    lich_su_phien: [...sessionHistory]
                };

                // ĐỐI CHIẾU DỰ ĐOÁN KHI CÓ DATA KẾT QUẢ THỰC TẾ
                const matchingPredict = predictHistory.find(p => p.phien === targetSid && p.trang_thai === "CHỜ KẾT QUẢ");
                if (matchingPredict) {
                    matchingPredict.ket_qua_thuc_te = resultText;
                    stats.tong_du_doan += 1;
                    
                    if (matchingPredict.du_doan === resultText) {
                        matchingPredict.trang_thai = "ĐÚNG";
                        stats.dung += 1;
                    } else {
                        matchingPredict.trang_thai = "SAI";
                        stats.sai += 1;
                    }
                    savePredictData();
                }
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý luồng tin nhắn:', e.message);
        }
    });

    ws.on('close', () => {
        clearInterval(pingInterval);
        clearTimeout(reconnectTimeout);
        reconnectTimeout = setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', () => {
        ws.close();
    });
}

// 1. ENDPOINT API: GET /predict
app.get('/predict', (req, res) => {
    if (!apiResponseData.phien_hien_tai) {
        return res.status(400).json({
            error: "Chưa kết nối dữ liệu máy chủ hoặc phiên chưa khởi tạo."
        });
    }

    const currentPhien = apiResponseData.phien_hien_tai;
    const nextPhien = currentPhien + 1;

    const analysis = analyzeAndPredictVIP(sessionHistory);

    let existingPredict = predictHistory.find(
        p => p.phien === nextPhien
    );

    if (!existingPredict) {
        existingPredict = {
            phien: nextPhien,
            du_doan: analysis.du_doan,
            ket_qua_thuc_te: "",
            trang_thai: "CHỜ KẾT QUẢ",
            do_tin_cay: analysis.do_tin_cay,
            phan_tich: analysis.phan_tich
        };

        predictHistory.push(existingPredict);

        if (predictHistory.length > MAX_PREDICT_HISTORY) {
            predictHistory.shift();
        }

        savePredictData();
    }

    let winRate = "0.00%";

    if (stats.tong_du_doan > 0) {
        winRate =
            ((stats.dung / stats.tong_du_doan) * 100).toFixed(2) + "%";
    }

    const lastSession =
        sessionHistory[sessionHistory.length - 1] || {};

    res.json({
        id: "HuyDaiXuVN",

        phien: lastSession.phien || null,
        xuc_xac_1: lastSession.xuc_xac_1 || null,
        xuc_xac_2: lastSession.xuc_xac_2 || null,
        xuc_xac_3: lastSession.xuc_xac_3 || null,
        tong: lastSession.tong || null,
        ket_qua: lastSession.ket_qua || null,

        phien: nextPhien,
        du_doan: existingPredict.du_doan,
        do_tin_cay: existingPredict.do_tin_cay,
        phan_tich: existingPredict.phan_tich,

        thang: stats.dung,
        thua: stats.sai,
        ty_le_dung: winRate
    });
});
// 2. ENDPOINT API: GET /history
app.get('/history', (req, res) => {
    const formattedHistory = [...predictHistory].reverse().map(item => ({
        phien: item.phien,
        du_doan: item.du_doan,
        ket_qua_thuc_te: item.ket_qua_thuc_te || "Chờ lắc...",
        trang_thai: item.trang_thai
    }));
    res.json(formattedHistory);
});

// 3. ENDPOINT API: GET /stats
app.get('/stats', (req, res) => {
    let winRate = "0.00%";
    if (stats.tong_du_doan > 0) {
        winRate = ((stats.dung / stats.tong_du_doan) * 100).toFixed(2) + "%";
    }
    res.json({
        tong_du_doan: stats.tong_du_doan,
        dung: stats.dung,
        sai: stats.sai,
        ty_le_dung: winRate
    });
});

// 4. ENDPOINT API: GET /sunlon
app.get('/sunlon', (req, res) => {
    res.json(apiResponseData);
});

// 5. ENDPOINT GIAO DIỆN SIÊU VIP: GET /status
app.get('/status', (req, res) => {
    res.send(`
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>HuyDaiXuVN - VIP Predictor System</title>
        <script src="https://cdn.jsdelivr.net/npm/@tailwindcss/browser@4"></script>
        <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700&family=Inter:wght@300;400;600&display=swap" rel="stylesheet">
        <style>
            body { font-family: 'Inter', sans-serif; background-color: #0b0f19; }
            .font-cyber { font-family: 'Orbitron', sans-serif; }
            .glow-tai { text-shadow: 0 0 12px rgba(239, 68, 68, 0.6); }
            .glow-xiu { text-shadow: 0 0 12px rgba(59, 130, 246, 0.6); }
        </style>
    </head>
    <body class="text-slate-200 min-h-screen flex flex-col justify-between">
        
        <header class="border-b border-slate-800 bg-slate-900/50 backdrop-blur px-6 py-4 flex flex-wrap justify-between items-center gap-4 shadow-lg shadow-black/40">
            <div class="flex items-center gap-3">
                <div class="h-4 w-4 rounded-full bg-emerald-500 animate-pulse"></div>
                <h1 class="font-cyber text-xl font-bold tracking-wider text-transparent bg-clip-text bg-gradient-to-r from-cyan-400 to-indigo-500">
                    HUYDAIXUVN <span class="text-sm font-sans font-normal text-slate-400">| VIP AI ENGINE</span>
                </h1>
            </div>
            <div class="text-sm text-slate-400 bg-slate-950 px-4 py-2 rounded-lg border border-slate-800">
                Hệ thống trạng thái: <span class="text-emerald-400 font-semibold">ONLINE</span>
            </div>
        </header>

        <main class="max-w-7xl w-full mx-auto p-4 lg:p-6 grid grid-cols-1 lg:grid-cols-3 gap-6 flex-grow">
            
            <div class="lg:col-span-2 flex flex-col gap-6">
                <div class="bg-gradient-to-b from-slate-900 to-slate-950 border border-slate-800 rounded-2xl p-6 shadow-xl relative overflow-hidden">
                    <div class="absolute top-0 right-0 w-48 h-48 bg-indigo-500/5 rounded-full blur-3xl pointer-events-none"></div>
                    
                    <div class="flex justify-between items-center mb-6">
                        <span class="text-xs font-semibold tracking-widest text-indigo-400 uppercase">Phiên Tiếp Theo</span>
                        <span id="next-phien" class="font-cyber text-xl font-bold text-slate-100 bg-slate-900 px-3 py-1 rounded-md border border-slate-800">#------</span>
                    </div>

                    <div class="flex flex-col items-center justify-center py-6 text-center">
                        <h2 class="text-sm text-slate-400 tracking-wide mb-2 uppercase">Kết Quả Hệ Thống Chọn</h2>
                        <div id="predict-badge" class="text-6xl font-black tracking-wide py-2 select-none font-cyber text-slate-500">
                            ---
                        </div>
                        <div class="mt-4 flex items-center gap-2 bg-slate-900/80 border border-slate-800 px-4 py-1.5 rounded-full">
                            <span class="text-xs text-slate-400">Độ tin cậy thuật toán:</span>
                            <span id="confidence-rate" class="text-sm font-cyber font-bold text-cyan-400">0%</span>
                        </div>
                    </div>

                    <div class="mt-6 pt-4 border-t border-slate-800/60">
                        <span class="text-xs text-slate-400 font-semibold uppercase block mb-2">Báo Cáo Phân Tích Chuỗi:</span>
                        <p id="analysis-text" class="text-sm text-slate-300 bg-slate-950/60 p-3 rounded-lg border border-slate-900/50 leading-relaxed italic">
                            Đang kết nối dữ liệu máy chủ và tính toán chuỗi nhịp cầu mẫu...
                        </p>
                    </div>
                </div>

                <div class="bg-slate-900/40 border border-slate-800/80 backdrop-blur rounded-2xl p-5 grid grid-cols-2 sm:grid-cols-4 gap-4">
                    <div class="bg-slate-950/50 p-3 rounded-xl border border-slate-900 text-center">
                        <span class="text-xs text-slate-400 block mb-1">Phiên vừa qua</span>
                        <span id="last-phien" class="font-cyber text-sm text-slate-200">#--</span>
                    </div>
                    <div class="bg-slate-950/50 p-3 rounded-xl border border-slate-900 text-center">
                        <span class="text-xs text-slate-400 block mb-1">Xúc xắc lắc</span>
                        <span id="last-dice" class="font-cyber text-sm text-amber-400">?, ?, ?</span>
                    </div>
                    <div class="bg-slate-950/50 p-3 rounded-xl border border-slate-900 text-center">
                        <span class="text-xs text-slate-400 block mb-1">Tổng điểm</span>
                        <span id="last-total" class="font-cyber text-sm text-slate-200">--</span>
                    </div>
                    <div class="bg-slate-950/50 p-3 rounded-xl border border-slate-900 text-center">
                        <span class="text-xs text-slate-400 block mb-1">Cầu thực tế</span>
                        <span id="last-result" class="text-sm font-bold">--</span>
                    </div>
                </div>
            </div>

            <div class="flex flex-col gap-6">
                <div class="bg-gradient-to-b from-slate-900 to-slate-950 border border-slate-800 rounded-2xl p-6 shadow-xl">
                    <h3 class="font-cyber text-sm font-bold tracking-wider mb-6 text-slate-300 uppercase border-b border-slate-800 pb-3">Thống Kê Hiệu Suất</h3>
                    
                    <div class="flex items-center justify-between mb-8">
                        <div>
                            <span class="text-4xl font-cyber font-bold text-slate-100" id="stat-rate">0.0%</span>
                            <span class="text-xs text-slate-400 block mt-1">Tỷ lệ đoán trúng chuẩn</span>
                        </div>
                        <div class="h-14 w-14 rounded-full border-4 border-indigo-500/30 border-t-indigo-500 animate-spin"></div>
                    </div>

                    <div class="space-y-4">
                        <div>
                            <div class="flex justify-between text-xs mb-1">
                                <span class="text-slate-400">Số phiên thắng (ĐÚNG)</span>
                                <span class="text-emerald-400 font-bold" id="stat-win">0</span>
                            </div>
                            <div class="w-full bg-slate-950 h-2 rounded-full overflow-hidden">
                                <div id="bar-win" class="bg-emerald-500 h-full rounded-full transition-all duration-500" style="width: 0%"></div>
                            </div>
                        </div>

                        <div>
                            <div class="flex justify-between text-xs mb-1">
                                <span class="text-slate-400">Số phiên thua (SAI)</span>
                                <span class="text-rose-400 font-bold" id="stat-loss">0</span>
                            </div>
                            <div class="w-full bg-slate-950 h-2 rounded-full overflow-hidden">
                                <div id="bar-loss" class="bg-rose-500 h-full rounded-full transition-all duration-500" style="width: 0%"></div>
                            </div>
                        </div>

                        <div class="flex justify-between text-xs pt-2 border-t border-slate-800/40 text-slate-400">
                            <span>Tổng số phiên đã quét:</span>
                            <span class="font-cyber text-slate-200 font-bold" id="stat-total">0</span>
                        </div>
                    </div>
                </div>
            </div>

            <div class="lg:col-span-3 bg-slate-900/60 border border-slate-800 rounded-2xl p-6 shadow-xl">
                <h3 class="font-cyber text-sm font-bold tracking-wider mb-4 text-slate-300 uppercase">Lịch sử 20 phiên dự đoán gần đây</h3>
                <div class="overflow-x-auto">
                    <table class="w-full text-left border-collapse">
                        <thead>
                            <tr class="border-b border-slate-800 text-xs font-semibold text-slate-400 uppercase bg-slate-950/40">
                                <th class="p-3">Mã Phiên</th>
                                <th class="p-3">Hệ Thống Dự Đoán</th>
                                <th class="p-3">Kết Quả Lắc Thực Tế</th>
                                <th class="p-3 text-center">Trạng Thái</th>
                            </tr>
                        </thead>
                        <tbody id="history-rows" class="text-sm divide-y divide-slate-800/40">
                            <tr>
                                <td colspan="4" class="p-4 text-center text-slate-500 italic">Đang đồng bộ bảng dữ liệu lịch sử từ bộ nhớ...</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </main>

        <footer class="text-center py-4 text-xs text-slate-500 border-t border-slate-900 bg-slate-950/40">
            HuyDaiXuVN © 2026 Engine VIP System. Phân tích hoàn toàn tự động dựa trên thuật toán cốt lõi.
        </footer>

        <script>
            async function updateDashboard() {
                try {
                    // 1. Lấy dữ liệu từ /predict
                    const resPredict = await fetch('/predict');
                    if (resPredict.ok) {
                        const data = await resPredict.json();
                        document.getElementById('next-phien').innerText = '#' + data.phien;
                        
                        const badge = document.getElementById('predict-badge');
                        badge.innerText = data.du_doan;
                        if (data.du_doan === 'Tài') {
                            badge.className = 'text-6xl font-black tracking-wide py-2 font-cyber text-rose-500 glow-tai';
                        } else {
                            badge.className = 'text-6xl font-black tracking-wide py-2 font-cyber text-blue-500 glow-xiu';
                        }
                        
                        document.getElementById('confidence-rate').innerText = data.do_tin_cay + '%';
                        document.getElementById('analysis-text').innerText = data.phan_tich;
                    }

                    // 2. Lấy dữ liệu từ /sunlon để xem kết quả phiên cũ gần nhất
                    const resSunwin = await fetch('/sunlon');
                    if (resSunwin.ok) {
                        const data = await resSunwin.json();
                        if (data.phien_hien_tai) {
                            document.getElementById('last-phien').innerText = '#' + data.phien_hien_tai;
                            document.getElementById('last-dice').innerText = bunderDice(data.xuc_xac_1, data.xuc_xac_2, data.xuc_xac_3);
                            document.getElementById('last-total').innerText = data.tong || '--';
                            
                            const lastResNode = document.getElementById('last-result');
                            lastResNode.innerText = data.ket_qua || '--';
                            if (data.ket_qua === 'Tài') lastResNode.className = 'text-sm font-bold text-rose-500';
                            else if (data.ket_qua === 'Xỉu') lastResNode.className = 'text-sm font-bold text-blue-500';
                        }
                    }

                    // 3. Lấy dữ liệu thống kê từ /stats
                    const resStats = await fetch('/stats');
                    if (resStats.ok) {
                        const data = await resStats.json();
                        document.getElementById('stat-rate').innerText = data.ty_le_dung;
                        document.getElementById('stat-win').innerText = data.dung;
                        document.getElementById('stat-loss').innerText = data.sai;
                        document.getElementById('stat-total').innerText = data.tong_du_doan;

                        // Tính tỷ lệ phần trăm vẽ thanh Progress Bar
                        if (data.tong_du_doan > 0) {
                            const pWin = (data.dung / data.tong_du_doan) * 100;
                            const pLoss = (data.sai / data.tong_du_doan) * 100;
                            document.getElementById('bar-win').style.width = pWin + '%';
                            document.getElementById('bar-loss').style.width = pLoss + '%';
                        }
                    }

                    // 4. Lấy mảng lịch sử từ /history
                    const resHistory = await fetch('/history');
                    if (resHistory.ok) {
                        const list = await resHistory.json();
                        const tbody = document.getElementById('history-rows');
                        if (list.length === 0) {
                            tbody.innerHTML = '<tr><td colspan="4" class="p-4 text-center text-slate-500">Chưa có phiên đối chiếu nào.</td></tr>';
                        } else {
                            let rowsHtml = '';
                            list.forEach(item => {
                                let statusBadge = '';
                                if (item.trang_thai === 'ĐÚNG') {
                                    statusBadge = '<span class="px-2 py-0.5 rounded text-xs font-bold bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">ĐÚNG</span>';
                                } else if (item.trang_thai === 'SAI') {
                                    statusBadge = '<span class="px-2 py-0.5 rounded text-xs font-bold bg-rose-500/10 text-rose-400 border border-rose-500/20">SAI</span>';
                                } else {
                                    statusBadge = '<span class="px-2 py-0.5 rounded text-xs font-bold bg-amber-500/10 text-amber-400 border border-amber-500/20 animate-pulse">CHỜ LẮC</span>';
                                }

                                let predColor = item.du_doan === 'Tài' ? 'text-rose-400' : 'text-blue-400';
                                let factColor = item.ket_qua_thuc_te === 'Tài' ? 'text-rose-400' : (item.ket_qua_thuc_te === 'Xỉu' ? 'text-blue-400' : 'text-slate-500');

                                rowsHtml += \`
                                    <tr class="hover:bg-slate-900/30 transition-colors">
                                        <td class="p-3 font-cyber text-slate-300">#\${item.phien}</td>
                                        <td class="p-3 font-semibold \${predColor}">\${item.du_doan}</td>
                                        <td class="p-3 font-semibold \${factColor}">\${item.ket_qua_thuc_te}</td>
                                        <td class="p-3 text-center">\${statusBadge}</td>
                                    </tr>
                                \`;
                            });
                            tbody.innerHTML = rowsHtml;
                        }
                    }
                } catch (err) {
                    console.error('Lỗi nạp luồng dữ liệu tự động:', err);
                }
            }

            function bunderDice(d1, d2, d3) {
                if(d1 === null || d1 === undefined) return '?, ?, ?';
                return \`\${d1}, \${d2}, \${d3}\`;
            }

            // Gọi hàm chạy lập tức khi load trang và thiết lập vòng lặp quét 2000ms (2 giây)
            updateDashboard();
            setInterval(updateDashboard, 2000);
        </script>
    </body>
    </html>
    `);
});

// Trực quan hóa gốc
app.get('/', (req, res) => {
    res.redirect('/status');
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server AI Siêu VIP đang hoạt động tại cổng ${PORT}`);
    console.log(`[📊] Xem giao diện VIP Dashboard thời gian thực tại: http://localhost:${PORT}/status`);
    connectWebSocket();
});
