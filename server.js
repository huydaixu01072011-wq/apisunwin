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

// Cấu hình WebSocket
const WEBSOCKET_URL = "wss://websocket.azhkthg1.net/websocket?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhbW91bnQiOjAsInVzZXJuYW1lIjoiU0NfYXBpc3Vud2luMTIzIn0.hgrRbSV6vnBwJMg9ZFtbx3rRu9mX_hZMZ_m5gMNhkw0";
const WS_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Origin": "https://play.sun.win"
};

const RECONNECT_DELAY = 2000;
const PING_INTERVAL = 10000;
const MAX_HISTORY = 100; // Tăng lên 100 để thuật toán có đủ data phân tích

// Biến lưu trữ trạng thái
let apiResponseData = { phien_hien_tai: null, lich_su_phien: [] };
let predictData = null;
let currentSessionId = null;
let lastSessionId = null;
const sessionHistory = [];

// Thống kê dự đoán
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

// --- THUẬT TOÁN DỰ ĐOÁN SIÊU VIP (PATTERN MATCHING) ---
function predictNext(history) {
    if (history.length < 5) {
        return {
            du_doan: Math.random() > 0.5 ? "Tài" : "Xỉu",
            do_tin_cay: 50 // Chưa đủ data
        };
    }

    const patternLength = 3; // Lấy 3 kết quả gần nhất làm mẫu
    const recentPattern = history.slice(-patternLength).map(x => x.ket_qua).join('-');

    let t_count = 0;
    let x_count = 0;

    // Quét lại toàn bộ lịch sử xem chuỗi này từng xuất hiện chưa, và kết quả tiếp theo là gì
    for (let i = 0; i < history.length - patternLength; i++) {
        const pattern = history.slice(i, i + patternLength).map(x => x.ket_qua).join('-');
        if (pattern === recentPattern) {
            const nextResult = history[i + patternLength].ket_qua;
            if (nextResult === 'Tài') t_count++;
            if (nextResult === 'Xỉu') x_count++;
        }
    }

    const totalMatches = t_count + x_count;

    if (totalMatches === 0) {
        // Nếu chuỗi mới hoàn toàn, dự đoán dựa trên xu hướng đang nghiêng về bên nào trong 10 phiên gần nhất
        const recentTai = history.slice(-10).filter(x => x.ket_qua === 'Tài').length;
        return {
            du_doan: recentTai >= 5 ? "Tài" : "Xỉu",
            do_tin_cay: 55
        };
    }

    const du_doan = t_count > x_count ? "Tài" : "Xỉu";
    const do_tin_cay = Math.round((Math.max(t_count, x_count) / totalMatches) * 100);

    return { du_doan, do_tin_cay: do_tin_cay < 50 ? 50 : do_tin_cay };
}

// --- XỬ LÝ WEBSOCKET ---
function heartbeat() {
    clearTimeout(heartbeatTimeout);
    heartbeatTimeout = setTimeout(() => {
        console.log('[⚠️] Mất kết nối mạng (Socket treo), đang ép khởi động lại...');
        if (ws) ws.terminate(); // Ép đóng để trigger event 'close' gọi kết nối lại
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
        heartbeat(); // Bắt đầu đếm ngược heartbeat

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

    ws.on('ping', heartbeat);
    ws.on('pong', () => {
        // console.log('[📶] Ping OK.');
        heartbeat(); // Reset đếm ngược khi nhận pong
    });

    ws.on('message', (message) => {
        try {
            const data = JSON.parse(message);

            if (!Array.isArray(data) || typeof data[1] !== 'object') return;

            const { cmd, sid, d1, d2, d3, gBB } = data[1];

            if (cmd === 1008 && sid) {
                currentSessionId = sid;
            }

            if (cmd === 1003 && gBB) {
                if (!d1 || !d2 || !d3) return;

                // Xử lý chống mất phiên
                let activeSession = currentSessionId;
                if (!activeSession && lastSessionId) activeSession = lastSessionId + 1;
                lastSessionId = activeSession; // Lưu lại phiên cuối

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

                sessionHistory.push(sessionData);
                if (sessionHistory.length > MAX_HISTORY) sessionHistory.shift();

                // --- KIỂM TRA DỰ ĐOÁN TRƯỚC ĐÓ ---
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

                    // Giữ lại lịch sử 30 dự đoán gần nhất
                    if (predictionStats.history.length > 30) predictionStats.history.pop();
                }

                // --- TẠO DỰ ĐOÁN CHO PHIÊN TỚI ---
                const nextPhien = activeSession ? activeSession + 1 : "Đang cập nhật";
                const prediction = predictNext(sessionHistory);
                pendingPrediction = { phien: nextPhien, du_doan: prediction.du_doan };

                // --- CẬP NHẬT DATA API ---
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
                    phien_hien_tai: nextPhien,
                    du_doan: prediction.du_doan,
                    do_tin_cay: `${prediction.do_tin_cay}%`
                };
                
                console.log(`[🎲] Phiên ${activeSession}: ${total} (${resultText}) | Tỉ lệ đúng/sai: ${predictionStats.correct}/${predictionStats.wrong}`);
                currentSessionId = null; // Reset
            }
        } catch (e) {
            console.error('[❌] Lỗi xử lý message:', e.message);
        }
    });

    ws.on('close', (code, reason) => {
        console.log(`[🔌] Mất kết nối. Bắt đầu kết nối lại sau ${RECONNECT_DELAY}ms...`);
        clearTimeout(heartbeatTimeout);
        clearInterval(pingInterval);
        setTimeout(connectWebSocket, RECONNECT_DELAY);
    });

    ws.on('error', (err) => {
        console.error('[❌] Lỗi Socket:', err.message);
        ws.terminate();
    });
}

// --- ROUTES ---

app.get('/sunlon', (req, res) => {
    res.json(apiResponseData);
});

app.get('/predict', (req, res) => {
    if (!predictData) {
        return res.json({ error: "Đang thu thập dữ liệu, vui lòng đợi..." });
    }
    res.json(predictData);
});

app.get('/status', (req, res) => {
    const winRate = predictionStats.total > 0 
        ? Math.round((predictionStats.correct / predictionStats.total) * 100) 
        : 0;

    res.json({
        thong_ke_he_thong: {
            tong_du_doan: predictionStats.total,
            du_doan_dung: predictionStats.correct,
            du_doan_sai: predictionStats.wrong,
            ti_le_thang: `${winRate}%`
        },
        lich_su_du_doan: predictionStats.history
    });
});

app.get('/', (req, res) => {
    res.send(`
        <h2>🎯 Hệ thống API Tài Xỉu Siêu VIP</h2>
        <ul>
            <li><a href="/sunlon">/sunlon</a> - Dữ liệu phiên mới nhất và lịch sử (JSON)</li>
            <li><a href="/predict">/predict</a> - Dự đoán kết quả phiên tiếp theo (JSON)</li>
            <li><a href="/status">/status</a> - Thống kê hiệu suất thuật toán (JSON)</li>
        </ul>
    `);
});

app.listen(PORT, '0.0.0.0', () => {
    console.log(`[🌐] Server đang chạy tại cổng ${PORT}`);
    connectWebSocket();
});
