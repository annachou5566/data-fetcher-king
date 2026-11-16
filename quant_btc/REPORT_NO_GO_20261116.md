# BTCUSDT USDⓈ-M Perpetual — nghiên cứu backtest đã khóa, 2026-11-16

**Kết luận: NO-GO.** Không triển khai signal bot, không tạo lệnh thật, không tiếp tục điều chỉnh cấu hình để 'cứu' blind test. Kết quả tiêu cực là kết quả nghiên cứu hợp lệ, **không** khẳng định thị trường không thể có bất kỳ lợi thế giao dịch nào.

## Tính toàn vẹn và trình tự

- Đã tự tải từ Binance Vision: 97 ZIP 1h, 97 ZIP 4h, 97 ZIP 1d, 82 ZIP funding. Tổng cộng 373/373 ZIP khớp SHA-256 chính thức. Binance public funding API bổ sung 45 sự kiện tháng 11/2026.
- Kline 1h: 60.264 nến; 4h: 15.066; 1d: 2.511, đúng slot UTC từ 2020-01-01 đến 2026-11-15. Không thiếu/trùng/sai OHLC nội tại. Funding: 7.533 sự kiện, đến 2026-11-15 16:00 UTC.
- Phát hiện 5 cửa sổ lệch khi tổng hợp 1h lên 4h/1d, bao gồm nến 2024-10-28 20:00 UTC 1h có volume=0 và lệch API Binance hiện tại. Có mask cấm giao dịch; toàn bộ trong giai đoạn 2020–2024. Trong blind có 0 giờ bị mask.
- PREREGISTRATION_V1.md đã cố định trước mọi chạy chiến lược; SHA-256 `52733a19faf42b6296808f77fe976642b34d30a1d84db06204f4a5193272268c`. 3 giả thuyết × 6 cấu hình = 18; thêm 2 phép biến thiên ±20% trên 1 ứng viên = **20/36** phép thử duy nhất. Không mở thêm cấu hình sau OOS.
- Train: 2020-01-01–2023-12-31; validation: 2024; **blind 2025-01-01–2026-11-15 chỉ chạy 1 lần** sau khi chọn ứng viên `H2_L96_A3`, 2 phép robustness đều qua tiêu chí validation.
- Test code lookahead: kiểm chứng sửa dữ liệu giá/funding tương lai không thay đổi tín hiệu trước đó (20/20 cấu hình). Mô hình khớp nến kế tiếp theo giá OPEN; stop kiểm tra intrabar sau khi khớp, trailing stop chỉ thay đổi sau đóng nến. Không có take-profit nên trường hợp SL và TP cùng nến không phát sinh.

## Mô hình chi phí / khớp / quản trị rủi ro

- Taker: **0,05% mỗi chiều**. Adverse slippage: **0,03% mỗi chiều**; stress **0,06% mỗi chiều**. Entry next-open, exit stop/next-open, fee hai chiều thực trừ vào USDT equity; không dựa vào maker/rebate.
- Funding: lịch sử settled rate và event timestamp thực từ Binance Vision / Binance public API; cashflow = `-side × quantity × price_proxy × actual_rate`. **Hạn chế:** price_proxy là open nến 1h tại giờ settlement, không phải dữ liệu mark price tick đúng thời điểm; vì vậy USDT funding là xấp xỉ có ghi rõ, không được tuyên bố chính xác đến đồng.
- Vốn khởi đầu: 10.000 USDT mỗi segment; định cỡ lệnh để mức mất dự kiến khi chạm stop gồm phí và stress-slip không quá 0,5% equity trước lệnh; giới hạn notional 3× equity, không bình quân lỗ. Không mô hình hóa thanh lý margin/gap cực đoan; maxDD đo trên equity hàng giờ.

## Blind test: một ứng viên duy nhất

**H2_L96_A3** = quyết định momentum từ 96 nến 1h đã đóng vào 23:00 UTC mỗi ngày; thay đổi vị thế ở nến 00:00 UTC kế tiếp; trailing stop 3× ATR(20). Chỉ số thực sự do `research.py` tính trên 2025-01-01 đến 2026-11-15 23:00 UTC (USDT). Số lệnh là các round-trip đã đóng.

| Chỉ tiêu | Mô hình gốc | Stress slippage ×2 |
|---|---:|---:|
| Số lệnh | 433 | 433 |
| Win rate | 30,254% | 29,561% |
| Expectancy (R) | −0,05978 | −0,09182 |
| Profit factor | 0,85276 | 0,79079 |
| Sharpe, 24/7 annualized, rf=0 | −0,82117 | −1,24823 |
| Max drawdown (hourly MTM) | 20,374% | 24,677% |
| Return | −13,019% | −18,849% |
| Net PnL trên 10.000 USDT | −1.301,85 USDT | −1.884,86 USDT |
| Tổng taker fees (đã gồm PnL) | 1.104,93 USDT | 1.068,78 USDT |
| Funding net (proxy mark) | +2,93 USDT | +2,30 USDT |

Các khoản phí trong mỗi cột phản ánh quy mô vị thế/equity phát triển khác nhau khi stress, không giữ nguyên quantity và cộng thêm một khoản phí nhân đôi giả tạo.

**Bootstrap:** 1.000 lần resample trade R với seed cố định 20261116; 95% CI expectancy = **[−0,17574R, +0,07773R]** → chứa 0.

### Theo năm (mỗi năm reset 10.000 USDT; khác với việc gộp năm có lãi kép)

| Năm | Số lệnh | PF | Exp (R) | Return | MaxDD | Sharpe |
|---|---:|---:|---:|---:|---:|---:|
| 2025 | 231 | 0,95479 | −0,01470 | −2,135% | 10,077% | −0,18334 |
| 2026 đến 15/11 | 202 | 0,73581 | −0,11134 | −11,121% | 14,945% | −1,58191 |

### Đối chứng

- 1× mua-giữ **perpetual long** (trừ phí hai chiều và funding theo mô hình cùng một giá mark proxy): return **−38,407%**, Sharpe **−0,3420**, MaxDD **56,898%**. **Không** dùng buy-and-hold R (không có stop); giá trị R của raw JSON ở đối chứng này chỉ là placeholder kỹ thuật, **không được diễn giải như expectancy theo R**.
- Buy-and-hold perennial exposure-scaled theo vol chiến lược: hệ số exposure **0,1852**, return **−7,112%**, Sharpe **−0,4895**. Đây là so sánh rủi ro gần đúng (scaled equity path), chưa phải vol-target tái cân bằng 24h và không phải benchmark chứng minh alpha.
- 1.000 ngẫu nhiên entry/direction với thời lượng dự kiến theo lệnh thật, ATR stop và cùng chi phí: percentile 95 Sharpe **−0,1409**; median **−1,3882**. Cấu hình thật Sharpe **−0,8212**, không vượt percentile 95.
- 1.000 dịch lệch lịch giao dịch giữ hướng theo lịch kết quả: percentile 95 Sharpe **−0,1415**; median **−1,4337**; chiến lược không vượt percentile 95.
- **Hạn chế đối chứng quan trọng:** do chồng lấp/lịch dịch và stop, số lệnh thực tế của 1.000 mô phỏng random là **403–425**, shifted **413–430**, so với **433** lệnh thật. Vì thế không thể khẳng định đây là các phép thử frequency-matched hoàn hảo và không dùng chúng để chứng minh có alpha. Kết luận NO-GO đã được chứng minh độc lập bằng PF, expectancy, DD, Sharpe, stress và từng năm không đạt.

## Đối chiếu tiêu chí đăng ký trước

- Có >=200 giao dịch OOS: **PASS 433**.
- PF>=1,3: **FAIL 0,853**.
- Exp R>0: **FAIL −0,05978**.
- MaxDD<=15%: **FAIL 20,37%**.
- Sharpe>0: **FAIL −0,821**.
- Năm 2025/2026 đều có lãi: **FAIL, cả hai lỗ**.
- 95% CI exp thấp hơn 0 nhưng cận trên >0: **FAIL**.
- Stress PF>=1,1, Exp R>0: **FAIL**.
- Better risk-adjusted than buy-hold: **FAIL**.
- Control Sharpe > p95 cả hai: **FAIL** (và caveat chênh số lệnh như trên).

## Nguyên nhân có thể khiến validation tạo ảo giác lợi thế

1. Data-snooping trong chính tập 18 cấu hình, rồi chọn một ứng viên từ 2024; kiểm soát 20/36 không xóa hoàn toàn multiple-testing.
2. Chế độ thị trường 2024 khác 2025–2026 (vi cấu trúc, các pha biến động, funding regime).
3. Giá vào/ra OHLC 1h, không có orderbook, latency, funding mark tick, partial fills hoặc thanh lý ký quỹ — backtest vẫn có sai số thực thi; giả định không tự biến một PF <1 thành có edge.
4. Tổ hợp momentum + trailing stop có lãi theo những trend nhất định nhưng thường thua bởi nhiễu, gãy trend và chi phí.
5. 4h/1d archives có khác biệt so với tổng hợp 1h; dữ liệu QC mask chỉ giảm một phần rủi ro và không chữa được mọi sai sót lịch sử.
6. Một BTC perpetual, không có diversification; nghiên cứu học thuật về time-series momentum đa tài sản, horizon tháng không được tự suy diễn sang một BTC 1h.
7. Bootstrap giao dịch độc lập không bảo toàn autocorrelation/regime; độ rộng CI có thể bị đánh giá thấp.
8. Đối chứng mô phỏng chưa frequency-match hoàn hảo; không dùng để chứng minh ý nghĩa thống kê mạnh.

## Quyết định hành động

**NO-GO cho việc phát tín hiệu giao dịch từ 3 giả thuyết/18 cấu hình đã đăng ký.** Không tạo live bot, không phát lệnh, không chuyển sang paper alert dựa trên chiến lược thua, không tiếp tục quét tham số hoặc dùng lại cùng blind test để tối ưu. Repo public `data-fetcher-king` nếu được dùng chỉ lưu mã nguồn/tài liệu để kiểm tra tái lập; **không đặt cron tự chạy lại blind**. Nếu sau này mở nghiên cứu giả thuyết *mới*, cần một OOS tương lai chưa từng thấy, đăng ký lại trước và giữ rõ ranh giới nghiên cứu độc lập.
