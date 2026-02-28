import os
import json
import boto3
from datetime import datetime, timezone
from botocore.config import Config
from supabase import create_client

# --- CẤU HÌNH ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL")
R2_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET_NAME")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError(f"❌ LỖI: Thiếu biến môi trường.")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
s3 = boto3.client('s3', endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_KEY_ID, aws_secret_access_key=R2_SECRET,
                  config=Config(signature_version='s3v4'))

def main():
    print(">>> BẮT ĐẦU MIGRATION HISTORY (ĐÃ FIX LOGIC TIME & KEY) <<<")

    response = supabase.table("tournaments").select("*").neq('id', -1).execute()
    all_tournaments = response.data
    print(f"-> Tổng số bản ghi trong DB: {len(all_tournaments)}")

    history_map = {}
    count_legacy = 0
    count_standard = 0
    
    # Lấy mốc thời gian hiện tại chuẩn UTC
    now_utc = datetime.utcnow()

    for record in all_tournaments:
        try:
            data = record.get("data") or {}
            db_id = record.get("id")

            is_history = False
            
            # --- 1. LẤY CÁC BIẾN TỪ CẢ CỘT DB LẪN JSON DATA ---
            # Trạng thái
            current_status = str(record.get("status") or data.get("status") or "").upper()
            is_finalized_flag = record.get("is_finalized") or data.get("is_finalized")
            ai_pred = data.get("ai_prediction") or {}
            status_label = ai_pred.get("status_label")
            
            # Thời gian
            end_at_str = record.get("end_at") or data.get("end_at")
            end_date_str = record.get("end") or data.get("end")
            end_time_str = record.get("endTime") or data.get("endTime") or "23:59:59"
            
            # --- 2. LOGIC XÁC ĐỊNH HISTORY ---
            # Check 1: Xác nhận tường minh qua cờ (Flags)
            if status_label == "FINALIZED" or current_status in ['ENDED', 'FINALIZED'] or is_finalized_flag:
                is_history = True
            else:
                # Check 2: Tính toán dựa trên thời gian kết thúc
                if end_at_str:
                    try:
                        end_at_dt = datetime.fromisoformat(end_at_str.replace("Z", "+00:00")).replace(tzinfo=None)
                        if now_utc > end_at_dt:
                            is_history = True
                    except: pass
                elif end_date_str:
                    try:
                        if len(end_time_str) == 5: 
                            end_time_str += ":00" # Sửa format giờ nếu chỉ có HH:MM
                        end_dt_str = f"{end_date_str}T{end_time_str}"
                        end_dt = datetime.strptime(end_dt_str, "%Y-%m-%dT%H:%M:%S")
                        if now_utc > end_dt:
                            is_history = True
                    except: pass

            # Bỏ qua nếu là giải Đang chạy (Running)
            if not is_history:
                continue

            # --- 3. XỬ LÝ KEY CHO DATA TRÊN R2 ---
            alpha_id = data.get("alphaId")
            
            if alpha_id:
                object_key = alpha_id
                count_standard += 1
            else:
                # SỬA LỖI CHÍ MẠNG: Dùng prefix ALPHA_ thay vì legacy_
                object_key = f"ALPHA_{db_id}"
                data["alphaId"] = object_key 
                count_legacy += 1

            # --- 4. CHUẨN HÓA DATA ---
            if not data.get("ai_prediction"):
                data["ai_prediction"] = {}
            data["ai_prediction"]["status_label"] = "FINALIZED"
            
            # Lưu trữ toàn bộ thông tin gốc của record vào data (Tránh thất thoát field)
            data["id"] = db_id
            if "name" not in data and record.get("name"): data["name"] = record.get("name")
            if "contract" not in data and record.get("contract"): data["contract"] = record.get("contract")

            history_map[object_key] = data
        
        except Exception as e:
            print(f"❌ Lỗi record ID {record.get('id')}: {e}")

    total_migrated = count_standard + count_legacy
    print("------------------------------------------------")
    print(f"✅ KẾT QUẢ QUÉT:")
    print(f"   - Giải chuẩn (Có AlphaID): {count_standard}")
    print(f"   - Giải thiếu ID (Đã fix):  {count_legacy}")
    print(f"   => TỔNG CỘNG HISTORY:      {total_migrated}")

    # --- 5. UPLOAD LÊN R2 ---
    if total_migrated > 0:
        file_key = "finalized_history.json"
        print(f"-> Đang upload '{file_key}' lên R2...")
        s3.put_object(
            Bucket=R2_BUCKET,
            Key=file_key,
            Body=json.dumps(history_map),
            ContentType='application/json'
        )
        print("🎉 UPLOAD THÀNH CÔNG! R2 ĐÃ CÓ DATA ĐẦY ĐỦ VÀ CHUẨN XÁC.")
    else:
        print("⚠️ Không tìm thấy dữ liệu history nào.")

if __name__ == "__main__":
    main()
