import os
import json
import boto3
from datetime import datetime
from botocore.config import Config
from supabase import create_client

# --- CẤU HÌNH TỪ ENV ---
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
R2_ENDPOINT = os.environ.get("R2_ENDPOINT_URL")
R2_KEY_ID = os.environ.get("R2_ACCESS_KEY_ID")
R2_SECRET = os.environ.get("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.environ.get("R2_BUCKET_NAME")

# --- KẾT NỐI ---
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
s3 = boto3.client('s3', endpoint_url=R2_ENDPOINT,
                  aws_access_key_id=R2_KEY_ID, aws_secret_access_key=R2_SECRET,
                  config=Config(signature_version='s3v4'))

def main():
    print(">>> BẮT ĐẦU CHUYỂN NHÀ: SUPABASE -> R2 <<<")

    # 1. Lấy toàn bộ dữ liệu từ Supabase (Chấp nhận nặng 1 lần đầu)
    # Lấy hết, sau đó về Python lọc cho an toàn
    print("1. Đang tải dữ liệu từ Supabase...")
    response = supabase.table("tournaments").select("*").neq('id', -1).execute()
    all_tournaments = response.data
    
    print(f"   -> Tìm thấy tổng cộng: {len(all_tournaments)} bản ghi.")

    history_map = {}
    count_migrated = 0
    today_str = datetime.utcnow().strftime('%Y-%m-%d')

    # 2. Lọc và Xử lý
    for record in all_tournaments:
        try:
            data = record.get("data", {})
            if not data: continue

            alpha_id = data.get("alphaId")
            if not alpha_id: continue

            # --- TIÊU CHÍ XÁC ĐỊNH LÀ HISTORY ---
            # Tiêu chí 1: Đã có nhãn FINALIZED
            is_finalized_label = False
            if data.get("ai_prediction") and data["ai_prediction"].get("status_label") == "FINALIZED":
                is_finalized_label = True

            # Tiêu chí 2: Ngày kết thúc (end) nhỏ hơn ngày hôm nay
            is_expired = False
            end_date = data.get("end")
            if end_date and end_date < today_str:
                is_expired = True

            # QUYẾT ĐỊNH: Chỉ chuyển nếu đã kết thúc
            if is_finalized_label or is_expired:
                # Chuẩn hóa lại dữ liệu lần cuối trước khi lưu
                # Đảm bảo status là FINALIZED nếu chưa có
                if not data.get("ai_prediction"):
                    data["ai_prediction"] = {}
                
                data["ai_prediction"]["status_label"] = "FINALIZED"
                
                # Lưu vào Map (Key là AlphaID để Node.js đọc nhanh)
                history_map[alpha_id] = data
                count_migrated += 1
                
        except Exception as e:
            print(f"❌ Lỗi khi xử lý bản ghi ID {record.get('id')}: {e}")

    print(f"2. Đã lọc được {count_migrated} giải đấu kết thúc (History).")

    # 3. Upload lên R2
    if count_migrated > 0:
        file_key = "finalized_history.json"
        print(f"3. Đang upload file '{file_key}' lên R2...")
        
        try:
            s3.put_object(
                Bucket=R2_BUCKET,
                Key=file_key,
                Body=json.dumps(history_map),
                ContentType='application/json',
                CacheControl='max-age=3600' # Cache 1 giờ vì history ít đổi
            )
            print("✅ UPLOAD THÀNH CÔNG! SẴN SÀNG CHO NODE.JS.")
        except Exception as e:
            print(f"❌ Lỗi Upload R2: {e}")
    else:
        print("⚠️ Không có dữ liệu history nào để chuyển.")

if __name__ == "__main__":
    main()
