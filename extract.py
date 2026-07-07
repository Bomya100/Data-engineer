import requests
import json
from datetime import datetime

# 1. ชี้เป้าไปที่ท่อน้ำ (API ฟรีของ CoinGecko ดึงราคา Bitcoin)
url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd,thb"

try:
    # 2. เปิดก๊อกเปิดน้ำ ดึงข้อมูลลงมา
    response = requests.get(url)
    data = response.json()
    
    print(data)  # แสดงข้อมูลดิบที่ดึงมาได้
    # 3. แปะป้ายเวลา (Timestamp) กำกับไว้ว่าเราไปดึงมาตอนกี่โมง
    data['bitcoin']['fetched_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # 4. เซฟข้อมูลดิบก้อนนี้ลงคอมเป็นไฟล์ JSON
    with open("raw_bitcoin.json", "w") as file:
        json.dump(data, file, indent=4)
        
    print("🎉 สเต็ป 1 ผ่าน! ดึงข้อมูลและเซฟไฟล์ raw_bitcoin.json สำเร็จแล้ว")
    
except Exception as e:
    print(f"❌ ท่อตัน! มีปัญหา: {e}")