import os
import requests
import json
from datetime import datetime, timedelta
import time
import subprocess
import threading
from m3u8_ts_to_tg import M3U8TSToTG


def retry_command_until_success(command, max_retries=10, retry_interval=5):
    for attempt in range(1, max_retries + 1):
        print(f"[Thread] Attempt {attempt}: Running command...")
        process = subprocess.Popen(command, shell=True)
        process.wait()
        if process.returncode == 0:
            print("[Thread] Command succeeded.")
            return
        else:
            print(
                f"[Thread] Failed with return code {process.returncode}. Retrying in {retry_interval}s..."
            )
            time.sleep(retry_interval)
    print("[Thread] Max retries reached. Command failed.")


if __name__ == "__main__":
    while True:
        try:
            m3u8_url = "https://live.mmf.moe/room/0204/index.m3u8"
            break
        except:
            time.sleep(5)
    # m3u8_url = "https://hls-css.live.showroom-live.com/live/xx.m3u8".replace("_abr", "")
    # command = f'./N_m3u8DL-RE --live-real-time-merge "{m3u8_url}" --save-name chunklist'
    # t = threading.Thread(target=retry_command_until_success, args=(command, 100, 5))
    # t.start()
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = channel_id = os.getenv("TELEGRAM_CHAT_ID")

    # process = subprocess.Popen(command, shell=True)
    m3u8_processor = M3U8TSToTG(
        m3u8_url=m3u8_url,  # URL will be fetched from API
        telegram_bot_token=TELEGRAM_BOT_TOKEN,
        telegram_chat_id=TELEGRAM_CHAT_ID,
        caption_prefix="kick",
        work_dir=".",
        merge_group_size=5,
    )
    m3u8_processor.run()
