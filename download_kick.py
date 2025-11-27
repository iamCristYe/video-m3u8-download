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
            m3u8_url = "https://fa723fc1b171.us-west-2.playback.live-video.net/api/video/v1/us-west-2.196233775518.channel.bgaViscsscMD.m3u8?browser_family=chrome&browser_version=142.0&cdm=wv&os_name=Windows&os_version=NT%2010.0&platform=web&player_backend=mediaplayer&player_version=1.45.0&supported_codecs=av1,h265,h264&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJFUzM4NCJ9.eyJhd3M6Y2hhbm5lbC1hcm4iOiJhcm46YXdzOml2czp1cy13ZXN0LTI6MTk2MjMzNzc1NTE4OmNoYW5uZWwvYmdhVmlzY3NzY01EIiwiYXdzOmFjY2Vzcy1jb250cm9sLWFsbG93LW9yaWdpbiI6Imh0dHBzOi8va2ljay5jb20saHR0cHM6Ly93d3cuZ3N0YXRpYy5jb20saHR0cHM6Ly8qLmtpY2subGl2ZSxodHRwczovL3BsYXllci5raWNrLmNvbSxodHRwczovL2FkbWluLmtpY2suY29tLGh0dHBzOi8vYmV0YS5raWNrLmNvbSxodHRwczovL25leHQua2ljay5jb20saHR0cHM6Ly9kYXNoYm9hcmQua2ljay5jb20saHR0cHM6Ly8qLnByZXZpZXcua2ljay5jb20saHR0cHM6Ly94Ym94LmtpY2suY29tLGh0dHBzOi8vcGxheXN0YXRpb24ua2ljay5jb20iLCJhd3M6c3RyaWN0LW9yaWdpbi1lbmZvcmNlbWVudCI6ZmFsc2UsImV4cCI6MTc2NDIzNTEyNn0.aOkfd-BfJAKsdDsf4lzKbM1g1V0ztQKYfdJm6hoG7Y_WQfs3THPBVVMH4WQSMWtkGuaF9nl_CvARYqbX1lACKPxHMqXaT7lXWLch_Yk_mFQyWEfBNxic81MLK5av3tcD"
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
        merge_group_size=15,
    )
    m3u8_processor.run()