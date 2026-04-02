import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
GUILD_ID = "1349045850331938826"
CONTENT_THREAD_ID = "1351488160206426227"  # Ветка, где ищем твиты за 30 дней
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

# Даты отсечки
DISCORD_TARGET = datetime.now(timezone.utc) - timedelta(days=2)
CONTENT_TARGET = datetime.now(timezone.utc) - timedelta(days=30)

def parse_xp_value(xp_str):
    try:
        xp_str = xp_str.upper().replace(' ', '').replace(',', '')
        multiplier = 1
        if 'K' in xp_str: multiplier = 1000; xp_str = xp_str.replace('K', '')
        elif 'M' in xp_str: multiplier = 1000000; xp_str = xp_str.replace('M', '')
        return int(float(xp_str) * multiplier)
    except: return 0

def get_discord_member_info(user_id, token):
    url = f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{user_id}"
    headers = {"Authorization": token}
    try:
        r = requests.get(url, headers=headers, timeout=5)
        if r.status_code == 200:
            data = r.json()
            return data.get('joined_at'), data.get('roles', [])
    except: pass
    return None, []

def get_discord_data(old_db_data):
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list = {}, []
    
    for tid in THREAD_IDS:
        is_content_thread = (tid == CONTENT_THREAD_ID)
        current_target = CONTENT_TARGET if is_content_thread else DISCORD_TARGET
        
        log(f"📡 Сбор: {tid} ({'30 дней' if is_content_thread else '2 дня'})")
        last_id, count = None, 0
        
        while True:
            try:
                time.sleep(1.2) # Анти-бан пауза между страницами
                url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
                if last_id: url += f"&before={last_id}"
                
                r = requests.get(url, headers=headers, timeout=10)
                
                if r.status_code == 429:
                    wait = r.json().get('retry_after', 5)
                    log(f"⏳ Rate Limit! Ждем {wait} сек...")
                    time.sleep(wait + 1)
                    continue
                
                if r.status_code != 200: break
                msgs = r.json()
                if not msgs: break
                
                for m in msgs:
                    dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    
                    # Если сообщение старее нашей цели для ЭТОЙ ветки — стоп
                    if dt < current_target:
                        last_id = "STOP"
                        break
                    
                    uid = m['author']['id']
                    
                    # Инициализация юзера
                    if uid not in user_stats:
                        avatar = m['author'].get('avatar')
                        user_stats[uid] = {
                            "user_id": uid, "username": m['author']['username'], 
                            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{avatar}.png" if avatar else None, 
                            "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, 
                            "twitter_replies": 0, "twitter_handle": "not_linked", "channels": set(), 
                            "total_score": 0, "discord_joined_at": None, "discord_roles": [], 
                            "prev_total_score": 0, "prev_discord_messages": 0
                        }

                    # Считаем сообщения ТОЛЬКО если они в окне 2 дней
                    if dt >= DISCORD_TARGET:
                        user_stats[uid]["discord_messages"] += 1
                        user_stats[uid]["channels"].add(tid)

                    # Собираем твиты (если ветка контента — за 30 дней, если обычная — за 2 дня)
                    links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/\w+/status/\d+', m.get('content', ''))
                    for l in links:
                        tweet_list.append((uid, l))
                    
                    last_id = m['id']
                    count += 1
                if last_id == "STOP": break
            except Exception as e:
                log(f"❌ Ошибка в цикле: {e}")
                break
        
        log(f"✅ Готово: {count} сообщ.")
        time.sleep(2) # Пауза между ветками

    log("🛡️ Обогащение ролями (пропускаем тех, кто уже есть в базе)...")
    for uid in user_stats:
        # Если юзер был в старой базе и у него есть роли — берем оттуда (экономим запросы)
        if uid in old_db_data and old_db_data[uid].get('discord_roles'):
            user_stats[uid]["discord_joined_at"] = old_db_data[uid].get('discord_joined_at')
            user_stats[uid]["discord_roles"] = old_db_data[uid].get('discord_roles')
        else:
            joined, roles = get_discord_member_info(uid, token)
            user_stats[uid]["discord_joined_at"] = joined
            user_stats[uid]["discord_roles"] = roles
            time.sleep(1.1) # Медленно, чтобы не забанили
    
    return user_stats, tweet_list

# Остальная часть (main и fetch_tweet) остается почти такой же, 
# но с учетом передачи old_data в get_discord_data
async def main():
    log("🚀 Запуск Hybrid Collector...")
    s_url, s_key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    tw_key = os.getenv('SOCIALDATA_KEY')
    supabase = create_client(s_url, s_key)
    
    log("🔍 Загрузка кэша из Supabase...")
    try:
        res = supabase.table("leaderboard_stats").select("*").execute()
        old_data = {row['user_id']: row for row in res.data}
    except: old_data = {}
    
    users, tweets = get_discord_data(old_data)
    
    # ... (логика fetch_tweet остается без изменений) ...
    # [Тут идет твой блок с aiohttp для проверки твитов]

    payload = []
    now = datetime.now(timezone.utc).isoformat()
    for uid, info in users.items():
        # Считаем XP на основе сообщений за 2 дня
        info["total_score"] = info["discord_messages"] * 10
        info["updated_at"] = now
        
        old_entry = old_data.get(uid, {})
        info["prev_total_score"] = old_entry.get("total_score", 0)
        info["prev_discord_messages"] = old_entry.get("discord_messages", 0)
        info["channels_count"] = len(info.get("channels", []))
        
        d = info.copy()
        if "channels" in d: del d["channels"]
        payload.append(d)
    
    if payload:
        log(f"📤 Сохранение {len(payload)} записей...")
        # Разбиваем на чанки по 50, чтобы Supabase не ругался
        for i in range(0, len(payload), 50):
            supabase.table("leaderboard_stats").upsert(payload[i:i+50], on_conflict="user_id").execute()
    log("🏁 ФИНИШ")

if __name__ == "__main__":
    asyncio.run(main())
