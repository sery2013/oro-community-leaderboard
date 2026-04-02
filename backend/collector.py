import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
GUILD_ID = "1349045850331938826"
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]
DAYS_BACK = 2 
TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

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

def get_discord_data(old_data):
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list = {}, []
    
    for tid in THREAD_IDS:
        log(f"📡 Сбор из ветки: {tid}")
        last_id, count = None, 0
        while True:
            try:
                url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
                if last_id: url += f"&before={last_id}"
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200: break
                msgs = r.json()
                if not msgs: break
                
                for m in msgs:
                    dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if dt < TARGET_DATE:
                        last_id = "STOP"
                        break
                    
                    uid = m['author']['id']
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
                    
                    user_stats[uid]["discord_messages"] += 1
                    user_stats[uid]["channels"].add(tid)
                    
                    links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/\w+/status/\d+', m.get('content', ''))
                    for l in links: tweet_list.append((uid, l))
                    last_id = m['id']
                    count += 1
                if last_id == "STOP": break
            except: break
        log(f"✅ Ветка {tid}: {count} сообщ.")
    
    log("🛡️ Обогащение данными (Роли)...")
    for i, uid in enumerate(user_stats):
        # Оптимизация: берем роли из старой базы, если они там есть
        if uid in old_data and old_data[uid].get('discord_roles'):
            user_stats[uid]["discord_joined_at"] = old_data[uid].get('discord_joined_at')
            user_stats[uid]["discord_roles"] = old_data[uid].get('discord_roles')
        else:
            joined, roles = get_discord_member_info(uid, token)
            user_stats[uid]["discord_joined_at"] = joined
            user_stats[uid]["discord_roles"] = roles
            time.sleep(0.4)
        if (i+1) % 50 == 0: log(f"⏳ Роли: {i+1}/{len(user_stats)}")
            
    return user_stats, tweet_list

async def main():
    log("🚀 Запуск Collector 2.2...")
    s_url, s_key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    tw_key = os.getenv('SOCIALDATA_KEY')
    if not s_url or not s_key: return log("❌ Ошибка конфига!")
    
    supabase = create_client(s_url, s_key)
    
    # Загружаем старые данные для бэкапа и ролей
    try:
        res = supabase.table("leaderboard_stats").select("*").execute()
        old_data = {row['user_id']: row for row in res.data}
    except: old_data = {}
    
    users, tweets = get_discord_data(old_data)
    
    # Тут можно вставить твой блок парсинга твитов (asyncio.gather), если он нужен.
    # Для краткости пропустим логику парсинга самих твитов, она у тебя была рабочая.

    payload = []
    now = datetime.utcnow().isoformat()
    
    for uid, info in users.items():
        info["total_score"] = info["discord_messages"] * 10
        old_entry = old_data.get(uid, {})
        info["prev_total_score"] = old_entry.get("total_score", 0)
        info["prev_discord_messages"] = old_entry.get("discord_messages", 0)
        info["channels_count"] = len(info.get("channels", []))
        info["updated_at"] = now
        
        d = info.copy()
        if "channels" in d: del d["channels"]
        payload.append(d)
    
    if payload:
        log(f"📤 Отправка {len(payload)} записей...")
        for i in range(0, len(payload), 50):
            chunk = payload[i:i+50]
            supabase.table("leaderboard_stats").upsert(chunk, on_conflict="user_id").execute()
        log("✅ Данные успешно обновлены!")
    
    log("🏁 ЗАВЕРШЕНО")

if __name__ == "__main__":
    asyncio.run(main())
