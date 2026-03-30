import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# ТВОЙ ID ДЛЯ КОНТРОЛЯ В ЛОГАХ
MY_DISCORD_ID = "829735798173728789" 

GUILD_ID = "1349045850331938826"
CONTENT_THREAD = "1389273374748049439"   
XP_SOURCE_THREAD = "1351492950768619552" 

# Список "родительских" каналов
THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

DAYS_BACK = 3
TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

def get_discord_data():
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list, processed_tweets = {}, [], set()
    
    # 🕵️ ШАГ 1: Находим все активные под-ветки (Threads) во всем сервере
    all_target_channels = list(set(THREAD_IDS))
    try:
        res = requests.get(f"https://discord.com/api/v10/guilds/{GUILD_ID}/threads/active", headers=headers)
        if res.status_code == 200:
            active_threads = res.json().get('threads', [])
            for t in active_threads:
                p_id = str(t.get('parent_id'))
                if p_id in THREAD_IDS:
                    all_target_channels.append(str(t.get('id')))
                    log(f"🧵 Обнаружен активный тред: '{t.get('name')}' в канале {p_id}")
    except Exception as e:
        log(f"⚠️ Ошибка поиска тредов: {e}")

    # 📡 ШАГ 2: Собираем сообщения из всех найденных каналов и тредов
    for tid in set(all_target_channels):
        log(f"📡 Сбор данных из: {tid}")
        last_id, count = None, 0
        while True:
            url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            
            r = requests.get(url, headers=headers)
            if r.status_code == 429:
                time.sleep(int(r.json().get('retry_after', 2))); continue
            if r.status_code != 200: break
            msgs = r.json()
            if not msgs: break
            
            for m in msgs:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if dt < TARGET_DATE: { last_id := "STOP" }; break
                
                uid = m['author']['id']
                if uid == MY_DISCORD_ID:
                    log(f"   🎯 ТВОЁ СООБЩЕНИЕ: {dt} в {tid}")

                if uid not in user_stats:
                    user_stats[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{m['author'].get('avatar')}.png" if m['author'].get('avatar') else None,
                        "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": "not_linked", "channels_count": 0, "total_score": 0,
                        "discord_joined_at": None, "discord_roles": [], "prev_total_score": 0, "prev_discord_messages": 0
                    }
                user_stats[uid]["discord_messages"] += 1
                last_id = m['id']
                count += 1
            if last_id == "STOP": break
        if count > 0: log(f"✅ Канал {tid}: найдено {count} сообщений")

    log("🛡️ Обогащение профилей...")
    for uid in user_stats:
        try:
            r = requests.get(f"https://discord.com/api/v10/guilds/{GUILD_ID}/members/{uid}", headers=headers)
            if r.status_code == 200:
                d = r.json()
                user_stats[uid]["discord_joined_at"] = d.get('joined_at')
                user_stats[uid]["discord_roles"] = d.get('roles', ["Member"])
            else:
                user_stats[uid]["discord_joined_at"] = datetime.now(timezone.utc).isoformat()
                user_stats[uid]["discord_roles"] = ["Contributor"]
        except: pass
    
    return user_stats, tweet_list

async def main():
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    users, tweets = get_discord_data()
    
    payload = []
    for uid, info in users.items():
        info["total_score"] = info["discord_messages"] * 10 # Базовый расчет XP
        payload.append(info)

    if payload:
        payload.sort(key=lambda x: x['discord_messages'], reverse=True)
        log("📊 ТОП-10:")
        for u in payload[:10]: log(f"👤 {u['username']} | MSG: {u['discord_messages']}")
        sb.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
        log("✅ БАЗА ОБНОВЛЕНА")

if __name__ == "__main__": asyncio.run(main())
