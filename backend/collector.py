import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
GUILD_ID = "1349045850331938826"
CONTENT_THREAD_ID = "1351488160206426227"  # Ветка с твитами (30 дней)
XP_BOT_THREAD_ID = "1351492950768619552"   # Ветка, где бот пишет XP
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
    user_stats, tweet_links = {}, []
    
    for tid in THREAD_IDS:
        is_content = (tid == CONTENT_THREAD_ID)
        target = CONTENT_TARGET if is_content else DISCORD_TARGET
        
        log(f"📡 Парсинг {tid} ({'30д' if is_content else '2д'})")
        last_id = None
        
        while True:
            try:
                time.sleep(1.1)
                url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
                if last_id: url += f"&before={last_id}"
                r = requests.get(url, headers=headers, timeout=10)
                if r.status_code != 200: break
                msgs = r.json()
                if not msgs: break
                
                for m in msgs:
                    dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                    if dt < target:
                        last_id = "STOP"
                        break
                    
                    uid = m['author']['id']
                    if uid not in user_stats:
                        avatar = m['author'].get('avatar')
                        user_stats[uid] = {
                            "user_id": uid, "username": m['author']['username'], 
                            "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{avatar}.png" if avatar else None, 
                            "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, 
                            "total_score": 0, "discord_roles": [], "channels": set()
                        }
                    
                    # Считаем сообщения только за последние 2 дня
                    if dt >= DISCORD_TARGET:
                        user_stats[uid]["discord_messages"] += 1
                        user_stats[uid]["channels"].add(tid)

                    # Логика XP из ветки бота
                    if tid == XP_BOT_THREAD_ID:
                        content = m.get('content', '')
                        # Ищем число рядом с XP (пример: 15400 XP)
                        xp_match = re.search(r'(\d[\d\s,.]*)\s*XP', content, re.IGNORECASE)
                        if xp_match:
                            val = int(re.sub(r'[^\d]', '', xp_match.group(1)))
                            if val > user_stats[uid]["total_score"]:
                                user_stats[uid]["total_score"] = val

                    # Сбор ссылок на твиты
                    links = re.findall(r'https?://(?:twitter\.com|x\.com)/\w+/status/(\d+)', m.get('content', ''))
                    for tweet_id in links:
                        tweet_links.append({"uid": uid, "url": f"https://x.com/i/status/{tweet_id}"})
                    
                    last_id = m['id']
                if last_id == "STOP": break
            except: break
            
    # Обогащение ролями из кэша или API
    for uid in user_stats:
        if uid in old_db_data and old_db_data[uid].get('discord_roles'):
            user_stats[uid]["discord_roles"] = old_db_data[uid]['discord_roles']
            user_stats[uid]["discord_joined_at"] = old_db_data[uid].get('discord_joined_at')
        else:
            joined, roles = get_discord_member_info(uid, token)
            user_stats[uid]["discord_joined_at"], user_stats[uid]["discord_roles"] = joined, roles
            time.sleep(1)
            
    return user_stats, tweet_links

async def main():
    log("🚀 Запуск с кэшированием твитов...")
    s_url, s_key = os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")
    tw_key = os.getenv('SOCIALDATA_KEY')
    supabase = create_client(s_url, s_key)
    
    # 1. Загружаем старых юзеров и КЭШ ТВИТОВ
    old_res = supabase.table("leaderboard_stats").select("*").execute()
    old_data = {r['user_id']: r for r in old_res.data}
    
    cache_res = supabase.table("tweet_cache").select("*").execute()
    tweet_cache = {r['tweet_url']: r for r in cache_res.data}
    
    users, tweets = get_discord_data(old_data)
    
    # 2. Обработка твитов через SocialData с проверкой кэша
    async with aiohttp.ClientSession() as session:
        for tw in tweets:
            url, uid = tw['url'], tw['uid']
            cached = tweet_cache.get(url)
            
            # Если твита нет в базе или он старее 24 часов - обновляем
            need_update = True
            if cached:
                upd = datetime.fromisoformat(cached['updated_at'].replace('Z', '+00:00'))
                if datetime.now(timezone.utc) - upd < timedelta(hours=24):
                    need_update = False
            
            if need_update:
                log(f"🔎 API запрос для твита: {url}")
                tw_id = url.split('/')[-1]
                async with session.get(f"https://api.socialdata.tools/twitter/tweets/{tw_id}", 
                                       headers={"Authorization": f"Bearer {tw_key}"}) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        c_data = {
                            "tweet_url": url, "user_id": uid,
                            "likes": data.get('favorite_count', 0),
                            "views": data.get('views_count', 0),
                            "updated_at": datetime.now(timezone.utc).isoformat()
                        }
                        supabase.table("tweet_cache").upsert(c_data).execute()
                        cached = c_data
                    await asyncio.sleep(1)
            
            if cached and uid in users:
                users[uid]["twitter_posts"] += 1
                users[uid]["twitter_likes"] += cached.get('likes', 0)
                users[uid]["twitter_views"] += cached.get('views', 0)

    # 3. Финальный расчет и сохранение
    now = datetime.now(timezone.utc).isoformat()
    payload = []
    for uid, info in users.items():
        # Если бот не нашел XP, считаем базу
        if info["total_score"] == 0:
            info["total_score"] = info["discord_messages"] * 10
            
        info["updated_at"] = now
        info["channels_count"] = len(info.pop("channels", []))
        payload.append(info)
        
    if payload:
        for i in range(0, len(payload), 50):
            supabase.table("leaderboard_stats").upsert(payload[i:i+50]).execute()
    log("🏁 Готово!")

if __name__ == "__main__":
    asyncio.run(main())
