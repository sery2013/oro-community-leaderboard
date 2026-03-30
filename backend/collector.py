import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# ТВОЙ ID ДЛЯ ПРОВЕРКИ (замени на свой реальный ID, если этот неверный)
MY_DISCORD_ID = "829735798173728789" 

GUILD_ID = "1349045850331938826"
CONTENT_THREAD = "1389273374748049439"   
XP_SOURCE_THREAD = "1351492950768619552" 

THREAD_IDS = [
    "1351487907042431027", "1351488160206426227", "1351488253332557867", 
    "1351492950768619552", "1367864741548261416", "1371904712001065000", 
    "1465733325149835295", "1371110511919497226", "1366338962813222993", 
    "1371904910324404325", "1371413462982594620", "1372149550793490505", 
    "1372149324192153620", "1372149873188536330", "1372242189240897596", 
    "1351488556924932128", "1389273374748049439"
]

DAYS_BACK = 3 # Увеличили запас по времени
TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

def get_discord_data():
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list, processed_tweets = {}, [], set()
    
    for tid in set(THREAD_IDS):
        log(f"📡 Сканирую ветку: {tid}...")
        last_id = None
        count_thread = 0
        
        while True:
            # Запрос строго по 100 сообщений
            url = f"https://discord.com/api/v10/channels/{tid}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            
            r = requests.get(url, headers=headers)
            if r.status_code == 429:
                wait = r.json().get('retry_after', 5)
                time.sleep(wait)
                continue
            if r.status_code != 200: break
            
            msgs = r.json()
            if not msgs: break
            
            for m in msgs:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                
                # Если сообщение старее нужной даты - ВЫХОДИМ
                if dt < TARGET_DATE:
                    last_id = "STOP"
                    break
                
                uid = m['author']['id']
                
                # ЛОГ ДЛЯ ТВОИХ СООБЩЕНИЙ (поможет понять, видит ли их скрипт)
                if uid == MY_DISCORD_ID:
                    log(f"   🎯 Нашел твое сообщение от {dt} в ветке {tid}")

                if uid not in user_stats:
                    user_stats[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{m['author'].get('avatar')}.png" if m['author'].get('avatar') else None,
                        "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": "not_linked", "channels": set(), "total_score": 0,
                        "discord_joined_at": None, "discord_roles": [], "prev_total_score": 0, "prev_discord_messages": 0
                    }
                
                user_stats[uid]["discord_messages"] += 1
                user_stats[uid]["channels"].add(tid)
                
                # Сбор твитов
                if tid == CONTENT_THREAD:
                    links = re.findall(r'status/(\d+)', m.get('content', '') + str(m.get('embeds', [])))
                    for t_id in links:
                        if t_id not in processed_tweets:
                            tweet_list.append((uid, f"https://x.com/i/status/{t_id}"))
                            processed_tweets.add(t_id)
                
                last_id = m['id']
                count_thread += 1
            
            if last_id == "STOP": break
        
        log(f"✅ Ветка {tid} завершена. Собрано: {count_thread}")
    
    # Обогащение данными (роли/дата)
    log("🛡️ Получение профилей...")
    for uid in user_stats:
        # Прямой запрос инфо о мембере
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

# Остальная часть main остается такой же... (сократил для краткости)
async def fetch_tweet(session, tweet_info, api_key):
    uid, url = tweet_info
    t_id = re.search(r"status/(\d+)", url)
    if not t_id: return uid, 0, 0, 0, "Unknown", None
    try:
        async with session.get(f"https://api.socialdata.tools/twitter/tweets/{t_id.group(1)}", headers={"Authorization": f"Bearer {api_key}"}) as resp:
            if resp.status == 200:
                data = await resp.json()
                u = data.get('user', {})
                return uid, data.get('favorite_count', 0), data.get('views_count', 0), data.get('reply_count', 0), "Found", u.get('screen_name')
    except: pass
    return uid, 0, 0, 0, "Error", None

async def main():
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    tw_key = os.getenv('SOCIALDATA_KEY')
    users, tweets = get_discord_data()
    
    if tweets:
        async with aiohttp.ClientSession() as sess:
            for i in range(0, len(tweets), 10):
                res = await asyncio.gather(*[fetch_tweet(sess, t, tw_key) for t in tweets[i:i+10]])
                for uid, l, v, r, status, handle in res:
                    if status == "Found" and uid in users:
                        users[uid]["twitter_posts"] += 1
                        users[uid]["twitter_likes"] += (l or 0)
                        users[uid]["twitter_views"] += (v or 0)
                        users[uid]["twitter_replies"] += (r or 0)
                        if handle: users[uid]["twitter_handle"] = handle

    payload = []
    for uid, info in users.items():
        info["total_score"] = max(info["discord_messages"] * 10, info["total_score"])
        info["channels_count"] = len(info.pop("channels"))
        payload.append(info)

    if payload:
        payload.sort(key=lambda x: x['discord_messages'], reverse=True)
        log("📊 ИТОГОВЫЙ ТОП:")
        for u in payload[:10]: log(f"👤 {u['username']} | Сообщений: {u['discord_messages']}")
        sb.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
        log("✅ ГОТОВО")

if __name__ == "__main__": asyncio.run(main())
