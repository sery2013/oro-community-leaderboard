import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# === КОНФИГУРАЦИЯ ===
MY_DISCORD_ID = "829735798173728789" # Вставь свой ID для контроля в логах
GUILD_ID = "1349045850331938826"

# Список "родительских" каналов (включая General и Региональные)
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

def get_discord_data():
    token = os.getenv('DISCORD_TOKEN')
    base_url = "https://discord.com/api/v9"
    
    headers = {
        "Authorization": token,
        "Accept": "*/*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "X-Discord-Locale": "ru-RU",
        "Referer": f"https://discord.com/channels/{GUILD_ID}",
        "Origin": "https://discord.com"
    }
    
    user_stats, tweet_list, processed_tweets = {}, [], set()
    
    # 🕵️ ШАГ 1: Поиск всех активных тредов (веток форумов)
    targets = list(set(THREAD_IDS))
    try:
        res = requests.get(f"{base_url}/guilds/{GUILD_ID}/threads/active", headers=headers)
        if res.status_code == 200:
            threads = res.json().get('threads', [])
            for t in threads:
                p_id = str(t.get('parent_id'))
                if p_id in THREAD_IDS:
                    targets.append(str(t.get('id')))
                    log(f"🧵 Найдена вложенная ветка: {t.get('name')}")
    except: pass

    # 📡 ШАГ 2: Сбор сообщений и ссылок на твиты
    for tid in set(targets):
        log(f"📡 Сканирую: {tid}")
        last_id, count = None, 0
        while True:
            time.sleep(0.4) # Защита от спам-фильтра Discord
            url = f"{base_url}/channels/{tid}/messages?limit=100"
            if last_id: url += f"&before={last_id}"
            
            r = requests.get(url, headers=headers)
            if r.status_code == 429:
                time.sleep(int(r.json().get('retry_after', 2))); continue
            if r.status_code == 403:
                log(f"🚫 Нет доступа к {tid} (нужно выбрать роль в Discord!)"); break
            if r.status_code != 200: break
            
            msgs = r.json()
            if not msgs: break
            
            for m in msgs:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if dt < TARGET_DATE: { last_id := "STOP" }; break
                
                uid = m['author']['id']
                if uid == MY_DISCORD_ID:
                    log(f"   🎯 ТВОЕ СООБЩЕНИЕ ({dt}) в {tid}")

                if uid not in user_stats:
                    user_stats[uid] = {
                        "user_id": uid, "username": m['author']['username'],
                        "avatar_url": f"https://cdn.discordapp.com/avatars/{uid}/{m['author'].get('avatar')}.png" if m['author'].get('avatar') else None,
                        "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0,
                        "twitter_handle": "not_linked", "total_score": 0
                    }
                user_stats[uid]["discord_messages"] += 1
                
                # Поиск ссылок на твиты во всех сообщениях
                content = m.get('content', '')
                found_ids = re.findall(r'status/(\d+)', content)
                for t_id in found_ids:
                    if t_id not in processed_tweets:
                        tweet_list.append((uid, t_id))
                        processed_tweets.add(t_id)
                
                last_id = m['id']
                count += 1
            if last_id == "STOP": break
        if count > 0: log(f"✅ Канал {tid}: найдено {count} сообщений")
    
    return user_stats, tweet_list

async def fetch_tweet_data(session, tweet_info, api_key):
    uid, tweet_id = tweet_info
    url = f"https://api.socialdata.tools/twitter/tweets/{tweet_id}"
    try:
        async with session.get(url, headers={"Authorization": f"Bearer {api_key}"}) as resp:
            if resp.status == 200:
                d = await resp.json()
                return uid, d.get('favorite_count', 0), d.get('views_count', 0), d.get('reply_count', 0), d.get('user', {}).get('screen_name')
    except: pass
    return uid, 0, 0, 0, None

async def main():
    sb = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
    tw_key = os.getenv('SOCIALDATA_KEY')
    
    users, tweets = get_discord_data()
    
    # 🐦 ШАГ 3: Получение данных из Twitter (SocialData)
    if tweets and tw_key:
        log(f"🐦 Найдено {len(tweets)} твитов. Начинаю проверку...")
        async with aiohttp.ClientSession() as sess:
            for i in range(0, len(tweets), 10):
                batch = tweets[i:i+10]
                results = await asyncio.gather(*[fetch_tweet_data(sess, t, tw_key) for t in batch])
                for uid, likes, views, replies, handle in results:
                    if uid in users:
                        users[uid]["twitter_posts"] += 1
                        users[uid]["twitter_likes"] += likes
                        users[uid]["twitter_views"] += views
                        users[uid]["twitter_replies"] += replies
                        if handle: users[uid]["twitter_handle"] = handle
                await asyncio.sleep(1.2) # Чтобы SocialData не забанил за скорость

    # 📊 ШАГ 4: Расчет баллов и отправка в базу
    payload = []
    for uid, info in users.items():
        # Формула: Сообщения*10 + Лайки*5 + Ответы*5 + (Просмотры/10)
        info["total_score"] = (info["discord_messages"] * 10) + \
                             (info["twitter_likes"] * 5) + \
                             (info["twitter_replies"] * 5) + \
                             (info["twitter_views"] // 10)
        payload.append(info)

    if payload:
        payload.sort(key=lambda x: x['total_score'], reverse=True)
        log(f"📊 ТОП-5 ПО БАЛЛАМ:")
        for u in payload[:5]: log(f"👤 {u['username']} | Score: {u['total_score']} | MSG: {u['discord_messages']}")
        
        sb.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
        log("✅ БАЗА ОБНОВЛЕНА")
    else:
        log("⚠️ Данных не найдено. Проверь доступ к каналам!")

if __name__ == "__main__":
    asyncio.run(main())
