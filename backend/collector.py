import os, asyncio, aiohttp, requests, re, time, sys
from datetime import datetime, timedelta, timezone
from supabase import create_client

def log(msg):
    print(msg)
    sys.stdout.flush()

# ✅ НАСТРОЙКИ ID
GUILD_ID = "1349045850331938826"
XP_SOURCE_THREAD = "1351492950768619552"       # Ветка для парсинга XP
CONTENT_THREAD = "1389273374748049439"         # Ветка для поиска твитов
MESSAGE_THREADS = [                             # Ветки для подсчета сообщений
    "1351488160206426227", "1351488253332557867", "1351492950768619552",
    "1367864741548261416", "1371904712001065000", "1465733325149835295",
    "1371110511919497226", "1366338962813222993", "1371904910324404325",
    "1371413462982594620", "1372149550793490505", "1372149324192153620",
    "1372149873188536330", "1372242189240897596", "1351488556924932128"
]

DAYS_BACK = 1
TARGET_DATE = datetime.now(timezone.utc) - timedelta(days=DAYS_BACK)

def parse_xp_value(xp_str):
    try:
        xp_str = xp_str.upper().replace(' ', '').replace(',', '')
        mult = 1000 if 'K' in xp_str else 1000000 if 'M' in xp_str else 1
        num_str = xp_str.replace('K', '').replace('M', '')
        return int(float(num_str) * mult)
    except: return 0

async def fetch_tweet(session, tweet_info, api_key):
    uid, url = tweet_info
    t_id = re.search(r"status/(\d+)", url)
    if not t_id: return uid, 0, 0, 0, "Error", None
    
    api_url = f"https://api.socialdata.tools/twitter/tweets/{t_id.group(1)}"
    try:
        async with session.get(api_url, headers={"Authorization": f"Bearer {api_key}"}, timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                u = data.get('user') or data.get('author') or {}
                return (uid, data.get('favorite_count', 0), data.get('views_count', 0),
                        data.get('reply_count', 0), "Found", u.get('screen_name') or u.get('username'))
    except: pass
    return uid, 0, 0, 0, "Fail", None

def get_discord_data():
    token = os.getenv('DISCORD_TOKEN')
    headers = {"Authorization": token}
    user_stats, tweet_list = {}, []

    # 1. СОБИРАЕМ ТВИТЫ (строго из одной ветки)
    log(f"📡 Поиск твитов в: {CONTENT_THREAD}")
    last_id = None
    while True:
        r = requests.get(f"https://discord.com/api/v10/channels/{CONTENT_THREAD}/messages?limit=100" + (f"&before={last_id}" if last_id else ""), headers=headers)
        if r.status_code != 200: break
        msgs = r.json()
        if not msgs: break
        for m in msgs:
            dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
            if dt < TARGET_DATE: { last_id := "STOP" }; break
            
            # Ищем ссылки в тексте и эмбедах
            full_txt = m.get('content', '')
            for e in m.get('embeds', []): full_txt += f" {e.get('url', '')} {e.get('description', '')}"
            
            links = re.findall(r'https?://(?:twitter\.com|x\.com|vxtwitter\.com|fxtwitter\.com)/[\w\d_]+/status/(\d+)', full_txt)
            for tid_str in links:
                tweet_list.append((m['author']['id'], f"https://twitter.com/i/status/{tid_str}"))
            last_id = m['id']
        if last_id == "STOP": break

    # 2. СОБИРАЕМ XP И СООБЩЕНИЯ (из списка веток)
    for tid in set(MESSAGE_THREADS):
        log(f"📡 Обработка ветки сообщений: {tid}")
        last_id = None
        while True:
            r = requests.get(f"https://discord.com/api/v10/channels/{tid}/messages?limit=100" + (f"&before={last_id}" if last_id else ""), headers=headers)
            if r.status_code != 200: break
            msgs = r.json()
            if not msgs: break
            for m in msgs:
                dt = datetime.fromisoformat(m['timestamp'].replace('Z', '+00:00'))
                if dt < TARGET_DATE: { last_id := "STOP" }; break
                
                uid = m['author']['id']
                if uid not in user_stats:
                    user_stats[uid] = {"user_id": uid, "username": m['author']['username'], "discord_messages": 0, "twitter_posts": 0, "twitter_likes": 0, "twitter_views": 0, "twitter_replies": 0, "twitter_handle": "not_linked", "total_score": 0, "channels_count": 0, "active_channels": set()}
                
                user_stats[uid]["discord_messages"] += 1
                user_stats[uid]["active_channels"].add(tid)

                # XP парсим ТОЛЬКО если это ветка для XP
                if tid == XP_SOURCE_THREAD and m.get('embeds'):
                    for emb in m['embeds']:
                        txt = (emb.get('description', '') + " ".join([f.get('value', '') for f in emb.get('fields', [])]))
                        xp_match = re.search(r'([\d\.,]+[KM]?)\s?/\s?[\d\.,]+[KM]?\s?XP', txt)
                        user_match = re.search(r'<@!?(\d+)>', txt)
                        if xp_match and user_match:
                            target_id = user_match.group(1)
                            val = parse_xp_value(xp_match.group(1))
                            if target_id in user_stats:
                                user_stats[target_id]["total_score"] = max(user_stats[target_id]["total_score"], val)
                
                last_id = m['id']
            if last_id == "STOP": break
    return user_stats, tweet_list

async def main():
    log("🚀 Старт...")
    tw_key = os.getenv('SOCIALDATA_KEY')
    supabase = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

    users, tweets = get_discord_data()

    if tweets:
        log(f"🐦 Найдено {len(tweets)} твитов. Запрос к Twitter API...")
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(tweets), 10):
                results = await asyncio.gather(*[fetch_tweet(session, t, tw_key) for t in tweets[i:i+10]])
                for uid, l, v, r, status, handle in results:
                    if status == "Found" and uid in users:
                        users[uid].update({"twitter_posts": users[uid]["twitter_posts"]+1, "twitter_likes": users[uid]["twitter_likes"]+l, "twitter_views": users[uid]["twitter_views"]+v, "twitter_replies": users[uid]["twitter_replies"]+r})
                        if handle: users[uid]["twitter_handle"] = handle
                log(f"⏳ {min(i+10, len(tweets))}/{len(tweets)}")

    payload = []
    for u in users.values():
        u["total_score"] = max(u["total_score"], u["discord_messages"] * 10)
        u["channels_count"] = len(u.pop("active_channels"))
        payload.append(u)

    if payload:
        log(f"📤 Сохранение {len(payload)} юзеров в Supabase...")
        supabase.table("leaderboard_stats").upsert(payload, on_conflict="user_id").execute()
        log("✅ Готово!")

if __name__ == "__main__": asyncio.run(main())
