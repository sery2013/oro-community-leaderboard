# OpenGradient Social Ranking Leaderboard

A real-time analytics dashboard that tracks and ranks community contributors based on their activity across Discord and X (formerly Twitter).

**Live Demo:** [https://discord-twitter-leaderboard.vercel.app/](https://discord-twitter-leaderboard.vercel.app/)

## 🚀 Overview
This project rewards ecosystem engagement by calculating a custom "Engage XP" score. It automatically collects data from social platforms and presents a competitive, high-tech leaderboard for the community.

## ✨ Key Features
- **Dynamic Ranking:** Automated daily sync of contributor stats.
- **Multi-Platform Tracking:**
  - **Discord:** Tracks message count and active participation.
  - **X (Twitter):** Monitors posts, likes, and total reach (views).
- **Custom XP Formula:** A weighted scoring system that determines a user's "Engage XP".
- **Real-Time Search:** Instantly find any contributor by their Discord username or X handle.
- **Cyberpunk UI:** Modern, responsive dark-mode interface with neon accents and fluid animations.

## 🛠 Tech Stack
- **Frontend:** Next.js 14, React, Lucide Icons.
- **Database:** Supabase (PostgreSQL).
- **Automation:** Python script running on GitHub Actions.
- **Data Sourcing:** Discord API & SocialData API (for X metrics).

## 📡 How it Works
1. **Collector:** A Python script runs once every 24 hours via GitHub Actions.
2. **Analysis:** The script parses designated Discord channels and X activity for specific keywords.
3. **Storage:** Processed data is upserted into a Supabase database.
4. **Display:** The Next.js frontend fetches the latest rankings and displays them to the users.

---
*Developed for the OpenGradient Ecosystem.*
