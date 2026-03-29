'use client';
import { useEffect, useState, useRef, useMemo } from 'react';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
const supabase = (supabaseUrl && supabaseAnonKey) ? createClient(supabaseUrl, supabaseAnonKey) : null;

const ITEMS_PER_PAGE = 25;

export default function Leaderboard() {
  const [users, setUsers] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedUser, setSelectedUser] = useState<any | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [serverStats, setServerStats] = useState({
    totalContributors: 0,
    totalMessages: 0,
    totalXP: 0,
    activeChannels: 0,
    twitterPosts24h: 0
  });
  const modalRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const script = document.createElement('script');
    script.src = "https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js";
    script.async = true;
    document.body.appendChild(script);

    async function fetchData() {
      if (!supabase) return;
      const { data } = await supabase
        .from('leaderboard_stats')
        .select('*')
        .order('total_score', { ascending: false });
      
      const validData = (data || []).filter(u => u.discord_joined_at || (u.discord_roles && u.discord_roles.length > 0));
      setUsers(validData);

      // 🔥 Расчёт статистики сервера
      if (validData.length > 0) {
        const totalContributors = validData.length;
        const totalMessages = validData.reduce((sum, u) => sum + (u.discord_messages || 0), 0);
        const totalXP = validData.reduce((sum, u) => sum + (u.total_score || 0), 0);
        const twitterPosts24h = validData.reduce((sum, u) => sum + (u.twitter_posts || 0), 0);
        const channelsSet = new Set();
        validData.forEach(u => {
          if (u.channels_count) {
            for (let i = 0; i < u.channels_count; i++) channelsSet.add(i);
          }
        });
        const activeChannels = channelsSet.size;
        setServerStats({
          totalContributors,
          totalMessages,
          totalXP,
          activeChannels,
          twitterPosts24h
        });
      }
      setLoading(false);
    }
    fetchData();
  }, []);

  // Сбрасываем на 1-ю страницу при изменении поиска
  useEffect(() => {
    setCurrentPage(1);
  }, [searchQuery]);

  const downloadCard = async () => {
    const h2c = (window as any).html2canvas;
    if (modalRef.current && h2c) {
      modalRef.current.classList.add('export-mode');
      const canvas = await h2c(modalRef.current, {
        backgroundColor: '#FFA500',
        scale: 2,
        useCORS: true,
        logging: false,
        ignoreElements: (el: HTMLElement) =>
          el.classList.contains('download-btn') ||
          el.classList.contains('close-btn') ||
          el.classList.contains('export-mode')
      });
      modalRef.current.classList.remove('export-mode');
      const link = document.createElement('a');
      link.download = `${selectedUser?.username || 'contributor'}-stats.png`;
      link.href = canvas.toDataURL('image/png');
      link.click();
    }
  };

  const formatDate = (isoString: string) => {
    if (!isoString) return 'NEW MEMBER';
    const date = new Date(isoString);
    if (isNaN(date.getTime())) return 'NEW MEMBER';
    return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
  };

  const formatCompactNumber = (number: number) => {
    if (!number) return '0';
    if (number >= 1000000) return (number / 1000000).toFixed(1) + 'M';
    if (number >= 1000) return (number / 1000).toFixed(1) + 'K';
    return number.toString();
  };

  const calculateChange = (current: number, previous: number) => {
    if (previous === 0) return { diff: current, percent: current > 0 ? 100 : 0 };
    const diff = current - previous;
    const percent = (diff / previous) * 100;
    return { diff, percent };
  };

  const filteredUsers = useMemo(() => {
    const term = searchQuery.toLowerCase();
    return users.filter((user) => {
      return (
        user.username?.toLowerCase().includes(term) ||
        user.twitter_handle?.toLowerCase().includes(term)
      );
    });
  }, [users, searchQuery]);

  // Пагинация
  const totalPages = Math.ceil(filteredUsers.length / ITEMS_PER_PAGE);
  const paginatedUsers = filteredUsers.slice(
    (currentPage - 1) * ITEMS_PER_PAGE,
    currentPage * ITEMS_PER_PAGE
  );

  const handleOverlayClick = (e: React.MouseEvent) => {
    if (e.target === e.currentTarget) setSelectedUser(null);
  };

  if (loading) return (
    <div className="loading-screen">
      <div className="loader"></div>
      <span>SYNCHRONIZING NETWORK DATA...</span>
      <style jsx>{`
        .loading-screen {
          height: 100vh;
          display: flex;
          flex-direction: column;
          justify-content: center;
          align-items: center;
          gap: 20px;
          background: #FF8C00;
          color: #000;
          letter-spacing: 4px;
          font-family: 'Space Grotesk', sans-serif;
        }
        .loader {
          width: 40px;
          height: 40px;
          border: 2px solid rgba(0,0,0,0.1);
          border-top-color: #000;
          border-radius: 50%;
          animation: spin 1s linear infinite;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );

  return (
    <div className="container">
      <div className="glow glow-1"></div>
      <div className="glow glow-2"></div>
      <div className="grid-overlay"></div>
      <div className="main-content">
        <header className="header-section">
          <div className="branding-banner">
            <div className="branding-content">
              <div className="status-dot"></div>
              <h1 className="main-title">ORO AI SOCIAL RANKING</h1>
              <div className="status-info">
                <span className="update-badge">24H SYNC</span>
                <span className="status-label">LEADERBOARD UPDATED ONCE DAILY</span>
              </div>
            </div>
          </div>
          <div className="action-bar">
            <div className="accent-line"></div>
            <div className="search-wrapper">
              <svg className="search-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <circle cx="11" cy="11" r="8"></circle>
                <path d="M21 21l-4.35-4.35"></path>
              </svg>
              <input
                type="text"
                placeholder="Find contributor by username or X handle..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="search-input"
              />
            </div>
          </div>
        </header>

        {/* === ПАНЕЛЬ СТАТИСТИКИ СЕРВЕРА === */}
        <div className="server-stats-panel">
          <div className="stat-item">
            <div className="stat-icon">👥</div>
            <div className="stat-content">
              <span className="stat-label">Total Contributors</span>
              <span className="stat-value">{serverStats.totalContributors}</span>
            </div>
          </div>
          <div className="stat-divider"></div>
          <div className="stat-item">
            <div className="stat-icon">💬</div>
            <div className="stat-content">
              <span className="stat-label">24h Messages</span>
              <span className="stat-value">{serverStats.totalMessages.toLocaleString()}</span>
            </div>
          </div>
          <div className="stat-divider"></div>
          <div className="stat-item">
            <div className="stat-icon">⚡</div>
            <div className="stat-content">
              <span className="stat-label">Total XP</span>
              <span className="stat-value">{serverStats.totalXP.toLocaleString()}</span>
            </div>
          </div>
          <div className="stat-divider"></div>
          <div className="stat-item">
            <div className="stat-icon">📢</div>
            <div className="stat-content">
              <span className="stat-label">Active Channels</span>
              <span className="stat-value">{serverStats.activeChannels}</span>
            </div>
          </div>
          <div className="stat-divider"></div>
          <div className="stat-item">
            <div className="stat-icon">🐦</div>
            <div className="stat-content">
              <span className="stat-label">24h Twitter Posts</span>
              <span className="stat-value highlight">{serverStats.twitterPosts24h}</span>
              <span className="stat-sub">Total posts today</span>
            </div>
          </div>
        </div>

        <div className="stats-grid">
          {paginatedUsers.length > 0 ? (
            paginatedUsers.map((user, index) => {
              const originalRank = users.findIndex(u => u.user_id === user.user_id) + 1;
              const xpChange = calculateChange(user.total_score || 0, user.prev_total_score || 0);
              const msgChange = calculateChange(user.discord_messages || 0, user.prev_discord_messages || 0);
              return (
                <div
                  key={user.user_id}
                  className="contributor-card"
                  style={{ '--i': index } as any}
                  onClick={() => setSelectedUser(user)}
                >
                  <div className="card-identity">
                    <div className="rank-container">
                      <span className="rank-hash">#</span>
                      <span className="rank-number">{originalRank}</span>
                    </div>
                    <div className="user-profile">
                      <div className="avatar-wrapper">
                        <img
                          src={user.avatar_url || 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png'}
                          className="user-avatar"
                          alt="avatar"
                        />
                        <div className="avatar-ring"></div>
                        {user.discord_roles && Array.isArray(user.discord_roles) && user.discord_roles.length > 0 && (
                          <div className="roles-badge" title={`${user.discord_roles.length} Discord Roles`}>
                            {user.discord_roles.length}
                          </div>
                        )}
                      </div>
                      <div className="name-box">
                        <div className="username-row">
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#000" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                            <circle cx="12" cy="7" r="4"></circle>
                          </svg>
                          <h2 className="display-name">{user.username}</h2>
                        </div>
                        {user.prev_total_score !== undefined && user.prev_discord_messages !== undefined && (
                          <div className="delta-container" style={{ marginTop: '6px', fontSize: '0.75rem' }}>
                            <div className="delta-row">
                              <span className={`value ${xpChange.diff > 0 ? 'positive' : xpChange.diff < 0 ? 'negative' : 'neutral'}`}>
                                {xpChange.diff > 0 ? '📈' : xpChange.diff < 0 ? '📉' : '→'}{' '}
                                {xpChange.diff} XP ({(xpChange.percent > 0 ? '+' : '') + xpChange.percent.toFixed(1)}%)
                              </span>
                            </div>
                            <div className="delta-row">
                              <span className={`value ${msgChange.diff > 0 ? 'positive' : msgChange.diff < 0 ? 'negative' : 'neutral'}`}>
                                {msgChange.diff > 0 ? '📈' : msgChange.diff < 0 ? '📉' : '→'}{' '}
                                {msgChange.diff} MSG ({(msgChange.percent > 0 ? '+' : '') + msgChange.percent.toFixed(1)}%)
                              </span>
                            </div>
                          </div>
                        )}
                        <div className="user-meta">
                          <div className="meta-badge">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                              <rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect>
                              <line x1="16" y1="2" x2="16" y2="6"></line>
                              <line x1="8" y1="2" x2="8" y2="6"></line>
                              <line x1="3" y1="10" x2="21" y2="10"></line>
                            </svg>
                            <span className="join-date">{formatDate(user.discord_joined_at)}</span>
                          </div>
                          <a
                            href={user.twitter_handle ? `https://x.com/${user.twitter_handle}` : '#'}
                            target="_blank"
                            rel="noopener noreferrer"
                            className={`meta-badge twitter-link ${!user.twitter_handle ? 'disabled' : ''}`}
                            onClick={(e) => {
                              if (!user.twitter_handle) e.preventDefault();
                              e.stopPropagation();
                            }}
                          >
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
                              <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
                            </svg>
                            <span>@{user.twitter_handle || 'not_linked'}</span>
                          </a>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="card-metrics">
                    <div className="metrics-inner">
                      <div className="metric-box">
                        <span className="metric-label">DISCORD MESSAGES</span>
                        <div className="stat-row">
                          <span className="stat-dot-small messages" style={{ background: '#000', boxShadow: '0 0 5px #000' }}></span>
                          <span className="stat-val">{user.discord_messages || 0}</span>
                          <span className="stat-suffix">MSG</span>
                        </div>
                        <div className="channel-activity-row">
                          <span className="stat-dot-small channels"></span>
                          <span className="stat-val">{user.channels_count || 0}</span>
                          <span className="stat-suffix">ACTIVE CHANNELS</span>
                        </div>
                      </div>
                      <div className="metric-box twitter-impact">
                        <span className="metric-label">TWITTER IMPACT</span>
                        <div className="twitter-stats-column">
                          <div className="stat-row">
                            <span className="stat-dot-small posts" style={{ background: '#000', boxShadow: '0 0 5px #000' }}></span>
                            <span className="stat-val">{user.twitter_posts || 0}</span>
                            <span className="stat-suffix">POSTS</span>
                          </div>
                          <div className="stat-row sub">
                            <span className="stat-dot-small likes"></span>
                            <span className="stat-val">{user.twitter_likes || 0}</span>
                            <span className="stat-suffix">LIKES</span>
                          </div>
                          <div className="stat-row sub">
                            <span className="stat-dot-small views"></span>
                            <span className="stat-val">{formatCompactNumber(user.twitter_views)}</span>
                            <span className="stat-suffix">VIEWS</span>
                          </div>
                          <div className="stat-row sub">
                            <span className="stat-dot-small replies"></span>
                            <span className="stat-val">{user.twitter_replies || 0}</span>
                            <span className="stat-suffix">REPLIES</span>
                          </div>
                        </div>
                      </div>
                      <div className="metric-box total">
                        <span className="metric-label">ENGAGE XP</span>
                        <span className="total-score-value">{Math.floor(user.total_score || 0)}</span>
                      </div>
                    </div>
                  </div>
                </div>
              );
            })
          ) : (
            <div className="empty-state">
              <p>NO CONTRIBUTORS MATCHING "{searchQuery}"</p>
            </div>
          )}
        </div>

        {/* === ПАГИНАЦИЯ === */}
        {totalPages > 1 && (
          <div className="pagination">
            <button
              className="pagination-btn"
              onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
              disabled={currentPage === 1}
            >
              ← Prev
            </button>
            <div className="pagination-info">
              Page <span className="pagination-current">{currentPage}</span> of {totalPages}
            </div>
            <button
              className="pagination-btn"
              onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
              disabled={currentPage === totalPages}
            >
              Next →
            </button>
          </div>
        )}

        <footer className="footer">
          <div className="footer-links">
            <a href="https://getoro.xyz" target="_blank" rel="noopener" className="f-link">
              © 2026 getoro.xyz
            </a>
            <span className="f-sep">|</span>
            <a href="https://x.com/kaye_moni" target="_blank" rel="noopener" className="f-link dev">
              <svg className="x-logo" width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
                <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
              </svg>
              <span>Developer: @kaye_moni</span>
            </a>
          </div>
        </footer>
      </div>

      {selectedUser && (
        <div className="modal-overlay" onClick={handleOverlayClick}>
          <div className="modal-content" ref={modalRef}>
            <button className="close-btn" onClick={() => setSelectedUser(null)}>&times;</button>
            <div className="modal-header">
              <div className="modal-avatar-wrapper">
                <img src={selectedUser.avatar_url} className="modal-avatar" alt="" />
                <div className="modal-avatar-glow"></div>
              </div>
              <div className="modal-titles">
                <div className="modal-rank-badge">RANK #{users.findIndex(u => u.user_id === selectedUser.user_id) + 1}</div>
                <h2>{selectedUser.username}</h2>
                <p>Member since {formatDate(selectedUser.discord_joined_at)}</p>
              </div>
            </div>
            <div className="modal-body">
              <div className="stat-grid-modal">
                <div className="stat-group-modal">
                  <h3>DISCORD PERFORMANCE</h3>
                  <div className="stat-item-modal">
                    <span>Messages</span>
                    <span className="val">{selectedUser.discord_messages || 0}</span>
                  </div>
                  <div className="stat-item-modal">
                    <span>Active Channels</span>
                    <span className="val">{selectedUser.channels_count || 0}</span>
                  </div>
                  <div className="stat-item-modal">
                    <span>Discord Roles</span>
                    <span className="val">{selectedUser.discord_roles?.length || 0}</span>
                  </div>
                </div>
                <div className="stat-group-modal">
                  <h3>X (TWITTER) IMPACT</h3>
                  <div className="stat-item-modal">
                    <span>Total Posts</span>
                    <span className="val">{selectedUser.twitter_posts || 0}</span>
                  </div>
                  <div className="stat-item-modal">
                    <span>Engagement Index</span>
                    <span className="val">
                      {Math.round(((selectedUser.twitter_likes + selectedUser.twitter_replies) / (selectedUser.twitter_posts || 1)) * 10) / 10}
                    </span>
                  </div>
                  <div className="stat-item-modal">
                    <span>Total Impressions</span>
                    <span className="val">{selectedUser.twitter_views?.toLocaleString() || 0}</span>
                  </div>
                </div>
              </div>
              <div className="modal-total-score">
                <div className="score-info">
                  <span className="score-label">AGGREGATED NETWORK POWER</span>
                  <span className="score-sub">Verified on-chain contribution</span>
                </div>
                <div className="score-value">{Math.floor(selectedUser.total_score)} XP</div>
              </div>
              <button className="download-btn" onClick={downloadCard}>
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                  <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                  <polyline points="7 10 12 15 17 10"></polyline>
                  <line x1="12" y1="15" x2="12" y2="3"></line>
                </svg>
                DOWNLOAD PNG
              </button>
            </div>
          </div>
        </div>
      )}

      <style jsx global>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;500;700&display=swap');
        body {
          margin: 0;
          background: #FF8C00;
          color: #000;
          font-family: 'Space Grotesk', sans-serif;
          overflow-x: hidden;
        }
        .container {
          position: relative;
          min-height: 100vh;
          padding: 60px 40px;
          display: flex;
          justify-content: center;
        }
        .grid-overlay {
          position: fixed;
          top: 0; left: 0; width: 100%; height: 100%;
          background-image: radial-gradient(rgba(0,0,0,0.1) 1px, transparent 1px);
          background-size: 40px 40px;
          z-index: 0;
          pointer-events: none;
        }
        .glow {
          position: fixed;
          width: 800px;
          height: 800px;
          filter: blur(160px);
          opacity: 0.2;
          z-index: 0;
          pointer-events: none;
        }
        .glow-1 { top: -200px; left: -100px; background: #FFD700; }
        .glow-2 { bottom: -200px; right: -100px; background: #FF6B35; }
        .main-content {
          width: 100%;
          max-width: 1400px;
          z-index: 10;
        }
        .header-section { margin-bottom: 50px; }
        .branding-banner {
          display: block;
          border: 2px solid rgba(0,0,0,0.3);
          background: rgba(255,255,255,0.15);
          padding: 25px 40px;
          border-radius: 20px;
          backdrop-filter: blur(10px);
          transition: 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
          margin-bottom: 25px;
        }
        .branding-content {
          display: flex;
          align-items: center;
          gap: 20px;
          flex-wrap: wrap;
        }
        .status-dot {
          width: 8px; height: 8px;
          background: #000;
          border-radius: 50%;
          box-shadow: 0 0 10px #000;
          animation: pulse 2s infinite;
        }
        @keyframes pulse {
          0% { opacity: 1; } 50% { opacity: 0.3; } 100% { opacity: 1; }
        }
        .main-title {
          font-size: 2rem;
          font-weight: 700;
          letter-spacing: 5px;
          margin: 0;
          flex-grow: 1;
          color: #000;
          text-shadow: 0 0 20px rgba(255,255,255,0.3);
        }
        .status-info {
          display: flex;
          flex-direction: column;
          align-items: flex-end;
          gap: 4px;
        }
        .update-badge {
          background: rgba(0,0,0,0.15);
          color: #000;
          padding: 2px 8px;
          border-radius: 4px;
          font-size: 0.6rem;
          font-weight: 700;
          border: 1px solid rgba(0,0,0,0.3);
        }
        .status-label {
          font-size: 0.7rem;
          letter-spacing: 2px;
          color: rgba(0,0,0,0.7);
          font-weight: 500;
        }
        .action-bar {
          display: flex;
          align-items: center;
          gap: 30px;
        }
        .accent-line {
          height: 1px;
          flex-grow: 1;
          background: linear-gradient(90deg, #000, rgba(0,0,0,0.3), transparent);
        }
        .search-wrapper {
          position: relative;
          width: 450px;
        }
        .search-icon {
          position: absolute;
          left: 18px; top: 50%;
          transform: translateY(-50%);
          color: #000;
          opacity: 0.7;
        }
        .search-input {
          width: 100%;
          background: rgba(255,255,255,0.2);
          border: 2px solid rgba(0,0,0,0.3);
          padding: 16px 20px 16px 55px;
          border-radius: 14px;
          color: #000;
          font-size: 1rem;
          transition: 0.3s;
        }
        .search-input::placeholder {
          color: rgba(0,0,0,0.5);
        }
        .search-input:focus {
          border-color: rgba(0,0,0,0.6);
          outline: none;
          background: rgba(255,255,255,0.3);
        }
        .stats-grid {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }
        .contributor-card {
          cursor: pointer;
          display: flex;
          background: rgba(255,255,255,0.15);
          border: 2px solid rgba(0,0,0,0.3);
          border-radius: 20px;
          overflow: hidden;
          opacity: 0;
          transform: translateY(20px);
          animation: fadeInUp 0.6s cubic-bezier(0.2, 0.8, 0.2, 1) forwards;
          animation-delay: calc(var(--i) * 0.08s);
          transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .contributor-card:hover {
          background: rgba(255,255,255,0.25);
          border-color: rgba(0,0,0,0.5);
          transform: translateY(-5px) scale(1.01);
          backdrop-filter: blur(12px);
          box-shadow: 0 15px 35px rgba(0,0,0,0.2);
          z-index: 5;
        }
        @keyframes fadeInUp {
          to { opacity: 1; transform: translateY(0); }
        }
        .card-identity {
          padding: 30px 40px;
          display: flex;
          align-items: center;
          gap: 40px;
          min-width: 480px;
          border-right: 1px solid rgba(0,0,0,0.2);
          background: rgba(255,255,255,0.08);
        }
        .rank-container {
          display: flex;
          align-items: baseline;
          min-width: 70px;
        }
        .rank-hash { color: #000; font-size: 1.2rem; font-weight: 300; }
        .rank-number { font-size: 2.5rem; font-weight: 700; color: #000; }
        .user-profile {
          display: flex;
          align-items: center;
          gap: 20px;
        }
        .avatar-wrapper {
          position: relative;
          width: 65px; height: 65px;
        }
        .user-avatar {
          width: 100%; height: 100%;
          border-radius: 18px;
          object-fit: cover;
          position: relative;
          z-index: 2;
          border: 2px solid rgba(0,0,0,0.3);
        }
        .avatar-ring {
          position: absolute;
          inset: -3px;
          border: 2px solid #000;
          border-radius: 20px;
          opacity: 0.2;
          z-index: 1;
        }
        .roles-badge {
          position: absolute;
          bottom: -5px; right: -5px;
          background: #000;
          color: #FF8C00;
          font-size: 0.65rem;
          font-weight: 700;
          padding: 2px 6px;
          border-radius: 6px;
          z-index: 10;
          border: 2px solid #FF8C00;
        }
        .name-box {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        .username-row {
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .display-name {
          margin: 0;
          font-size: 1.4rem;
          font-weight: 700;
          color: #000;
          letter-spacing: -0.5px;
        }
        .delta-container {
          font-size: 0.75rem;
          line-height: 1.4;
        }
        .delta-row {
          display: flex;
          justify-content: flex-start;
          margin-bottom: 2px;
        }
        .delta-row:last-child {
          margin-bottom: 0;
        }
        .value.positive { color: #059669; }
        .value.negative { color: #DC2626; }
        .value.neutral { color: #6B7280; }
        .user-meta {
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
        }
        .meta-badge {
          display: flex;
          align-items: center;
          gap: 6px;
          background: rgba(255,255,255,0.2);
          border: 1px solid rgba(0,0,0,0.3);
          padding: 4px 10px;
          border-radius: 8px;
          font-size: 0.75rem;
          color: rgba(0,0,0,0.8);
          text-decoration: none;
          transition: 0.2s;
        }
        .twitter-link {
          color: #000;
          border-color: rgba(0,0,0,0.3);
          cursor: pointer;
        }
        .twitter-link:hover:not(.disabled) {
          background: rgba(255,255,255,0.3);
          border-color: #000;
        }
        .twitter-link.disabled {
          opacity: 0.4;
          filter: grayscale(0.5);
          cursor: default;
        }
        .card-metrics {
          flex-grow: 1;
          padding: 0 40px;
          display: flex;
          align-items: center;
        }
        .metrics-inner {
          display: grid;
          grid-template-columns: 1.2fr 1.5fr 1.5fr;
          width: 100%;
          gap: 20px;
        }
        .metric-box { padding: 10px 0; }
        .twitter-stats-column { display: flex; flex-direction: column; gap: 2px; }
        .stat-row { display: flex; align-items: baseline; gap: 6px; }
        .stat-row.sub { opacity: 0.7; margin-top: -1px; }
        .stat-val { font-size: 1.1rem; font-weight: 700; color: #000; }
        .stat-suffix { font-size: 0.6rem; letter-spacing: 1px; }
        .stat-dot-small { width: 4px; height: 4px; border-radius: 50%; margin-bottom: 2px; }
        .stat-dot-small.likes { background: #000; box-shadow: 0 0 5px #000; }
        .stat-dot-small.views { background: #000; box-shadow: 0 0 5px #000; }
        .stat-dot-small.replies { background: #000; box-shadow: 0 0 5px #000; }
        .channel-activity-row { display: flex; align-items: baseline; gap: 6px; margin-top: 4px; opacity: 0.8; }
        .stat-dot-small.channels { background: #000; box-shadow: 0 0 5px #000; }
        .metric-label {
          display: block;
          font-size: 0.6rem;
          color: rgba(0,0,0,0.6);
          letter-spacing: 2px;
          margin-bottom: 8px;
        }
        .metric-box.total {
          background: rgba(255,255,255,0.15);
          border-radius: 14px;
          padding: 15px 25px;
          text-align: right;
          border: 2px solid rgba(0,0,0,0.3);
        }
        .total-score-value {
          font-size: 2.2rem;
          font-weight: 700;
          color: #000;
          text-shadow: 0 0 20px rgba(255,255,255,0.3);
        }
        /* === ПАНЕЛЬ СТАТИСТИКИ СЕРВЕРА === */
        .server-stats-panel {
          display: flex;
          align-items: center;
          justify-content: space-around;
          gap: 20px;
          padding: 20px 30px;
          background: rgba(255,255,255,0.15);
          border: 2px solid rgba(0,0,0,0.3);
          border-radius: 16px;
          margin-bottom: 24px;
          backdrop-filter: blur(10px);
          transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        }
        .server-stats-panel:hover {
          background: rgba(255,255,255,0.25);
          border-color: rgba(0,0,0,0.5);
          box-shadow: 0 0 30px rgba(255,255,255,0.2);
          transform: translateY(-2px);
        }
        .stat-item {
          display: flex;
          align-items: center;
          gap: 12px;
        }
        .stat-icon {
          font-size: 1.8rem;
        }
        .stat-content {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }
        .stat-label {
          font-size: 0.65rem;
          color: rgba(0,0,0,0.6);
          letter-spacing: 1px;
          text-transform: uppercase;
        }
        .stat-value {
          font-size: 1.3rem;
          font-weight: 700;
          color: #000;
        }
        .stat-value.highlight {
          color: #059669;
        }
        .stat-sub {
          font-size: 0.75rem;
          color: rgba(0,0,0,0.6);
        }
        .stat-divider {
          width: 1px;
          height: 40px;
          background: linear-gradient(to bottom, transparent, rgba(0,0,0,0.3), transparent);
        }
        @media (max-width: 900px) {
          .server-stats-panel {
            flex-wrap: wrap;
            padding: 16px 20px;
          }
          .stat-divider {
            display: none;
          }
          .stat-item {
            flex: 1 1 calc(50% - 20px);
            min-width: 150px;
          }
        }
        /* === ПАГИНАЦИЯ === */
        .pagination {
          display: flex;
          justify-content: center;
          align-items: center;
          gap: 20px;
          margin: 40px 0 60px;
          padding: 20px;
          background: rgba(255,255,255,0.15);
          border: 2px solid rgba(0,0,0,0.3);
          border-radius: 16px;
        }
        .pagination-btn {
          background: transparent;
          border: 2px solid rgba(0,0,0,0.3);
          color: #000;
          padding: 10px 24px;
          border-radius: 12px;
          cursor: pointer;
          font-family: 'Space Grotesk', sans-serif;
          font-weight: 600;
          font-size: 0.9rem;
          transition: all 0.2s ease;
        }
        .pagination-btn:hover:not(:disabled) {
          background: rgba(255,255,255,0.3);
          border-color: #000;
        }
        .pagination-btn:disabled {
          opacity: 0.3;
          cursor: not-allowed;
        }
        .pagination-info {
          font-size: 0.9rem;
          color: rgba(0,0,0,0.7);
          letter-spacing: 1px;
        }
        .pagination-current {
          color: #000;
          font-weight: 700;
        }
        .modal-overlay {
          position: fixed;
          top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0,0,0,0.7);
          backdrop-filter: blur(12px);
          display: flex;
          justify-content: center;
          align-items: center;
          z-index: 1000;
          padding: 20px;
          animation: modalFadeIn 0.3s ease;
        }
        .modal-content {
          background: #FFA500;
          width: 100%;
          max-width: 600px;
          border-radius: 32px;
          border: 2px solid rgba(0,0,0,0.3);
          padding: 40px;
          position: relative;
          box-shadow: 0 25px 50px rgba(0,0,0,0.3);
          animation: modalSlideUp 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        .close-btn {
          position: absolute;
          top: 30px; right: 30px;
          background: none;
          border: none;
          color: #000;
          font-size: 32px;
          cursor: pointer;
          transition: 0.2s;
        }
        .close-btn:hover {
          color: #FF8C00;
          transform: rotate(90deg);
        }
        .modal-header {
          display: flex;
          align-items: center;
          gap: 30px;
          margin-bottom: 40px;
        }
        .modal-avatar-wrapper {
          position: relative;
          width: 100px; height: 100px;
        }
        .modal-avatar {
          width: 100%; height: 100%;
          border-radius: 24px;
          border: 2px solid #000;
          position: relative;
          z-index: 2;
        }
        .modal-avatar-glow {
          position: absolute;
          inset: 0;
          background: #000;
          filter: blur(20px);
          opacity: 0.2;
          z-index: 1;
        }
        .modal-titles h2 {
          margin: 10px 0 5px;
          font-size: 2.2rem;
          color: #000;
        }
        .modal-titles p {
          margin: 0;
          color: rgba(0,0,0,0.7);
          font-size: 1rem;
        }
        .modal-rank-badge {
          display: inline-block;
          background: #000;
          color: #FF8C00;
          font-weight: 700;
          font-size: 0.7rem;
          padding: 4px 12px;
          border-radius: 20px;
          letter-spacing: 1px;
        }
        .stat-grid-modal {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 30px;
          margin-bottom: 40px;
        }
        .stat-group-modal h3 {
          font-size: 0.7rem;
          color: #000;
          letter-spacing: 2px;
          opacity: 0.8;
          margin-bottom: 15px;
          border-bottom: 1px solid rgba(0,0,0,0.2);
          padding-bottom: 10px;
        }
        .stat-item-modal {
          display: flex;
          justify-content: space-between;
          padding: 10px 0;
          border-bottom: 1px solid rgba(0,0,0,0.1);
          font-size: 0.95rem;
          color: rgba(0,0,0,0.8);
        }
        .stat-item-modal .val {
          color: #000;
          font-weight: 700;
        }
        .modal-total-score {
          background: rgba(255,255,255,0.2);
          border: 2px solid rgba(0,0,0,0.3);
          border-radius: 20px;
          padding: 25px 30px;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }
        .score-label {
          display: block;
          font-weight: 700;
          color: #000;
          font-size: 1.1rem;
        }
        .score-sub {
          display: block;
          color: rgba(0,0,0,0.7);
          font-size: 0.8rem;
        }
        .score-value {
          font-size: 2.5rem;
          font-weight: 700;
          color: #000;
        }
        .download-btn {
          margin-top: 30px;
          width: 100%;
          background: transparent;
          border: 2px dashed rgba(0,0,0,0.4);
          color: #000;
          padding: 14px;
          border-radius: 16px;
          cursor: pointer;
          font-family: 'Space Grotesk', sans-serif;
          font-weight: 700;
          font-size: 0.8rem;
          letter-spacing: 2px;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 10px;
          transition: 0.3s;
        }
        .download-btn:hover {
          background: rgba(255,255,255,0.2);
          border-style: solid;
        }
        .modal-content.export-mode,
        .contributor-card.export-mode {
          filter: none !important;
          backdrop-filter: none !important;
          box-shadow: none !important;
        }
        @keyframes modalFadeIn { from { opacity: 0; } }
        @keyframes modalSlideUp { from { transform: translateY(40px); opacity: 0; } }
        .footer {
          text-align: center;
          padding: 40px 0;
          margin-top: 60px;
          border-top: 2px solid rgba(0,0,0,0.2);
        }
        .footer-links {
          display: flex;
          justify-content: center;
          align-items: center;
          gap: 15px;
        }
        .f-link {
          color: #000;
          text-decoration: none;
          opacity: 0.8;
          transition: 0.2s;
          display: flex;
          align-items: center;
          gap: 8px;
        }
        .f-link:hover {
          opacity: 1;
        }
        .f-sep {
          color: rgba(0,0,0,0.3);
        }
        @media (max-width: 1100px) {
          .card-identity {
            min-width: auto;
            flex-direction: column;
            align-items: flex-start;
            gap: 20px;
            border-right: none;
            border-bottom: 1px solid rgba(0,0,0,0.2);
          }
        }
      `}</style>
    </div>
  );
}
