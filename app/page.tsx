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
  const [lastUpdate, setLastUpdate] = useState<string>('Syncing...');
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

      if (data && data.length > 0 && data[0].updated_at) {
        const date = new Date(data[0].updated_at);
        const formatted = date.toLocaleString('en-GB', { 
          day: '2-digit', 
          month: 'short', 
          hour: '2-digit', 
          minute: '2-digit' 
        });
        setLastUpdate(formatted);
      } else {
        setLastUpdate('Daily Sync');
      }

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
                <span className="update-badge">{lastUpdate}</span>
                <span className="status-label">DATA UPDATED MANUALLY</span>
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
              <span className="stat-label">48h Messages</span>
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
              <span className="stat-label">48h Twitter Posts</span>
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
                          <div className="meta-badge twitter-link">
                            <svg width="12" height="12" viewBox="0 0 24 24" fill="currentColor">
                              <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"/>
                            </svg>
                            <span>@{user.twitter_handle || 'not_linked'}</span>
                          </div>
                        </div>
                      </div>
                    </div>
                  </div>
                  <div className="card-metrics">
                    <div className="metrics-inner">
                      <div className="metric-box">
                        <span className="metric-label">DISCORD MESSAGES</span>
                        <div className="stat-row">
                          <span className="stat-val">{user.discord_messages || 0}</span>
                          <span className="stat-suffix">MSG</span>
                        </div>
                        <div className="channel-activity-row">
                          <span className="stat-val">{user.channels_count || 0}</span>
                          <span className="stat-suffix">ACTIVE CHANNELS</span>
                        </div>
                      </div>
                      <div className="metric-box twitter-impact">
                        <span className="metric-label">TWITTER IMPACT</span>
                        <div className="twitter-stats-column">
                          <div className="stat-row">
                            <span className="stat-val">{user.twitter_posts || 0}</span>
                            <span className="stat-suffix">POSTS</span>
                          </div>
                          <div className="stat-row sub">
                            <span className="stat-val">{formatCompactNumber(user.twitter_views)}</span>
                            <span className="stat-suffix">VIEWS</span>
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
                </div>
                <div className="stat-group-modal">
                  <h3>X (TWITTER) IMPACT</h3>
                  <div className="stat-item-modal">
                    <span>Total Posts</span>
                    <span className="val">{selectedUser.twitter_posts || 0}</span>
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
                </div>
                <div className="score-value">{Math.floor(selectedUser.total_score)} XP</div>
              </div>
              <button className="download-btn" onClick={downloadCard}>
                DOWNLOAD PNG
              </button>
            </div>
          </div>
        </div>
      )}

      <style jsx global>{`
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@300;400;500;600;700;800&display=swap');
        
        body {
          margin: 0;
          background: #FF8C00;
          background-image: 
            url("data:image/svg+xml,%3Csvg viewBox='0 0 200 200' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.65' numOctaves='3'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E"),
            linear-gradient(135deg, #FF8C00 0%, #FFA500 50%, #FFB347 100%);
          background-blend-mode: overlay;
          color: #000;
          font-family: 'Space Grotesk', sans-serif;
          overflow-x: hidden;
          min-height: 100vh;
        }

        .container {
          position: relative;
          min-height: 100vh;
          padding: 60px 40px;
          display: flex;
          justify-content: center;
          z-index: 1;
        }

        .grid-overlay {
          position: fixed;
          top: 0; left: 0; width: 100%; height: 100%;
          background-image: radial-gradient(rgba(0,0,0,0.08) 1px, transparent 1px);
          background-size: 30px 30px;
          z-index: 0;
          pointer-events: none;
        }

        .main-content {
          width: 100%;
          max-width: 1300px;
          position: relative;
          z-index: 10;
        }

        .header-section {
          margin-bottom: 40px;
        }

        .branding-banner {
          background: rgba(255, 255, 255, 0.1);
          backdrop-filter: blur(15px);
          -webkit-backdrop-filter: blur(15px);
          border: 2px solid rgba(0, 0, 0, 0.2);
          border-radius: 24px;
          padding: 35px 45px;
          margin-bottom: 30px;
          box-shadow: 0 10px 40px rgba(0, 0, 0, 0.05);
        }

        .branding-content {
          display: flex;
          align-items: center;
          justify-content: space-between;
          gap: 20px;
        }

        .status-dot {
          width: 12px;
          height: 12px;
          background: #000;
          border-radius: 50%;
          box-shadow: 0 0 15px rgba(0,0,0,0.3);
        }

        .main-title {
          font-size: 2.4rem;
          font-weight: 900;
          letter-spacing: 5px;
          margin: 0;
          flex-grow: 1;
          background: linear-gradient(90deg, #000 0%, #444 50%, #000 100%);
          background-size: 200% auto;
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          animation: shine 4s linear infinite;
          text-transform: uppercase;
        }

        @keyframes shine { to { background-position: 200% center; } }

        .status-info {
          display: flex;
          flex-direction: column;
          align-items: flex-end;
          gap: 6px;
        }

        .update-badge {
          background: #000;
          color: #FF8C00;
          padding: 5px 12px;
          border-radius: 8px;
          font-size: 0.75rem;
          font-weight: 800;
          letter-spacing: 1px;
        }

        .status-label {
          font-size: 0.7rem;
          font-weight: 700;
          letter-spacing: 2px;
          opacity: 0.7;
          text-transform: uppercase;
        }

        .action-bar {
          display: flex;
          align-items: center;
          gap: 25px;
        }

        .accent-line {
          height: 2px;
          flex-grow: 1;
          background: rgba(0, 0, 0, 0.15);
          border-radius: 2px;
        }

        .search-wrapper {
          position: relative;
          width: 480px;
        }

        .search-icon {
          position: absolute;
          left: 20px;
          top: 50%;
          transform: translateY(-50%);
          opacity: 0.6;
          color: #000;
        }

        .search-input {
          width: 100%;
          background: rgba(255, 255, 255, 0.2);
          border: 2px solid rgba(0, 0, 0, 0.1);
          padding: 18px 25px 18px 60px;
          border-radius: 16px;
          font-size: 1.05rem;
          font-weight: 500;
          color: #000;
          transition: all 0.3s ease;
        }

        .search-input::placeholder { color: rgba(0,0,0,0.4); }
        .search-input:focus {
          background: rgba(255, 255, 255, 0.4);
          border-color: #000;
          outline: none;
          box-shadow: 0 0 20px rgba(0,0,0,0.05);
        }

        .server-stats-panel {
          display: flex;
          justify-content: space-around;
          background: rgba(255, 255, 255, 0.1);
          backdrop-filter: blur(10px);
          border: 2px solid rgba(0, 0, 0, 0.08);
          padding: 30px;
          border-radius: 24px;
          margin-bottom: 40px;
        }

        .stat-item {
          display: flex;
          align-items: center;
          gap: 18px;
        }

        .stat-icon { font-size: 1.5rem; }
        .stat-label {
          display: block;
          font-size: 0.65rem;
          font-weight: 800;
          text-transform: uppercase;
          letter-spacing: 1.5px;
          opacity: 0.5;
          margin-bottom: 2px;
        }

        .stat-value {
          font-size: 1.5rem;
          font-weight: 800;
          color: #000;
        }

        .stat-divider {
          width: 1px;
          height: 45px;
          background: rgba(0, 0, 0, 0.1);
        }

        .stats-grid {
          display: flex;
          flex-direction: column;
          gap: 18px;
        }

        .contributor-card {
          cursor: pointer;
          display: flex;
          background: rgba(255, 255, 255, 0.1);
          backdrop-filter: blur(12px);
          -webkit-backdrop-filter: blur(12px);
          border: 1px solid rgba(0, 0, 0, 0.05);
          border-radius: 28px;
          overflow: hidden;
          transition: all 0.4s cubic-bezier(0.16, 1, 0.3, 1);
          animation: cardSlideUp 0.6s ease forwards;
          opacity: 0;
          transform: translateY(25px);
          animation-delay: calc(var(--i) * 0.05s);
        }

        @keyframes cardSlideUp { to { opacity: 1; transform: translateY(0); } }

        .contributor-card:hover {
          background: rgba(255, 255, 255, 0.25);
          border-color: #000;
          transform: translateY(-6px) scale(1.005);
          box-shadow: 0 25px 50px rgba(0, 0, 0, 0.1);
        }

        .card-identity {
          padding: 35px;
          display: flex;
          align-items: center;
          gap: 35px;
          min-width: 480px;
          border-right: 1px solid rgba(0, 0, 0, 0.08);
        }

        .rank-container { display: flex; align-items: baseline; gap: 4px; }
        .rank-hash { font-size: 1.2rem; font-weight: 800; opacity: 0.3; }
        .rank-number { font-size: 2.8rem; font-weight: 900; line-height: 1; }

        .avatar-wrapper { position: relative; width: 75px; height: 75px; }
        .user-avatar { width: 100%; height: 100%; border-radius: 20px; border: 2.5px solid #000; object-fit: cover; }
        .roles-badge {
          position: absolute; bottom: -6px; right: -6px;
          background: #000; color: #FF8C00;
          font-size: 0.75rem; font-weight: 900;
          padding: 3px 8px; border-radius: 8px;
          border: 2px solid #FF8C00;
        }

        .display-name { font-size: 1.5rem; font-weight: 800; margin: 0; }
        
        .delta-container { display: flex; flex-direction: column; gap: 3px; margin: 8px 0; }
        .delta-row { font-weight: 700; font-size: 0.75rem; }
        .positive { color: #006b1b; }
        .negative { color: #9e0000; }
        .neutral { color: #444; }

        .user-meta { display: flex; gap: 12px; margin-top: 12px; }
        .meta-badge {
          background: rgba(0, 0, 0, 0.06);
          padding: 6px 12px;
          border-radius: 10px;
          font-size: 0.75rem;
          font-weight: 700;
          display: flex;
          align-items: center;
          gap: 6px;
        }

        .card-metrics { flex-grow: 1; padding: 0 40px; display: flex; align-items: center; }
        .metrics-inner { display: grid; grid-template-columns: 1.2fr 1.2fr 1fr; width: 100%; gap: 25px; }
        
        .metric-label { font-size: 0.65rem; font-weight: 800; opacity: 0.4; letter-spacing: 1.5px; margin-bottom: 6px; display: block; }
        .stat-val { font-size: 1.3rem; font-weight: 800; }
        .stat-suffix { font-size: 0.75rem; opacity: 0.5; margin-left: 6px; font-weight: 700; }

        .metric-box.total {
          background: rgba(0, 0, 0, 0.04);
          padding: 20px 30px;
          border-radius: 20px;
          text-align: right;
          border: 2px solid rgba(0, 0, 0, 0.05);
          transition: 0.3s ease;
        }

        .contributor-card:hover .metric-box.total { background: #000; border-color: #000; color: #FF8C00; }
        .total-score-value { font-size: 2.6rem; font-weight: 900; line-height: 1; }

        .pagination { display: flex; justify-content: center; align-items: center; gap: 25px; margin: 50px 0; }
        .pagination-btn {
          background: #000; color: #fff; border: none;
          padding: 14px 30px; border-radius: 14px;
          font-weight: 800; cursor: pointer; transition: 0.3s;
        }
        .pagination-btn:hover:not(:disabled) { transform: scale(1.05); box-shadow: 0 10px 20px rgba(0,0,0,0.15); }
        .pagination-btn:disabled { opacity: 0.25; cursor: not-allowed; }

        .modal-overlay {
          position: fixed; top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0, 0, 0, 0.7); backdrop-filter: blur(12px);
          display: flex; justify-content: center; align-items: center; z-index: 1000;
        }

        .modal-content {
          background: #FFA500; border: 4px solid #000;
          padding: 50px; border-radius: 35px; width: 92%; max-width: 600px;
          position: relative; box-shadow: 0 40px 100px rgba(0,0,0,0.3);
        }

        .close-btn { position: absolute; right: 30px; top: 30px; font-size: 2.5rem; background: none; border: none; cursor: pointer; font-weight: 300; }
        
        .modal-header { display: flex; align-items: center; gap: 30px; margin-bottom: 40px; }
        .modal-avatar { width: 95px; height: 95px; border-radius: 25px; border: 4px solid #000; box-shadow: 0 10px 20px rgba(0,0,0,0.1); }
        .modal-rank-badge { background: #000; color: #fff; padding: 6px 16px; border-radius: 10px; font-weight: 900; font-size: 1rem; margin-bottom: 12px; display: inline-block; }
        
        .stat-grid-modal { display: grid; grid-template-columns: 1fr 1fr; gap: 30px; margin-bottom: 40px; }
        .stat-group-modal h3 { font-size: 0.85rem; font-weight: 900; opacity: 0.8; margin-bottom: 20px; border-bottom: 2px solid rgba(0,0,0,0.1); padding-bottom: 8px; }
        .stat-item-modal { display: flex; justify-content: space-between; margin-bottom: 12px; font-weight: 700; font-size: 1.05rem; }
        .stat-item-modal .val { font-weight: 900; }

        .modal-total-score {
          background: #000; color: #FF8C00;
          padding: 30px; border-radius: 24px;
          display: flex; justify-content: space-between; align-items: center;
        }

        .score-value { font-size: 2.8rem; font-weight: 900; }
        .download-btn {
          width: 100%; background: #000; color: #fff;
          padding: 22px; border-radius: 18px;
          font-weight: 900; font-size: 1.1rem;
          margin-top: 25px; cursor: pointer; transition: 0.3s;
          border: none; text-transform: uppercase; letter-spacing: 1px;
        }
        .download-btn:hover { transform: translateY(-4px); box-shadow: 0 15px 30px rgba(0,0,0,0.25); }

        .footer { padding: 60px 0; border-top: 2px solid rgba(0, 0, 0, 0.1); text-align: center; }
        .f-link { color: #000; text-decoration: none; font-weight: 800; opacity: 0.6; transition: 0.2s; }
        .f-link:hover { opacity: 1; }
        
        @media (max-width: 1150px) {
          .contributor-card { flex-direction: column; }
          .card-identity { border-right: none; border-bottom: 1px solid rgba(0,0,0,0.1); min-width: auto; padding: 30px; }
          .card-metrics { padding: 30px; }
          .metrics-inner { grid-template-columns: 1fr 1fr; }
          .metric-box.total { grid-column: span 2; text-align: left; }
          .search-wrapper { width: 100%; }
          .action-bar { flex-direction: column; align-items: stretch; }
          .server-stats-panel { flex-wrap: wrap; gap: 20px; }
          .stat-divider { display: none; }
        }
      `}</style>
    </div>
  );
}
