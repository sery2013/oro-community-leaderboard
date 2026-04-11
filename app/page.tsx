'use client';
import { useEffect, useState, useRef, useMemo } from 'react';
import { createClient } from '@supabase/supabase-js';

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || '';
const supabaseAnonKey = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY || '';
const supabase = (supabaseUrl && supabaseAnonKey) ? createClient(supabaseUrl, supabaseAnonKey) : null;
const ITEMS_PER_PAGE = 25;

// === ДОБАВЛЕННЫЙ БЛОК РОЛЕЙ ===
const PRIORITY_ROLES: Record<string, string> = {
  "1468552780238033009": "Bronze",
  "1468552336204103774": "Iron",
  "1468552865759891596": "Silver",
  "1468552932034351280": "Gold",
  "1468692622242484385": "Creator T1",
  "1468692668325302272": "Creator T2",
  "1468692694296563884": "Creator T3",
  "1468692722436149536": "Creator T4"
};

export default function Leaderboard() {
  const [users, setUsers] = useState<any[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState('');
  const [selectedUser, setSelectedUser] = useState<any | null>(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [lastUpdate, setLastUpdate] = useState('Syncing...');
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

  // === ОБНОВЛЁННАЯ ФУНКЦИЯ СКАЧИВАНИЯ ===
  const downloadCard = async () => {
    if (!modalRef.current) return;
    const cardElement = modalRef.current.querySelector('.modal-content') as HTMLElement;
    if (!cardElement) return;

    try {
      // Включаем режим экспорта (отключает блюр в CSS)
      cardElement.classList.add('export-mode');
      
      // Даем 150мс на перерисовку без блюра
      await new Promise(resolve => setTimeout(resolve, 150));

      const canvas = await (window as any).html2canvas(cardElement, {
        useCORS: true,
        scale: 2, // Оптимально для четкости и памяти
        backgroundColor: '#0a0a0a',
        logging: false,
        allowTaint: true
      });

      const link = document.createElement('a');
      link.download = `oro-identity-${selectedUser?.username || 'user'}.png`;
      link.href = canvas.toDataURL("image/png", 1.0);
      link.click();
    } catch (err) {
      console.error("Download Error:", err);
    } finally {
      // Возвращаем блюр
      cardElement.classList.remove('export-mode');
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

  const handleRefresh = () => {
    window.location.reload();
  };

  if (loading) return (
    <div className="loading-screen">
      <div>SYNCHRONIZING NETWORK DATA...</div>
      <div className="loader"></div>
      <style jsx>{`
        .loading-screen { height: 100vh; display: flex; flex-direction: column; justify-content: center; align-items: center; gap: 20px; background: linear-gradient(135deg, #1a0f0a 0%, #2d1f1a 100%); color: #FFA500; letter-spacing: 4px; font-family: 'Space Grotesk', sans-serif; }
        .loader { width: 40px; height: 40px; border: 3px solid rgba(255,165,0,0.2); border-top-color: #FFA500; border-radius: 50%; animation: spin 1s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
      `}</style>
    </div>
  );

  return (
    <div className="container">
      <div className="grid-overlay"></div>
      <div className="glow glow-1"></div>
      <div className="glow glow-2"></div>

      <div className="main-content">
        <div className="header-section">
          <div className="branding-banner">
            <div className="branding-content">
              <div className="status-dot"></div>
              <h1 className="main-title clickable-title" onClick={handleRefresh} title="Click to refresh">
                ORO AI SOCIAL RANKING
              </h1>
              <div className="status-info">
                <span className="update-badge">{lastUpdate}</span>
                <span className="status-label">LEADERBOARD DATA UPDATED MANUALLY</span>
              </div>
            </div>
          </div>

          <div className="action-bar">
            <div className="accent-line"></div>
            <div className="search-wrapper">
              <svg className="search-icon" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                <circle cx="11" cy="11" r="8"></circle>
                <path d="m21 21-4.35-4.35"></path>
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
        </div>

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
              <span className="stat-label">7D Messages</span>
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
              <span className="stat-label">30D Twitter Posts</span>
              <span className="stat-value highlight">{serverStats.twitterPosts24h}</span>
              <span className="stat-sub">Total posts </span>
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
                  style={{ '--i': index } as React.CSSProperties}
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
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
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
                              <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"></path>
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
                          <span className="stat-dot-small messages"></span>
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
                            <span className="stat-dot-small posts"></span>
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
                <path d="M18.244 2.25h3.308l-7.227 8.26 8.502 11.24H16.17l-5.214-6.817L4.99 21.75H1.68l7.73-8.835L1.254 2.25H8.08l4.713 6.231zm-1.161 17.52h1.833L7.084 4.126H5.117z"></path>
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
                
                {/* ОБНОВЛЕННЫЙ БЛОК РОЛЕЙ */}
                <div className="roles-container">
                  {selectedUser.discord_roles
                    ?.filter((id: string) => PRIORITY_ROLES[id])
                    .map((id: string) => (
                      <span key={id} className="role-badge">
                        {PRIORITY_ROLES[id]}
                      </span>
                    ))
                  }
                  <span className="user-id-label">
                    ID: {selectedUser.user_id}
                  </span>
                </div>
                
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
        
        * {
          box-sizing: border-box;
        }
        
        body {
          margin: 0;
          background: linear-gradient(135deg, #1a0f0a 0%, #2d1f1a 50%, #1a0f0a 100%);
          color: #fff;
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
        }
        
        .grid-overlay {
          position: fixed;
          top: 0; left: 0; width: 100%; height: 100%;
          background-image: 
            radial-gradient(rgba(255,165,0,0.1) 1px, transparent 1px),
            radial-gradient(rgba(255,165,0,0.05) 1px, transparent 1px);
          background-size: 50px 50px, 30px 30px;
          background-position: 0 0, 15px 15px;
          z-index: 0;
          pointer-events: none;
        }
        
        .glow {
          position: fixed;
          width: 600px;
          height: 600px;
          filter: blur(120px);
          opacity: 0.15;
          z-index: 0;
          pointer-events: none;
          border-radius: 50%;
        }
        
        .glow-1 { 
          top: -200px; 
          left: -100px; 
          background: radial-gradient(circle, #FF8C00 0%, transparent 70%);
          animation: float 8s ease-in-out infinite;
        }
        
        .glow-2 { 
          bottom: -200px; 
          right: -100px; 
          background: radial-gradient(circle, #FFA500 0%, transparent 70%);
          animation: float 10s ease-in-out infinite reverse;
        }
        
        @keyframes float {
          0%, 100% { transform: translate(0, 0); }
          50% { transform: translate(30px, -30px); }
        }
        
        .main-content {
          width: 100%;
          max-width: 1400px;
          z-index: 10;
        }
        
        .header-section { 
          margin-bottom: 50px; 
        }
        
        .branding-banner {
          border: 2px solid rgba(255,165,0,0.3);
          background: linear-gradient(135deg, rgba(255,140,0,0.15) 0%, rgba(255,165,0,0.05) 100%);
          padding: 30px 40px;
          border-radius: 20px;
          backdrop-filter: blur(20px);
          transition: all 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
          margin-bottom: 25px;
          box-shadow: 0 8px 32px rgba(0,0,0,0.3);
        }
        
        .branding-banner:hover {
          border-color: rgba(255,165,0,0.6);
          transform: translateY(-3px);
          box-shadow: 0 12px 40px rgba(255,140,0,0.2);
        }
        
        .branding-content {
          display: flex;
          align-items: center;
          gap: 20px;
          flex-wrap: wrap;
        }
        
        .status-dot {
          width: 10px; 
          height: 10px;
          background: #FFA500;
          border-radius: 50%;
          box-shadow: 0 0 20px #FFA500, 0 0 40px #FF8C00;
          animation: pulse 2s infinite;
        }
        
        @keyframes pulse {
          0%, 100% { opacity: 1; transform: scale(1); } 
          50% { opacity: 0.5; transform: scale(0.9); }
        }
        
        .main-title {
          font-size: 2.2rem;
          font-weight: 700;
          letter-spacing: 4px;
          margin: 0;
          flex-grow: 1;
          background: linear-gradient(135deg, #FFD700 0%, #FFA500 50%, #FFD700 100%);
          background-size: 200% auto;
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          animation: shimmer 4s linear infinite;
          text-shadow: none;
          filter: drop-shadow(0 0 25px rgba(255,215,0,0.5));
        }
        
        @keyframes shimmer {
          0% { background-position: 0% center; }
          100% { background-position: 200% center; }
        }

        .clickable-title {
          cursor: pointer;
          transition: all 0.3s ease;
          user-select: none;
        }

        .clickable-title:hover {
          filter: drop-shadow(0 0 35px rgba(255,215,0,0.7));
          transform: scale(1.02);
        }

        .clickable-title:active {
          transform: scale(0.98);
        }
        
        .status-info {
          display: flex;
          flex-direction: column;
          align-items: flex-end;
          gap: 4px;
        }
        
        .update-badge {
          background: rgba(255,165,0,0.2);
          color: #FFD700;
          padding: 4px 12px;
          border-radius: 6px;
          font-size: 0.65rem;
          font-weight: 700;
          border: 1px solid rgba(255,165,0,0.4);
          letter-spacing: 1px;
        }
        
        .status-label {
          font-size: 0.7rem;
          letter-spacing: 2px;
          color: rgba(255,255,255,0.6);
          font-weight: 500;
          text-transform: uppercase;
        }
        
        .action-bar {
          display: flex;
          align-items: center;
          gap: 30px;
        }
        
        .accent-line {
          height: 2px; 
          flex-grow: 1;
          background: linear-gradient(90deg, rgba(255,165,0,0.6), rgba(255,165,0,0.2), transparent);
        }
        
        .search-wrapper {
          position: relative;
          width: 450px;
        }
        
        .search-icon {
          position: absolute;
          left: 18px; 
          top: 50%;
          transform: translateY(-50%);
          color: #FFA500;
          opacity: 0.7;
        }
        
        .search-input {
          width: 100%;
          background: rgba(255,255,255,0.05);
          border: 2px solid rgba(255,165,0,0.3);
          padding: 16px 20px 16px 55px;
          border-radius: 14px;
          color: #fff;
          font-size: 1rem;
          transition: all 0.3s;
          backdrop-filter: blur(10px);
        }
        
        .search-input::placeholder {
          color: rgba(255,255,255,0.4);
        }
        
        .search-input:focus {
          border-color: #FFA500;
          outline: none;
          background: rgba(255,255,255,0.08);
          box-shadow: 0 0 0 4px rgba(255, 165, 0, 0.15), 0 0 30px rgba(255,165,0,0.2);
        }
        
        .stats-grid {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }
        
        .contributor-card {
          cursor: pointer;
          display: flex;
          background: linear-gradient(135deg, rgba(255,140,0,0.12) 0%, rgba(255,165,0,0.06) 100%);
          border: 2px solid rgba(255,165,0,0.25);
          border-radius: 20px;
          overflow: hidden;
          opacity: 0;
          transform: translateY(20px);
          animation: fadeInUp 0.6s cubic-bezier(0.2, 0.8, 0.2, 1) forwards;
          animation-delay: calc(var(--i) * 0.08s);
          transition: all 0.4s cubic-bezier(0.4, 0, 0.2, 1);
          position: relative;
          backdrop-filter: blur(10px);
          box-shadow: 0 4px 20px rgba(0,0,0,0.2);
        }
        
        .contributor-card::before {
          content: '';
          position: absolute;
          top: 0;
          left: -100%;
          width: 100%;
          height: 100%;
          background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
          transition: left 0.7s;
          pointer-events: none;
        }
        
        .contributor-card:hover::before {
          left: 100%;
        }
        
        .contributor-card:hover {
          background: linear-gradient(135deg, rgba(255,140,0,0.18) 0%, rgba(255,165,0,0.1) 100%);
          border-color: rgba(255, 215, 0, 0.6);
          transform: translateY(-5px) scale(1.01) translateX(8px);
          box-shadow: 0 20px 50px rgba(0,0,0,0.3), 0 0 60px rgba(255, 140, 0, 0.25);
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
          border-right: 1px solid rgba(255,165,0,0.2);
          background: rgba(0,0,0,0.2);
        }
        
        .rank-container {
          display: flex;
          align-items: baseline;
          min-width: 70px;
        }
        
        .rank-hash { 
          color: #FFA500; 
          font-size: 1.2rem; 
          font-weight: 300; 
        }
        
        .rank-number { 
          font-size: 2.5rem; 
          font-weight: 700; 
          background: linear-gradient(135deg, #FFD700, #FFA500);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          filter: drop-shadow(0 0 15px rgba(255,215,0,0.4));
        }
        
        .user-profile {
          display: flex;
          align-items: center;
          gap: 20px;
        }
        
        .avatar-wrapper {
          position: relative;
          width: 65px; 
          height: 65px;
        }
        
        .user-avatar {
          width: 100%; 
          height: 100%;
          border-radius: 18px;
          object-fit: cover;
          position: relative;
          z-index: 2;
          border: 2px solid rgba(255,165,0,0.4);
          box-shadow: 0 4px 15px rgba(0,0,0,0.3);
        }
        
        .avatar-ring {
          position: absolute;
          inset: -3px;
          border: 2px solid #FFA500;
          border-radius: 20px;
          opacity: 0.3;
          z-index: 1;
        }
        
        .roles-badge {
          position: absolute;
          bottom: -5px; 
          right: -5px;
          background: linear-gradient(135deg, #FFA500, #FF8C00);
          color: #000;
          font-size: 0.65rem;
          font-weight: 700;
          padding: 2px 6px;
          border-radius: 6px;
          z-index: 10;
          border: 2px solid #1a0f0a;
          box-shadow: 0 2px 8px rgba(0,0,0,0.4);
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
          font-size: 1.5rem;
          font-weight: 700;
          color: #fff;
          letter-spacing: -0.5px;
          text-shadow: 0 2px 15px rgba(255,255,255,0.3);
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
        
        .value.positive { color: #10b981; text-shadow: 0 0 10px rgba(16,185,129,0.3); }
        .value.negative { color: #ef4444; text-shadow: 0 0 10px rgba(239,68,68,0.3); }
        .value.neutral { color: rgba(255,255,255,0.5); }
        
        .user-meta {
          display: flex;
          gap: 8px;
          flex-wrap: wrap;
        }
        
        .meta-badge {
          display: flex;
          align-items: center;
          gap: 6px;
          background: rgba(255,255,255,0.08);
          border: 1px solid rgba(255,165,0,0.3);
          padding: 4px 10px;
          border-radius: 8px;
          font-size: 0.75rem;
          color: rgba(255,255,255,0.8);
          text-decoration: none;
          transition: all 0.2s;
        }
        
        .twitter-link {
          color: #fff;
          border-color: rgba(255,165,0,0.3);
          cursor: pointer;
        }
        
        .twitter-link:hover:not(.disabled) {
          background: rgba(255,165,0,0.2);
          border-color: #FFA500;
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
        
        .metric-box { 
          padding: 10px 0; 
        }
        
        .twitter-stats-column { 
          display: flex; 
          flex-direction: column; 
          gap: 2px; 
        }
        
        .stat-row { 
          display: flex; 
          align-items: baseline; 
          gap: 6px; 
        }
        
        .stat-row.sub { 
          opacity: 0.75; 
          margin-top: -1px; 
        }
        
        .stat-val { 
          font-size: 1.1rem; 
          font-weight: 700; 
          color: #fff;
          text-shadow: 0 2px 12px rgba(255,255,255,0.4);
        }
        
        .stat-suffix { 
          font-size: 0.6rem; 
          letter-spacing: 1px;
          color: rgba(255,255,255,0.6);
        }
        
        .stat-dot-small { 
          width: 5px; 
          height: 5px; 
          border-radius: 50%; 
          margin-bottom: 2px;
          box-shadow: 0 0 8px currentColor;
        }
        
        .stat-dot-small.messages,
        .stat-dot-small.posts { 
          background: #FFA500; 
          color: #FFA500;
        }
        
        .stat-dot-small.likes { 
          background: #ec4899; 
          color: #ec4899;
        }
        
        .stat-dot-small.views { 
          background: #8b5cf6; 
          color: #8b5cf6;
        }
        
        .stat-dot-small.replies { 
          background: #10b981; 
          color: #10b981;
        }
        
        .channel-activity-row { 
          display: flex; 
          align-items: baseline; 
          gap: 6px; 
          margin-top: 4px; 
          opacity: 0.85; 
        }
        
        .stat-dot-small.channels { 
          background: #3b82f6; 
          color: #3b82f6;
        }
        
        .metric-label {
          display: block;
          font-size: 0.6rem;
          color: rgba(255,255,255,0.7);
          letter-spacing: 2px;
          margin-bottom: 8px;
          text-transform: uppercase;
        }
        
        .metric-box.total {
          background: linear-gradient(135deg, rgba(255,165,0,0.25), rgba(255,140,0,0.15));
          border-radius: 14px;
          padding: 15px 25px;
          text-align: right;
          border: 2px solid rgba(255,165,0,0.4);
          box-shadow: 0 4px 20px rgba(255,165,0,0.15);
        }
        
        .total-score-value {
          font-size: 2.2rem;
          font-weight: 700;
          background: linear-gradient(135deg, #FFD700, #FFA500);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          text-shadow: none;
          filter: drop-shadow(0 0 15px rgba(255,215,0,0.4));
        }
        
        .server-stats-panel {
          display: flex;
          align-items: center;
          justify-content: space-around;
          gap: 20px;
          padding: 25px 30px;
          background: linear-gradient(135deg, rgba(255,140,0,0.18) 0%, rgba(255,165,0,0.1) 100%);
          border: 2px solid rgba(255,165,0,0.35);
          border-radius: 16px;
          margin-bottom: 24px;
          backdrop-filter: blur(15px);
          transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
          box-shadow: 0 8px 32px rgba(0,0,0,0.25);
        }
        
        .server-stats-panel:hover {
          border-color: rgba(255,165,0,0.6);
          box-shadow: 0 12px 40px rgba(255,140,0,0.2);
          transform: translateY(-2px);
        }
        
        .stat-item {
          display: flex;
          align-items: center;
          gap: 12px;
        }
        
        .stat-icon {
          font-size: 1.8rem;
          filter: drop-shadow(0 0 10px rgba(255,165,0,0.3));
        }
        
        .stat-content {
          display: flex;
          flex-direction: column;
          gap: 2px;
        }
        
        .stat-label {
          font-size: 0.65rem;
          color: rgba(255,255,255,0.7);
          letter-spacing: 1px;
          text-transform: uppercase;
        }
        
        .stat-value {
          font-size: 1.5rem;
          font-weight: 700;
          color: #fff;
          text-shadow: 0 2px 15px rgba(255,255,255,0.4);
        }
        
        .stat-value.highlight {
          color: #10b981;
          text-shadow: 0 0 20px rgba(16,185,129,0.5);
        }
        
        .stat-sub {
          font-size: 0.75rem;
          color: rgba(255,255,255,0.6);
        }
        
        .stat-divider {
          width: 1px;
          height: 40px;
          background: linear-gradient(to bottom, transparent, rgba(255,165,0,0.4), transparent);
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
        
        .pagination {
          display: flex;
          justify-content: center;
          align-items: center;
          gap: 20px;
          margin: 40px 0 60px;
          padding: 20px;
          background: rgba(255,255,255,0.05);
          border: 2px solid rgba(255,165,0,0.3);
          border-radius: 16px;
          backdrop-filter: blur(10px);
        }
        
        .pagination-btn {
          background: rgba(255,165,0,0.1);
          border: 2px solid rgba(255,165,0,0.3);
          color: #FFA500;
          padding: 10px 24px;
          border-radius: 12px;
          cursor: pointer;
          font-family: 'Space Grotesk', sans-serif;
          font-weight: 600;
          font-size: 0.9rem;
          transition: all 0.2s ease;
        }
        
        .pagination-btn:hover:not(:disabled) {
          background: rgba(255,165,0,0.25);
          border-color: #FFA500;
          box-shadow: 0 0 20px rgba(255,165,0,0.2);
        }
        
        .pagination-btn:disabled {
          opacity: 0.3;
          cursor: not-allowed;
        }
        
        .pagination-info {
          font-size: 0.9rem;
          color: rgba(255,255,255,0.7);
          letter-spacing: 1px;
        }
        
        .pagination-current {
          color: #FFA500;
          font-weight: 700;
        }
        
        .modal-overlay {
          position: fixed;
          top: 0; left: 0; width: 100%; height: 100%;
          background: rgba(0,0,0,0.8);
          backdrop-filter: blur(12px);
          display: flex;
          justify-content: center;
          align-items: center;
          z-index: 1000;
          padding: 20px;
          animation: modalFadeIn 0.3s ease;
        }
        
        .modal-content {
          background: linear-gradient(135deg, #1a0f0a 0%, #2d1f1a 100%);
          width: 100%;
          max-width: 600px;
          border-radius: 32px;
          border: 2px solid rgba(255,165,0,0.4);
          padding: 40px;
          position: relative;
          box-shadow: 0 25px 50px rgba(0,0,0,0.5), 0 0 60px rgba(255,165,0,0.15);
          animation: modalSlideUp 0.4s cubic-bezier(0.2, 0.8, 0.2, 1);
        }
        
        .close-btn {
          position: absolute;
          top: 30px; right: 30px;
          background: none;
          border: none;
          color: #FFA500;
          font-size: 32px;
          cursor: pointer;
          transition: all 0.2s;
        }
        
        .close-btn:hover {
          color: #FFD700;
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
          border: 2px solid #FFA500;
          position: relative;
          z-index: 2;
          box-shadow: 0 8px 30px rgba(0,0,0,0.4);
        }
        
        .modal-avatar-glow {
          position: absolute;
          inset: 0;
          background: #FFA500;
          filter: blur(20px);
          opacity: 0.2;
          z-index: 1;
        }
        
        .modal-titles {
          display: flex;
          flex-direction: column;
          gap: 8px;
        }
        
        .modal-titles h2 {
          margin: 0;
          font-size: 2.2rem;
          color: #fff;
          text-shadow: 0 2px 15px rgba(0,0,0,0.3);
        }
        
        .modal-titles p { 
          margin: 0;
          color: rgba(255,255,255,0.6);
          font-size: 1rem;
        }
        
        .modal-rank-badge {
          display: inline-block;
          background: linear-gradient(135deg, #FFA500, #FF8C00);
          color: #000;
          font-weight: 700;
          font-size: 0.7rem;
          padding: 4px 12px;
          border-radius: 20px;
          letter-spacing: 1px;
          box-shadow: 0 4px 15px rgba(255,165,0,0.3);
          width: fit-content;
        }
        
        /* === НОВЫЕ СТИЛИ ДЛЯ БЛОКА РОЛЕЙ === */
        .roles-container {
          display: flex;
          flex-wrap: wrap;
          gap: 6px;
          align-items: center;
          margin-top: 4px;
        }
        
        .role-badge {
          display: inline-block;
          padding: 2px 8px;
          background: rgba(255,165,0,0.1);
          border: 1px solid rgba(255,165,0,0.3);
          border-radius: 4px;
          font-size: 9px;
          font-weight: 700;
          text-transform: uppercase;
          letter-spacing: 0.5px;
          color: #FFA500;
          line-height: 1;
        }
        
        .user-id-label {
          font-size: 10px;
          color: rgba(255,255,255,0.4);
          margin-left: 4px;
          font-family: monospace;
        }
        /* === КОНЕЦ НОВЫХ СТИЛЕЙ === */
        
        .stat-grid-modal {
          display: grid;
          grid-template-columns: 1fr 1fr;
          gap: 30px;
          margin-bottom: 40px;
        }
        
        .stat-group-modal h3 {
          font-size: 0.7rem;
          color: #FFD700;
          letter-spacing: 2px;
          margin-bottom: 15px;
          border-bottom: 1px solid rgba(255,165,0,0.4);
          padding-bottom: 10px;
          text-transform: uppercase;
        }
        
        .stat-item-modal {
          display: flex;
          justify-content: space-between;
          padding: 10px 0;
          border-bottom: 1px solid rgba(255,255,255,0.1);
          font-size: 0.95rem;
          color: rgba(255,255,255,0.8);
        }
        
        .stat-item-modal .val {
          color: #fff;
          font-weight: 700;
        }
        
        .modal-total-score {
          background: linear-gradient(135deg, rgba(255,165,0,0.2), rgba(255,140,0,0.1));
          border: 2px solid rgba(255,165,0,0.4);
          border-radius: 20px;
          padding: 25px 30px;
          display: flex;
          justify-content: space-between;
          align-items: center;
        }
        
        .score-label {
          display: block;
          font-weight: 700;
          color: #fff;
          font-size: 1.1rem;
        }
        
        .score-sub {
          display: block;
          color: rgba(255,255,255,0.6);
          font-size: 0.8rem;
        }
        
        .score-value {
          font-size: 2.5rem;
          font-weight: 700;
          background: linear-gradient(135deg, #FFD700, #FFA500);
          -webkit-background-clip: text;
          -webkit-text-fill-color: transparent;
          background-clip: text;
          filter: drop-shadow(0 0 15px rgba(255,215,0,0.4));
        }
        
        .download-btn {
          margin-top: 30px;
          width: 100%;
          background: rgba(255,165,0,0.1);
          border: 2px solid rgba(255,165,0,0.4);
          color: #FFA500;
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
          transition: all 0.3s;
        }
        
        .download-btn:hover {
          background: rgba(255,165,0,0.25);
          border-color: #FFA500;
          box-shadow: 0 0 30px rgba(255,165,0,0.2);
        }
        
        .modal-content.export-mode,
        .contributor-card.export-mode {
          filter: none !important;
          backdrop-filter: none !important;
          box-shadow: none !important;
        }
        
        @keyframes modalFadeIn { from { opacity: 0; } }
        
        @keyframes modalSlideUp { 
          from { transform: translateY(40px); opacity: 0; } 
        }
        
        .footer {
          text-align: center;
          padding: 40px 0;
          margin-top: 60px;
          border-top: 2px solid rgba(255,165,0,0.2);
        }
        
        .footer-links {
          display: flex;
          justify-content: center;
          align-items: center;
          gap: 15px;
        }
        
        .f-link {
          color: rgba(255,255,255,0.7);
          text-decoration: none;
          opacity: 0.8;
          transition: all 0.2s;
          display: flex;
          align-items: center;
          gap: 8px;
        }
        
        .f-link:hover {
          opacity: 1;
          color: #FFA500;
        }
        
        .f-sep {
          color: rgba(255,165,0,0.3);
        }
        
        @media (max-width: 1100px) {
          .card-identity {
            min-width: auto;
            flex-direction: column;
            align-items: flex-start;
            gap: 20px;
            border-right: none;
            border-bottom: 1px solid rgba(255,165,0,0.2);
          }
        }
      `}</style>
    </div>
  );
}
