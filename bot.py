"""
Bot Scalping v18.8 — MIRROR REVERSAL MODE (FIXED + FEE-AWARE)
==================================================================
CHANGELOG dari v18.8 original:
  [FIX-1] Fee sekarang dikurangi dari realizedPnl (ambil field 'commission' dari API)
  [FIX-2] Guard double-count: set _logged_closes mencegah posisi di-log dua kali
  [FIX-3] Sync race condition: backoff -1109, interval t_monitor dinaikkan 3s
  [FIX-4] process_closed_position pakai startTime filter agar tidak ambil trade lama
  [FIX-5] paper_open reset _logged_closes saat posisi baru dibuka
  [FIX-6] Stats sekarang tampilkan gross_pnl vs net_pnl vs total_fee untuk diagnostik
"""

