#!/usr/bin/env python3
import sys
import asyncio
import datetime, os, time, threading, math
import random
import requests  # Added for proxy testing and usage

# On Windows, use the SelectorEventLoopPolicy.
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import ccxt.async_support as ccxt
from colorama import Fore, Style, init
import curses

# Initialize colorama.
init(autoreset=True)

# ANSI escape code for orange (if not available in colorama)
ORANGE = "\033[38;5;208m"

def colored(text, fg='white', style='normal'):
    style_code = Style.NORMAL if style != 'bold' else Style.BRIGHT
    if fg.startswith("\033"):
        color_code = fg
    else:
        color_map = {
            'red': Fore.RED,
            'green': Fore.GREEN,
            'yellow': Fore.YELLOW,
            'blue': Fore.BLUE,
            'cyan': Fore.CYAN,
            'magenta': Fore.MAGENTA,
            'white': Fore.WHITE,
        }
        color_code = color_map.get(fg, Fore.WHITE)
    return f"{style_code}{color_code}{text}{Style.RESET_ALL}"

def format_symbol(symbol):
    if "/" in symbol:
        base, quote = symbol.split("/")
        return f"{base}/{quote}:{quote}"
    return symbol

def display_symbol(full_key):
    if ":" in full_key:
        source, symbol = full_key.split(":", 1)
        if "/" in symbol:
            base = symbol.split("/")[0]
        else:
            base = symbol
        return source, base
    else:
        if "/" in full_key:
            base = full_key.split("/")[0]
        else:
            base = full_key
        return None, base

def format_num(val, decimals=3):
    try:
        num = float(val)
        return f"{round(num, decimals):.{decimals}f}"
    except Exception:
        return str(val)

# --- Rate Limiter for Exchange requests ---
class RateLimiter:
    def __init__(self, rate, per):
        self._rate = rate      # e.g. 10 tokens per 'per' seconds
        self._per = per
        self._tokens = rate
        self._last = asyncio.get_event_loop().time()
        self._lock = asyncio.Lock()
    
    async def acquire(self):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            elapsed = now - self._last
            self._tokens = min(self._rate, self._tokens + elapsed * (self._rate / self._per))
            if self._tokens < 1:
                await asyncio.sleep((1 - self._tokens) * (self._per / self._rate))
                now = asyncio.get_event_loop().time()
                elapsed = now - self._last
                self._tokens = min(self._rate, self._tokens + elapsed * (self._rate / self._per))
            self._tokens -= 1
            self._last = now

# --- Proxy Configuration Data ---
PROXIES = {
    'usa': [
        "http://iQT9izzMkFyF4MU9:DNjLn5aLM6i4dQr9@geo.g-w.info:10080",
        "http://LgMiXLaZfI1zV38i:rAVNrErDLIjUoJ0G@geo.g-w.info:10080",
        "http://3jXtrSaKwobie8Ah:hnav5SaOGdS1TzXR@geo.g-w.info:10080",
        "http://p3hInr6GLhP8ULQB:WAt7C5HFUOUHj384@geo.g-w.info:10080",
        "http://JLLp4zMmM0DpjJFY:vVuBsIXwMpsejJqs@geo.g-w.info:10080"
    ],
    'japan': [
        "http://U5Mp30AsGk0StMww:jNR4zXzZ9ZlCcNpH@geo.g-w.info:10080",
        "http://saWma1wHyvfRoMa6:RK0uWs90GcBuhAYw@geo.g-w.info:10080",
        "http://pc48PI0adR4o473r:uX3wiR5x68LF1qRV@geo.g-w.info:10080"
    ]
}

def test_proxy(proxy_url):
    test_url = "https://api.ipify.org"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        start = time.time()
        r = requests.get(test_url, proxies=proxies, timeout=5)
        elapsed = time.time() - start
        return elapsed
    except Exception:
        return None

def get_best_proxy(region):
    proxies_list = PROXIES.get(region, [])
    best_proxy = None
    best_time = float('inf')
    for proxy in proxies_list:
        t = test_proxy(proxy)
        if t is not None and t < best_time:
            best_time = t
            best_proxy = proxy
    if best_proxy is not None:
        return best_proxy, best_time
    else:
        return None, None

def set_active_proxy(proxy_url, screener):
    screener.proxy = proxy_url
    # Update existing exchanges with new proxy settings
    for ex in screener.exchanges.values():
        try:
            ex.proxies = {"http": proxy_url, "https": proxy_url}
        except Exception:
            pass
    print(colored(f"Proxy set to {proxy_url}", fg='cyan'))

def disable_proxy(screener):
    screener.proxy = None
    for ex in screener.exchanges.values():
        try:
            ex.proxies = {}
        except Exception:
            pass
    print(colored("Proxy disabled.", fg='cyan'))

def list_proxies():
    results = {}
    for region, proxy_list in PROXIES.items():
        region_results = []
        for idx, proxy in enumerate(proxy_list, start=1):
            t = test_proxy(proxy)
            if t is None:
                ping_str = "Timeout"
                ping = float('inf')
            else:
                ping_str = f"{t*1000:.0f}ms"
                ping = t
            region_results.append((idx, proxy, ping, ping_str))
        region_results.sort(key=lambda x: x[2])
        results[region] = region_results
    for region, proxies_info in results.items():
        print(colored(f"\n{region.upper()} Proxies:", fg='magenta', style='bold'))
        for idx, proxy, ping, ping_str in proxies_info:
            print(f" {idx}. {proxy} - {ping_str}")
    print()

# --- Full-Screen Curses Display Function (Scrolling) ---
def curses_display_metric(selected_metric, screener, stop_event):
    sel_metric = selected_metric.upper()
    default_cols = ["PRICE", "5MUP", "5MDOWN", "TPS", "TPS5M", "SPREAD%", "FUNDING", "24HVOL", "VOLATILITY", "OI", "OICH1H", "VOLDEL1H"]
    col_keys = {
        "PRICE": "price",
        "5MUP": "change_5m_up",
        "5MDOWN": "change_5m_down",
        "TPS": "tps",
        "TPS5M": "tps_5m",
        "SPREAD%": "spread",
        "FUNDING": "funding",
        "24HVOL": "vol_24h",
        "VOLATILITY": "volatility",
        "OI": "oi",
        "OICH1H": "oi_change_1h",
        "VOLDEL1H": "volume_delta_1h"
    }
    if sel_metric == "PRICE":
        cols_order = ["PRICE"] + [col for col in default_cols if col != "PRICE"]
    else:
        cols_order = ["PRICE", sel_metric] + [col for col in default_cols if col not in ("PRICE", sel_metric)]
    include_source = len(screener.selected_sources) > 1
    header_cols = (["Source", "Symbol"] if include_source else ["Symbol"]) + cols_order
    header_fmt = ("{:<10}" if include_source else "") + "{:<15}" + " {:<12}" * len(cols_order)
    header = header_fmt.format(*header_cols)
    
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    stdscr.nodelay(True)
    if curses.has_colors():
        curses.start_color()
        curses.init_pair(1, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    top = 0
    try:
        while not stop_event.is_set():
            stdscr.clear()
            all_data = screener.get_all_data()
            filtered_data = {k: d for k, d in all_data.items() if "/USDT" in k}
            unique_data = {}
            for key, d in filtered_data.items():
                source, base = display_symbol(key)
                uniq_key = f"{source}:{base}" if source else base
                if uniq_key in unique_data:
                    if d.get("timestamp", 0) > unique_data[uniq_key][1].get("timestamp", 0):
                        unique_data[uniq_key] = (key, d)
                else:
                    unique_data[uniq_key] = (key, d)
            unique_list = list(unique_data.values())
            sort_key = col_keys.get(sel_metric, "price")
            sorted_list = sorted(unique_list, key=lambda x: (x[1].get(sort_key) or 0), reverse=True)
            total_rows = len(sorted_list)
            max_y, max_x = stdscr.getmaxyx()
            display_rows = max_y - 1
            if top > total_rows - display_rows:
                top = max(0, total_rows - display_rows)
            visible_data = sorted_list[top:top + display_rows]
            try:
                stdscr.addstr(0, 0, header, curses.A_UNDERLINE)
            except curses.error:
                pass
            for idx, (key, d) in enumerate(visible_data, start=1):
                source, base = display_symbol(key)
                row_vals = [str(d.get("price") or 0)]
                if sel_metric != "PRICE":
                    row_vals.append(format_num(d.get(col_keys.get(sel_metric)) or 0))
                for col in default_cols:
                    if col == "PRICE":
                        continue
                    if sel_metric != "PRICE" and col == sel_metric:
                        continue
                    row_vals.append(format_num(d.get(col_keys[col]) or 0))
                if include_source:
                    row_fmt = "{:<10}{:<15}" + " {:<12}" * len(row_vals)
                    row_str = row_fmt.format(source, base, *row_vals)
                else:
                    row_fmt = "{:<15}" + " {:<12}" * len(row_vals)
                    row_str = row_fmt.format(base, *row_vals)
                try:
                    stdscr.addstr(idx, 0, row_str)
                    if sel_metric != "PRICE":
                        sel_x = (10 + 15 if include_source else 15) + 12
                        sel_val = format_num(d.get(col_keys.get(sel_metric)) or 0)
                    else:
                        sel_x = (10 + 15 if include_source else 15)
                        sel_val = str(d.get("price") or 0)
                    stdscr.addstr(idx, sel_x, " {:<12}".format(sel_val), curses.color_pair(1))
                except curses.error:
                    pass
            stdscr.refresh()
            try:
                key_input = stdscr.getch()
                if key_input == curses.KEY_DOWN and top < total_rows - display_rows:
                    top += 1
                elif key_input == curses.KEY_UP and top > 0:
                    top -= 1
            except Exception:
                pass
            time.sleep(1)
    finally:
        curses.echo()
        curses.nocbreak()
        curses.endwin()

# --- Full-Screen Market Conditions Scan (One Vertical Column; Skip Categories with 0 Assigned Coins) ---
def curses_market_conditions_scan(screener, stop_event):
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    stdscr.nodelay(True)
    if curses.has_colors():
        curses.start_color()
        curses.init_pair(1, curses.COLOR_YELLOW, curses.COLOR_BLACK)
    header_width = 60  # skinnier header
    try:
        while not stop_event.is_set():
            stdscr.clear()
            now = int(datetime.datetime.now().timestamp() * 1000)
            # Use only fresh data (within 1 hour) and for active sources.
            fresh_data = {k: v for k, v in screener.get_all_data().items()
                          if (now - v.get('timestamp', 0) <= 3600 * 1000) and (k.split(":")[0] in screener.selected_sources)}
            if not fresh_data:
                try:
                    stdscr.addstr(0, 0, "No fresh market data available for active sources.", curses.color_pair(1))
                except curses.error:
                    pass
                stdscr.refresh()
                time.sleep(1)
                continue

            # Compute normalization values.
            vol24_values = [d.get('vol_24h', 0) for d in fresh_data.values()]
            max_vol_24h = max(vol24_values) if vol24_values else 0
            min_vol_24h = min(vol24_values) if vol24_values else 0
            max_tps_5m = max([d.get('tps_5m', 0) for d in fresh_data.values()] or [0])
            max_volatility = max([d.get('volatility', 0) for d in fresh_data.values()] or [0])

            # Fixed ranking order.
            category_order = [
                "Moderate Volatility",
                "Sufficient Liquidity",
                "Trending Markets with Clear Bias",
                "Mean-Reversion Opportunities",
                "Significant Price Movements (Above Noise Level)",
                "Balanced Spread Environment",
                "High Trade Frequency and Active Tape Feed",
                "Stable and Responsive API Performance",
                "Manageable Market Noise and Order Book Shifts",
                "Multi-Pair Trading Environment",
                "Controlled Downside (Not Extreme Market Crashes)",
                "Other"
            ]

            # For each coin, compute scores.
            coin_eligibility = {}
            for key, d in fresh_data.items():
                change_5m = d.get('change_5m_up', 0) if d.get('change_5m_up', 0) != 0 else d.get('change_5m_down', 0)
                vol = d.get('volatility', 0)
                scores = {}
                ideal_vol = 2.0
                scores["Moderate Volatility"] = min(max(0, 1 - abs(vol - ideal_vol) / ideal_vol) * 100, 100)
                if max_vol_24h > min_vol_24h:
                    scores["Sufficient Liquidity"] = min((d.get('vol_24h', 0) - min_vol_24h) / (max_vol_24h - min_vol_24h) * 100, 100)
                else:
                    scores["Sufficient Liquidity"] = 0
                if abs(change_5m) > 1:
                    scores["Trending Markets with Clear Bias"] = min((abs(change_5m) - 1) / 9 * 100, 100)
                else:
                    scores["Trending Markets with Clear Bias"] = 0
                if vol > 3 and abs(change_5m) < 0.5:
                    scores["Mean-Reversion Opportunities"] = min(((vol - 3) / (max_volatility - 3)) * 100 if max_volatility > 3 else 0, 100)
                else:
                    scores["Mean-Reversion Opportunities"] = 0
                noise_threshold = 0.2
                if abs(change_5m) > noise_threshold:
                    scores["Significant Price Movements (Above Noise Level)"] = min((abs(change_5m) - noise_threshold) / (10 - noise_threshold) * 100, 100)
                else:
                    scores["Significant Price Movements (Above Noise Level)"] = 0
                spread_val = d.get('spread', 0)
                if spread_val >= 0.1:
                    scores["Balanced Spread Environment"] = min(max(0, 1 - abs(spread_val - 0.5) / 0.5) * 100, 100)
                else:
                    scores["Balanced Spread Environment"] = 0
                scores["High Trade Frequency and Active Tape Feed"] = min((d.get('tps_5m', 0) / max_tps_5m * 100) if max_tps_5m > 0 else 0, 100)
                scores["Stable and Responsive API Performance"] = 50
                raw_noise = max(0, (3 - vol) / 2) * 100
                scores["Manageable Market Noise and Order Book Shifts"] = min(raw_noise, 100)
                scores["Multi-Pair Trading Environment"] = 50
                if change_5m >= 0:
                    scores["Controlled Downside (Not Extreme Market Crashes)"] = 100
                else:
                    scores["Controlled Downside (Not Extreme Market Crashes)"] = min(max(0, 100 - (abs(change_5m) / 10 * 100)), 100)
                elig = [(cat, score) for cat, score in scores.items() if score > 0]
                if not elig:
                    elig = [("Other", 0)]
                else:
                    elig.sort(key=lambda x: category_order.index(x[0]))
                coin_eligibility[key] = elig

            # Compute assigned coins per category.
            assigned = {}
            category_assigned = {cat: [] for cat in category_order}
            unassigned = set(coin_eligibility.keys())
            for coin in list(unassigned):
                elig = coin_eligibility[coin]
                for cat, score in elig:
                    if cat == "Other":
                        continue
                    if len(category_assigned[cat]) < 5:
                        assigned[coin] = (cat, score)
                        category_assigned[cat].append((coin, score))
                        unassigned.remove(coin)
                        break
            changed = True
            while changed:
                changed = False
                for coin in list(unassigned):
                    elig = coin_eligibility[coin]
                    for cat, score in elig:
                        if cat == "Other":
                            continue
                        if category_assigned[cat]:
                            min_coin, min_score = min(category_assigned[cat], key=lambda x: x[1])
                            if score > min_score:
                                category_assigned[cat].remove((min_coin, min_score))
                                assigned.pop(min_coin, None)
                                unassigned.add(min_coin)
                                assigned[coin] = (cat, score)
                                category_assigned[cat].append((coin, score))
                                unassigned.remove(coin)
                                changed = True
                                break
                    if coin not in unassigned:
                        continue
            for coin in unassigned:
                assigned[coin] = ("Other", 0)
                category_assigned["Other"].append((coin, 0))
            unassigned.clear()

            # Now display only categories with >0 assigned coins in one vertical column.
            line = 0
            for i, cat in enumerate(category_order, 1):
                assigned_coins = category_assigned.get(cat, [])
                if len(assigned_coins) == 0:
                    continue  # Skip category if no coin is assigned.
                header = f"({i}) {cat} ({len(assigned_coins)} coin(s))"
                try:
                    stdscr.addstr(line, 0, header.center(header_width, "-"), curses.color_pair(1) | curses.A_BOLD)
                except curses.error:
                    pass
                line += 1
                # Sort assigned coins descending by score.
                assigned_coins = sorted(assigned_coins, key=lambda x: x[1], reverse=True)
                for coin, score in assigned_coins[:5]:
                    src, base = coin.split(":", 1)
                    coin_label = f"{src.upper()} {base}"
                    bias = ""
                    for c, s in coin_eligibility[coin]:
                        if c == "Trending Markets with Clear Bias":
                            d = fresh_data[coin]
                            change_5m = d.get('change_5m_up', 0) if d.get('change_5m_up', 0) != 0 else d.get('change_5m_down', 0)
                            bias = "Bullish" if change_5m > 0 else ("Bearish" if change_5m < 0 else "")
                            break
                    if cat == "Trending Markets with Clear Bias" and bias:
                        coin_label += f" [{bias}]"
                    coin_label = coin_label.ljust(20)
                    score_str = f"{score:5.1f}%"
                    try:
                        stdscr.addstr(line, 2, f"- {coin_label} {score_str}")
                    except curses.error:
                        pass
                    line += 1
                line += 1
            stdscr.refresh()
            time.sleep(3)
    finally:
        curses.echo()
        curses.nocbreak()
        curses.endwin()

# --- Multi-Exchange Data Screener ---
class MultiScreener:
    def __init__(self):
        self.exchanges = {}
        self.symbols = {}
        self.selected_sources = []  # Remains empty until "sources" command is given.
        self.oi_history = {}
        self.market_data = {}
        self.trade_history = {}
        self.tape_queue = asyncio.Queue()
        self.update_task = None  # Will be set when sources are defined.
        self.proxy = None  # Active proxy URL, if any
        # Rate limiters for each exchange
        self.rate_limiters = {
            'coinbase': RateLimiter(rate=10, per=1),  # 10 req/s public
            'htx': RateLimiter(rate=100, per=10),     # 100 req/10s public
            'bybitperps': RateLimiter(rate=120, per=60),  # 120 req/min public REST
            'binanceperps': RateLimiter(rate=1200, per=60),  # 1200 req/min public (weight-based, adjusted)
            'binancespot': RateLimiter(rate=1200, per=60)  # 1200 req/min public (weight-based, adjusted)
        }

    async def set_sources(self, sources_arg):
        valid_options = ["htx", "coinbase", "bybitperps", "binanceperps", "binancespot", "all"]
        if sources_arg.lower() not in valid_options:
            print(colored(f"Invalid source '{sources_arg}'. Valid options: htx, coinbase, bybitperps, binanceperps, binancespot, all.", fg='red'))
            return

        # Close existing connections if any.
        for ex in self.exchanges.values():
            try:
                await ex.close()
            except Exception:
                pass
        self.exchanges.clear()
        self.symbols.clear()
        self.selected_sources.clear()

        if sources_arg.lower() == "all":
            srcs = ["coinbase", "htx", "bybitperps", "binanceperps", "binancespot"]
        else:
            srcs = [sources_arg.lower()]

        self.selected_sources = srcs

        for src in srcs:
            options = {'enableRateLimit': True}
            if self.proxy:
                options['proxies'] = {"http": self.proxy, "https": self.proxy}
            if src == "htx":
                ex = ccxt.htx(options)
            elif src == "coinbase":
                ex = ccxt.coinbase(options)
            elif src == "bybitperps":
                ex = ccxt.bybit({
                    'options': {
                        'defaultType': 'future',
                        'category': 'linear'  # Explicitly set to linear for USDT perpetuals
                    }
                } | options)
                ex.urls['api']['public'] = 'https://api.bybit.com'  # Base v5 API URL, CCXT appends endpoints
            elif src == "binanceperps":
                ex = ccxt.binance({'options': {'defaultType': 'future'}} | options)
                ex.urls['api']['public'] = 'https://fapi.binance.com/fapi/v1'  # Correct futures endpoint
            elif src == "binancespot":
                ex = ccxt.binance(options)
                ex.urls['api']['public'] = 'https://api.binance.com/api/v3'  # Correct spot endpoint
            else:
                print(colored(f"Unknown source '{src}'. Skipping.", fg='red'))
                continue
            try:
                markets = await ex.load_markets()
                all_symbols = list(markets.keys())
                filtered_symbols = []
                for symbol in all_symbols:
                    info = markets[symbol].get('info', {})
                    if "/USDT" in symbol and markets[symbol].get('active', True):
                        if src in ['bybitperps', 'binanceperps']:
                            if markets[symbol].get('contract', False) and 'linear' in markets[symbol].get('type', ''):
                                filtered_symbols.append(symbol)
                        elif src == 'binancespot':
                            if not markets[symbol].get('contract', False):
                                filtered_symbols.append(symbol)
                        else:
                            filtered_symbols.append(symbol)
                self.exchanges[src] = ex
                self.symbols[src] = filtered_symbols
                print(colored(f"Loaded {len(all_symbols)} symbols from {src.upper()}.", fg='cyan', style='bold'))
                print(colored(f"Filtered to {len(filtered_symbols)} active USDT symbols for {src.upper()}.", fg='cyan', style='bold'))
            except Exception as e:
                print(colored(f"Error loading markets for {src}: {e}", fg='red'))
                try:
                    await ex.close()
                except Exception as close_e:
                    print(colored(f"Error closing {src} exchange: {close_e}", fg='red'))
                continue
        # Start the update loop if not already running and there are valid sources.
        if self.exchanges and self.update_task is None:
            self.update_task = asyncio.create_task(self.update_loop())

    async def update_loop(self):
        semaphore = asyncio.Semaphore(10)
        while True:
            tasks = []
            for src in self.selected_sources:
                exchange = self.exchanges.get(src)
                if not exchange:
                    continue
                syms = self.symbols.get(src, [])
                for symbol in syms:
                    tasks.append(self.fetch_data_for_symbol(src, symbol, semaphore))
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(60)

    async def fetch_data_for_symbol(self, src, symbol, semaphore):
        async with semaphore:
            exchange = self.exchanges.get(src)
            if not exchange:
                return

            max_retries = 5
            base_delay = 1
            rate_limiter = self.rate_limiters.get(src)

            for attempt in range(max_retries):
                try:
                    await rate_limiter.acquire()
                    now = exchange.milliseconds()
                    ticker = await exchange.fetch_ticker(symbol)
                    last_price = ticker.get('last')
                    vol_24h = ticker.get('quoteVolume') or ticker.get('baseVolume') or 0
                    info = ticker.get('info', {})
                    funding = info.get('fundingRate') if src in ['bybitperps', 'binanceperps'] else None
                    oi = info.get('openInterest') if src in ['bybitperps', 'binanceperps'] else None
                    try:
                        ohlcv_5m = await exchange.fetch_ohlcv(symbol, timeframe='5m', limit=2)
                        if len(ohlcv_5m) >= 2:
                            open_5m = ohlcv_5m[0][1]
                            close_5m = ohlcv_5m[-1][4]
                            change_5m = ((close_5m - open_5m) / open_5m) * 100 if open_5m != 0 else 0
                            change_5m_up = change_5m if change_5m > 0 else 0
                            change_5m_down = change_5m if change_5m < 0 else 0
                        else:
                            change_5m = change_5m_up = change_5m_down = 0
                    except Exception:
                        change_5m = change_5m_up = change_5m_down = 0

                    five_min_ago = now - (5 * 60 * 1000)
                    try:
                        trades = await exchange.fetch_trades(symbol, since=five_min_ago)
                        tps_5m = len(trades) / (5 * 60) if trades else 0
                        trades_last_min = [t for t in trades if t['timestamp'] >= now - (60 * 1000)]
                        tps = len(trades_last_min) / 60 if trades_last_min else 0
                    except Exception:
                        tps = tps_5m = 0

                    try:
                        order_book = await exchange.fetch_order_book(symbol)
                        bid = order_book['bids'][0][0] if order_book['bids'] else None
                        ask = order_book['asks'][0][0] if order_book['asks'] else None
                        if bid and ask:
                            mid = (bid + ask) / 2
                            spread = (ask - bid) / mid * 100
                        else:
                            spread = 0
                    except Exception:
                        spread = 0

                    volatility = abs(change_5m)
                    key = f"{src}:{symbol}"
                    previous = self.market_data.get(key, {})
                    previous_oi = self.oi_history.get(key, {}).get('oi')
                    oi_change_1h = (oi - previous_oi) if previous_oi and oi else 0
                    self.oi_history[key] = {'timestamp': now, 'oi': oi}
                    volume_delta_1h = 0

                    self.market_data[key] = {
                        'price': last_price,
                        'change_5m_up': change_5m_up,
                        'change_5m_down': change_5m_down,
                        'tps': tps,
                        'tps_5m': tps_5m,
                        'spread': spread,
                        'funding': funding,
                        'vol_24h': vol_24h,
                        'volatility': volatility,
                        'oi': oi,
                        'oi_change_1h': oi_change_1h,
                        'volume_delta_1h': volume_delta_1h,
                        'timestamp': now
                    }
                    return

                except ccxt.RateLimitExceeded:
                    wait_time = base_delay * (2 ** attempt) + random.uniform(0, 1)
                    print(colored(f"[Rate Limited] Retrying {src}:{symbol} in {wait_time:.2f}s", fg='yellow'))
                    await asyncio.sleep(wait_time)
                except ccxt.NetworkError as e:
                    print(colored(f"[Network Error] {src}:{symbol}: {e}", fg='red'))
                    await asyncio.sleep(2)
                except Exception as e:
                    print(colored(f"Error fetching data for {src}:{symbol}: {e}", fg='red'))
                    await asyncio.sleep(2)
            print(colored(f"Failed to fetch data for {src}:{symbol} after {max_retries} attempts.", fg='red'))

    def get_pair_data(self, pair):
        if ":" in pair:
            return self.market_data.get(pair, None)
        else:
            if len(self.selected_sources) == 1:
                key = f"{self.selected_sources[0]}:{pair}"
                return self.market_data.get(key, None)
            else:
                results = {}
                for src in self.selected_sources:
                    key = f"{src}:{pair}"
                    if key in self.market_data:
                        results[src] = self.market_data[key]
                return results if results else None

    def get_all_data(self):
        return self.market_data

# --- Other Command Functions ---
def display_info(pair, screener):
    data = screener.get_pair_data(pair)
    if not data:
        print(colored(f"No data for pair {pair}", fg='red'))
        return
    if isinstance(data, dict) and 'price' in data:
        sources = [None]
        datas = [data]
    else:
        sources = list(data.keys())
        datas = list(data.values())
    for i, d in enumerate(datas):
        src = sources[i] if sources[i] else (screener.selected_sources[0] if screener.selected_sources else "")
        base = pair.split(":")[-1] if ":" in pair else pair
        title = f"{src.upper()} {base}" if src else base
        print(colored(f"\nInfo for {title}:", fg='cyan', style='bold'))
        print(f" Price:             {colored(d.get('price') or 0, fg='green')}")
        print(f" 5m Change Up:      {colored(d.get('change_5m_up') or 0, fg='green')}")
        print(f" 5m Change Down:    {colored(d.get('change_5m_down') or 0, fg='red')}")
        print(f" TPS:               {colored(round(d.get('tps') or 0, 2), fg='yellow')}")
        print(f" TPS (5m):          {colored(round(d.get('tps_5m') or 0, 2), fg='yellow')}")
        print(f" Spread Width (%):  {colored(round(d.get('spread') or 0, 2), fg='magenta')}")
        print(f" Funding:           {colored(d.get('funding') or 0, fg='blue')}")
        print(f" 24hr Volume:       {colored(d.get('vol_24h') or 0, fg='cyan')}")
        print(f" Volatility:        {colored(round(d.get('volatility') or 0, 2), fg='red')}")
        print(f" Open Interest:     {colored(d.get('oi') or 0, fg='green')}")
        print(f" OI Change 1h:      {colored(d.get('oi_change_1h') or 0, fg='yellow')}")
        print(f" Volume Delta 1h:   {colored(d.get('volume_delta_1h') or 0, fg='magenta')}\n")

def display_funding(direction, screener):
    all_data = screener.get_all_data()
    filtered = {k: d for k, d in all_data.items() if d.get('funding') is not None}
    if not filtered:
        print(colored("No funding data available.", fg='red'))
        return
    reverse = True if direction == 'up' else False
    sorted_pairs = sorted(filtered.items(), key=lambda x: (x[1].get('funding') or 0), reverse=reverse)
    print(colored(f"\nFunding ranking ({'highest' if direction=='up' else 'lowest'} first):", fg='cyan', style='bold'))
    for key, d in sorted_pairs:
        source, base = display_symbol(key)
        label = f"{source.upper()} {base}" if source else base
        print(f" {label:<15} Funding: {colored(d.get('funding') or 0, fg='blue')}")
    print()

def display_metric(metric, screener):
    valid = ['price', 'change_5m_up', 'change_5m_down', 'tps', 'tps_5m',
             'spread', 'funding', 'vol_24h', 'volatility', 'oi', 'oi_change_1h', 'volume_delta_1h']
    if metric not in valid:
        print(colored(f"Invalid metric '{metric}'. Valid metrics are: {', '.join(valid)}", fg='red'))
        return
    all_data = screener.get_all_data()
    sorted_data = sorted(all_data.items(), key=lambda x: (x[1].get(metric) or 0), reverse=True)
    include_source = len(screener.selected_sources) > 1
    header = (("{:<10}{:<15}" if include_source else "{:<15}") +
              " {:<10} {:<8} {:<8} {:<6} {:<6} {:<8} {:<10} {:<10} {:<12} {:<8} {:<10} {:<10}" 
              if include_source else
              "{:<15} {:<10} {:<8} {:<8} {:<6} {:<6} {:<8} {:<10} {:<10} {:<12} {:<8} {:<10} {:<10}")
    print(colored(header, fg='cyan', style='bold'))
    for key, d in sorted_data:
        source, base = display_symbol(key)
        price = f"{(d.get('price') or 0):<10}"
        change_5m_up = f"{(d.get('change_5m_up') or 0):<8}"
        change_5m_down = f"{(d.get('change_5m_down') or 0):<8}"
        tps = f"{(d.get('tps') or 0):<6}"
        tps_5m = f"{(d.get('tps_5m') or 0):<6}"
        spread = f"{(d.get('spread') or 0):<8}"
        funding = f"{(d.get('funding') or 0):<10}"
        vol_24h = f"{(d.get('vol_24h') or 0):<10}"
        volatility = f"{(d.get('volatility') or 0):<12}"
        oi = f"{(d.get('oi') or 0):<8}"
        oi_change_1h = f"{(d.get('oi_change_1h') or 0):<10}"
        vol_delta_1h = f"{(d.get('volume_delta_1h') or 0):<10}"
        if include_source:
            line = ("{:<10}{:<15} {} {} {} {} {} {} {} {} {} {} {} {}"
                    .format(source.upper(), base, price, change_5m_up, change_5m_down, tps, tps_5m, spread,
                            funding, vol_24h, volatility, oi, oi_change_1h, vol_delta_1h))
        else:
            line = ("{:<15} {} {} {} {} {} {} {} {} {} {} {} {}"
                    .format(base, price, change_5m_up, change_5m_down, tps, tps_5m, spread,
                            funding, vol_24h, volatility, oi, oi_change_1h, vol_delta_1h))
        print(line)
    print()

async def display_metric_auto(metric, screener):
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            print(colored(f"Displaying all metrics sorted by {metric.upper()} (auto-updating):", fg='cyan', style='bold'))
            display_metric(metric, screener)
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("\nAuto display stopped.\n")

async def display_oi(screener):
    try:
        while True:
            os.system('cls' if os.name == 'nt' else 'clear')
            all_data = screener.get_all_data()
            sorted_oi = sorted(all_data.items(), key=lambda x: (x[1].get('oi') or 0), reverse=True)
            print(colored("Open Interest Ranking (Top to Bottom):", fg='cyan', style='bold'))
            for key, d in sorted_oi:
                source, base = display_symbol(key)
                label = f"{source.upper()} {base}" if source else base
                print(f" {label:<15} OI: {colored(d.get('oi') or 0, fg='green')}  (Change 1h: {colored(d.get('oi_change_1h') or 0, fg='yellow')})")
            await asyncio.sleep(5)
    except asyncio.CancelledError:
        print("\nExiting OI display.\n")

async def tape_printer(screener):
    while True:
        _, msg = await screener.tape_queue.get()
        print(msg)
        screener.tape_queue.task_done()

async def fetch_and_print_trades(src, sym, exchange, last_trade_ids, feed_start, semaphore, screener):
    async with semaphore:
        async def fetch_regular():
            await screener.rate_limiters[src].acquire()
            try:
                trades = await exchange.fetch_trades(sym)
                return trades
            except Exception as e:
                await screener.tape_queue.put((exchange.milliseconds(), 
                                      colored(f"Error fetching trades for {src}:{format_symbol(sym)}: {e}", fg='red')))
                return []

        async def fetch_liquidation():
            if src not in ['bybitperps', 'binanceperps']:
                return []
            await screener.rate_limiters[src].acquire()
            try:
                liq_trades = await exchange.fetch_trades(sym, params={'liquidation': True})
                for t in liq_trades:
                    t['liquidation'] = True
                return liq_trades
            except Exception:
                return []

        regular_trades, liq_trades = await asyncio.gather(fetch_regular(), fetch_liquidation())
        all_trades = regular_trades + liq_trades

        key = f"{src}:{sym}"
        for trade in all_trades:
            if trade['timestamp'] < feed_start:
                continue
            if last_trade_ids.get(key) is None or int(trade['id']) > int(last_trade_ids[key]):
                side = trade.get('side', 'unknown').lower()
                if side == 'buy':
                    arrow = "↑"
                    side_text = colored("Buy", fg='green', style='bold')
                    color_for_trade = 'green'
                elif side == 'sell':
                    arrow = "↓"
                    side_text = colored("Sell", fg='red', style='bold')
                    color_for_trade = 'red'
                else:
                    arrow = ""
                    side_text = side.capitalize()
                    color_for_trade = 'white'

                trade_timestamp = trade['timestamp']
                ts = datetime.datetime.fromtimestamp(trade_timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
                price = trade['price']
                amount = trade['amount']
                try:
                    usd_value = float(price) * float(amount)
                except Exception:
                    usd_value = 0
                usd_value_str = format_num(usd_value, decimals=2)
                now_ms = exchange.milliseconds()
                if key not in screener.trade_history:
                    screener.trade_history[key] = []
                screener.trade_history[key].append((now_ms, usd_value))
                thirty_min_ago = now_ms - (30 * 60 * 1000)
                screener.trade_history[key] = [(t, v) for t, v in screener.trade_history[key] if t >= thirty_min_ago]
                if screener.trade_history[key]:
                    avg_usd = sum(v for t, v in screener.trade_history[key]) / len(screener.trade_history[key])
                else:
                    avg_usd = usd_value if usd_value > 0 else 1
                scale_factor = 5
                ratio = usd_value / avg_usd if avg_usd > 0 else 1
                num_bars = max(1, int(ratio * scale_factor))
                bar_visual = "|" * num_bars
                base = sym.split("/")[0]
                if trade.get('liquidation'):
                    if side == 'sell':
                        highlight_color = ORANGE
                    elif side == 'buy':
                        highlight_color = Fore.BLUE
                    else:
                        highlight_color = Fore.WHITE
                    part1 = f"{arrow} {side_text}"
                    part2 = f" | {src.upper()} {base} | Price: {price} | Time: {ts} | Size: {usd_value_str} USD | {bar_visual}"
                    msg = f"{part1}{colored(part2, fg=highlight_color)}"
                else:
                    msg = colored(f"{arrow} {side_text} | {src.upper()} {base} | Price: {price} | Time: {ts} | Size: {usd_value_str} USD | {bar_visual}",
                                  fg=color_for_trade)
                await screener.tape_queue.put((trade_timestamp, msg))
                last_trade_ids[key] = trade['id']

async def tape_feed(pair, screener):
    feed_sources = {}
    if pair.lower() == 'all':
        for src in screener.selected_sources:
            feed_sources[src] = screener.symbols.get(src, [])
    else:
        if ":" in pair:
            src, sym = pair.split(":", 1)
            if src in screener.selected_sources and sym in screener.symbols.get(src, []):
                feed_sources[src] = [sym]
            else:
                print(colored(f"Pair {pair} not found.", fg='red'))
                return
        else:
            for src in screener.selected_sources:
                if pair in screener.symbols.get(src, []):
                    feed_sources.setdefault(src, []).append(pair)
            if not feed_sources:
                print(colored(f"Pair {pair} not found in any active source.", fg='red'))
                return

    feed_start = {}
    for src in feed_sources:
        exchange = screener.exchanges.get(src)
        if exchange:
            feed_start[src] = exchange.milliseconds()
        else:
            print(colored(f"Skipping {src} in tape feed due to no exchange instance.", fg='yellow'))
            del feed_sources[src]
    if not feed_sources:
        print(colored("No valid sources available for tape feed.", fg='red'))
        return

    print(colored("Starting live tape feed for:", fg='cyan', style='bold'))
    for src, syms in feed_sources.items():
        print(colored(f" {src.upper()}: " + ", ".join([format_symbol(sym) for sym in syms]), fg='cyan'))
    last_trade_ids = {}
    for src, syms in feed_sources.items():
        for sym in syms:
            last_trade_ids[f"{src}:{sym}"] = None

    semaphores = {
        'coinbase': asyncio.Semaphore(5),
        'htx': asyncio.Semaphore(50),
        'bybitperps': asyncio.Semaphore(20),
        'binanceperps': asyncio.Semaphore(50),
        'binancespot': asyncio.Semaphore(50)
    }

    try:
        while True:
            tasks = []
            for src, syms in feed_sources.items():
                exchange = screener.exchanges[src]
                for sym in syms:
                    tasks.append(
                        fetch_and_print_trades(src, sym, exchange, last_trade_ids, feed_start[src], semaphores[src], screener)
                    )
            await asyncio.gather(*tasks, return_exceptions=True)
            await asyncio.sleep(0.5)
    except asyncio.CancelledError:
        print(colored("\nTape feed stopped.\n", fg='yellow'))

async def command_loop(screener):
    loop = asyncio.get_event_loop()
    display_task = None
    full_display_task = None
    oi_task = None
    tape_task = None
    mmscan_task = None
    full_disp_stop_event = None
    mmscan_stop_event = None
    try:
        while True:
            cmd = await loop.run_in_executor(None, input, colored(">> ", fg='magenta'))
            parts = cmd.strip().split()
            if not parts:
                continue
            if parts[0] == 'help':
                print(colored("\nCommands:", fg='cyan', style='bold'))
                print(" sources <htx|coinbase|bybitperps|binanceperps|binancespot|all> - Set the data source(s).")
                print(" info <pair>                   - Show info for a specific pair. (Prefix with source if needed: bybitperps:BTC/USDT)")
                print(" info funding up|down          - Show funding ranking (up: highest first, down: lowest first).")
                print(" display <metric>              - Continuously display all metrics sorted by the given metric.")
                print("    (Valid metrics: price, change_5m_up, change_5m_down, tps, tps_5m, spread,")
                print("     funding, vol_24h, volatility, oi, oi_change_1h, volume_delta_1h)")
                print(" displayfull <metric>          - Full-screen display (fills window, scrollable) sorted by the given metric.")
                print(" display oi                    - Display open interest ranking (auto-updating).")
                print(" tape <pair|all>               - Start live tape feed for a pair or all pairs. (Prefix pair with source if desired)")
                print(" tape stop                     - Stop the live tape feed.")
                print(" displayfull stop              - Stop the full-screen display.")
                print(" mmscan                        - Start continuously scanning market conditions (full-screen, non-scrolling).")
                print(" mmscan stop                   - Stop the market conditions scan.")
                print(" proxy <usa|japan> <number|best> - Set proxy for the given region by number or choose the best connection.")
                print(" proxy list                    - List available proxies with ping times.")
                print(" proxy off                     - Disable the current proxy.")
                print(" exit                          - Exit the screener.\n")
            elif parts[0] == 'sources':
                if len(parts) == 2:
                    new_src = parts[1]
                    await screener.set_sources(new_src)
                    print(colored(f"Data source(s) set to: {', '.join(screener.selected_sources)}", fg='cyan', style='bold'))
                else:
                    print(colored("Usage: sources <htx|coinbase|bybitperps|binanceperps|binancespot|all>", fg='red'))
            elif parts[0] == 'info':
                if len(parts) == 2:
                    display_info(parts[1], screener)
                elif len(parts) == 3 and parts[1] == 'funding' and parts[2] in ['up', 'down']:
                    display_funding(parts[2], screener)
                else:
                    print(colored("Invalid info command.", fg='red'))
            elif parts[0] == 'display':
                if len(parts) == 2:
                    if parts[1].lower() == 'oi':
                        if oi_task is not None:
                            oi_task.cancel()
                        oi_task = asyncio.create_task(display_oi(screener))
                    else:
                        if display_task is not None:
                            display_task.cancel()
                        display_task = asyncio.create_task(display_metric_auto(parts[1], screener))
                else:
                    print(colored("Invalid display command.", fg='red'))
            elif parts[0] == 'displayfull':
                if len(parts) == 2 and parts[1].lower() != 'stop':
                    if full_display_task is not None:
                        full_disp_stop_event.set()
                        full_display_task.cancel()
                        full_display_task = None
                    full_disp_stop_event = threading.Event()
                    full_display_task = asyncio.create_task(asyncio.to_thread(curses_display_metric, parts[1], screener, full_disp_stop_event))
                elif len(parts) == 2 and parts[1].lower() == 'stop':
                    if full_display_task is not None:
                        full_disp_stop_event.set()
                        full_display_task.cancel()
                        full_display_task = None
                        print(colored("Full-screen display stopped.", fg='yellow'))
                    else:
                        print(colored("No full-screen display is running.", fg='red'))
                else:
                    print(colored("Invalid displayfull command.", fg='red'))
            elif parts[0] == 'tape':
                if len(parts) == 2 and parts[1].lower() == 'stop':
                    if tape_task is not None:
                        tape_task.cancel()
                        tape_task = None
                        print(colored("Tape feed stopped.", fg='yellow'))
                    else:
                        print(colored("No tape feed is running.", fg='red'))
                elif len(parts) == 2:
                    if tape_task is not None:
                        tape_task.cancel()
                    tape_task = asyncio.create_task(tape_feed(parts[1], screener))
                else:
                    print(colored("Invalid tape command.", fg='red'))
            elif parts[0] == 'mmscan':
                if len(parts) == 2 and parts[1].lower() == 'stop':
                    if mmscan_task is not None:
                        mmscan_stop_event.set()
                        mmscan_task.cancel()
                        mmscan_task = None
                        print(colored("Market conditions scan stopped.", fg='yellow'))
                    else:
                        print(colored("No market conditions scan is running.", fg='red'))
                elif len(parts) == 1:
                    if mmscan_task is not None:
                        mmscan_stop_event.set()
                        mmscan_task.cancel()
                    mmscan_stop_event = threading.Event()
                    mmscan_task = asyncio.create_task(asyncio.to_thread(curses_market_conditions_scan, screener, mmscan_stop_event))
                else:
                    print(colored("Invalid mmscan command.", fg='red'))
            elif parts[0] == 'proxy':
                if len(parts) < 2:
                    print(colored("Usage: proxy <usa|japan> <number|best> | proxy list | proxy off", fg='red'))
                    continue
                subcmd = parts[1].lower()
                if subcmd in ['usa', 'japan']:
                    if len(parts) != 3:
                        print(colored("Usage: proxy <usa|japan> <number|best>", fg='red'))
                        continue
                    option = parts[2].lower()
                    if option == 'best':
                        best, ping = get_best_proxy(subcmd)
                        if best:
                            set_active_proxy(best, screener)
                            print(colored(f"Best {subcmd.upper()} proxy selected with ping {ping*1000:.0f}ms", fg='cyan'))
                        else:
                            print(colored(f"No working proxy found for region {subcmd.upper()}", fg='red'))
                    else:
                        try:
                            idx = int(option)
                            proxies_list = PROXIES.get(subcmd, [])
                            if 1 <= idx <= len(proxies_list):
                                chosen = proxies_list[idx-1]
                                t = test_proxy(chosen)
                                if t is None:
                                    print(colored("Selected proxy timed out during test.", fg='red'))
                                else:
                                    set_active_proxy(chosen, screener)
                                    print(colored(f"{subcmd.upper()} proxy #{idx} selected with ping {t*1000:.0f}ms", fg='cyan'))
                            else:
                                print(colored(f"Invalid proxy number for region {subcmd.upper()}.", fg='red'))
                        except ValueError:
                            print(colored("Proxy number must be an integer or 'best'.", fg='red'))
                elif subcmd == 'list':
                    list_proxies()
                elif subcmd == 'off':
                    disable_proxy(screener)
                else:
                    print(colored("Invalid proxy command. Usage: proxy <usa|japan> <number|best> | proxy list | proxy off", fg='red'))
            elif parts[0] == 'exit':
                print("Exiting screener.")
                if display_task is not None:
                    display_task.cancel()
                if oi_task is not None:
                    oi_task.cancel()
                if tape_task is not None:
                    tape_task.cancel()
                if full_display_task is not None:
                    full_disp_stop_event.set()
                    full_display_task.cancel()
                if mmscan_task is not None:
                    mmscan_stop_event.set()
                    mmscan_task.cancel()
                break
            else:
                print(colored("Unknown command. Type 'help' for list of commands.", fg='red'))
    except Exception as e:
        print(colored(f"Command loop error: {e}", fg='red'))

async def main():
    # Do not connect to any exchange until the "sources" command is given.
    screener = MultiScreener()
    printer_task = asyncio.create_task(tape_printer(screener))
    try:
        await command_loop(screener)
    finally:
        if screener.update_task is not None:
            screener.update_task.cancel()
        printer_task.cancel()
        for ex in screener.exchanges.values():
            try:
                await ex.close()
            except Exception as e:
                print(colored(f"Error closing exchange: {e}", fg='red'))

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram interrupted by user.")
