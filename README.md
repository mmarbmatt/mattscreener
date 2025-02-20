# mattscreener
a spot and perp futures crypto currency screener


Once running, the program displays a prompt (>>) for user commands. Type help to see the list of commands. Key commands include:

sources <htx|coinbase|all>

Sets the data source(s) for market data.
Example: sources coinbase
info <pair>

Displays detailed market info for the specified pair. Optionally, prefix the pair with the source (e.g., coinbase:BTC/USDT).
info funding <up|down>

Shows funding rankings based on funding rate, either highest or lowest first.
Example: info funding up
display <metric>

Continuously displays all metrics sorted by the specified metric. Valid metrics include: price, change_5m_up, change_5m_down, tps, tps_5m, spread, funding, vol_24h, volatility, oi, oi_change_1h, volume_delta_1h.
Example: display price
displayfull <metric>

Activates a full-screen (scrollable) display sorted by the given metric.
To stop the full-screen display, type: displayfull stop.
tape <pair|all>

Starts a live tape feed for a specific pair or for all pairs.
To stop the tape feed, type: tape stop.
mmscan

Initiates a full-screen market conditions scan that categorizes coins based on various calculated metrics.
To stop the scan, type: mmscan stop.
proxy <usa|japan> <number|best>

Sets the proxy for the specified region. You can choose a specific proxy by number or opt for the best connection.
Example: proxy usa best or proxy japan 2
Use proxy list to see available proxies and their response times.
Use proxy off to disable the active proxy.
exit

Terminates the screener and gracefully shuts down all tasks.
