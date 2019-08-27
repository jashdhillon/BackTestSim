from backtest_utils import *


# Universe Calculation: Fetches set of existing symbols and derives and returns a subset of that set
# Required Arguments:
#   universe: list holds the calculated universe for every period executed thus far
#
# Returns: Appends next universe to list of previously calculated universes
def calc_universe(args={}):
    result = [s for s in fetch_symbol_list() if len(s) == 1]
    args["universe"].append(result)


# Execute Strategy: Given the state of the portfolio and the state of the market, signals(trade_signals) are generated on whether to buy or sell stock
# Required Arguments:
#   time: the current period being executed
#   universe: list holds the calculated universe for every period executed thus far
#   prices: list that holds the prices of all stocks possible and their price changes thus far
#   volumes: list that holds the volumes of all stocks possible and their volume changes thus far
#   funds: list that holds the history of the user's available liquid funds thus far
#   shares: list that holds the history of the user's shares bought thus far
#   trade_signals: list that holds the history of the trade signals generated thus far
#   transaction_cost: cost that each transaction(buy or sell) will cost the user
#
#   Returns: Appends generated trade signals to the list that holds the history of the trade signals generated thus far
def exec_strategy(args={}):
    time = args["time"]
    universe = args["universe"][time]
    prices = args["prices"]
    volumes = args["volumes"]
    funds = args["funds"][time]
    shares = args["shares"][time]
    trade_signals = args["trade_signals"][time]
    transaction_cost = args["transaction_cost"]

    balance_buffer = funds

    # Looping through all of the symbols available in the current universe and executing a simple strategy
    for s in universe:
        old_price, new_price, price_diff = fetch_symbol_price_change(time, prices, volumes, s)
        stock_shares = shares[s] if s in shares else 0
        volume = get_volume(time, volumes, s)

        old_bal = balance_buffer

        if new_price < old_price:
            # Sell
            if stock_shares > 0:
                max_sell_out_percentage = 0.10
                sell_out_capacity = int(max_sell_out_percentage * stock_shares)
                sell_target = randint(0, sell_out_capacity)

                if sell_target > 0 and sell_target * new_price >= transaction_cost:
                    trade_signals.append(gen_sell_signal(s, sell_target))
                    balance_buffer += (sell_target * new_price - transaction_cost)
                    args["sell_count"][time] += 1

            # TODO: Remove test code
            if balance_buffer < old_bal:
                print("Sell Error")
        elif new_price > old_price:
            # Buy
            max_buy_out_percentage = 0.10
            max_buy_out_capcity = int(max_buy_out_percentage * volume)
            buy_capcity = int((balance_buffer - transaction_cost) / new_price)
            buy_target = min(randint(0, max_buy_out_capcity), buy_capcity)

            if buy_target > 0:
                trade_signals.append(gen_buy_signal(s, buy_target))
                balance_buffer -= (buy_target * new_price + transaction_cost)
                args["buy_count"][time] += 1

            # TODO: Remove test code
            if balance_buffer > old_bal:
                print("Buy Error")


# Rebalance Portfolio: Base on arguments defined by the user, the list of trade signals generated is modified to include more or less trades of more or less quantity to ensure the allocated assets are within the allocations set
# Required Arguments:
#   time: the current period being executed
#   prices: list that holds the prices of all stocks possible and their price changes thus far
#   funds: list that holds the history of the user's available liquid funds thus far
#   shares: list that holds the history of the user's shares bought thus far
#   trade_signals: list that holds the history of the trade signals generated thus far
#   transaction_cost: cost that each transaction(buy or sell) will cost the user
#   max_stock_percentage: maximum allocation for a stock's percentage of the user's total balance
#
#   Returns: Modifies and appends generated trade signals to the list that holds the history of the trade signals generated thus far
def rebal_portfolio(args={}):
    time = args["time"]
    prices = args["prices"]
    funds = args["funds"][time]
    shares = args["shares"][time]
    trade_signals = args["trade_signals"][time]
    transaction_cost = args["transaction_cost"]
    max_stock_percentage = args["max_stock_percentage"]
    balance = calc_balance(time, prices, funds, shares)

    def get_corresponding_signal(symbol):
        for s in trade_signals:
            if s.symbol == symbol:
                return s

    signals_to_remove = []

    for s, c in shares.items():
        price = get_price(time, prices, s)
        trade_signal = get_corresponding_signal(s)
        stock_percentage = (price * c) / balance
        post_trade_stock_percentage = stock_percentage if not trade_signal else \
                    (price * (c + trade_signal.signal_type * trade_signal.quantity))

        if stock_percentage > max_stock_percentage:
            # Reduce Signal Quantity And Stock Quantity If Needed

            post_trade_quantity = c

            if trade_signal:
                if trade_signal.signal_type < 0:
                    post_trade_quantity -= trade_signal.quantity
                elif trade_signal.signal_type > 0 and post_trade_stock_percentage > max_stock_percentage:
                    # Remove Order
                    signals_to_remove.append(trade_signal)

            post_trade_stock_percentage = (price * post_trade_quantity) / balance

            if post_trade_stock_percentage > max_stock_percentage:
                sell_target = int((post_trade_stock_percentage - max_stock_percentage) * balance / price)

                if sell_target > 0:
                    # print("Reduce Target", sell_target, "|", (price * (post_trade_quantity - sell_target)) / balance)
                    trade_signals.append(gen_sell_signal(s, sell_target))
                    funds += (sell_target * price - transaction_cost)
        elif stock_percentage < max_stock_percentage:
            # Reduce Signal Quantity If Needed

            post_trade_quantity = c

            if trade_signal and trade_signal.signal_type > 0:
                post_trade_quantity += trade_signal.quantity

                post_trade_stock_percentage = (price * post_trade_quantity) / balance

                if post_trade_stock_percentage > max_stock_percentage:
                    # Reduce Buy Quantity

                    reduce_target = int((post_trade_stock_percentage - max_stock_percentage) * balance / price)

                    if trade_signal.quantity > reduce_target:
                        # print("Reduce Target", reduce_target, "|", (price * (post_trade_quantity - reduce_target)) / balance)
                        trade_signal.quantity -= reduce_target
                    elif trade_signal.quantity < reduce_target:
                        print("Error: Trade Signal Quantity is Less than Reduce Quantity")
                    else:
                        # Remove Order
                        signals_to_remove.append(trade_signal)
                        print("Removing Zero Order")

    for s in signals_to_remove:
        trade_signals.remove(s)


# Generate Order: Generates and carries out orders based on the trade signals generated by the previous exec_strat and rebal_portfolio steps
# Required Arguments:
#   time: the current period being executed
#   prices: list that holds the prices of all stocks possible and their price changes thus far
#   funds: list that holds the history of the user's available liquid funds thus far
#   shares: list that holds the history of the user's shares bought thus far
#   trade_signals: list that holds the history of the trade signals generated thus far
#   transaction_cost: cost that each transaction(buy or sell) will cost the user
#
#   Returns: Modifies the shares and the funds of the user
def gen_order(args={}):
    time = args["time"]
    prices = args["prices"]
    funds = args["funds"][time]
    shares = args["shares"][time]
    trade_signals = args["trade_signals"][time]
    transaction_cost = 6

    for s in trade_signals:
        # Lazy add symbols to shares list if they don't already exist
        if s.symbol not in shares:
            shares[s.symbol] = 0

        shares[s.symbol] += (s.signal_type * s.quantity)
        funds += (-s.signal_type * s.quantity * get_price(time, prices, s.symbol))

    args["funds"][time] = funds - transaction_cost * len(trade_signals)


# Calculate Statistics: Calculates som basic statistics of the current execution period
# Required Arguments:
#   time: the current period being executed
#   initial_funds: The user defined initial funds available
#   prices: list that holds the prices of all stocks possible and their price changes thus far
#   funds: list that holds the history of the user's available liquid funds thus far
#   shares: list that holds the history of the user's shares bought thus far
#   buy_count: list that holds the total number of buy trade signals generated for each period thus far
#   sell_count list that holds the total number of sell trade signals generated for each period thus far
#   statistics: list that holds the history of the calculated statistics thus far
#
#   Returns: Appends statistics to list that holds the history of the calculated statistics thus far
def calc_stats(args={}):
    time = args["time"]
    initial_funds = args["initial_funds"]

    # Storing Prices of Bought Shares to Supply Price Reference for RabbitMQ Execution Mode Calc Balance Computation
    price_data = args["prices"]

    prices = []

    # Storing only counted for last period and counted for both sync and rabbitmq to equal io write work
    if time == args["period"] - 1:
        for i in range(len(args["prices"])):
            period_share_prices = dict()

            period_shares = args["shares"][i]

            for s, _ in period_shares.items():
                period_share_prices[s] = price_data[i][s]

            prices.append(period_share_prices)

    funds = args["funds"][time]
    shares = args["shares"][time]
    buy_count = args["buy_count"][time]
    sell_count = args["sell_count"][time]
    balance = calc_balance(args["time"], args["prices"], funds, shares)
    net = balance - initial_funds

    args["statistics"].append(
        {"time": time, "initial_funds": initial_funds, "funds": funds, "shares": shares, "balance": balance, "net": net,
         "buy_count": buy_count, "sell_count": sell_count, "prices": prices})


# Push Data: Saves or pushed the data to a "data base" for later use
# Required Arguments:
#   user_id: the id of the user
#   strategy_id: the id of the strategy being executed
#   backtest_id: the id of the backtest
#   statistics: list that holds the history of the calculated statistics thus far
#
#   Returns: Saves the statistics received to the "database"
def push_data(args={}):
    user_id = args["user_id"]
    strategy_id = args["strategy_id"]
    backtest_id = args["backtest_id"]
    statistics = args["statistics"]

    save_data(user_id, strategy_id, backtest_id, statistics)
