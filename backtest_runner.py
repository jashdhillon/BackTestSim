from backtest_synchronous import BackTest as BTS
from backtest_rabbitmq import BackTest as BTR

scheduled_backtests = dict()

# Single process
# All steps of backtest are run sequentially
EM_SYNCHRONOUS = 0

# DEPRECATED: NOT FUNCTIONAL
EM_MULTIPROCESSING = 1

# Single process spawns 3 subprocess for backtest to be run in 3 stages
# Stage A: Universe Calcuation
# Stage B: Strategy Execution
# Stage C: Portfolio Rebalancing/Order Generation/Statistic Calculation/Data Pushing
EM_RABBITMQ = 2


# Schedules backtest to be run in its own process and uses execution_mode to determine the mode of execution of the backtest
def signal_backtest(user_id, strategy_id, execution_mode=0, kwargs={"period": 30}):
    print("Scheduling Backtest")

    global scheduled_backtests
    backtest_id = len(scheduled_backtests.items())
    period = kwargs["period"]
    backtest = BTR(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id, period=period) if execution_mode == EM_RABBITMQ else (BTS(user_id=user_id, strategy_id=strategy_id, backtest_id=backtest_id, period=period) if execution_mode == EM_SYNCHRONOUS else None)

    if not backtest:
        print("Invalid Execution Mode Specified:", execution_mode)
        quit(0)

    # Storing Backtest with Unique Composite Key: (user_id, strategy_id, backtest_id)
    scheduled_backtests[user_id, strategy_id, backtest_id] = backtest
    backtest.start()

    print("Finished Scheduling Backtest")


# Ensuring all spawned processes are cleaned up before termination
def clean_up():
    global scheduled_backtests

    for _, value in scheduled_backtests.items():
        value.join()

    print("Cleaning Up")
