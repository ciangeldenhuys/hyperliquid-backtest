from bayes_opt import BayesianOptimization
from strategy.volume_executor import VolumeExecutor
from source.backtest import Backtest
from datetime import datetime
import numpy as np
import strategy.config.volume_config as config

def objective_function(threshold, threshold_s, z_score_max, flush_minutes, short_buf_hours, long_buf_days, rsi_buy, rsi_sell):
    # Convert parameters to appropriate units
    flush = int(flush_minutes * 60)  # Convert to seconds
    short_buf = int(short_buf_hours * 3600)  # Convert to seconds
    long_buf = int(long_buf_days * 86400)  # Convert to seconds
    
    # Update config parameters
    config.THRESHOLD = threshold
    config.THRESHOLD_S = threshold_s
    config.Z_SCORE_MAX = z_score_max
    config.FLUSH = flush
    config.SHORT_BUF = short_buf
    config.LONG_BUF = long_buf
    config.THRESHOLD_RSI_B = rsi_buy
    config.THRESHOLD_RSI_S = rsi_sell
    
    # Create backtest instance
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 2, 1)
    initial_capital = 10000  # Fixed initial capital
    backtest = Backtest("XRPUSDT", start_date, end_date, initial_capital)
    
    # Create and run strategy
    executor = VolumeExecutor(backtest, initial_capital)
    executor.start()
    
    # Calculate performance metrics
    final_balance = backtest.current_total_usd()
    max_drawdown = executor.calculate_max_drawdown()
    
    # Calculate return percentage
    return_pct = (final_balance - initial_capital) / initial_capital
    
    # Objective: balance between return and drawdown
    # Higher return and lower drawdown = better
    objective = return_pct * (1 - max_drawdown)
    
    return objective

def optimize_parameters():
    # Define parameter bounds
    pbounds = {
        'threshold': (0.5, 5.0),      # Z-score threshold for buying
        'threshold_s': (0.5, 5.0),    # Z-score threshold for selling
        'z_score_max': (3.0, 15.0),   # Maximum z-score for position sizing
        'flush_minutes': (1, 15),     # Flush interval in minutes
        'short_buf_hours': (0.5, 4),  # Short buffer in hours
        'long_buf_days': (1, 10),     # Long buffer in days
        'rsi_buy': (50, 80),          # RSI threshold for buying
        'rsi_sell': (20, 50)          # RSI threshold for selling
    }
    
    # Initialize optimizer
    optimizer = BayesianOptimization(
        f=objective_function,
        pbounds=pbounds,
        random_state=1
    )
    
    # Run optimization
    optimizer.maximize(
        init_points=5,    # Number of initial random points
        n_iter=20         # Number of optimization iterations
    )
    
    # Print best parameters
    print("\nBest parameters found:")
    for param, value in optimizer.max['params'].items():
        print(f"{param}: {value}")
    print(f"\nBest objective value: {optimizer.max['target']}")

if __name__ == "__main__":
    optimize_parameters()
