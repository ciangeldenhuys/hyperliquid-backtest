import pandas as pd
import os

class Correlation:
    """
    In progress.
    """
    @staticmethod
    def compute(coin1, coin2, step, start=None, end=None):
        """
        Compute correlation between two coins.

        Parameters:
            coin1 (str): Name of first coin.

            coin2 (str): Name of second coin.

            step (int): Time step (ms) between datapoints in sum.

            start (int): Time (ms) to start correlation from.

            end (int): Time (ms) to end correlation at.

        Returns:
            correlation: The correlation between the two coins on the given interval.
        """
        datadir = 'historical_data\\spot\\totals'

        for filename in os.listdir(datadir):
            if filename.startswith(coin1):
                data1 = pd.read_csv(os.path.join(filename))
            if filename.startswith(coin2):
                data2 = pd.read_csv(os.path.join(filename))

        