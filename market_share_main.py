import pandas as pd
from query_market_share import get_snowflake_connection, query_amg_totals, query_market_totals, query_latest_week
from sklearn.ensemble import RandomForestRegressor
import update_table
import adjust_view

YEAR = 2025

DEP_VAR_AMG = 'REMAINING_AMG_ALBUMS'
INDEP_VARS_AMG = ['WEEK', 'YEAR', 'QUARTER', 'AMG_ALBUMS',
              'FOUR_WEEK_ROLLING_SUM_AMG', 'FOUR_WEEK_AMG_ROLLING_AVERAGE_ALBUMS_MOVED',
              'EIGHT_WEEK_AMG_ROLLING_AVERAGE_ALBUMS_MOVED',
              'AMG_WEEKLY_ALBUMS_FOUR_WEEK_DIFFERENCE']

DEP_VAR_MARKET = 'REMAINING_MARKET_ALBUMS'
INDEP_VARS_MARKET = ['WEEK', 'YEAR', 'QUARTER', 'MARKET_ALBUMS',
              'FOUR_WEEK_ROLLING_SUM_MARKET', 'FOUR_WEEK_MARKET_ROLLING_AVERAGE_ALBUMS_MOVED',
              'EIGHT_WEEK_MARKET_ROLLING_AVERAGE_ALBUMS_MOVED',
              'MARKET_WEEKLY_ALBUMS_FOUR_WEEK_DIFFERENCE']

class Chart:
    """
    Instance initialized via amg and market dataframes.
    Has two methods to retrieve the respective dataframes.
    Parent class of TrainingChart.
    """
    def __init__(self, amg:pd.DataFrame, market:pd.DataFrame):
        self._amg = amg
        self._market = market
    
    def get_amg_df(self):
        return self._amg
        
    def get_market_df(self):
        return self._market


class TrainingChart(Chart):
    """
    Instance initialized by 3 kwargs: max week, and the amg and market
    dataframes. Is a superclass of Chart.
    Stores market chart data historicals for each year up to the max week, which
    the model uses to train.
    """
    def __init__(self, *, max_week:int, amg:pd.DataFrame, market:pd.DataFrame):
        super().__init__(amg, market)
        self._max_week = max_week
    
    def get_max_week(self):
        return self._max_week


def query_training(sf, max_week:int):
    """
    Queries amg totals and market totals using Snowflake connection and chart week number.
    Returns TrainingChart objects for the max week.
    """
    amg_totals = query_amg_totals(sf, final_week=max_week)
    market_totals = query_market_totals(sf, final_week=max_week)
    return TrainingChart(max_week=max_week, amg=amg_totals, market=market_totals)


def get_forecast(chart:TrainingChart):
    """
    Takes in TrainingChart object to collect historicals to train a RandomForestRegressor on.
    The model is trained using previous years' data and then is used to predict the current data
    for the specified maximum week in the TrainingChart object.
    A dictionary with the results is returned, containing the week, expected EOY metrics for
    both amg and market, and the market share percentage.
    """
    amg_df = chart.get_amg_df()
    market_df = chart.get_market_df()
    pred_week = chart.get_max_week()

    # Get historicals to train model on
    amg_df_historicals = amg_df[amg_df['YEAR'] < YEAR]
    market_df_historicals = market_df[market_df['YEAR'] < YEAR]

    # Select independent and dependent variables for model
    X_historical_amg = amg_df_historicals[INDEP_VARS_AMG]
    y_historical_amg = amg_df_historicals[DEP_VAR_AMG]

    X_historical_market = market_df_historicals[INDEP_VARS_MARKET]
    y_historical_market = market_df_historicals[DEP_VAR_MARKET]

    # Train model based off previous year results
    amg_rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    amg_rf.fit(X_historical_amg, y_historical_amg)

    market_rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    market_rf.fit(X_historical_market, y_historical_market)

    # Get latest year data
    amg_year_df = amg_df[amg_df['YEAR'] == YEAR]
    market_year_df = market_df[market_df['YEAR'] == YEAR]
    max_week = amg_year_df['WEEK'].max()

    # Predict current year based off trained model
    amg_predict_data = amg_year_df[amg_year_df['WEEK'] == max_week]
    market_predict_data = market_year_df[market_year_df['WEEK'] == max_week]

    X_predict_amg = amg_predict_data[INDEP_VARS_AMG]
    X_predict_market = market_predict_data[INDEP_VARS_MARKET]

    predicted_amg_remaining = amg_rf.predict(X_predict_amg)[0]
    predicted_market_remaining = market_rf.predict(X_predict_market)[0]

    amg_ytd = amg_df.query(f'YEAR == {YEAR} and WEEK == {max_week}')['AMG_YTD_ALBUMS'].iloc[0]
    market_ytd = market_df.query(f'YEAR == {YEAR} and WEEK == {max_week}')['MARKET_YTD_ALBUMS'].iloc[0]

    # Calculate results
    projected_amg_eoy = amg_ytd + predicted_amg_remaining
    projected_market_eoy = market_ytd + predicted_market_remaining
    projected_amg_market_share = projected_amg_eoy / projected_market_eoy * 100

    # return results as dataframe
    results_dict = {
        'week': pred_week,
        'pred_amg_albums': round(projected_amg_eoy),
        'pred_market_albums': round(projected_market_eoy),
        'pred_amg_market_share_percentage': round(projected_amg_market_share, 4)
    }
    return results_dict


def update_essential_tables(_sf):
    """
    Updates tables needed to run models, the market_share_weekly_historicals and the 
    market_share_building_ytd table on Snowflake.
    """
    # Set database and schema context first
    cs = _sf.conn.cursor()
    cs.execute("USE DATABASE CURRENT_DEV")
    cs.execute("USE SCHEMA DATA")
    
    # Update tables
    update_table.populate_table_market_share_weekly_historicals(cs)
    update_table.populate_market_share_building_ytd(cs)


def adjust_table_views():
    """
    Adjusts table views when needed based off queries in create_table Not used in main.
    """
    with get_snowflake_connection() as sf:
        adjust_view.create_market_share_weekly_historicals(sf)
        adjust_view.create_market_share_forecasts_ytd_table(sf)
        adjust_view.create_market_share_building_ytd(sf)


def main():
    # Collect forecasted results by week
    results_list = []
    with get_snowflake_connection() as sf:
        update_essential_tables(sf) # Update tables first
        
        latest_week = query_latest_week(sf, YEAR).iloc[0, 0]
        for week in range(latest_week + 1, 53):
            training_data = query_training(sf, week)
            results = get_forecast(training_data)
            results_list.append(results)

        # Turn results into a dataframe
        results_df = pd.DataFrame(results_list)
        results_df_sorted = results_df.sort_values(by='week')
        update_table.update_market_share_forecasts_ytd(sf, results_df_sorted)


if __name__ == '__main__':
    main()
