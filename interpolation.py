import pandas as pd

import plotly.io as pio
#pio.kaleido.scope.default_format = "svg"
import plotly.graph_objs as go
import numpy as np
from plotly.subplots import make_subplots

def interpolate_function(df):
    #from cao_hydrology_analysis_graph import average_value_graph
    df = pd.read_json(r"cao_hydrology/{}_from_sql_{}.json".format (parameter, study), orient ='split', compression = 'infer')
    df = df.sort_values(by=["datetime"])
    
    if "site_id" in df.columns:
        df = df.drop(columns = "site_id")
    # create an average value for all sites across each timestamp  

    # create constrain columns
    df['min'] = df.groupby(["watershed"])[parameter].transform("min").round(2)   
    df['max'] = df.groupby(["watershed"])[parameter].transform("max").round(2)  
    
    
    if (parameter == "discharge") or (parameter == "water_temperature"):
        #print(df.loc[df.watershed == "north_seidel_creek"].head(5))
        

        # sort by datetime
        df = df.sort_values(["watershed","water_year", "datetime"], ascending=False)
        # create month helper column
        # since data is sorted on datetime, data should sort within the month, month provides a good balance in resolution
        df["month"] = df.datetime.dt.strftime('%m')
        df = df.sort_values(["watershed", "water_year", 'datetime'], ascending=True)

        # grouping by watershed doesnt do anything because zero values for watershed wont be filled
        df[f'average_{parameter}'] = df.groupby(["datetime"])[parameter].transform("mean").round(2)
       
        # this works the best so far because anything at day resolution cuts off peaks 
        # sort by average parameter then month
        df = df.sort_values([f"average_{parameter}", "month"], ascending=True)
       
        # this works pretty well
        df[parameter] = df.groupby(["watershed"])[parameter].fillna(method = "bfill")
        df = df.drop(columns=["month"])
        df.reset_index(inplace=True)
        # turn less then zeros into zeros, only a few -.01 values that should really be zero
        if parameter == "water_temperature":
              df.loc[df[parameter] < 0, parameter]  = 0

       

       
       
    if parameter == "conductivity":
        print("interpolate conductivity")
        # remove bad values
        if parameter == "conductivity" and study == 1:
            #print(df.loc[(df.watershed == "south_seidel_creek") & (df[parameter] == 5.2)])
            df.loc[(df.watershed == "south_seidel_creek") & (df[parameter] == 5.2), parameter] = np.nan
        # import interpolated discharge data
        df_q = pd.read_json(r"cao_hydrology/{}_cao_cleaned_data_{}.csv".format ("discharge", study), orient ='split', compression = 'infer')
        df_q = df_q[["watershed", "water_year", "datetime", "discharge"]]

        df = df.merge(df_q, on = ["watershed", "water_year", "datetime"], how = "right")
        df['site_code'] = df.groupby('watershed')['site_code'].fillna(method = 'bfill')
        df['type'] = df.groupby('watershed')['type'].fillna(method = 'bfill')
        df.loc[df.est == np.nan, "est"] = "TRUE"

        # remove data abnormalities top .03% of data
        df['c_90'] = df.loc[df.conductivity > 0].groupby(["watershed", "water_year"])["conductivity"].transform(lambda x: x.quantile(0.97))
        df['c_90'] = df.loc[df.conductivity > 0].groupby(["watershed", "water_year"])['c_90'].transform(lambda x: x.mean()).round(2)
        df.loc[df.conductivity >= df.c_90, "conductivity"] = np.nan

        df = df.sort_values(["watershed", "datetime"], ascending=True)
        df[parameter] = df.groupby(by = ["watershed", "water_year", "discharge"])[parameter].apply(lambda x: x.interpolate(method='linear'))
        df = df.sort_values(["datetime"], ascending=False)
        
        df = df.sort_values(["watershed", "discharge"], ascending=True)
        df[parameter] = df.groupby(by = ["watershed", "water_year"])[parameter].apply(lambda x: x.interpolate(method='linear'))
        df = df.sort_values(["watershed", "datetime"], ascending=True)
        df[parameter] = df.groupby(by = ["watershed"])[parameter].apply(lambda x: x.interpolate(method='linear'))
       
        df[f'average_{parameter}'] = df[parameter]
      
        
        # reset index, not sure if it is actually needed, was part of qa routine
        
        df.set_index(["watershed", "datetime"], inplace=True)
        df.sort_values(by = ["watershed", "datetime"], inplace=True)
        df.reset_index(inplace=True)

        # drop conductivity helper columns
        df = df.drop(['discharge'], axis=1)
        df = df.drop(['c_90'], axis=1)
    
    # filter by constraints
    # set data greater then max to max value
    df.loc[df[parameter] > df['max'], parameter] = df['max']
    # set data less then min value to value
    df.loc[df[parameter] < df['min'], parameter] = df['min']
    # graph
    #average_value_graph(df, parameter, study)
    df = df.drop([f'average_{parameter}'], axis=1)
    
    

    # interpolate filter
    # study 1
    shed = "weiss_creek"
    df.loc[df.watershed == shed].to_csv(r"C:/Users/ihiggins/OneDrive - King County/{}_{}_{}_interpolate.csv".format(shed, parameter, study))
    shed = "north_seidel_creek"
    df.loc[df.watershed == shed].to_csv(r"C:/Users/ihiggins/OneDrive - King County/{}_{}_{}_interpolate.csv".format(shed, parameter, study))
    
    df.to_json(r"cao_hydrology/{}_cao_cleaned_data_{}.csv".format(parameter, study), orient = 'split', compression = 'infer', index = 'false')
    print(f"interpolate {parameter} complete")


color_map = {
    'north_seidel_creek': r'#EF553B',
    'south_seidel_creek': r'#FFA15A',
    'webster_creek': r'#EECA3B',
    'cherry_trib': r'#636EFA',
    'fisher_creek': r'#AB63FA',
    'judd_creek': r'#19D3FA',
    'tahlequah_creek': r'#7E7DCD',
    'taylor_creek': r'#00CC96',
    'weiss_creek': r'#1CFFCE',
    1: r'#72B7B2',
    2: r'#F8A19F',
    'mean_discharge' : r'#316395', # dark blue
    "min7q_rolling_helper" : r"#2DE9FF",
    'min7q' : r"#00B5F7",
    'mean_temperature' : r"#D62728",
    'max_temperature' : r'#AF0038',
    'min_temperature' : r"#FF9DA6",

    'mean_conductivity' : r"#FECB52",
    'max_conductivity' : r'#FEAA16',
    'min_conductivity' : r"#F7E1A0",

    'mean_discharge' : r"#00B5F7",
    'max_discharge' : r'#2E91E5',
    'min_discharge' : r"rgb(179, 225, 207)",

    "high_pulse" : r"#DC587D",
    "low_pulse" : r"#F7E1A0",
   

    'mean_conductivity' : r'#FEAF16',
    'low_flow_peroid_water_temperature' : r"#F8A19F",
    'low_flow_peroid_box' : r'rgba(99, 110, 250, 0.3)',
    'summer_season_box' : r'rgba(99, 110, 250, 0.1)',

    #"water_year_7q" : r"rgba(204, 204, 204, 0.1)",
    "water_year_7q" : r"rgba(127, 60, 141, 0.9)",
    "min_7d" :  r"rgba(222, 172, 242, 0.9)",
    
    "low_flow_peroid_7q" : r"rgba(204, 204, 204, 0.3)",
    "summer_season_7q" : r"rgba(204, 204, 204, 0.6)"
    # Add more mappings as needed
    }

def site_interpolate():
    df = pd.read_csv("interpolate\\58a_daily.csv", parse_dates=[0], usecols=[0,1,2,3])
    df = df.rename(columns={df.columns[0]: "date"})
    df = df.rename(columns={df.columns[1]: "mean_flow"})
    df = df.rename(columns={df.columns[2]: "max_flow"})
    df = df.rename(columns={df.columns[3]: "min_flow"})
    df['mean_flow'] = pd.to_numeric(df['mean_flow'], errors='coerce')
    df['max_flow'] = pd.to_numeric(df['max_flow'], errors='coerce')
    df['min_flow'] = pd.to_numeric(df['min_flow'], errors='coerce')
    df = df.sort_values(by="date")
    #df.set_index('date', inplace=True)
    print(df)
    #fig = make_subplots(rows=1, cols=1, subplot_titles=df.index.unique(), specs=[[{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}], [{"secondary_y": True}]])
    #fig = make_subplots(rows=3, cols=2, specs=[[{"secondary_y": True}] * columns] * rows)
    fig = make_subplots(rows=3, cols=2, specs=[[{"secondary_y": True}] * 2] * 3)
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    fig.update_layout(plot_bgcolor='rgba(0,0,0,0)')
    #fig.update_xaxes(showline=True, linewidth=1, linecolor='black', mirror=True)
    #fig.update_layout(autosize=False,width=2000,height=3000)

    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["mean_flow"],
                    line=dict(color = color_map.get("mean_discharge", 'black'), width = 1),
                    name="mean_flow",showlegend=True,),row=1, col=1, secondary_y=False),
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["max_flow"],
                    line=dict(color = color_map.get('max_discharge', 'black'), width = 1),
                    name="max",showlegend=True,),row=1, col=1, secondary_y=False),
    #
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["min_flow"],
                    line=dict(color = color_map.get('min_discharge', 'black'), width = 1),
                    name="min",showlegend=True,),row=1, col=1, secondary_y=False),

    # long term week ie average flow for first week in august across dataset
    df["lt_week"] = df['date'].dt.strftime('%U')
    df["lt_week_avg"] = df.groupby(["lt_week"])["mean_flow"].transform("mean").round(2)
    # year week is average for for ie first week of month for the year august week 1,2,3,4 for year
    df["y_week"] = df['date'].dt.strftime('%Y-%U')
    df["y_week_avg"] = df.groupby(["y_week"])["mean_flow"].transform("mean").round(2)


    #
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["lt_week_avg"],
                    line=dict(color = color_map.get('mean_temperature', 'black'), width = 1),
                    name="min",showlegend=True,),row=2, col=1, secondary_y=False),
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["y_week_avg"],
                    line=dict(color = color_map.get('max_temperature', 'black'), width = 1),
                    name="min",showlegend=True,),row=2, col=1, secondary_y=False),

    # long term month ie average august value across dataset
    df["lt_month"] = df.date.dt.strftime('%m')
    df["lt_month_avg"] = df.groupby(["lt_month"])["mean_flow"].transform("mean").round(2)
    # year month ie monthly averages per year
    df["y_month"] = df.date.dt.strftime('%Y-%m')
    df["y_month_avg"] = df.groupby(["y_month"])["mean_flow"].transform("mean").round(2)


    #
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["lt_month_avg"],
                    line=dict(color = color_map.get('mean_temperature', 'black'), width = 1),
                    name="min",showlegend=True,),row=3, col=1, secondary_y=False),
    fig.add_trace(go.Scatter(
                    x=df['date'],
                    y=df["y_month_avg"],
                    line=dict(color = color_map.get('max_temperature', 'black'), width = 1),
                    name="min",showlegend=True,),row=3, col=1, secondary_y=False),

    # long term mont
    df_month = df.drop_duplicates(subset='lt_month')
    min_month = df_month.loc[df_month["lt_month_avg"] == df_month["lt_month_avg"].min(), "lt_month"].item()
    print(min_month)
    df_month['relative_wy'] = df_month['y_month'].apply(lambda x: pd.Period(x, "M") - (int(min_month) -1) )
    df_month['relative_wy'] = df_month['relative_wy'].dt.strftime('%m')
    df_month = df_month.sort_values(by = ['relative_wy'])
    #irst_month = df_month.loc[df_month['lt_month_avg'].min(), "lt_month"]
    #print(first_month)
    #df_month = df_month.sort_values(by="lt_month_avg")
    fig.add_trace(go.Scatter(
                    x=df_month['relative_wy'],
                    y=df_month["lt_month_avg"],
                    line=dict(color = color_map.get('mean_temperature', 'black'), width = 1),
                    name="min",showlegend=True,),row=3, col=2, secondary_y=False),
    print(df_month)
    fig.show()


def cache_comparison_interpolation(df, site, site_sql_id, parameter, start_date, end_date):



    #from import_data import sql_import_all_datetimes
    #df_historical = sql_import_all_datetimes(parameter, site_sql_id)
    #df = df.sort_values(by=["datetime"])
    
    ## calculate long term data
    #df_historical['long_term_mean'] = df_historical['corrected_data'].mean()
    #df_historical['long_term_max'] = df_historical['corrected_data'].max()
    #df_historical['long_term_min'] = df_historical['corrected_data'].min()
   
    ##calculate monthly data
    #df_historical["month"] = df_historical.datetime.dt.strftime('%m')
    #df_historical["month_mean"] = df_historical.groupby(["month"])["corrected_data"].transform("mean")
    #df_historical["month_max"] = df_historical.groupby(["month"])["corrected_data"].transform("max")
    #df_historical["month_min"] = df_historical.groupby(["month"])["corrected_data"].transform("min")

    #calculate monthly data
    #df_historical["month"] = df_historical.datetime.dt.strftime('%m')
    #df_historical["month_mean"] = df_historical.groupby(["month"])["corrected_data"].transform("mean")
    #df_historical["month_max"] = df_historical.groupby(["month"])["corrected_data"].transform("max")
    #df_historical["month_min"] = df_historical.groupby(["month"])["corrected_data"].transform("min")

        ### calculate weekly data
        ##calculate monthly data
        ##df_historical["week"] = df_historical.datetime.dt.strftime('%U')
        ##df_historical["week_mean"] = df_historical.groupby(["week"])["corrected_data"].transform("mean")
        ##df_historical["week_max"] = df_historical.groupby(["week"])["corrected_data"].transform("max")
        #df_historical["week_min"] = df_historical.groupby(["week"])["corrected_data"].transform("min")
    
    

    # calculate relative water year: while water year is oct-sep. we wnat to start with lowest longg term month somewhere between aug and oct
    #df_month = df_historical.drop_duplicates(subset='month')
    #min_month = df_month.loc[df_month["month_mean"] == df_month["month_mean"].min(), "month"].item()
    
    #df_historical['relative_month'] = df_historical['datetime'].apply(lambda x: pd.Period(x, "M") - (int(min_month) -1) )
    #df_historical['relative_month'] = df_historical['relative_month'].dt.strftime('%m')
    #df_historical = df_historical.sort_values(by = ['relative_month'])
   


    #df["month"] = df.datetime.dt.strftime('%m')
    # df relative wy
    #df['relative_month'] = df['datetime'].apply(lambda x: pd.Period(x, "M") - (int(min_month) -1) )
    #df['relative_month'] = df['relative_month'].dt.strftime('%m')
    #df = df.sort_values(by = ['relative_month'])


    #df_historical = df_historical.drop_duplicates(subset='month')   
    #df_historical = df_historical[["month", "relative_month", "month_mean", "month_min", "month_max"]]
   
    #df = df.merge(df_historical, left_on = ["month", "relative_month"], right_on = ["month",  "relative_month"], how = "outer")
   #comparison
    if "comparison" in df.columns:# if there is something to compare with
        df['data'] = pd.to_numeric(df['data'], errors='coerce')
        df['corrected_data'] = pd.to_numeric(df['corrected_data'], errors='coerce')
        df['comparison'] = pd.to_numeric(df['comparison'], errors='coerce')
        # create year
        #df["year"] = df.datetime.dt.strftime('%m')
        # create year column
        df["year"] = df.datetime.dt.strftime('%Y')
        # create month column
        df["month"] = df.datetime.dt.strftime('%m')
        # create week column
        df["week"] = df.datetime.dt.strftime('%U')

        # create day column
        df["day"] = df.datetime.dt.strftime('%j')
        
        # calculate month data
        df["month_mean"] = df.groupby(["month"])["corrected_data"].transform("mean")
        df["c_month_mean"] = df.groupby(["month"])["comparison"].transform("mean")

        # calculate daily data
        df["day_mean"] = df.groupby(["day"])["corrected_data"].transform("mean")
        df["c_day_mean"] = df.groupby(["day"])["comparison"].transform("mean")

        # calculate relative day wy
        # Create day column
        df["day"] = df.datetime.dt.strftime('%j')

        

        # Find minimum value in c_day_mean column
        #c_min_day = df.loc[df["c_day_mean"] == df["c_day_mean"].min(), "day"].item()

        # Calculate relative day
        # Create day column


        # Find minimum value in c_day_mean column
        c_min_month = df.loc[df["c_month_mean"] == df["c_month_mean"].min(), "month"].iloc[0]
        # Calculate relative day
        df['c_relative_month'] = df['datetime'].apply(lambda x: pd.Period(x, "M") - (int(c_min_month) - 1))
        df['c_relative_month'] = df['c_relative_month'].dt.strftime('%m')

        df['c_relative_water_year'] = (df['datetime'] + pd.DateOffset(months=12 - (int(c_min_month) - 1))).dt.strftime('%Y')
        df['water_year'] = (df['datetime'] + pd.DateOffset(months=3)).dt.strftime('%Y')
    #df = df.sort_values(by = ['relative_month'])

        # calculate difference
        df['difference'] = abs(df['corrected_data']-df['comparison'])
        df.loc[pd.isna(df['corrected_data']), "difference"] = np.nan
        df.loc[pd.isna(df['comparison']), "difference"] = np.nan


        # rolling daily mean
        ### 7 day rolling average parameter
        #df[f'daily_rolling_mean'] = df.groupby(["watershed", "water_year"])[f"{parameter}_daily_mean"].transform(lambda x: x.rolling(7,7, center = True, closed = 'both').mean()).round(2)

        # rolling sum
        #df['rolling_sum'] = df['companion'].rolling(window=3, min_periods=1).sum()
        #df = df.sort_values(by=['c_relative_water_year', 'c_relative_month', "datetime"], ascending=True)
        #df['rolling_sum'] = df.groupby(['c_relative_water_year'])['comparison'].apply(lambda x: x.cumsum())
        #df['mean_rolling_sum'] = df.groupby(['day'])['rolling_sum'].transform('mean')

        #df = df.sort_values(["watershed", "water_year", 'datetime'], ascending=True)

        # grouping by watershed doesnt do anything because zero values for watershed wont be filled
        #df[f'average_{parameter}'] = df.groupby(["datetime"])[parameter].transform("mean").round(2)
       
        # this works the best so far because anything at day resolution cuts off peaks 
        # sort by average parameter then month

        # this actually worked the best
        df.loc[df['data'].isnull() | (df['data'] == ''), 'data'] = df["comparison"]

        #df = df.sort_values([f"comparison", "month"], ascending=True)
        #df["corrected_data"] = df["corrected_data"].fillna(method = "bfill")
        


        
        #df = df.sort_values(["c_relative_month", "comparison"], ascending=True)
        #df = df.sort_values(["comparison", "c_relative_month", "datetime"], ascending=True)

        ### the most stringent filter
        ## first sort by comparison relative month this creates a trend for the data to follow
        ## then sort by compariosn values within that month
        #df.sort_values(by=['c_relative_month', 'comparison'], ascending=[True, True], inplace=True)
        #df["corrected_data"].interpolate( method='linear', inplace=True, axis=0, limit_direction='both', limit_area = "inside")

        # limit_area = "inside"
       
        ## this works pretty well but jups at the start of wy
        #df.sort_values(by=['c_relative_month', 'comparison'], ascending=[True, True], inplace=True)
        #df.sort_values(by=['c_relative_month', 'comparison', 'c_day_mean'], ascending=[True, True, True], inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_water_year', 'c_relative_month'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        #df.sort_values(by=['comparison', 'c_relative_month'], ascending=[True, True], inplace=True)
        
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        
        # meh
        #df.sort_values(by=['c_relative_month', 'comparison'], ascending=[True, True], inplace=True)
        #df.sort_values(by=['c_relative_month', 'comparison', 'c_day_mean'], ascending=[True, True, True], inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_month', 'c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))

        #df.sort_values(by=['comparison', 'c_relative_month'], ascending=[True, True], inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))

        #df.sort_values(by=['comparison'], ascending=[True, True], inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))

        # fill daily holes with rolling sum
        #df.sort_values(by=['datetime'], ascending=True, inplace=True)
        #df.sort_values(by=['mean_rolling_sum'], ascending=True, inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        #df.sort_values(by=['comparison', 'c_relative_month'], ascending=[True, True], inplace=True)
        #df.sort_values(by=['comparison', 'corrected_data'], ascending=[True, True], inplace=True)
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        #df['corrected_data'] = df['corrected_data'].interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        # try only sorting by relative wy now

        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.ffill().bfill())
        #df['corrected_data'] = df.groupby(['c_relative_month'])['corrected_data'].apply(lambda x: x.interpolate(method='linear', limit_direction='both', limit_area = "inside"))
        
        # df["corrected_data"] = df["corrected_data"].fillna(method = "bfill")
        #df['corrected_data'] = df.groupby(['c_relative_water_year'])['corrected_data'].apply(lambda x: x.ffill().bfill())
        
        #df.to_csv(r"C:/Users/ihiggins/OneDrive - King County/Documents/compare.csv")
        df = df.sort_values([f"datetime"], ascending=True)
        # drop year column
        df = df.drop(columns=['year'])
        # drop month column
        df = df.drop(columns=['month'])
        # drop week column
        df = df.drop(columns=['week'])
        # drop day column
        df = df.drop(columns=['day'])
        # drop month data
        df = df.drop(columns=['month_mean'])
        df = df.drop(columns=['c_month_mean'])
        # drop day data
        df = df.drop(columns=["day_mean"])
        df = df.drop(columns=["c_day_mean"])

        df = df.drop(columns=['c_relative_month'])
        # drop difference
        df = df.drop(columns=['difference'])


        df = df.drop(columns=['c_relative_water_year'])
        df = df.drop(columns=['water_year'])

        #df = df.drop(columns=['mean_rolling_sum'])
        #df = df.drop(columns=['rolling_sum'])
        
        #df["comparison"]
    #    df["difference"] = df["corrected_data"] - df["comparison"]

    #    df["c_month_mean"] = df.groupby(["month"])["comparison"].transform("mean")
    #    df["week"] = df.datetime.dt.strftime('%U')
    #    df["c_week_mean"] = df.groupby(["week"])["comparison"].transform("mean")
   
   
    '''
    if "comparison" in df.columns(): # if there is something to compare with
        # create constrain columns
        #df['min'] = df.groupby(["corrected_data"])[parameter].transform("min").round(2)   
        #df['max'] = df.groupby(["corrected_data"])[parameter].transform("max").round(2)  
    
        #if (parameter == "discharge") or (parameter == "water_temperature"):
           
            # sort by datetime
        df = df.sort_values(["datetime"], ascending=False)
            # create month helper column
            # since data is sorted on datetime, data should sort within the month, month provides a good balance in resolution
        #df["month"] = df.datetime.dt.strftime('%m')
        df = df.sort_values(['datetime'], ascending=True)

            # grouping by watershed doesnt do anything because zero values for watershed wont be filled
        df[f'average_{parameter}'] = df.groupby(["datetime"])[["corrected_data", "comparison"]].transform("mean").round(2)
        
            # this works the best so far because anything at day resolution cuts off peaks 
            # sort by average parameter then month
        df = df.sort_values([f"average_{parameter}"], ascending=True)
        
            # this works pretty well
        df["corrected_data"] = df["corrected_data"].fillna(method = "bfill")
        df["corrected_data"] = df["corrected_data"].fillna(method = "bfill")
        #df["corrected_data"] = df.groupby(["watershed"])[parameter].fillna(method = "bfill")
        df = df.drop(columns=["month"])
        df.reset_index(inplace=True)
            # turn less then zeros into zeros, only a few -.01 values that should really be zero
        if parameter == "water_temperature":
            df.loc[df[parameter] < 0, parameter]  = 0

        
        print("comparison interpolation complete3")
       
        print(df)
        # filter by constraints
        # set data greater then max to max value
        #df.loc[df[parameter] > df['max'], parameter] = df['max']
        # set data less then min value to value
        #df.loc[df[parameter] < df['min'], parameter] = df['min']
        # graph
        #average_value_graph(df, parameter, study)
        #df = df.drop([f'average_{parameter}'], axis=1)
        
        '''

    df = df.sort_values(by='datetime', ascending=True)
    #df.to_csv(r"C:/Users/ihiggins/OneDrive - King County/Documents/df_relative_wy.csv")
    #df = df.drop(columns=['month', 'relative_month', 'month_mean', 'month_min', 'month_max'])
    #if "comparison" in df.columns():
    #    df = df.drop(columns=['difference', 'c_month_mean', 'week', 'c_week_mean'])
    print(f"interpolate {parameter} complete")
    
    return(df)