import streamlit as st
import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from days360 import days360
import numpy_financial as npf
import time
from io import StringIO
import threading
import matplotlib.pyplot as plt
import zstandard as zstd
import brotli
pd.set_option('display.show_dimensions', False)

url = "https://www.nseindia.com/api/liveBonds-traded-on-cm?type=gsec"
base_url = "https://www.nseindia.com"
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "referer": "https://www.nseindia.com/market-data/bonds-traded-in-capital-market"
}

curr_filter = ['563GS2026','574GS2026','585GS2030','610GS2031','622GS2035','645GS2029','664GS2035','667GS2050','68GS2060','699GS2051','702GS2031','704GS2029','709GS2054','716GS2050','717GS2030','718GJ28','718GS2037','719GS2060','723GS2039','725GJ26','725GS2063','726KA25','732GS2030','733GS2026','734GS2064','736GS2052','737GS2028','739ML26','741GS2036','743GJ31','746GS2073','749GJ28','74GJ27','754GS2036','754KA41','755GJ31','758GJ26','759TS37','763GS2059','763TS43','767AP38','769GS2043','769HR27','773MH32','773UP34','774AP32A','774GA32','774MP43','774RJ33A','77AP32','77MH33A','783RJ50','789WB40','794TN32']

# Function to get new cookies
def get_new_cookies(session):
    response = session.get(base_url, headers=headers)
    if response.status_code == 200:
        cookies = response.cookies.get_dict()
        return cookies
    else:
        raise Exception("Failed to get new cookies")

# Function to update headers with new cookies
def update_headers_with_cookies(session):
    cookies = get_new_cookies(session)
    session.cookies.update(cookies)

# Initial session setup
session = requests.Session()
update_headers_with_cookies(session)

masterdebtresponse = session.get("https://nsearchives.nseindia.com/content/equities/DEBT.csv", headers=headers)
csv_data = StringIO(masterdebtresponse.text)
master_debt = pd.read_csv(csv_data, index_col=False)
debt_columns = ["SYMBOL", " IP RATE", " REDEMPTION DATE"]
master_debt = master_debt[debt_columns]
master_debt.rename(columns = {'SYMBOL':'Symbol'}, inplace = True)
master_debt = pd.DataFrame(master_debt)
master_debt.Symbol = master_debt.Symbol.astype(str)
master_debt.fillna({' IP RATE':0}, inplace=True)

today = datetime.today()
weekday = today.weekday()

if weekday == 4: 
    settlement_date = today + timedelta(days=3)
elif weekday == 5: 
    settlement_date = today + timedelta(days=2)
else:
    settlement_date = today + timedelta(days=1)
    
master_debt['settlement date'] = settlement_date.strftime('%Y-%m-%d')
master_debt= master_debt.dropna().reset_index(drop=True)
master_debt[" REDEMPTION DATE"] = master_debt[" REDEMPTION DATE"].astype(str)
master_debt= master_debt.dropna().reset_index(drop=True)

def calculate_last_coupon_date(row):
    redemption_date = datetime.strptime(row[" REDEMPTION DATE"], "%d-%b-%Y")
    settlement_date = datetime.strptime(row["settlement date"], "%Y-%m-%d") 
    last_coupon_date = redemption_date
    while last_coupon_date > settlement_date:
        last_coupon_date -= relativedelta(months=6)
    return last_coupon_date.strftime("%Y-%m-%d")

def calculate_next_coupon_date(row):
    redemption_date = datetime.strptime(row[" REDEMPTION DATE"], "%d-%b-%Y")
    settlement_date = datetime.strptime(row["settlement date"], "%Y-%m-%d") 
    last_coupon_date = redemption_date
    while last_coupon_date > settlement_date:
        last_coupon_date -= relativedelta(months=6)
    next_coupon_date = last_coupon_date + relativedelta(months=6)
    return next_coupon_date.strftime("%d-%b-%Y")

def calculate_days_between(row):
    settlement_date = datetime.strptime(row["settlement date"], "%Y-%m-%d")
    last_coupon_date = datetime.strptime(row["last coupon date"], "%Y-%m-%d")
    days_between = days360(last_coupon_date, settlement_date, method="US")
    return days_between

def calculate_nper(row):
    settlement_date = datetime.strptime(row["settlement date"], "%Y-%m-%d")
    maturity_date = datetime.strptime(row[" REDEMPTION DATE"], "%d-%b-%Y")
    nper = (maturity_date - settlement_date).days/365
    return nper

def calculate_days_maturity(row):
    settlement_date = datetime.strptime(row["settlement date"], "%Y-%m-%d")
    maturity_date = datetime.strptime(row[" REDEMPTION DATE"], "%d-%b-%Y")
    days_maturity = (maturity_date - settlement_date).days
    return days_maturity

master_debt["last coupon date"] = master_debt.apply(calculate_last_coupon_date, axis=1)
master_debt["next coupon date"] = master_debt.apply(calculate_next_coupon_date, axis=1)
master_debt["Days Between"] = master_debt.apply(calculate_days_between, axis=1)
master_debt['Accrued Interest'] = (master_debt[' IP RATE'] / 360) * master_debt['Days Between']
master_debt["nper"] = master_debt.apply(calculate_nper, axis=1)
master_debt["days to maturity"] = master_debt.apply(calculate_days_maturity, axis=1)
master_debt.loc[master_debt[' IP RATE'] == 0, 'next coupon date'] = "DNE"


def fetch_update():
    while True:
        update_headers_with_cookies(session)
        response = session.get(url, headers=headers)
        print("request sent", response.status_code)
        if response.status_code == 200:
            try:
                decompressed_content = brotli.decompress(response.content)
                response_text = decompressed_content.decode('utf-8')  # Decode as UTF-8
                    
                # Process data (your processing code goes here)
                
            except brotli.error as e:
                print(f"Brotli decompression error: {e}")
                response_text = response.content

            data = json.loads(response_text)
            response.close()
            data_list = data['data']
            extracted_data = []
            for entry in data_list:
                for i in range(1, 6):
                    extracted_data.append([
                        {
                            'Symbol': entry['symbol'],
                            'Series': entry['series'],
                            'ISIN': entry['isinCode'],
                            'bidprice': entry[f'buyPrice{i}'],
                            'bidquantity': entry[f'buyQuantity{i}'],
                            'askprice': entry[f'sellPrice{i}'],
                            'askquantity': entry[f'sellQuantity{i}'],
                            'Volume': entry['totalTradedVolume'],
                            'VWATP': entry['averagePrice']
                            
                        } 
                    ])
            flattened_data = []
            for entry_list in extracted_data:
                for entry_dict in entry_list:
                    flattened_data.append(entry_dict)
            bid_ask = pd.DataFrame(flattened_data)
            
            
        else:
            print(response.status_code)
             
        bid_ask.Symbol = bid_ask.Symbol.astype(str)
        bid_ask = bid_ask.merge(master_debt[['Symbol', ' IP RATE', ' REDEMPTION DATE', "settlement date","next coupon date", "last coupon date", "Days Between", "Accrued Interest", "nper", "days to maturity"]], how= "left", on="Symbol")
        bid_ask["clean_bid_price"] = bid_ask["bidprice"] - bid_ask["Accrued Interest"]
        bid_ask["clean_ask_price"] = bid_ask["askprice"] - bid_ask["Accrued Interest"]
        bid_ask["clean_avg_price"] = bid_ask["VWATP"] - bid_ask["Accrued Interest"]
        bid_ask.fillna({"ISIN": "TEST"}, inplace=True)
        bid_ask= bid_ask.dropna().reset_index(drop=True)
        
        def calculate_askyield(row):
            if row[" IP RATE"] != 0 and row["askprice"] != 0:
                askyield = (npf.rate(row["nper"]*2, row[" IP RATE"]/2, -row["clean_ask_price"], 100))*2
            elif row[" IP RATE"] == 0 and row["clean_ask_price"] != 0:
                askyield = (100 - row["clean_ask_price"])/row["clean_ask_price"]*(365/row["days to maturity"])
            else:
                askyield = 0
            return askyield

        def calculate_bidyield(row):
            if row[" IP RATE"] != 0 and row["bidprice"] != 0:
                bidyield = (npf.rate(row["nper"]*2, row[" IP RATE"]/2, -row["clean_bid_price"], 100))*2
            elif row[" IP RATE"] == 0 and row["clean_bid_price"] != 0:
                bidyield = (100 - row["clean_bid_price"])/row["clean_bid_price"]*(365/row["days to maturity"])
            else:
                bidyield = 0
            return bidyield
        def calculate_avgyield(row):
            if row[" IP RATE"] != 0 and row["VWATP"] != 0:
                avgyield = (npf.rate(row["nper"]*2, row[" IP RATE"]/2, -row["clean_avg_price"], 100))*2
            elif row[" IP RATE"] == 0 and row["clean_avg_price"] != 0:
                avgyield = (100 - row["clean_avg_price"])/row["clean_avg_price"]*(365/row["days to maturity"])
            else:
                avgyield = 0
            return avgyield

        bid_ask["askyield"] = bid_ask.apply(calculate_askyield, axis = 1)
        bid_ask["bidyield"] = bid_ask.apply(calculate_bidyield, axis = 1)
        bid_ask["avgyield"] = bid_ask.apply(calculate_avgyield, axis = 1)
        yield_columns = ["next coupon date","Symbol", "bidquantity", "bidyield", "bidprice", "askprice","askyield", "askquantity", "Volume", "VWATP","avgyield", "Series", "nper"]
        final_yield = bid_ask[yield_columns]
        final_yield.loc[:, "bidyield"] = (final_yield["bidyield"] * 100)#.apply(lambda x: f"{x:.2f}%")
        final_yield.loc[:, "askyield"] = (final_yield["askyield"] * 100)#.apply(lambda x: f"{x:.2f}%")
        final_yield.loc[:, "avgyield"] = (final_yield["avgyield"] * 100)#.apply(lambda x: f"{x:.2f}%")
        #final_yield = final_yield[(final_yield["bidquantity"] != 0) | (final_yield["askquantity"] != 0)]
        return final_yield

latest_data = pd.DataFrame()
# Data update function
def update_data():
    global latest_data
    while True:
        latest_data = fetch_update()
        time.sleep(5)  # Update every 5 seconds

def format_percentage(val):
    return f"{val:.2f}%"
def format_round(val):
    return f"{val:.2f}"
def highlight_columns(x):
    color = 'background-color: {}'
    df_styler = pd.DataFrame('', index=x.index, columns=x.columns)
    df_styler['askprice'] = color.format('red')
    df_styler['askyield'] = color.format('red')
    df_styler['askquantity'] = color.format('red')
    df_styler['bidprice'] = color.format('green')
    df_styler['bidyield'] = color.format('green')
    df_styler['bidquantity'] = color.format('green')
    return df_styler
def plot_data(data):
    # Filter data for GS and SG
    data_gs = data[(data['Series'] == 'GS') & (data["next coupon date"] != "DNE") & (data["Volume"] != 0)]
    data_gs = data_gs.groupby('Symbol').first().reset_index()
    data_sg = data[(data['Series'] == 'SG') & (data["next coupon date"] != "DNE") & (data["Volume"] != 0)]
    data_sg = data_sg.groupby('Symbol').first().reset_index()

    # Filter yield data for the range 6-9.5 for GS
    filtered_gs = data[(data['Series'] == 'GS') & (data["next coupon date"] != "DNE")]
    
    filtered_sg = data[(data['Series'] == 'SG') & (data["next coupon date"] != "DNE")]
                          
    # Determine the number of subplots required 
    num_plots = 0
    if not data_gs.empty:
        num_plots += 2
    if not data_sg.empty:
        num_plots += 2
    if not filtered_gs.empty:
        num_plots += 1
    if not filtered_sg.empty:
        num_plots += 1
        
    fig, axs = plt.subplots(nrows=(num_plots + 1) // 2, ncols=2, figsize=(16, num_plots * 5))
    
    plot_index = 0
    if not data_gs.empty:
        # Scatter plot for filtered yield data for GS
        filtered_data_gs = data_gs.copy()
        filtered_data_gs = filtered_data_gs[(filtered_data_gs['avgyield'] >= 6) & (filtered_data_gs['avgyield'] <= 10)]
        row, col = divmod(plot_index, 2)
        axs[row, col].scatter(filtered_data_gs['nper'], filtered_data_gs['avgyield'], label='Avg Yield', marker='x', color='red')
        axs[row, col].set_title('Yield Curve for GS (Traded)', fontsize=14)
        axs[row, col].set_xlabel('Years to Maturity', fontsize=12)
        axs[row, col].set_ylabel('Yield', fontsize=12)
        axs[row, col].legend()
        axs[row, col].grid(True)
        plot_index += 1
    
    if not data_sg.empty:
        filtered_data_sg = data_sg.copy()
        filtered_data_sg = filtered_data_sg[(filtered_data_sg['avgyield'] >= 6) & (filtered_data_sg['avgyield'] <= 10)]
        # Scatter plot for filtered yield data for GS
        row, col = divmod(plot_index, 2)
        axs[row, col].scatter(filtered_data_sg['nper'], filtered_data_sg['avgyield'], label='Avg Yield', marker='o', color='blue')
        axs[row, col].set_title('Yield Curve for SG (Traded)', fontsize=14)
        axs[row, col].set_xlabel('Years to Maturity', fontsize=12)
        axs[row, col].set_ylabel('Yield', fontsize=12)
        axs[row, col].legend()
        axs[row, col].grid(True)
        plot_index += 1
    
    if not data_gs.empty:
        # Sort GS data by Volume in descending order
        sorted_gs = data_gs.sort_values(by='Volume', ascending=False)
    
        # Bar chart with volume numbers on top for GS
        row, col = divmod(plot_index, 2)
        bars = axs[row, col].bar(sorted_gs['Symbol'], sorted_gs['Volume'], color='blue', tick_label=sorted_gs['Symbol'])
        axs[row, col].set_title('Volume Traded for GS', fontsize=14)
        axs[row, col].set_xlabel('Symbol', fontsize=12)
        axs[row, col].set_ylabel('Volume', fontsize=12)
        axs[row, col].set_xticks(range(len(sorted_gs['Symbol'])))
        axs[row, col].set_xticklabels(sorted_gs['Symbol'], rotation=90, fontsize=10)
    
        for bar in bars:
            yval = bar.get_height()
            axs[row, col].text(bar.get_x() + bar.get_width() / 2, yval + 0.02 * yval, int(yval), ha='center', va='bottom', fontsize=10, color='red', rotation='vertical')
        plot_index += 1
        
    if not data_sg.empty:
        # Sort SG data by Volume in descending order
        sorted_sg = data_sg.sort_values(by='Volume', ascending=False)
    
        # Bar chart with volume numbers on top for SG
        row, col = divmod(plot_index, 2)
        bars = axs[row, col].bar(sorted_sg['Symbol'], sorted_sg['Volume'], color='green', tick_label=sorted_sg['Symbol'])
        axs[row, col].set_title('Volume Traded for SG', fontsize=14)
        axs[row, col].set_xlabel('Symbol', fontsize=12)
        axs[row, col].set_ylabel('Volume', fontsize=12)
        axs[row, col].set_xticks(range(len(sorted_sg['Symbol'])))
        axs[row, col].set_xticklabels(sorted_sg['Symbol'], rotation=90, fontsize=10)
    
        for bar in bars:
            yval = bar.get_height()
            axs[row, col].text(bar.get_x() + bar.get_width() / 2, yval + 0.02 * yval, int(yval), ha='center', va='bottom', fontsize=10, color='green', rotation='vertical')
        plot_index += 1
        
    if not filtered_gs.empty:
        # Scatter plot for filtered yield data for GS
        row, col = divmod(plot_index, 2)
        bid_gs = filtered_gs.copy()
        bid_gs = bid_gs[(bid_gs['bidyield'] >= 6) & (bid_gs['bidyield'] <= 11)]
        axs[row, col].scatter(bid_gs['nper'], bid_gs['bidyield'], label='Bid Yield', marker='o', color='blue')
        ask_gs = filtered_gs.copy()
        ask_gs = ask_gs[(ask_gs['askyield'] >= 6) & (ask_gs['askyield'] <= 11)]
        axs[row, col].scatter(ask_gs['nper'], ask_gs['askyield'], label='Ask Yield', marker='x', color='red')
        axs[row, col].set_title('Yield Curve for GS (Order Book)', fontsize=14)
        axs[row, col].set_xlabel('Years to Maturity', fontsize=12)
        axs[row, col].set_ylabel('Yield', fontsize=12)
        axs[row, col].legend()
        axs[row, col].grid(True)
        plot_index += 1
    
    if not filtered_sg.empty:
        # Scatter plot for filtered yield data for SG
        row, col = divmod(plot_index, 2)
        bid_sg = filtered_sg.copy()
        bid_sg = bid_sg[(bid_sg['bidyield'] >= 6) & (bid_sg['bidyield'] <= 11)]
        axs[row, col].scatter(bid_sg['nper'], bid_sg['bidyield'], label='Bid Yield', marker='o', color='blue')
        ask_sg = filtered_sg.copy()
        ask_sg = ask_sg[(ask_sg['askyield'] >= 6) & (ask_sg['askyield'] <= 11)]
        axs[row, col].scatter(ask_sg['nper'], ask_sg['askyield'], label='Ask Yield', marker='x', color='red')
        axs[row, col].set_title('Yield Curve for SG (Order Book)', fontsize=14)
        axs[row, col].set_xlabel('Years to Maturity', fontsize=12)
        axs[row, col].set_ylabel('Yield', fontsize=12)
        axs[row, col].legend()
        axs[row, col].grid(True)
        plot_index += 1
    
    # Remove any unused subplots
    for ax in axs.flat:
        if not ax.has_data():
            fig.delaxes(ax)
    
    plt.tight_layout()
    return fig, axs
  
def main():
    st.title("Composite Edge Debt View")
    # Page selection in the sidebar
    page = st.sidebar.radio("Select Page", ["GS", "SG", "TB", "Selling", "Market Statistics"])
    
    # Data placeholders
    data_placeholder = st.empty()
    if 'thread' not in globals():
        global thread
        thread = threading.Thread(target=update_data)
        thread.start()
  
    # Display the latest fetched data
    while True:
        if not latest_data.empty:
            latest_data['bidprice'] = latest_data['bidprice'].round(2)
            latest_data['askprice'] = latest_data['askprice'].round(2)
            latest_data['VWATP'] = latest_data['VWATP'].round(2)
            if page == "GS":
                filter_df = latest_data.copy()
                filter_df = filter_df[(filter_df["bidquantity"] != 0) | (filter_df["askquantity"] != 0)]
                filter_df.drop(columns=['nper'], inplace=True)
                filter_df = filter_df[filter_df["Series"].str.contains("GS")]
                filter_df = filter_df[filter_df["next coupon date"] != "DNE"]
                #filter_df = filter_df.set_index("Symbol")
                filter_df = filter_df.style.apply(highlight_columns, axis=None).format({'bidyield': format_percentage, 'askyield': format_percentage, 'avgyield': format_percentage, 'bidprice': format_round, 'askprice': format_round, 'VWATP': format_round})
                data_placeholder.dataframe(filter_df, width=2000)
            elif page == "SG":
                filter_sg = latest_data.copy()
                filter_sg = filter_sg[(filter_sg["bidquantity"] != 0) | (filter_sg["askquantity"] != 0)]
                filter_sg.drop(columns=['nper'], inplace=True)
                filter_sg = filter_sg[filter_sg["Series"].str.contains("SG")]
                #filter_sg = filter_sg.set_index("Symbol")
                filter_sg = filter_sg.style.apply(highlight_columns, axis=None).format({'bidyield': format_percentage, 'askyield': format_percentage, 'avgyield': format_percentage, 'bidprice': format_round, 'askprice': format_round, 'VWATP': format_round})
                data_placeholder.dataframe(filter_sg, width=2000)
            elif page == "TB":
                filter_tb = latest_data.copy()
                filter_tb = filter_tb[(filter_tb["bidquantity"] != 0) | (filter_tb["askquantity"] != 0)]
                filter_tb.drop(columns=['nper'], inplace=True)
                filter_tb.drop(columns=['next coupon date'], inplace=True)
                filter_tb = filter_tb[filter_tb["Series"].str.contains("TB")]
                #filter_tb = filter_tb.set_index("Symbol")
                filter_tb = filter_tb.style.apply(highlight_columns, axis=None).format({'bidyield': format_percentage, 'askyield': format_percentage, 'avgyield': format_percentage, 'bidprice': format_round, 'askprice': format_round, 'VWATP': format_round})
                data_placeholder.dataframe(filter_tb, width=2000)
            elif page == "Selling":
                filter_sell = latest_data.copy()
                filter_sell = filter_sell[(filter_sell["bidquantity"] != 0)]
                filter_sell.drop(columns=['nper'], inplace=True)
                filter_sell = filter_sell[filter_sell["Symbol"].isin(curr_filter)]
                filter_sell = filter_sell.style.apply(highlight_columns, axis=None).format({'bidyield': format_percentage, 'askyield': format_percentage, 'avgyield': format_percentage, 'bidprice': format_round, 'askprice': format_round, 'VWATP': format_round})
                data_placeholder.dataframe(filter_sell, width=2000)
                
            elif page == "Market Statistics":
                fig, axs = plot_data(latest_data)
                with data_placeholder:
                    st.pyplot(fig)  # Use st.pyplot within the placeholder       
        else:
            data_placeholder.warning("Waiting for data...")
        # Add a delay before updating again
        time.sleep(1)
    
if __name__ == "__main__":
    main()
