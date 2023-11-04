from statistics import mean
from bs4 import BeautifulSoup as bs
import numpy as np
from datetime import datetime, timedelta, date
import requests
from entsoe import EntsoePandasClient
import pandas as pd
from  pytz import timezone

from config import Config, load_config

def get_temp_and_cloudiness(dt_start, dt_end, area="espoo"):
    default_temps = {1: -5.0, 2: -6.0, 3: -2.0, 4: 4.0, 5: 10.0, 6: 15.0, 7: 18.0, 8: 16.0, 9: 11.0, 10: 6.0, 11: 1.0, 12: -3.0}
    fmt = '%Y-%m-%dT%H:%M:%SZ'
    day_utc_hours = list(range(6,16))
    url = Config().config["fmi"]["url2"] + area
    res = requests.get(url)
    if res.status_code == 200:
        data = bs(res.content, "lxml-xml")#works eventhough data is xml
        #Temperature
        tempdata = data.find('wml2:MeasurementTimeseries', attrs={'gml:id': 'mts-1-1-temperature'})
        times = [datetime.strptime(i.text, fmt) for i in tempdata.find_all("wml2:time")]
        temps = [None if i.text == "NaN" else float(i.text) for i in tempdata.find_all("wml2:value")]
        temp_data = [t for dt, t in zip(times, temps) if dt_start <= dt < dt_end]
        mean_temp = round(np.mean(temp_data), 1)
        #Cloudiness
        clouddata = data.find('wml2:MeasurementTimeseries', attrs={'gml:id': 'mts-1-1-totalcloudcover'})
        times = [datetime.strptime(i.text, fmt) for i in clouddata.find_all("wml2:time")]
        clouds = [None if i.text == "NaN" else float(i.text) for i in clouddata.find_all("wml2:value")]
        cloud_data = [c for dt, c in zip(times, clouds) if (dt_start <= dt < dt_end) and (dt.hour in day_utc_hours)]
        mean_cloud = round(np.mean(cloud_data), 1)
    else:
        print("FMI area was not found {}".format(area))
        return default_temps[dt_start.month], 0
    return mean_temp, mean_cloud

def dayahead_entso_spot_prices(tomorrow: datetime, resolution='60T') -> list:
    start = pd.Timestamp(str(tomorrow), tz='Europe/Stockholm')
    end = pd.Timestamp(str(tomorrow+timedelta(days=1)), tz='Europe/Stockholm')
    client = EntsoePandasClient(api_key=Config().config["entsoe"]["api-key"])#Rajapinta tarvitsee entso-e api-key mutta sen saa heidän Transparency platformilta ilmaiseksi
    prices_df = client.query_day_ahead_prices('FI', start=start, end=end, resolution=resolution)
    prices_df.index = prices_df.index.tz_convert('UTC').tz_localize(None)
    spot_values = list(zip(prices_df.index, prices_df))
    return spot_values

def days_heating_need(temp, cloudiness, month):
    day_heating_kwh_at_minus_10 = 30
    no_heating_temp = 10
    sunny_factor = 0.2 #How much heating less if clear sky
    sunny_factor_start_month = 4
    sunny_factor_end_month = 11
    heating_kwh_need = interpolate_kwh(kwh1=day_heating_kwh_at_minus_10, temp2=no_heating_temp, temp=temp)
    if sunny_factor_start_month <= month < sunny_factor_end_month:
        heating_kwh_need -= heating_kwh_need * (sunny_factor * (100-cloudiness)/100)
    return heating_kwh_need

def interpolate_kwh(kwh1, temp2, temp):
    temp1 = -10
    kwh2 = 0
    m = (kwh2 - kwh1) / (temp2 - temp1)
    b = kwh1 - m * temp1
    heating_kwh_need = max(0, m * temp + b)
    return heating_kwh_need

def days_heating_need(temp, cloudiness, month):
    day_heating_kwh_at_minus_10 = 30
    no_heating_temp = 10
    sunny_factor = 0.2 #How much heating less if clear sky
    sunny_factor_start_month = 4
    sunny_factor_end_month = 11
    heating_kwh_need = interpolate_kwh(kwh1=day_heating_kwh_at_minus_10, temp2=no_heating_temp, temp=temp)
    if sunny_factor_start_month <= month < sunny_factor_end_month:
        heating_kwh_need -= heating_kwh_need * (sunny_factor * (100-cloudiness)/100)
    return heating_kwh_need

def adjust_temp_by_price(heating_kwh_need, prices):
    price_level = prices['price'].nsmallest(8).mean()
    reduce_at_200 = 0.2 #20% reduction at 200 eur/MWh
    do_not_reduce_price = 100
    incerease_heat_at = 5 #+20% at 5 eur/MWh
    if price_level > 200:
        reduce = reduce_at_200
    elif price_level > do_not_reduce_price:
        reduce = reduce_at_200 * (price_level - do_not_reduce_price) / (200 - do_not_reduce_price)
    elif price_level < incerease_heat_at:
        reduce = -0.2
    else:
        reduce = 0
    return heating_kwh_need * (1 - reduce)


def allocate_heating(heating_kwh_need, prices, month, site="taavis"):
    #Arvioi tähän paljonko lämpöpatterit kuluttavat sähkö eri kuukausia. Mitä kylmempi kuukausi, sitä enempi
    radiator_power_by_month = {1:1, 2:1, 3:1, 4:1.0, 5:0.5, 6:0.0, 7:0.0, 8:0.0, 9:0.0, 10:0.5, 11:1, 12:1} #month:kw
    heating_kwh_need = adjust_temp_by_price(heating_kwh_need, prices)
    price_cap = 500 #Kun on tätä kalliimpaa, ei lämmitetä
    heat_alway = 15 #Tätä halvemmalla lämmitetään aina
    #Arvioi tähän paljonko lattialämmityksen teho
    floor_power = 1 #kW
    prices_sorted = prices.sort_values(by="price")
    schedule = []
    now_utc = datetime.now(timezone('UTC'))
    for dt, price in prices_sorted.iterrows():
        if ((heating_kwh_need > 0) & (price["price"] < price_cap)) or (price["price"] <= heat_alway):
            floor = True
            radiator = True
            heating_kwh_need -= (floor_power + radiator_power_by_month[month])
        else:
            floor = False
            radiator = False
        schedule.append((site, dt, floor, radiator, now_utc))
    return schedule, radiator_power_by_month[month]

def total_prices(prices):
    day_hours = list(range(7,23))
    prices.loc[prices.index.hour.isin(day_hours), 'price'] += 3
    return prices

def get_tomorrow_utc_dts(tz_at="EET"):
    now_utc = datetime.now(timezone(tz_at))
    tomorrow_eet = (now_utc + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_utc_start = tomorrow_eet.astimezone(timezone('UTC'))
    tomorrow_utc_end = tomorrow_utc_start + timedelta(days=1)
    return tomorrow_utc_start.replace(tzinfo=None), tomorrow_utc_end.replace(tzinfo=None)

#Tämä ajetaan klo joka päivä klo 17 kun pörssisähkön hinta on tullut
def schedule_heating_tomorrow():
    try:
        dt_start, dt_end = get_tomorrow_utc_dts("EET")
        dt_start_utc, _ = get_tomorrow_utc_dts("UTC")
        temp, cloudiness = get_temp_and_cloudiness(dt_start, dt_end)
        heating_kwh_need = days_heating_need(temp, cloudiness, dt_start.month)
        prices = dayahead_entso_spot_prices(dt_start_utc)
        prices = pd.DataFrame(prices, columns=["dt", "price"]).set_index("dt")
        last_index = prices.index[-1]
        last_value = prices.iloc[-1].values[0]
        prices.loc[last_index + pd.Timedelta(minutes=45)] = last_value
        prices = total_prices(prices)
        schedule, radiator_heat = allocate_heating(heating_kwh_need, prices, dt_start.month)
        #Tallenna tässä ohjausaikataulu tietokantaan tai tiedostoon serverille
        insert_sql(upsert_schedule_sql(), schedule)
        insert_sql(upsert_schedule_info(), [("taavis", dt_start.date(), heating_kwh_need, temp, cloudiness, radiator_heat, datetime.now())])
    except Exception as e:
        print(f"Error scheduling heating: {e}")

#Tämä koodi täytyy ajaa kerran tunnissa
def read_schedule_and_set_heating(dt):
    #See the status now and two previous ones. If different then send command
    url = Config().config["webhook_url"]
    fmt = '%Y-%m-%d %H:%M:%S'
    dt_list = [dt - timedelta(hours=1),
               dt - timedelta(minutes=45),
               dt - timedelta(minutes=30),
               dt - timedelta(minutes=15),
               dt]
    dt_str_list = tuple(dt.strftime(fmt) for dt in dt_list)
    sched = select_sql(select_schedules(dt_str_list)) #Hae tässä tietokannasta tai tiedostosta lämmitysaikataulu [("koti", datetime(2023,1,1,1), True, True), ("koti", datetime(2023,1,1,2), False, False)]
    sched = pd.DataFrame(sched, columns=["site", "dt", "floor_heating", "radiator_heating"])
    if len(sched[['floor_heating', 'radiator_heating']].drop_duplicates())>1:
        try:
            #Tässä tehdään Webhook Rest kutsu, payload on sellainen kun miten määrittelit sen IFTT:ssä
            floor_val = sched["floor_heating"].iloc[0]
            radiator_val = sched["radiator_heating"].iloc[0]
            floor_val_str = "on" if floor_val else "off"
            radiator_val_str = "high" if radiator_val else "low"
            resp1 = requests.post(url=url.format("floor_heating_"+floor_val_str))
            resp2 = requests.post(url=url.format("radiator_"+radiator_val_str))
            print(f"Success with: {resp1.status_code} and {resp2.status_code} - {floor_val_str} and {radiator_val_str}")
        except Exception as e:
            print("Error sending command to webhook: {}".format(e))
    else:
        print("No heating change needed")
