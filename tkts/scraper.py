import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pytz import timezone
import pprint

def get_tkts_html():
    url = "https://www.tdf.org/discount-ticket-programs/tkts-by-tdf/tkts-live/?tab=TimesSquare"
    response = requests.get(url)
    response.raise_for_status()

    return response.text

def process_div(div, div_location, soup):

    div_id = div_location + div["Div"]

    data = []

    if not soup.find("div", id=div_id):
        print(f"No data found for {div_id}")
        return data

    table = soup.find("div", id=div_id).find("table")
    rows = table.find_all("tr")[1 if div["header"] else 0:]  # Skip header row

    for row in rows:
        cells = row.find_all("td")
        if len(cells) == 4:  # Ensure we have the right number of columns
            time = cells[0].get_text(strip=True)
            is_matinee = True
            if "PM" in time:
                hour = int(time.split(":")[0])
                if hour >= 4:
                    is_matinee = False

            # Convert time like "4:00 PM" or "11:00 AM" to "HH:MM:SS" in 24-hour format
            performance_time = datetime.strptime(time, "%I:%M %p").strftime("%H:%M:%S")

            discount = cells[1].get_text(strip=True).replace('%', '')
            price = cells[2].get_text(strip=True)

            # Handle price ranges with different dash formats
            price = price.replace('--', '-').replace('---', '-')
            if '-' in price:
                low_price, high_price = price.replace('$', '').split('-')
                low_price = low_price.strip()
                high_price = high_price.strip()
            else:
                low_price = high_price = price.replace('$', '').strip()
                
            if not low_price:
                low_price = None
            if not high_price:
                high_price = None

            title = cells[3].get_text(strip=True).replace('"', '')
            data.append({
                "title": title,
                "discount_percent": discount,
                "low_price": low_price,
                "high_price": high_price,
                "performance_time": performance_time,
                "is_matinee": is_matinee,
                "performance_date": div["Date"],
                "on_broadway": div["onBroadway"]
            })

    return data

def location_is_closed(location_name, html_content):
    return f"The {location_name} booth is currently <span class=\"underlined\">closed</span>" in html_content

def get_tkts_data():
    html_content = get_tkts_html()

    soup = BeautifulSoup(html_content, "html.parser")

    current_date = datetime.now(timezone('US/Eastern'))

    divs = [
        {"Div": "-broadway-shows", "Date": current_date.strftime("%Y-%m-%d"), "onBroadway": True, "header": True},
        {"Div": "-off-broadway-shows", "Date": current_date.strftime("%Y-%m-%d"), "onBroadway": False, "header": True},
        {"Div": "-next-day-matinee-broadway-shows", "Date": (current_date + timedelta(days=1)).strftime("%Y-%m-%d"), "onBroadway": True, "header": False},
        {"Div": "-next-day-matinee-off-broadway-shows", "Date": (current_date + timedelta(days=1)).strftime("%Y-%m-%d"), "onBroadway": False, "header": False}
    ]

    tkts_data = []

    for location in [{"div": "TimesSquare", "name": "Times Square"}, {"div": "LincolnCenter", "name": "Lincoln Center"}]:
        if location_is_closed(location['name'], html_content):
            print(f"{location['name']} booth is closed.")
            continue

        for div in divs:
            tkts_data += process_div(div, location["div"], soup)
    
    return tkts_data

if __name__ == "__main__":
    data = get_tkts_data()
    pprint.pprint(data)