from supabase import create_client
import requests
import re
from datetime import datetime
from pprint import pprint
import pytz
import smtplib
from email.message import EmailMessage
from datetime import datetime
import os, sys


# Add parent directory to path to import keys
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)
from keys.keys import SUPABASE_KEY, SUPABASE_URL, EMAIL, EMAIL_PASSWORD

URLS = {
    "broadway": "https://www.tdf.org/on-stage/show-finder/?page=1&pageSize=100&tdfMembership=true&venueId=1",
    "off_broadway": "https://www.tdf.org/on-stage/show-finder/?page=1&pageSize=100&tdfMembership=true&venueId=2",
    "off_off_broadway": "https://www.tdf.org/on-stage/show-finder/?page=1&pageSize=100&tdfMembership=true&venueId=3"
}

VENUES = URLS.keys()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# use requests to find current TDF offers
def get_current_tdf_offers():
    
    current_tdf_offers = {}

    for venue in VENUES:

        response = requests.get(URLS[venue])
        html_content = response.text.replace('&#x27;', "'").replace('&amp;', "&")

        # Use regex to find all alt attributes in img tags with class "to-be-scaled img-el"
        show_titles = re.findall(r'<img[^>]*class="to-be-scaled img-el"[^>]*alt="([^"]+)"', html_content)
        
        current_tdf_offers[venue] = show_titles

    return current_tdf_offers

def store_current_tdf_offers(current_tdf_offers = None):
    if current_tdf_offers is None:
        current_tdf_offers = get_current_tdf_offers()
        
    try:
        supabase.table("TDF Shows").insert({
            "broadway": current_tdf_offers.get("broadway", []),
            "off_broadway": current_tdf_offers.get("off_broadway", []),
            "off_off_broadway": current_tdf_offers.get("off_off_broadway", [])
        }).execute()
    except Exception as e:
        pprint(f"Error storing current TDF offers: {e}")

def get_last_tdf_offers():
    try:
        last_tdf_offers = (
            supabase.table("TDF Shows")
            .select("broadway, off_broadway, off_off_broadway")
            .order("created_at", desc=True)
            .limit(1)
            .execute()
        )
        return last_tdf_offers.data[0]
    
    except Exception as e:
        pprint(f"Error fetching last TDF offers: {e}")
        return {"broadway": [], "off_broadway": [], "off_off_broadway": []}


# get TDF emails according to filter
# args of form: broadway ensures that only users interested in Broadway shows are returned
def get_filtered_tdf_emails(*args, frequency = None):
    try:
        query = supabase.table("TDF User Profiles").select("email")
        for arg in args:
            query = query.eq(arg, True)
        if frequency:
            query = query.eq("frequency", frequency)
        users = query.execute()
        return [i['email'] for i in users.data if i['email']]

    except Exception as e:
        pprint(f"Error fetching users: {e}")
        return []

def is_difference_in_offers(current_tdf_offers, last_tdf_offers):
    for venue in VENUES:
        if set(current_tdf_offers.get(venue, [])) != set(last_tdf_offers.get(venue, [])):
            return True
    return False

def get_new_tdf_offers(current_tdf_offers = None, last_tdf_offers = None):

    if current_tdf_offers is None: current_tdf_offers = get_current_tdf_offers()
    if last_tdf_offers is None: last_tdf_offers = get_last_tdf_offers()

    new_offers = {}
    
    for venue in VENUES:
        # pprint.pprint({"venue": venue, "last_tdf_offers": last_tdf_offers})
        new_titles = set(current_tdf_offers.get(venue, [])) - set(last_tdf_offers.get(venue, []))
        if new_titles:
            new_offers[venue] = list(new_titles)

    return new_offers

# given the name of a show, return the last day it was available and the duration it was last available
def get_show_time_info(show_name, venue):
    
    if venue not in VENUES:
        return None, None

    # Get the latest entry where the show is present
    last_entry_show_is_in_table = (
        supabase.table("TDF Shows")
        .select("created_at")
        .contains(venue, [show_name])
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    ).data

    if last_entry_show_is_in_table and len(last_entry_show_is_in_table) > 0: 
        last_entry_show_is_in_table = last_entry_show_is_in_table[0]["created_at"]

    # Get the entry immediately after (i.e., the next most recent entry)
    next_entry = (
        supabase.table("TDF Shows")
        .select("created_at")
        .not_.contains(venue, [show_name])
        .order("created_at")
        .gt("created_at", last_entry_show_is_in_table if len(last_entry_show_is_in_table) > 0 else None)
        .limit(1)
        .execute()
    ).data if last_entry_show_is_in_table else None

    if next_entry and len(next_entry) > 0:
        next_entry = next_entry[0]["created_at"]

    return last_entry_show_is_in_table, next_entry

# last_date in format 2025-08-20T22:09:02.681815+00:00
def get_email_body(show_title, venue):

    last_date, next_date = get_show_time_info(show_title, venue)
    
    subtitle = f"<p class=\"subtitle\">This is the first time {show_title} is available on TDF.</p>"

    if last_date:
        # Parse ISO format with timezone
        # Format: August 20, 2025 at 10:09 PM UTC
        dt = datetime.fromisoformat(last_date)
        eastern = pytz.timezone("US/Eastern")
        dt_eastern = dt.astimezone(eastern)
        formatted_date = dt_eastern.strftime("%B %-d, %Y")

        subtitle = f"<p class=\"subtitle\">{show_title} was last available on TDF on {formatted_date}.</p>"

        if next_date:

            difference_time = datetime.fromisoformat(next_date) - datetime.fromisoformat(last_date)
            if difference_time.days >= 7:
                weeks = difference_time.days // 7
                formatted_difference = f"{weeks} week{'s' if weeks != 1 else ''}"
            elif difference_time.days > 0:
                formatted_difference = f"{difference_time.days} day{'s' if difference_time.days != 1 else ''}"
            elif difference_time.seconds >= 3600:
                hours = difference_time.seconds // 3600
                formatted_difference = f"{hours} hour{'s' if hours != 1 else ''}"
            elif difference_time.seconds >= 60:
                minutes = difference_time.seconds // 60
                formatted_difference = f"{minutes} minute{'s' if minutes != 1 else ''}"
            else:
                formatted_difference = f"{difference_time.seconds} second{'s' if difference_time.seconds != 1 else ''}"

            subtitle = f"<p class=\"subtitle\">The last time {show_title} was on TDF, it stayed on TDF for {formatted_difference}. It ultimately left TDF on {next_date}.</p>"

    with open('tdf/email.html', 'r') as file:
        TEMPLATE = file.read()

    return TEMPLATE.replace("{{ShowTitle}}", show_title).replace("{{Subtitle}}", subtitle)


def send_email(show_title, venue, recipients):

    msg = EmailMessage()
    msg.set_content(get_email_body(show_title, venue), subtype='html')

    msg['From'] = EMAIL
    msg['To'] = EMAIL
    msg['Subject'] = f"{show_title} is Now Available on TDF"
    msg['Bcc'] = recipients

    with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
        server.login(EMAIL, EMAIL_PASSWORD)
        server.send_message(msg)


# update tdf offers and send emails to users with immediate frequency
def main():
    current_tdf_offers = get_current_tdf_offers()
    last_tdf_offers = get_last_tdf_offers()
    new_offers = get_new_tdf_offers(current_tdf_offers, last_tdf_offers)
    

    if not is_difference_in_offers(current_tdf_offers, last_tdf_offers):
        pprint(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: TDF offers are the same.")
        return

    if not new_offers:
        pprint(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: No new TDF offers found.")
        return


    for venue in VENUES:
        bcc_list = get_filtered_tdf_emails(venue, "email_verified", frequency="immediate")
        for new_title in new_offers.get(venue, []):
            pprint(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}: New {venue} show available: {new_title}. Sending emails to {len(bcc_list)} users.")
            send_email(new_title, venue, bcc_list)
            
    # update supabase with current offers
    store_current_tdf_offers(current_tdf_offers)

main()