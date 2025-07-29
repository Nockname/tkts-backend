import database
import scraper
import datetime
from pytz import timezone

def update_discount_record(new_record, previous_record):
    print(f"Found previous record for {new_record['title']} on {new_record['performance_date']} (Matinee: {new_record['is_matinee']})")
    db = database.SupabaseConnection()

    print(f"Updating last available time for {new_record['title']}.")
    db.update_discount(
        record_id=previous_record["id"],
        last_available_time=datetime.datetime.now(timezone('US/Eastern')).strftime("%Y-%m-%d %H:%M:%S%z")
    )

    if float(new_record["discount_percent"]) > float(previous_record["discount_percent"]):
        print(f"Updating discount for {new_record['title']} from {previous_record['discount_percent']}% to {new_record['discount_percent']}%")
        db.update_discount(
            record_id=previous_record["id"],
            discount_percent=new_record["discount_percent"]
        )

    if float(new_record["low_price"]) < float(previous_record["low_price"]):
        print(f"Updating low price for {new_record['title']} from {previous_record['low_price']} to {new_record['low_price']}")
        db.update_discount(
            record_id=previous_record["id"],
            low_price=new_record["low_price"]
        )

    if float(new_record["high_price"]) > float(previous_record["high_price"]):
        print(f"Updating high price for {new_record['title']} from {previous_record['high_price']} to {new_record['high_price']}")
        db.update_discount(
            record_id=previous_record["id"],
            high_price=new_record["high_price"]
        )

def update_database():

    print("Updating TKTS database...")

    db = database.SupabaseConnection()
    db.test_connection()

    # Get TKTS data
    tkts_data = scraper.get_tkts_data()

    for record in tkts_data:

        # Get show ID by title
        # If show does not exist, create a new record
        show_id = db.get_show_id_by_name_or_create(record["title"], record["on_broadway"])

        # Find if discount record already exists
        previous_record = db.get_discount_record_by_fields(
            show_id=show_id,
            performance_date=record["performance_date"],
            is_matinee=record["is_matinee"]
        )

        if previous_record:
            update_discount_record(record, previous_record)
            continue

        # Add each new record to the TKTS Discounts table
        db.add_discount_record(
            show_id=show_id,
            discount_percent=record["discount_percent"],
            low_price=record["low_price"],
            high_price=record["high_price"],
            performance_time=record["performance_time"],
            performance_date=record["performance_date"],
            is_matinee=record["is_matinee"]
        )

    # update the change log
    html = scraper.get_tkts_html()
    db.add_change_log(lincoln_center_open=not scraper.location_is_closed("Lincoln Center", html),
                      times_square_open=not scraper.location_is_closed("Times Square", html))
    print("TKTS database updated successfully.")

if __name__ == "__main__":
    update_database()