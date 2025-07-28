import database
import scraper

def average_discount_percent(**kwargs):
    """
    Calculate the average discount percent for all shows in the TKTS Discounts table.
    
    Returns:
        float: Average discount percent, or None if no records found
    """
    db = database.SupabaseConnection()
    discounts = db.get_all_discounts()

    if not discounts:
        return None

    total_discount = sum(float(discount["discount_percent"]) for discount in discounts)
    average_discount = total_discount / len(discounts)

    return average_discount

def average_statistic_by_show(show_id, statistic, filter=None):
    db = database.SupabaseConnection()
    discounts = db.get_discounts_by_show(show_id)

    if not discounts:
        return None

    filtered_discounts = [discount for discount in discounts if filter is None or filter(discount)]
    if not filtered_discounts:
        return None
    
    total_statistic = sum(float(discount[statistic]) for discount in filtered_discounts)
    average_statistic = total_statistic / len(filtered_discounts)

    return average_statistic


if __name__ == "__main__":
    db = database.SupabaseConnection()
    show_id = db.search_shows_by_name("Gatsby")[0]["id"]
    print(average_statistic_by_show(show_id, "high_price"))

