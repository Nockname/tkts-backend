from supabase import create_client, Client
import sys
import os

# Add parent directory to path to import keys
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)

from keys.keys import SUPABASE_URL, SUPABASE_KEY

class SupabaseConnection:
    def __init__(self):
        """Initialize Supabase client"""
        self.url = SUPABASE_URL
        self.key = SUPABASE_KEY
        self.supabase: Client = create_client(self.url, self.key)
    
    def get_client(self):
        """Return the Supabase client"""
        return self.supabase
    
    def test_connection(self):
        """Test the connection to Supabase"""
        try:
            # Test connection with the new table names
            response = self.supabase.table('TKTS Discounts').select("*").limit(1).execute()
            print("✅ Connection to Supabase successful!")
            return True
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
    
    # TKTS Discounts table methods
    def add_discount_record(self, show_id, discount_percent, low_price, high_price, performance_time, performance_date=None, is_matinee=False):
        """
        Add a new discount record to the TKTS Discounts table
        
        Args:
            show_id (int): ID of the show
            discount_percent (int): Discount percentage
            low_price (int): Low price
            high_price (int): High price
            performance_date (str): Performance date (YYYY-MM-DD format)
            is_matinee (bool): Whether this is a matinee performance
        
        Returns:
            dict: Response from Supabase
        """
        try:
            data = {
                "show_id": show_id,
                "discount_percent": discount_percent,
                "low_price": low_price,
                "high_price": high_price,
                "performance_date": performance_date,
                "is_matinee": is_matinee,
                "performance_time": performance_time,
            }
            response = self.supabase.table('TKTS Discounts').insert(data).execute()
            print(f"✅ Successfully added discount record for {self.get_show_name_by_id(show_id)} on {performance_date} (Matinee: {is_matinee})")
            return response.data
        except Exception as e:
            print(f"❌ Failed to add discount record: {e}")
            return None
        
    def get_discount_record_by_fields(self, **kwargs):
        """
        Get a discount record by arbitrary fields
        
        Args:
            **kwargs: Arbitrary fields to filter by (e.g., show_id, performance_date, is_matinee)
        
        Returns:
            dict: Discount record if found, None otherwise
        """
        try:
            response = self.supabase.table('TKTS Discounts').select("*").match(kwargs).execute()
            if response.data:
                return response.data[0]
            return None
        except Exception as e:
            print(f"❌ Failed to fetch discount record: {e}")
            return None

    def update_discount(self, record_id, **kwargs):
        """
        Update a discount record
        
        Args:
            record_id (int): ID of the record to update
            **kwargs: Fields to update (show_id, discount_percent, low_price, high_price, performance_date, is_matinee)
        
        Returns:
            dict: Updated record
        """
        try:
            response = self.supabase.table('TKTS Discounts').update(kwargs).eq('id', record_id).execute()
            print(f"✅ Successfully updated discount record {record_id}.")
            return response.data
        except Exception as e:
            print(f"❌ Failed to update discount record {record_id}: {e}")
            return None
    
    def delete_discount(self, record_id):
        """
        Delete a discount record
        
        Args:
            record_id (int): ID of the record to delete
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            response = self.supabase.table('TKTS Discounts').delete().eq('id', record_id).execute()
            print(f"✅ Successfully deleted discount record {record_id}")
            return True
        except Exception as e:
            print(f"❌ Failed to delete discount record {record_id}: {e}")
            return False

    # Show Information table methods
    def add_show_mapping(self, show_name, is_broadway=None):
        """
        Add a new show to the Show Information table
        
        Args:
            show_name (str): Name of the show
            is_broadway (bool): Whether this is a Broadway show (optional)
        
        Returns:
            dict: Response from Supabase with the new mapping
        """
        try:
            data = {
                "show_name": show_name,
                "is_broadway": is_broadway
            }
            response = self.supabase.table('Show Information').insert(data).execute()
            print(f"✅ Successfully added show mapping for '{show_name}'")
            return response.data
        except Exception as e:
            print(f"❌ Failed to add show mapping: {e}")
            return None

    def get_all_show_mappings(self):
        """
        Get all show information records
        
        Returns:
            list: All show mappings
        """
        try:
            response = self.supabase.table('Show Information').select("*").execute()
            return response.data
        except Exception as e:
            print(f"❌ Failed to fetch show mappings: {e}")
            return []

    def get_show_id_by_name(self, show_name):
        """
        Get show ID by show name
        
        Args:
            show_name (str): Name of the show
        
        Returns:
            int: Show ID if found, None otherwise
        """
        try:
            response = self.supabase.table('Show Information').select("show_id").eq('show_name', show_name).execute()
            if response.data:
                return response.data[0]['show_id']
            return None
        except Exception as e:
            print(f"❌ Failed to fetch show ID for '{show_name}': {e}")
            return None
        
    def get_show_id_by_name_or_create(self, show_name, theatre=None):
        show_id = self.get_show_id_by_name(show_name)
        if not show_id:
            print(f"Show '{show_name}' not found, creating new record.")
            self.add_show_mapping(show_name, theatre)
            show_id = self.get_show_id_by_name(show_name)
        return show_id

    def get_show_name_by_id(self, show_id):
        """
        Get show name by show ID
        
        Args:
            show_id (int): ID of the show
        
        Returns:
            str: Show name if found, None otherwise
        """
        try:
            response = self.supabase.table('Show Information').select("show_name").eq('show_id', show_id).execute()
            if response.data:
                return response.data[0]['show_name']
            return None
        except Exception as e:
            print(f"❌ Failed to fetch show name for ID {show_id}: {e}")
            return None

    def update_show_mapping(self, mapping_id, show_name=None, is_broadway=None):
        """
        Update a show information record
        
        Args:
            mapping_id (int): ID of the mapping record to update
            show_name (str): New show name (optional)
            is_broadway (bool): Whether this is a Broadway show (optional)
        
        Returns:
            dict: Updated record
        """
        try:
            update_data = {}
            if show_name is not None:
                update_data["show_name"] = show_name
            if is_broadway is not None:
                update_data["is_broadway"] = is_broadway
                
            response = self.supabase.table('Show Information').update(update_data).eq('id', mapping_id).execute()
            print(f"✅ Successfully updated show mapping {mapping_id}")
            return response.data
        except Exception as e:
            print(f"❌ Failed to update show mapping {mapping_id}: {e}")
            return None

    def search_shows_by_name(self, search_term):
        """
        Search for shows by partial name match
        
        Args:
            search_term (str): Partial show name to search for
        
        Returns:
            list: List of matching show records
        """
        try:
            response = self.supabase.table('Show Information').select("*").ilike('show_name', f'%{search_term}%').execute()
            return response.data
        except Exception as e:
            print(f"❌ Failed to search shows with term '{search_term}': {e}")
            return []

    def add_change_log(self, **kwargs):
        """
        Update the logs table with a new entry

        Args:
            **kwargs: Arbitrary fields for the log entry (e.g., timestamp, description)

        Returns:
            dict: Response from Supabase with the new log entry
        """
        try:
            response = self.supabase.table('Logs').insert(kwargs).execute()
            print(f"✅ Successfully added log entry")
            return response.data
        except Exception as e:
            print(f"❌ Failed to add log entry: {e}")
            return None

    
def main():
    """Example usage of Supabase connection with both tables"""
    # Initialize connection
    db = SupabaseConnection()
    
    # Test connection
    db.test_connection()
    
if __name__ == "__main__":
    main()