import sqlite3
import os
import json
import csv
from tabulate import tabulate
from datetime import datetime
import shutil

class NextGenFitnessDB:
    def __init__(self):
        # Get the directory where the script is located
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        # Set the database path relative to the script location
        self.db_path = os.path.join(self.script_dir, "NextGenFitness.db")
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        """Connect to the database"""
        try:
            # Ensure the database directory exists
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            
            self.conn = sqlite3.connect(self.db_path)
            self.cursor = self.conn.cursor()
            print(f"Successfully connected to database at: {self.db_path}")
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def close(self):
        """Close the database connection"""
        if self.conn:
            self.conn.close()
            print("Database connection closed")

    def view_all_tables(self):
        """View all tables in the database"""
        try:
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = self.cursor.fetchall()
            if tables:
                print("\nAvailable tables:")
                for table in tables:
                    print(f"- {table[0]}")
            else:
                print("No tables found in the database")
        except sqlite3.Error as e:
            print(f"Error viewing tables: {e}")

    def view_table_data(self, table_name):
        """View all data in a specific table"""
        try:
            self.cursor.execute(f"SELECT * FROM {table_name}")
            rows = self.cursor.fetchall()
            
            # Get column names
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [column[1] for column in self.cursor.fetchall()]
            
            if rows:
                print(f"\nData in {table_name} table:")
                print(tabulate(rows, headers=columns, tablefmt="grid"))
            else:
                print(f"No data found in {table_name} table")
        except sqlite3.Error as e:
            print(f"Error viewing table data: {e}")

    def insert_record(self, table_name):
        """Insert a new record into a table"""
        try:
            # Get table structure
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            # Get values from user
            values = []
            for column in columns:
                col_name = column[1]
                col_type = column[2]
                value = input(f"Enter {col_name} ({col_type}): ")
                
                # Convert value based on column type
                if col_type == "INTEGER":
                    value = int(value) if value else None
                elif col_type == "REAL":
                    value = float(value) if value else None
                
                values.append(value)
            
            # Create the INSERT query
            placeholders = ", ".join(["?" for _ in columns])
            columns_str = ", ".join([col[1] for col in columns])
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            self.cursor.execute(query, values)
            self.conn.commit()
            print("Record inserted successfully")
        except sqlite3.Error as e:
            print(f"Error inserting record: {e}")

    def update_record(self, table_name):
        """Update a record in a table"""
        try:
            # Get table structure
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            # Get primary key column name
            primary_key = None
            for col in columns:
                if col[5]:  # col[5] is the pk flag
                    primary_key = col[1]  # col[1] is the column name
                    break
            
            if not primary_key:
                print(f"Error: No primary key found in table {table_name}")
                return
            
            # Show current data
            self.view_table_data(table_name)
            
            # Get primary key value
            primary_key_value = input(f"\nEnter the {primary_key} of the record to update: ")
            
            # Get new values
            print("\nEnter new values (press Enter to keep current value):")
            updates = []
            values = []
            for column in columns:
                col_name = column[1]
                if col_name != primary_key:  # Skip primary key
                    new_value = input(f"New {col_name}: ")
                    if new_value:
                        updates.append(f"{col_name} = ?")
                        values.append(new_value)
            
            if updates:
                query = f"UPDATE {table_name} SET {', '.join(updates)} WHERE {primary_key} = ?"
                values.append(primary_key_value)
                self.cursor.execute(query, values)
                self.conn.commit()
                print("Record updated successfully")
            else:
                print("No changes made")
        except sqlite3.Error as e:
            print(f"Error updating record: {e}")

    def delete_record(self, table_name):
        """Delete a record from a table"""
        try:
            # Show current data
            self.view_table_data(table_name)
            
            # Get primary key column name
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            primary_key = None
            for col in columns:
                if col[5]:  # col[5] is the pk flag
                    primary_key = col[1]  # col[1] is the column name
                    break
            
            if not primary_key:
                print(f"Error: No primary key found in table {table_name}")
                return
            
            # Get record ID to delete
            record_id = input(f"\nEnter the {primary_key} of the record to delete: ")
            
            # Confirm deletion
            confirm = input(f"Are you sure you want to delete record {record_id}? (y/n): ")
            if confirm.lower() == 'y':
                self.cursor.execute(f"DELETE FROM {table_name} WHERE {primary_key} = ?", (record_id,))
                self.conn.commit()
                print("Record deleted successfully")
            else:
                print("Deletion cancelled")
        except sqlite3.Error as e:
            print(f"Error deleting record: {e}")

    def create_new_table(self):
        """Create a new table in the database with a user-friendly interface"""
        try:
            print("\n=== Create New Table ===")
            table_name = input("\nEnter the name for the new table: ").strip()
            
            # Validate table name
            if not table_name or not table_name.replace('_', '').isalnum():
                print("Error: Table name must contain only letters, numbers, and underscores")
                return

            # Get columns
            columns = []
            print("\nLet's define the columns for your table.")
            print("First, let's add the primary key column (usually 'id'):")
            
            # Add primary key column
            pk_name = input("Enter primary key (default: 'id'): ").strip() or "id"
            columns.append(f"{pk_name} INTEGER PRIMARY KEY")
            
            print("\nNow, let's add other columns to your table.")
            print("Available data types:")
            print("1. TEXT - For text data")
            print("2. INTEGER - For whole numbers")
            print("3. REAL - For decimal numbers")
            print("4. BOOLEAN - For true/false values")
            print("5. TIMESTAMP - For date and time")
            print("6. BLOB - For binary data")
            
            while True:
                print("\nColumn Definition:")
                print("-----------------")
                
                # Get column name
                col_name = input("\nEnter attribute name (or 'done' to finish): ").strip()
                if col_name.lower() == 'done':
                    break
                
                if not col_name or not col_name.replace('_', '').isalnum():
                    print("Error: Attribute name must contain only letters, numbers, and underscores")
                    continue
                
                # Get data type
                while True:
                    print("\nSelect data type:")
                    print("1. TEXT")
                    print("2. INTEGER")
                    print("3. REAL")
                    print("4. BOOLEAN")
                    print("5. TIMESTAMP")
                    print("6. BLOB")
                    
                    type_choice = input("Enter your choice (1-6): ").strip()
                    
                    data_types = {
                        "1": "TEXT",
                        "2": "INTEGER",
                        "3": "REAL",
                        "4": "BOOLEAN",
                        "5": "TIMESTAMP",
                        "6": "BLOB"
                    }
                    
                    if type_choice in data_types:
                        data_type = data_types[type_choice]
                        break
                    else:
                        print("Invalid choice. Please try again.")
                
                # Get constraints
                constraints = []
                
                # NOT NULL constraint
                not_null = input("Should this column be required? (y/n): ").strip().lower()
                if not_null == 'y':
                    constraints.append("NOT NULL")
                
                # UNIQUE constraint
                if data_type in ["TEXT", "INTEGER", "REAL"]:
                    unique = input("Should this column have unique values? (y/n): ").strip().lower()
                    if unique == 'y':
                        constraints.append("UNIQUE")
                
                # DEFAULT value
                if data_type in ["TEXT", "INTEGER", "REAL", "BOOLEAN"]:
                    default = input("Should this column have a default value? (y/n): ").strip().lower()
                    if default == 'y':
                        if data_type == "TEXT":
                            default_value = input("Enter default text value: ").strip()
                            constraints.append(f"DEFAULT '{default_value}'")
                        elif data_type == "BOOLEAN":
                            default_value = input("Enter default boolean value (true/false): ").strip().lower()
                            constraints.append(f"DEFAULT {default_value}")
                        else:
                            default_value = input(f"Enter default {data_type.lower()} value: ").strip()
                            constraints.append(f"DEFAULT {default_value}")
                
                # For TIMESTAMP, add common default
                if data_type == "TIMESTAMP":
                    constraints.append("DEFAULT CURRENT_TIMESTAMP")
                
                # Build column definition
                column_def = f"{col_name} {data_type}"
                if constraints:
                    column_def += " " + " ".join(constraints)
                
                columns.append(column_def)
                print(f"\nColumn '{col_name}' added successfully!")
            
            if len(columns) < 2:  # Only primary key
                print("Error: Table must have at least one column besides the primary key")
                return
            
            # Show table preview
            print("\nTable Preview:")
            print("-------------")
            print(f"CREATE TABLE {table_name} (")
            for col in columns:
                print(f"    {col},")
            print(")")
            
            # Confirm creation
            confirm = input("\nDo you want to create this table? (y/n): ").strip().lower()
            if confirm != 'y':
                print("Table creation cancelled.")
                return
            
            # Create the table
            create_query = f"CREATE TABLE {table_name} (\n    {',\n    '.join(columns)}\n)"
            self.cursor.execute(create_query)
            self.conn.commit()
            print(f"\nTable '{table_name}' created successfully!")
            
            # Show table structure
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            table_info = self.cursor.fetchall()
            print("\nTable structure:")
            print(tabulate(table_info, headers=['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'], tablefmt="grid"))
            
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")

    def delete_table(self):
        """Delete a table from the database"""
        try:
            print("\n=== Delete Table ===")
            
            # Show all tables
            self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = self.cursor.fetchall()
            
            if not tables:
                print("No tables found in the database")
                return
            
            print("\nAvailable tables:")
            for i, table in enumerate(tables, 1):
                print(f"{i}. {table[0]}")
            
            # Get table selection
            while True:
                try:
                    choice = input("\nEnter the number of the table to delete (or 'cancel' to go back): ").strip()
                    if choice.lower() == 'cancel':
                        print("Table deletion cancelled.")
                        return
                    
                    choice = int(choice)
                    if 1 <= choice <= len(tables):
                        table_name = tables[choice-1][0]
                        break
                    else:
                        print("Invalid selection. Please try again.")
                except ValueError:
                    print("Please enter a valid number.")
            
            # Show table structure before deletion
            print(f"\nTable structure of '{table_name}':")
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            table_info = self.cursor.fetchall()
            print(tabulate(table_info, headers=['cid', 'name', 'type', 'notnull', 'dflt_value', 'pk'], tablefmt="grid"))
            
            # Show row count
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
            print(f"\nThis table contains {row_count} rows.")
            
            # Confirm deletion
            confirm = input(f"\nAre you sure you want to delete the table '{table_name}'? This action cannot be undone! (yes/no): ").strip().lower()
            if confirm == 'yes':
                self.cursor.execute(f"DROP TABLE {table_name}")
                self.conn.commit()
                print(f"\nTable '{table_name}' has been successfully deleted.")
            else:
                print("Table deletion cancelled.")
                
        except sqlite3.Error as e:
            print(f"Error deleting table: {e}")

    def backup_database(self):
        """Create a backup of the entire database"""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = os.path.join(self.script_dir, "backups")
            os.makedirs(backup_dir, exist_ok=True)
            backup_path = os.path.join(backup_dir, f"NextGenFitness_backup_{timestamp}.db")
            
            # Create backup
            shutil.copy2(self.db_path, backup_path)
            print(f"\nDatabase backup created successfully: {backup_path}")
            
            # Show backup size
            backup_size = os.path.getsize(backup_path) / 1024  # Size in KB
            print(f"Backup size: {backup_size:.2f} KB")
            
        except Exception as e:
            print(f"Error creating backup: {e}")

    def export_table(self, table_name):
        """Export table data to CSV or JSON"""
        try:
            print("\n=== Export Table Data ===")
            print("1. Export to CSV")
            print("2. Export to JSON")
            
            format_choice = input("\nChoose export format (1-2): ").strip()
            
            # Get data
            self.cursor.execute(f"SELECT * FROM {table_name}")
            rows = self.cursor.fetchall()
            
            # Get column names
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [column[1] for column in self.cursor.fetchall()]
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_dir = os.path.join(self.script_dir, "exports")
            os.makedirs(export_dir, exist_ok=True)
            
            if format_choice == "1":
                # Export to CSV
                filename = os.path.join(export_dir, f"{table_name}_{timestamp}.csv")
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(columns)
                    writer.writerows(rows)
                print(f"\nData exported to {filename}")
                
            elif format_choice == "2":
                # Export to JSON
                filename = os.path.join(export_dir, f"{table_name}_{timestamp}.json")
                data = [dict(zip(columns, row)) for row in rows]
                with open(filename, 'w') as jsonfile:
                    json.dump(data, jsonfile, indent=2)
                print(f"\nData exported to {filename}")
                
            else:
                print("Invalid choice")
                return
                
        except Exception as e:
            print(f"Error exporting data: {e}")

    def analyze_table(self, table_name):
        """Show detailed analysis of a table"""
        try:
            print(f"\n=== Table Analysis: {table_name} ===")
            
            # Get table info
            self.cursor.execute(f"PRAGMA table_info({table_name})")
            columns = self.cursor.fetchall()
            
            # Get row count
            self.cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = self.cursor.fetchone()[0]
            
            # Calculate table size
            self.cursor.execute(f"SELECT page_count * page_size as size FROM pragma_page_count(), pragma_page_size()")
            size_bytes = self.cursor.fetchone()[0]
            size_kb = size_bytes / 1024
            
            print(f"\nTable Statistics:")
            print(f"Total Rows: {row_count}")
            print(f"Table Size: {size_kb:.2f} KB")
            print(f"Number of Columns: {len(columns)}")
            
            print("\nColumn Analysis:")
            for col in columns:
                col_name = col[1]
                col_type = col[2]
                
                # Get null count
                self.cursor.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col_name} IS NULL")
                null_count = self.cursor.fetchone()[0]
                
                # Get unique values count
                self.cursor.execute(f"SELECT COUNT(DISTINCT {col_name}) FROM {table_name}")
                unique_count = self.cursor.fetchone()[0]
                
                print(f"\n{col_name} ({col_type}):")
                print(f"  Null values: {null_count}")
                print(f"  Unique values: {unique_count}")
                
                # For numeric columns, show min, max, avg
                if col_type in ["INTEGER", "REAL"]:
                    self.cursor.execute(f"SELECT MIN({col_name}), MAX({col_name}), AVG({col_name}) FROM {table_name}")
                    min_val, max_val, avg_val = self.cursor.fetchone()
                    print(f"  Min value: {min_val}")
                    print(f"  Max value: {max_val}")
                    print(f"  Average value: {avg_val:.2f}")
                
        except sqlite3.Error as e:
            print(f"Error analyzing table: {e}")

    def modify_table(self, table_name):
        """Modify table structure"""
        try:
            print(f"\n=== Modify Table: {table_name} ===")
            print("1. Add new column")
            print("2. Rename column")
            print("3. Drop column")
            print("4. Return to previous menu")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == "1":
                # Add new column
                col_name = input("\nEnter new attribute name: ").strip()
                print("\nSelect data type:")
                print("1. TEXT")
                print("2. INTEGER")
                print("3. REAL")
                print("4. BOOLEAN")
                print("5. TIMESTAMP")
                
                type_choice = input("Enter your choice (1-5): ").strip()
                data_types = {
                    "1": "TEXT",
                    "2": "INTEGER",
                    "3": "REAL",
                    "4": "BOOLEAN",
                    "5": "TIMESTAMP"
                }
                
                if type_choice in data_types:
                    data_type = data_types[type_choice]
                    self.cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {data_type}")
                    self.conn.commit()
                    print(f"Column '{col_name}' added successfully")
                else:
                    print("Invalid data type choice")
                    
            elif choice == "2":
                # Rename column
                print("\nCurrent columns:")
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = self.cursor.fetchall()
                for col in columns:
                    print(f"- {col[1]}")
                
                old_name = input("\nEnter attribute name to rename: ").strip()
                new_name = input("Enter new attribute name: ").strip()
                
                # SQLite doesn't support direct column rename, so we need to:
                # 1. Create new table
                # 2. Copy data
                # 3. Drop old table
                # 4. Rename new table
                
                # Get all column names
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in self.cursor.fetchall()]
                
                # Create new table with renamed column
                new_columns = [f"{new_name if col == old_name else col}" for col in columns]
                columns_str = ", ".join(new_columns)
                
                self.cursor.execute(f"CREATE TABLE {table_name}_new AS SELECT {columns_str} FROM {table_name}")
                self.cursor.execute(f"DROP TABLE {table_name}")
                self.cursor.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
                self.conn.commit()
                
                print(f"Column '{old_name}' renamed to '{new_name}' successfully")
                
            elif choice == "3":
                # Drop column
                print("\nCurrent columns:")
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = self.cursor.fetchall()
                for col in columns:
                    print(f"- {col[1]}")
                
                col_name = input("\nEnter attribute name to drop: ").strip()
                
                # Similar to rename, we need to create a new table without the column
                self.cursor.execute(f"PRAGMA table_info({table_name})")
                columns = [col[1] for col in self.cursor.fetchall() if col[1] != col_name]
                columns_str = ", ".join(columns)
                
                self.cursor.execute(f"CREATE TABLE {table_name}_new AS SELECT {columns_str} FROM {table_name}")
                self.cursor.execute(f"DROP TABLE {table_name}")
                self.cursor.execute(f"ALTER TABLE {table_name}_new RENAME TO {table_name}")
                self.conn.commit()
                
                print(f"Column '{col_name}' dropped successfully")
                
            elif choice == "4":
                return
                
            else:
                print("Invalid choice")
                
        except sqlite3.Error as e:
            print(f"Error modifying table: {e}")

    def execute_query(self):
        """Execute custom SQL queries"""
        try:
            print("\n=== Execute SQL Query ===")
            print("Enter your SQL query (type 'exit' to return to menu)")
            print("Example queries:")
            print("  SELECT * FROM User;")
            print("  INSERT INTO User (user_id, username, email) VALUES ('U002', 'john', 'john@example.com');")
            print("  UPDATE User SET username = 'John Doe' WHERE user_id = 'U001';")
            print("  DELETE FROM User WHERE user_id = 'U001';")
            print("  CREATE TABLE new_table (id INTEGER PRIMARY KEY, name TEXT);")
            print("\nNote: For multi-line queries, end each line with a semicolon")
            
            query_lines = []
            while True:
                line = input("> ").strip()
                if line.lower() == 'exit':
                    return
                
                query_lines.append(line)
                if line.endswith(';'):
                    break
            
            query = " ".join(query_lines)
            
            # Execute the query
            self.cursor.execute(query)
            
            # If it's a SELECT query, show results
            if query.strip().upper().startswith('SELECT'):
                rows = self.cursor.fetchall()
                if rows:
                    # Get column names from cursor description
                    columns = [description[0] for description in self.cursor.description]
                    print("\nQuery Results:")
                    print(tabulate(rows, headers=columns, tablefmt="grid"))
                    print(f"\nTotal rows: {len(rows)}")
                else:
                    print("\nNo results found")
            else:
                # For non-SELECT queries, commit the changes
                self.conn.commit()
                print("\nQuery executed successfully")
                
                # Show affected rows
                print(f"Rows affected: {self.cursor.rowcount}")
                
        except sqlite3.Error as e:
            print(f"\nError executing query: {e}")
            # Rollback in case of error
            self.conn.rollback()

def manage_current_tables(db):
    """Manage existing tables"""
    while True:
        print("\n=== Current Tables Management ===")
        print("1. View all tables")
        print("2. View table data")
        print("3. Insert record")
        print("4. Update record")
        print("5. Delete record")
        print("6. Delete table")
        print("7. Export table data")
        print("8. Analyze table")
        print("9. Modify table")
        print("10. Execute Query")
        print("11. Return to main menu")
        
        choice = input("\nEnter your choice (1-11): ")
        
        if choice == "1":
            db.view_all_tables()
        
        elif choice == "2":
            table_name = input("Enter table name: ")
            db.view_table_data(table_name)
        
        elif choice == "3":
            table_name = input("Enter table name: ")
            db.insert_record(table_name)
        
        elif choice == "4":
            table_name = input("Enter table name: ")
            db.update_record(table_name)
        
        elif choice == "5":
            table_name = input("Enter table name: ")
            db.delete_record(table_name)
        
        elif choice == "6":
            db.delete_table()
        
        elif choice == "7":
            table_name = input("Enter table name: ")
            db.export_table(table_name)
        
        elif choice == "8":
            table_name = input("Enter table name: ")
            db.analyze_table(table_name)
        
        elif choice == "9":
            table_name = input("Enter table name: ")
            db.modify_table(table_name)
        
        elif choice == "10":
            db.execute_query()
        
        elif choice == "11":
            break
        
        else:
            print("Invalid choice. Please try again.")

def main():
    db = NextGenFitnessDB()
    
    while True:
        print("\n=== NextGenFitness Database Manager ===")
        print("1. Manage Current Tables")
        print("2. Add New Table")
        print("3. Backup Database")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ")
        
        if choice == "1":
            manage_current_tables(db)
        
        elif choice == "2":
            db.create_new_table()
        
        elif choice == "3":
            db.backup_database()
        
        elif choice == "4":
            db.close()
            print("Goodbye!")
            break
        
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main() 