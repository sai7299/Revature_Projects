import mysql.connector
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import csv
import json

#createing DataBase connection
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="admin",
            database="crime_db"
        )
        
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None

# Inserting crime record details into the database
def insert_crime(connection, city, place, crime_type, victims, weapon, arrested, report_date, report_time):
    cursor = connection.cursor()
    query = """
    INSERT INTO crime_records
    (city, location, crime_type, victims, weapon_used, arrest_made, date_reported, time_reported)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    values = (city, place, crime_type, victims, weapon, arrested, report_date, report_time)
    try:
        cursor.execute(query, values)
        connection.commit()
        print("Crime record inserted successfully.")
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    finally:
        cursor.close()

# Here show all crime records
def show_crimes(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM crime_records")
    records = cursor.fetchall()

    if not records:
        print("No records found.")
        return pd.DataFrame()

    df = pd.DataFrame(records, columns=[ 
        "ID", "City", "Location", "Crime Type", "Victims",
        "Weapon", "Arrested", "Report Date", "Report Time"
    ])
    print(df)
    return df

# Exporting data to CSV
def save_csv(df):
    if not df.empty:
        df.to_csv("crime_output.csv", index=False)
        print("CSV file saved successfully.")
    else:
        print("No data available to export.")

# Exporting data to JSON
def save_json(df):
    if not df.empty:
        df.to_json("crime_output.json", orient="records", indent=2)
        print("JSON file saved successfully.")
    else:
        print("No data available to export.")

# Here Function to analyze data and show visualizations
def basic_analysis(connection):
    data = show_crimes(connection)
    if data.empty:
        print("No data available for analysis.")
        return

    # Converting time and date for processing
    if data["Report Time"].dtype == "O":  # Check if it's a string (object)
        data["Hour"] = pd.to_datetime(data["Report Time"], format="%H:%M:%S").dt.hour
    else:  # If it's already in timedelta format
        data["Hour"] = data["Report Time"].dt.total_seconds() // 3600  # Convert seconds to hours
    
    data["Month"] = pd.to_datetime(data["Report Date"]).dt.month

    #Here mapping months to seasons
    season_map = {
        12: "Winter", 1: "Winter", 2: "Winter",
        3: "Spring", 4: "Spring", 5: "Spring",
        6: "Summer", 7: "Summer", 8: "Summer",
        9: "Rainy", 10: "Rainy", 11: "Rainy"
    }
    data["Season"] = data["Month"].map(season_map)

    print("\n--- Crime Types Count ---")
    print(data["Crime Type"].value_counts())

    print("\n--- Victims by City ---")
    print(data.groupby("City")["Victims"].sum())

    print("\n--- Arrest Rate by Weapon ---")
    print(data.groupby("Weapon")["Arrested"].mean().round(2))

    night_crimes = data[(data["Hour"] >= 20) | (data["Hour"] < 6)]
    print("\nTotal Night Crimes:", len(night_crimes))

    print("\n--- Crimes by Season ---")
    print(data["Season"].value_counts())

    # Here creating a 2x2 grid of subplots
    fig, axs = plt.subplots(2, 2, figsize=(14, 10))
    plt.tight_layout(pad=5.0)

    # Visualization 1: Crime Type Distribution
    sns.countplot(data=data, x="Crime Type", order=data["Crime Type"].value_counts().index, ax=axs[0, 0])
    axs[0, 0].set_title("Number of Crimes by Type")
    axs[0, 0].tick_params(axis="x", rotation=90)

    # Visualization 2: Total Victims by City
    victims_by_city = data.groupby("City")["Victims"].sum()
    victims_by_city.plot(kind='bar', color='skyblue', ax=axs[0, 1])
    axs[0, 1].set_title("Total Victims by City")
    axs[0, 1].set_ylabel("Number of Victims")
    axs[0, 1].tick_params(axis="x", rotation=90)

    # Visualization 3: Arrest Rate by Weapon Used
    arrest_by_weapon = data.groupby("Weapon")["Arrested"].mean().round(2)
    arrest_by_weapon.plot(kind='bar', color='orange', ax=axs[1, 0])
    axs[1, 0].set_title("Arrest Rate by Weapon Used")
    axs[1, 0].set_ylabel("Arrest Rate")
    axs[1, 0].tick_params(axis="x", rotation=90)

    # Visualization 4: Crimes by Season (Pie Chart)
    season_counts = data["Season"].value_counts()
    axs[1, 1].pie(season_counts, labels=season_counts.index, autopct="%1.1f%%", colors=sns.color_palette("pastel"))
    axs[1, 1].axis("equal")  # Make pie chart a circle
    axs[1, 1].text( 0,-1.2, "Crimes by Season", ha='center', va='center', fontsize=13)

    plt.tight_layout(rect=[0.029, 0.8, 0.95, 0.95])
    plt.subplots_adjust(hspace=0.99,wspace=0.3)
    plt.show()

# Main menu loop
def main_menu():
    connection = create_db_connection()
    if connection is None:
        print("Database connection failed.")
        return

    while True:
        print("\n== Crime Management System ==")
        print("1. Add Crime Record")
        print("2. View Crime Records")
        print("3. Export to CSV")
        print("4. Export to JSON")
        print("5. Analyze Crime Data")
        print("6. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            city = input("City: ")
            location = input("Location: ")
            crime_type = input("Crime Type: ")
            victims = int(input("Number of Victims: "))
            weapon = input("Weapon Used: ")
            arrested = input("Arrest Made? (yes/no): ").lower() == "yes"
            report_date = input("Report Date (YYYY-MM-DD): ")
            report_time = input("Report Time (HH:MM:SS): ")

            insert_crime(connection, city, location, crime_type, victims, weapon, arrested, report_date, report_time)

        elif choice == '2':
            show_crimes(connection)

        elif choice == '3':
            df = show_crimes(connection)
            save_csv(df)

        elif choice == '4':
            df = show_crimes(connection)
            save_json(df)

        elif choice == '5':
            basic_analysis(connection)

        elif choice == '6':
            print("Exiting program.")
            connection.close()
            break

        else:
            print("Invalid option. Please try again.")

# Start the program
if __name__ == "__main__":
    main_menu()