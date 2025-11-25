# Read the data from a file and print it
import pandas as pd
data = pd.read_excel(r'D:\Ramakrishnan S\Guvi\Mini Projects\Police check\traffic_stops.xlsx')
df = pd.DataFrame(data)
print(df.head())
print("Data loaded successfully.")

# Check for missing values
df_missing = df.isnull().sum()
print(df_missing)

# Fill missing values in the Data frame
df.ffill(inplace =True)
df.bfill(inplace=True)
print("Missing values filled.")

# To verify whether missing values are filled after applying the fillna method
df_missing_after = df.isnull().sum()
print(df_missing_after)

# Parse date columns to datetime format
if 'stop_date' in df.columns:
    df['stop_date'] = pd.to_datetime(df['stop_date'],format = '%Y-%M-%D',errors = 'coerce').dt.date
if 'stop_time' in df.columns:
    df['stop_time'] = pd.to_datetime(df['stop_time'],format = '%H:%M:%S',errors = 'coerce').dt.time
df = df.dropna(subset=['stop_date', 'stop_time']) # remove rows where date parsing failed
print("Date columns parsed to datetime format.")

#Connect the database from python to mySQL  
import mysql.connector      
conn = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = "Visakan@4351")
cursor = conn.cursor()
print("Database connected successfully.")

# Create a database named police_secure_check
cursor.execute("CREATE DATABASE IF NOT EXISTS police_secure_check_db;")
print("MySQL database 'police_secure_check_db' created successfully!")

# Create Table in the database
cursor.execute("USE police_secure_check_db;") # Use the created database
cursor.execute("""CREATE TABLE IF NOT EXISTS traffic_stops(
    stop_date DATE,
    stop_time TIME,
    country_name VARCHAR(50),
    driver_gender VARCHAR(50),
    driver_age_raw INT,
    driver_age INT,
    driver_race VARCHAR(50),
    violation_raw VARCHAR(50),
    violation VARCHAR(50),
    search_conducted BOOLEAN,
    search_type VARCHAR(50),
    stop_outcome VARCHAR(50),
    is_arrested BOOLEAN,
    stop_duration VARCHAR(50),
    drugs_related_stop BOOLEAN,
    vehicle_number VARCHAR(50));
    """)
conn.commit()
print("Table 'traffic_stops' created successfully in the database.")

#Insert Data into the table
data_tuples = [tuple(x) for x in df.values.tolist()]
print(len(data_tuples)) # Print the number of records to be inserted
query = """
    INSERT INTO traffic_stops (stop_date, stop_time, country_name, driver_gender,
    driver_age_raw, driver_age, driver_race,violation_raw,violation,
    search_conducted,search_type,stop_outcome,is_arrested,stop_duration,
    drugs_related_stop,vehicle_number) VALUES (%s, %s, %s, %s, %s, %s, %s, %s,
    %s, %s, %s, %s, %s, %s, %s, %s) 
"""
cursor.executemany(query, data_tuples)
conn.commit()
print("Data inserted using to_list() and datas inserted successfully into the table.")

# Top 10 vehicle number involved in traffic stops - Query 1
query1 = """ SELECT vehicle_number, count(vehicle_number) AS count
FROM traffic_stops
WHERE drugs_related_stop = TRUE
GROUP BY vehicle_number 
ORDER BY count desc 
LIMIT 10; """
cursor.execute(query1)
result = cursor.fetchall()
df_result = pd.DataFrame(result, columns=['vehicle_number', 'count'])
print("Top 10 vehicle numbers involved in drug-related traffic stops",df_result)

# Most frequently searched vehicle ?? Query 2
query2 = """ SELECT vehicle_number, count(vehicle_number) AS total_no_of_searches
FROM traffic_stops
WHERE search_conducted = TRUE
GROUP BY vehicle_number 
ORDER BY total_no_of_searches DESC 
LIMIT 5; """
cursor.execute(query2)
result1 = cursor.fetchall()
df_result1 = pd.DataFrame(result1, columns=['vehicle_number', 'total_no_of_searches'])
print("Most Frequently searched vehicle",df_result1)

# Which driver age group highest arrest rate ??  Query 3
query3 = """ SELECT driver_age, SUM(is_arrested) AS total_arrests
FROM traffic_stops
WHERE is_arrested = TRUE
GROUP BY driver_age
ORDER BY total_arrests DESC
LIMIT 5; """
cursor.execute(query3)
result2 = cursor.fetchall()
df_result2 = pd.DataFrame(result2, columns=['driver_age', 'total_arrests'])
print("Top 5 Driver age group having Hihgest arrest rate",df_result2)

#What is the gender distribution of drivers stopped in each country? ?? Query 4
query4 = """ SELECT country_name, COUNT(*) AS total_drivers,
SUM(CASE WHEN driver_gender = 'M' THEN 1 ELSE 0 END) AS Male_Drivers,
SUM(CASE WHEN driver_gender = 'F' THEN 1 ELSE 0 END) AS Female_Drivers
FROM traffic_stops
GROUP BY country_name
ORDER BY total_drivers DESC; """
cursor.execute(query4)
result3 = cursor.fetchall()
df_result3 = pd.DataFrame(result3, columns=['country_name', 'total_drivers','Male_Drivers','Female_Drivers'])
print("Gender distribution of drivers stopped in each country",df_result3)

# 6.Which race and gender combination has the highest search rate? Query 5
query5 = """ SELECT driver_race, driver_gender, COUNT(*) AS highest_search_rate,
SUM(CASE WHEN driver_gender = 'M' THEN 1 ELSE 0 END) AS Male_Genders,
SUM(CASE WHEN driver_gender = 'F' THEN 1 ELSE 0 END) AS Female_Genders
FROM traffic_stops
GROUP BY driver_race,driver_gender
ORDER BY highest_search_rate DESC; """
cursor.execute(query5)
result4 = cursor.fetchall()
df_result4 = pd.DataFrame(result4, columns=['driver_race','driver_gender','Male_Genders', 'Female_Genders','highest_search_rate'])
print("Highest search rate with race and Gender combination",df_result4)

#7.	What time of day sees the most traffic stops? Query 6
query6 = """ SELECT
    CASE 
        WHEN HOUR(stop_time) BETWEEN 0 AND 5 THEN 'Midnight to 6 AM'
        WHEN HOUR(stop_time) BETWEEN 6 AND 11 THEN '6 AM to Noon'
        WHEN HOUR(stop_time) BETWEEN 12 AND 17 THEN 'Noon to 6 PM'
        WHEN HOUR(stop_time) BETWEEN 18 AND 23 THEN '6 PM to Midnight'
    END AS time_of_day,
    COUNT(*) AS total_stops
    FROM traffic_stops
    GROUP BY time_of_day
    ORDER BY total_stops DESC; """
cursor.execute(query6)
result5 = cursor.fetchall()
df_result5 = pd.DataFrame(result5, columns=['time_of_day','total_stops'])
print("Time of day that sees the most traffic stops",df_result5)

#8.	What is the average stop duration for different violations? Query 7
query7 = """ SELECT violation,
    AVG(CAST(REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(stop_duration, ' ', 1),'-', 1)
,'+', '') AS UNSIGNED))                            
     AS avg_minutes
    FROM traffic_stops
    GROUP BY violation  
    ORDER BY avg_minutes DESC; """
cursor.execute(query7)
result6 = cursor.fetchall()
df_result6 = pd.DataFrame(result6, columns=['violation','avg_minutes'])
print("Time of day that sees the most traffic stops",df_result6)

# 9.Are stops during the night more likely to lead to arrests? Query 8
query8 = """ SELECT
    CASE 
        WHEN HOUR(stop_time) BETWEEN 0 AND 5 THEN 'Midnight to 6 AM'
        WHEN HOUR(stop_time) BETWEEN 6 AND 11 THEN '6 AM to Noon'
        WHEN HOUR(stop_time) BETWEEN 12 AND 17 THEN 'Noon to 6 PM'
        WHEN HOUR(stop_time) BETWEEN 18 AND 23 THEN '6 PM to Midnight'
    END AS time_of_day,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND((SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY time_of_day
    ORDER BY total_stops DESC; """
cursor.execute(query8)
result7 = cursor.fetchall()
df_result7 = pd.DataFrame(result7, columns=['time_of_day','total_stops','total_arrests','arrest_rate_percentage'])
print("Are stops during the night more likely to lead to arrests?",df_result7) 

# Which violations are most associated with searches or arrests? Query 9   
query9 = """ SELECT violation,
    COUNT(*) AS total_violations,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100*(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS search_rate_percentage,
    ROUND(100*(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY violation
    ORDER BY total_searches DESC, total_arrests DESC; """
cursor.execute(query9)
result8 = cursor.fetchall()
df_result8 = pd.DataFrame(result8, columns=['violation','total_violations','total_searches','total_arrests','search_rate_percentage','arrest_rate_percentage'])
print("Violations most associated with searches or arrests",df_result8)

#Which violations are most common among younger drivers (<25)? Query 10 
query10 = """ SELECT violation,
    COUNT(*) AS total_violations
    FROM traffic_stops
    WHERE driver_age < 25
    GROUP BY violation
    ORDER BY total_violations DESC
    LIMIT 5; """
cursor.execute(query10)
result9 = cursor.fetchall()
df_result9 = pd.DataFrame(result9, columns=['violation','total_violations'])
print("Most common violations among younger drivers (<25)",df_result9)

#Is there a violation that rarely results in search or arrest? Query 11
query11 = """ SELECT violation,
    COUNT(*) AS total_violations,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100*(SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS search_rate_percentage,
    ROUND(100*(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY violation
    HAVING search_rate_percentage < 50 AND arrest_rate_percentage < 50 # Less than 5 % and less than 25% data not available and hence less than 50% data has taken
    ORDER BY total_violations; """
cursor.execute(query11)
result10 = cursor.fetchall()
print("Violations that rarely result in search or arrest")
print(f"Number of rows returned: {len(result10)}")
for violation, total_violations, total_searches, total_arrests, search_rate_percentage, arrest_rate_percentage in result10:
    print(f"Violation: {violation}, Total Violations: {total_violations}, Total Searches: {total_searches}, Total Arrests: {total_arrests}, Search Rate Percentage: {search_rate_percentage}%, Arrest Rate Percentage: {arrest_rate_percentage}%")

# Which countries report the highest rate of drug-related stops? Query 12 
query12 = """ SELECT country_name,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) AS drug_related_stops,
    ROUND(100*(SUM(CASE WHEN drugs_related_stop = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS drug_stop_rate_percentage
    FROM traffic_stops
    GROUP BY country_name
    ORDER BY drug_stop_rate_percentage DESC
    LIMIT 5; """
cursor.execute(query12)
result11 = cursor.fetchall()
df_result11 = pd.DataFrame(result11, columns=['country_name','total_stops','drug_related_stops','drug_stop_rate_percentage'])
print("Countries reporting the highest rate of drug-related stops",df_result11)

# What is the arrest rate by country and violation? Query 13
query13 = """ SELECT country_name,violation,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND(100*(SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)), 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY country_name,violation
    ORDER BY arrest_rate_percentage DESC
    LIMIT 5; """
cursor.execute(query13)
result12 = cursor.fetchall()
df_result12 = pd.DataFrame(result12, columns=['country_name','total_stops','total_arrests','violation','arrest_rate_percentage'])
print("Arrest rate by country and violation",df_result12)

# 15.Which country has the most stops with search conducted? Query 14
query14 = """ SELECT country_name,
    COUNT(*) AS total_stops,
    SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS stops_with_search
    FROM traffic_stops
    GROUP BY country_name
    ORDER BY stops_with_search DESC
    LIMIT 5; """
cursor.execute(query14)
result13 = cursor.fetchall()
df_result13 = pd.DataFrame(result13, columns=['country_name','total_stops','stops_with_search'])
print("Countries with the most stops with search conducted",df_result13)

# Complex Queries
# Yearly Breakdown of Stops and Arrests by Country (Using Subquery and Window Functions)
# Query 15
query15 = """ SELECT country_name, stop_year, total_stops, total_arrests,
    ROUND((total_arrests / total_stops) * 100, 2) AS arrest_rate_percentage
    FROM (
        SELECT country_name,
            YEAR(stop_date) AS stop_year,
            COUNT(*) AS total_stops,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
            ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY YEAR(stop_date)) AS rn
        FROM traffic_stops
        GROUP BY country_name, YEAR(stop_date)
    ) AS yearly_data
    WHERE rn <= 5
    ORDER BY country_name, stop_year; """
cursor.execute(query15)
result14 = cursor.fetchall()
df_result14 = pd.DataFrame(result14, columns=['country_name','stop_year','total_stops','total_arrests','arrest_rate_percentage'])
print("Yearly Breakdown of Stops and Arrests by Country",df_result14)

# Driver Violation Trends Based on Age and Race (Join with Subquery)
# Query 16
query16 = """ SELECT ts.driver_age, ts.driver_race, ts.violation,
    COUNT(*) AS total_violations,
    SUM(CASE WHEN ts.is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND((SUM(CASE WHEN ts.is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS arrest_rate_percentage
    FROM traffic_stops ts
    JOIN (
        SELECT driver_age, driver_race
        FROM traffic_stops
        GROUP BY driver_age, driver_race
        HAVING COUNT(*) > 50
    ) AS filtered_drivers
    ON ts.driver_age = filtered_drivers.driver_age AND ts.driver_race = filtered_drivers.driver_race
    GROUP BY ts.driver_age, ts.driver_race, ts.violation
    ORDER BY total_violations DESC
    LIMIT 10; """
cursor.execute(query16)
result15 = cursor.fetchall()
df_result15 = pd.DataFrame(result15, columns=['driver_age','driver_race','violation','total_violations','total_arrests','arrest_rate_percentage'])
print("Driver Violation Trends Based on Age and Race",df_result15)

# Time Period Analysis of Stops (Joining with Date Functions),
# - Number of Stops by Year,Month, Hour of the Day
# Query 17
query17 = """ SELECT
    YEAR(stop_date) AS stop_year,
    MONTH(stop_date) AS stop_month,
    HOUR(stop_time) AS stop_hour,
    COUNT(*) AS total_stops
    FROM traffic_stops
    GROUP BY stop_year, stop_month, stop_hour
    ORDER BY stop_year, stop_month, stop_hour; """
cursor.execute(query17)
result16 = cursor.fetchall()
df_result16 = pd.DataFrame(result16, columns=['stop_year','stop_month','stop_hour','total_stops'])
print("Time Period Analysis of Stops with date function",df_result16)

# Violations with High Search and Arrest Rates (Window Function)
# Query 18
query18 = """ SELECT violation, total_violations, total_searches, total_arrests ,
    ROUND((total_searches / total_violations) * 100, 2) AS search_rate_percentage,
    ROUND((total_arrests / total_violations) * 100, 2) AS arrest_rate_percentage
    FROM (
        SELECT violation,
            COUNT(*) AS total_violations,
            SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) AS total_searches,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
            RANK() OVER (ORDER BY SUM(CASE WHEN search_conducted = TRUE THEN 1 ELSE 0 END) DESC,
            SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) DESC) AS rnk
        FROM traffic_stops
        GROUP BY violation
    ) AS violation_data
    WHERE rnk <= 5; """
cursor.execute(query18)
result17 = cursor.fetchall()
df_result17 = pd.DataFrame(result17, columns=['violation','total_violations','total_searches','total_arrests','search_rate_percentage','arrest_rate_percentage'])
print("Violations with High Search and Arrest Rates",df_result17)
    
# Driver Demographics by Country (Age, Gender, and Race)
# Query 19
query19 = """ SELECT country_name, driver_age, driver_gender, driver_race,
    COUNT(*) AS total_drivers,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND((SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY country_name, driver_age, driver_gender, driver_race
    ORDER BY total_drivers DESC 
    LIMIT 10; """   
cursor.execute(query19)
result18 = cursor.fetchall()
df_result18 = pd.DataFrame(result18, columns=['country_name','driver_age','driver_gender','driver_race','total_drivers','total_arrests','arrest_rate_percentage'])
print("Driver Demographics by Country (Age, Gender, and Race)",df_result18)

# Top 5 Violations with Highest Arrest Rates
# Query 20
query20 = """ SELECT violation,
    COUNT(*) AS total_violations,
    SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) AS total_arrests,
    ROUND((SUM(CASE WHEN is_arrested = TRUE THEN 1 ELSE 0 END) / COUNT(*)) * 100, 2) AS arrest_rate_percentage
    FROM traffic_stops
    GROUP BY violation
    HAVING total_violations > 50
    ORDER BY arrest_rate_percentage DESC
    LIMIT 5; """
cursor.execute(query20)
result19 = cursor.fetchall()
df_result19 = pd.DataFrame(result19, columns=['violation','total_violations','total_arrests','arrest_rate_percentage'])
print("Top 5 Violations with Highest Arrest Rates",df_result19)


# Creating a streamlit app to display the results

import streamlit as st
import pandas as pd
st.set_page_config(page_title="Police Secure Check", page_icon="🚨", layout="wide")
st.header("🚨 Police Secure Check - Traffic Stops Analysis")
st.write("This Project is useful for analyzing traffic stop data and predicting outcomes based on various factors.")

# Advanced queries results display
st.header("📊 Advanced Queries Results")

selected_query = st.selectbox("select the Query to display results", 
    ["Top 10 vehicle numbers involved in drug-related traffic stops",
    "Most Frequently searched vehicle",
    "Top 5 Driver age group having Hihgest arrest rate",
    "Gender distribution of drivers stopped in each country",
    "Highest search rate with race and Gender combination",
    "Time of day that sees the most traffic stops",
    "Time of day that sees the most traffic stops",
    "Are stops during the night more likely to lead to arrests?",
    "Violations most associated with searches or arrests",
    "Most common violations among younger drivers (<25)",
    "Violations that rarely result in search or arrest",
    "Countries reporting the highest rate of drug-related stops",
    "Arrest rate by country and violation",
    "Countries with the most stops with search conducted",
    "Yearly Breakdown of Stops and Arrests by Country",
    "Driver Violation Trends Based on Age and Race",
    "Time Period Analysis of Stops with date function",
    "Violations with High Search and Arrest Rates",
    "Driver Demographics by Country (Age, Gender, and Race)",
    "Top 5 Violations with Highest Arrest Rates"])
query_map = {
    "Top 10 vehicle numbers involved in drug-related traffic stops": df_result,
    "Most Frequently searched vehicle": df_result1,
    "Top 5 Driver age group having Hihgest arrest rate": df_result2,
    "Gender distribution of drivers stopped in each country": df_result3,
    "Highest search rate with race and Gender combination": df_result4,
    "Time of day that sees the most traffic stops": df_result5,
    "Time of day that sees the most traffic stops": df_result6,
    "Are stops during the night more likely to lead to arrests?": df_result7,
    "Violations most associated with searches or arrests": df_result8,
    "Most common violations among younger drivers (<25)": df_result9,
    "Violations that rarely result in search or arrest": result10,
    "Countries reporting the highest rate of drug-related stops": df_result11,
    "Arrest rate by country and violation": df_result12,
    "Countries with the most stops with search conducted": df_result13,
    "Yearly Breakdown of Stops and Arrests by Country": df_result14,
    "Driver Violation Trends Based on Age and Race": df_result15,
    "Time Period Analysis of Stops with date function": df_result16,
    "Violations with High Search and Arrest Rates": df_result17,
    "Driver Demographics by Country (Age, Gender, and Race)": df_result18,
    "Top 5 Violations with Highest Arrest Rates": df_result19,
    }

if st.button("Show Results"):
    result_to_display = query_map[selected_query]
    if isinstance(result_to_display, pd.DataFrame):
        st.dataframe(result_to_display)
    else:
        st.text(result_to_display)
        
# Predictive Analysis Section
st.header("📝 Add New Police log & Predict Outcome and Violation Type")
with st.form("New log form "):
    country_name = st.selectbox("Country Name",["Canada","India","USA"])
    vehicle_number = st.text_input("Vehicle Number")
    driver_gender = st.selectbox("Driver Gender",["M","F"])
    driver_age = st.number_input("Driver Age", min_value=16, max_value=99)
    search_conducted = st.selectbox("Search Conducted",[0,1])
    stop_duration = st.selectbox("Stop Duration",["0-15 Min","16-30 Min","30+ Min"])
    drugs_related_stop = st.selectbox("Drugs Related Stop", [0,1])
    stop_date = st.date_input("Stop Date")
    stop_time = st.text_input("Stop Time (HH:MM)")
    submitted = st.form_submit_button("Predict stop outcome and violation type")

if submitted:
    filtered_data = df[
         (df['country_name'] == country_name)&
         (df['vehicle_number'] == vehicle_number)&
         (df['driver_gender'] == driver_gender) &
         (df['driver_age'] == driver_age) &
         (df['search_conducted'] == int(search_conducted))& 
         (df['stop_duration'] == stop_duration)&
         (df['drugs_related_stop'] == drugs_related_stop)]
    
# Predicted stop outcome:
    if not filtered_data.empty:
        Predicted_outcome = filtered_data['stop_outcome'].mode()[0]
        Predicted_violation = filtered_data['violation'].mode()[0]
    else:
        Predicted_outcome = "warning"
        Predicted_violation = "Speeding"    
    
    search_text = "A Search was conducted" if int(search_conducted) else "No search was conducted"
    drug_text = "was drug related" if int(drugs_related_stop) else "was not drug related"
     
# Formatting the Date and Time
    st.markdown(f"""**Predicted Summary:**
- **Predicted violation:** {Predicted_violation}
- **Predicted Stop Outcome:** {Predicted_outcome}
A {driver_age} years old {driver_gender} driver in {country_name} with vehicle {vehicle_number} was stopped at {stop_time} on {stop_date}. {search_text}; the stop {drug_text}.
    """)
cursor.close()
conn.close()
