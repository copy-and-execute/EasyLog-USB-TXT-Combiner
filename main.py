import os
import pandas as pd

def read_and_parse_txt_file(file_path):
    try:
        # csv einlesen
        df = pd.read_csv(file_path, skiprows=1, encoding='ISO-8859-1')
        # columns renamen
        df.columns = ['id', 'timestamp', 'temperature', 'serial_number']
        
        # id und serial number entfernen
        df = df.drop(columns=['id', 'serial_number'], errors='ignore')
        
        return df
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return pd.DataFrame()

def process_directory(directory_path, output_file):
    all_data = []
    
    # nach dateien scannen
    for filename in os.listdir(directory_path):
        if filename.endswith(".txt"):
            file_path = os.path.join(directory_path, filename)
            df = read_and_parse_txt_file(file_path)
            
            if not df.empty:
                # datei hinzufügen
                all_data.append(df)
    
    if all_data:
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # timestamp zu datetime
        combined_data['timestamp'] = pd.to_datetime(combined_data['timestamp'], errors='coerce')
        
        # Nach Timestamp sortieren
        sorted_data = combined_data.sort_values(by='timestamp')
        
        # IDs resetten
        sorted_data = sorted_data.reset_index(drop=True)
        sorted_data.insert(0, 'id', sorted_data.index + 1)  # mit IDs von 1 starten
        
        # CSV Datei speichern
        sorted_data[['id', 'timestamp', 'temperature']].to_csv(output_file, index=False)
        print(f"Kombiniert und gespeichert: {output_file}")
    else:
        print("Keine Datei gefunden.")

# Verzeichnis mit TXT-Dateien
directory_path = './data'

# CSV Datei-Name
output_file = './easylogusb-combined.csv'

process_directory(directory_path, output_file)
