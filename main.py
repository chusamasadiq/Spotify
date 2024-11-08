import sys
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
import pyodbc


def check_data(df: pd.DataFrame) -> bool:
    """
    Validates the song data DataFrame.
    - Checks if the DataFrame is empty.
    - Ensures the 'played_at' column has unique values.
    - Checks for any null values.

    Parameters:
        df (pd.DataFrame): The DataFrame containing song data.

    Returns:
        bool: True if all checks pass, False otherwise.
    """
    if df.empty:
        print("No songs imported")
        return False

    if not df['played_at'].is_unique:
        print("Error: Duplicates found")
        return False

    if df.isnull().values.any():
        print("Error: Null data found")
        return False

    return True


def retrieve_recently_played(sp) -> pd.DataFrame:
    """
    Fetches and processes the user's recently played songs from Spotify.

    Parameters:
        sp (spotipy.Spotify): The authenticated Spotify client.

    Returns:
        pd.DataFrame: A DataFrame containing song name, artist name, and play timestamp.
    """
    # Retrieve recently played songs with a limit of 10
    data = sp.current_user_recently_played(limit=10)

    # Parse data and store song details in a dictionary
    song_info = {
        "song_name": [song["track"]["name"] for song in data["items"]],
        "artist_name": [song["track"]["album"]["artists"][0]["name"] for song in data["items"]],
        "played_at": [song["played_at"] for song in data["items"]],
    }

    # Convert dictionary to DataFrame for easier data manipulation and validation
    song_df = pd.DataFrame(song_info)
    return song_df


# Save Data in Database
def save_data(song_df):
    # Load Stage
    server = 'mssql.chester.network'
    database = 'db_2313527_lab05'
    # Create variables to store the server and database info

    connection_string = pyodbc.connect(
        'DRIVER={SQL Server}; SERVER=' + server + '; DATABASE=' + database + '; UID=user_db_2313527_lab05; PWD=@Laptop830;')
    # Define the connection string

    cursor = connection_string.cursor()
    # Create the connection cursor

    print("Opened database successfully")

    for index, row in song_df.iterrows():
        cursor.execute("INSERT INTO td_songs (song_name, artist_name, played_at) VALUES (?, ?, ?)",
                       row.song_name, row.artist_name, row.played_at)
        # Loop through the DataFrame and insert each row into the specified columns

    connection_string.commit()
    # Confirm the changes

    connection_string.close()
    print("Closed database successfully")
    # Close the connection to the database


def main():
    """
    Main function to authenticate Spotify, retrieve recently played songs,
    validate the data, and print the results.
    """
    # Spotify API credentials and required scope
    client_id = '56e64b54a071478aa654c021301b01ce'
    client_secret = '70bfa81f44344cd19bff7f962afdba4e'
    redirect_uri = 'http://localhost/'
    scope = 'user-library-read user-read-recently-played'

    try:
        # Authenticate and create Spotify client using SpotifyOAuth
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(client_id=client_id,
                                                       client_secret=client_secret,
                                                       redirect_uri=redirect_uri,
                                                       scope=scope))
        print("Token created successfully")

        # Retrieve and display recently played songs as a DataFrame
        song_df = retrieve_recently_played(sp)
        print(song_df)

        # Validate the retrieved data using the check_data function
        if check_data(song_df):
            print("All checks passed")
            save_data(song_df)


    except Exception as e:
        # Print error message with exception details
        print("An error occurred:", e, sys.exc_info()[0])


# Entry point for script execution
if __name__ == "__main__":
    main()
