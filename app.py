# Importing necessary libraries
import streamlit as st
import pandas as pd
import snowflake.connector
from dotenv import load_dotenv
import os
import requests
import openai

# Load environment variables
load_dotenv()

# Set page title and icon
st.set_page_config(
    page_title="RhythmFlow",
    page_icon=":material/music_note:",
    layout="wide"
)

# Snowflake connection setup
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    return conn
conn = connect_to_snowflake()

# Fetch top 50 songs from Snowflake
def fetch_top_10_songs():
    conn = connect_to_snowflake()
    query = """
    SELECT track_name as "Track Name", 
    track_url as "Track URL",
    artist_name as "Artist Name",
    album_name as "Album Name",
    round(duration_ms/60000, 2) as "Duration (mins)",
    popularity as "Popularity"
    FROM tracks
    ORDER BY popularity DESC
    LIMIT 10;
    """
    df = pd.read_sql(query, conn)
    conn.close()

    # Create clickable links for the track_url column
    df['Track URL'] = df['Track URL'].apply(
        lambda x: f'<a href="{x}" target="_blank">Listen</a>' if pd.notnull(x) else "No URL"
    )
    return df

def get_song_recommendations(api_key, song_title):
    # Build the pompt
    prompt = f"Give me 5 songs recommendation for - {song_title}. Only give me the list of songs. Don't put extra characters"
    client = openai.OpenAI(base_url="https://api-inference.huggingface.co/v1/",	api_key=api_key)
    messages = [
        {
            "role": "user",
            "content": prompt
        }
    ]
    completion = client.chat.completions.create(
        model="microsoft/Phi-3-mini-4k-instruct", 
        messages=messages, 
        max_tokens=500
    )

    return (completion.choices[0].message.content)

def generate_ai_playlist(api_key, song_title):
    # Build the pompt
    prompt = f"Generate a playlist of 10 songs including the songs in the list - {song_title}. And add similar songs to make the total songs equal to 10."
    client = openai.OpenAI(base_url="https://api-inference.huggingface.co/v1/",	api_key=api_key)
    messages = [
        {
            "role": "user",
            "content": prompt
        }
    ]
    completion = client.chat.completions.create(
        model="microsoft/Phi-3-mini-4k-instruct", 
        messages=messages, 
        max_tokens=500
    )

    return (completion.choices[0].message.content)

def generate_song_summary(api_key, song_title, artist_name):
    # Build the pompt
    prompt = f"Generate a creative summary text about the song - {song_title} sung by {artist_name}. Write in less than 100 words"
    client = openai.OpenAI(base_url="https://api-inference.huggingface.co/v1/",	api_key=api_key)
    messages = [
        {
            "role": "user",
            "content": prompt
        }
    ]
    completion = client.chat.completions.create(
        model="microsoft/Phi-3-mini-4k-instruct", 
        messages=messages, 
        max_tokens=500
    )

    return (completion.choices[0].message.content)
    
    

def load_tableau_dashboard():
    # st.iframe(src="https://public.tableau.com/views/SpotifyAnalytics/Dashboard1?:embed=y&:display_count=yes")
    return None


# Add CSS to set background image
def set_background(image_url):
    st.markdown(f"""
    <style>
        .stApp{{
        background-image: url("{image_url}");
        background-position: center;
        background-repeat: no-repeat;
        background-size: cover;
        top: 0;
        left: 0;
        width: 100%;
        height: 100%;
        }}
        [data-testid="stHeader"]{{
        background-color: transparent;}}

        table.dataframe {{
            font-family: Arial, sans-serif;
            font-size: 0.8em
            border-collapse: collapse;
        }}
        table.dataframe td, table.dataframe th {{
            border: 1px solid #ddd;
            padding: 8px;
        }}
        table.dataframe tr:nth-child(even) {{
            background-color: #573aa8;
        }}
        table.dataframe tr:nth-child(odd), table.dataframe th {{
            background-color: #27013e;
        }}
        table.dataframe th {{
            text-align: center;
        }}
    </style>
""", unsafe_allow_html=True)
    
main_bg_url = "https://i.postimg.cc/CxR29DTW/bg-spotify-Mesa-de-trabajo-1.png"
set_background(main_bg_url)

def main():
    st.markdown('<h1 style="text-align: left"><span style="font-size:2em; color:#ff4361; align:center;font-weight: 700;  font-family: Poppins, sans-serif; font-style: normal;">  R H Y T H M &ensp; F L O W</span></h1>', unsafe_allow_html=True)

    huggingFace_api_key = os.getenv("HF_KEY")
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Documentation","Chart Surge","TuneMender", "AI Harmonizer", "LyricScope", "Music Insights"])
    
    with tab1:
        st.header("Introduction")
        st.write("The Rhythm Flow app is a music analytics platform that integrates the Spotify API with Snowflake, Airflow, Kafka, Streamlit, and Tableau. It collects and processes user data in real-time, leveraging Kafka for streaming and Airflow for orchestration. Data is stored and analyzed using Snowflake, and the results are visualized in a Streamlit web interface. This app provides insights into user listening habits, playlists, and top tracks, offering a seamless experience to explore and analyze music data dynamically.")

        st.header("Features of App")
        st.markdown("- Chart Surge: Get real-time top 10 global songs by popularity.
        - TuneMender: Get recommendation of 5 songs based on your favorite songs.
        - AI Harmonizer: AI-generated personalized playlists using Phi-3.5-Instruct model.
        - LyricScope: Generate summary of song from on its lyrics.
        - Music Insights: A Tableau Dashboard for visalizing the trends of albums, artists, and tracks.")
        
    with tab2:
        st.header("Chart Surge")
        try:
            top_songs = fetch_top_10_songs()
            st.markdown(
            top_songs.to_html(escape=False, index=False),
            unsafe_allow_html=True
        )
        except Exception as e:
            st.error(f"Error fetching songs: {e}")
    

    with tab3:
        st.header("TuneMender")
        with st.form(key="recommendation_form", border=False):
            song = st.text_input("Enter your favorite song")
            submit = st.form_submit_button("Submit")
            if submit:
                generated_text = get_song_recommendations(huggingFace_api_key, song)
                st.markdown(f"""<div style="text-align: justify;background-color: #27013e; padding: 20px; border-radius: 10px;">{generated_text}</div>
                    """, unsafe_allow_html=True)
    
    with tab4:
        st.header("AI Harmonizer")
        with st.form(key="ai_playlist_form", border=False):
            song = st.text_input("Enter your 5 favorite songs")
            submit = st.form_submit_button("Submit")
            if submit:
                generated_text = generate_ai_playlist(huggingFace_api_key, song)
                st.markdown(f"""<div style="text-align: justify;background-color: #27013e; padding: 20px; border-radius: 10px;">{generated_text}</div>
                    """, unsafe_allow_html=True)
    
    with tab5:
        st.header("LyricScope")
        with st.form(key="summary_form", border=False):
            song = st.text_input("Enter your favorite song")
            artist = st.text_input("Enter your favorite song's artist")
            submit = st.form_submit_button("Submit")
            if submit:
                generated_text = generate_song_summary(huggingFace_api_key, song, artist)
                st.markdown(f"""<div style="text-align: justify;background-color: #27013e; padding: 20px; border-radius: 10px;">{generated_text}</div>
                    """, unsafe_allow_html=True)

    with tab6:
        st.header("Music Insights")
        st.write("Comming Soon!!")
    


    
if __name__ == "__main__":
    main()
