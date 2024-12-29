# Rhythm Flow

The Rhythm Flow app [(Demo)](https://rhythmflow.streamlit.app/) is a music analytics platform that integrates the Spotify API with Snowflake, Airflow, Kafka, PySpark, Streamlit, and Tableau. It collects and processes user data in real-time, leveraging Kafka for streaming and Airflow for orchestration. Data is stored and analyzed using Snowflake, and the results are visualized in a Streamlit web interface. This app provides insights into user listening habits, playlists, and top tracks, offering a seamless experience to explore and analyze music data dynamically.


---

## System Architecture
![arch](https://github.com/adarsh-k-tiwari/Spotify/blob/main/architecture.png?raw=true)

---
## Snowflake Tables Relationship
![table](https://github.com/adarsh-k-tiwari/Spotify/blob/main/snowflake_schema.png?raw=true)

## Features

- **Chart Surge**: Get real-time top 10 global songs by popularity.
- **TuneMender**: Get recommendation of 5 songs based on your favorite songs.
- **AI Harmonizer**: AI-generated personalized playlists using Phi-3.5-Instruct model.
- **LyricScope**: Generate summary of song from on its lyrics.
- **Music Insights**: A Tableau Dashboard for visalizing the trends of albums, artists, and tracks.

---
## Folder Structure

```
.Spotify/
|-- .streamlit/
|   |-- config.toml                    # Streamlit configuration file
|
|-- airflow/
|   |-- config/
|   |   | airflow.cfg                  # Airflow configuration file
|   |-- dags/
|   |   | spotify_dag.py               # Airflow DAG file
|   |   | spotify_kafka_producer.py    # Kafka Producer File
|   |   | spotify_kafka_consumer.py    # Kafka Consumer File
|   |-- logs/                          # Airflow logs folder
|   |-- plugins/                       # Airflow plugins folder
|   |-- .env                           # Airflow environment variable file
|   |-- docker-compose.yaml            # Airflow docker-compose file
|   |-- Dockerfile                     # Airflow DockerFile
|
|-- app.py                             # Main application code
|-- requirements.txt                   # Python dependencies
|-- .env                               # Environment variables (not pushed to GitHub)
|-- .gitignore                         # Files and folders to exclude from version control
|-- README.md                          # Project documentation (this file)
|-- LICENSE                            # Project's MIT License
```

---

## Installation (Locally)

1. Clone this repository:
   ```bash
   git clone <repository_url>
   cd final
   ```

2. Build the Docker Image:
   ```bash
   docker-compose -d --build
   ```

3. Set up your environment variables:
   - Create a `.env` file in the root directory.
   - Add the following keys:
     ```
     SNOWFLAKE_USER={user_id}
     SNOWFLAKE_PASSWORD={user_password}
     SNOWFLAKE_ACCOUNT={account_id}
     SNOWFLAKE_DATABASE={database_name}
     SNOWFLAKE_SCHEMA={table_name} 
     HF_KEY= {your_huggingface_api_key}
     SPOTIFY_CLIENT_ID={your_spotify_client_id}
     SPOTIFY_CLIENT_SECRET={your_spotify_client_secret}
     ```
---

## Usage
1. Run the Docker container.
2. Open the app in your browser at [http://localhost:8080](http://localhost:8080).
3. Click on the DAGs in Airflow UI, and trigger the DAGs.
4. Once the data pipeline is executed and the data is refreshed, run the `streamlit` application.
---

## API Configuration

This project uses the following APIs:

1. **Spotify API**: To fetch tracks, artists, and albums details.
2. **Hugging Face API**: For text generation using AI models.

   `Make sure to obtain API keys for all three services and configure them in your `.env` file.`

---

## Dependencies

All dependencies are listed in the `requirements.txt` file.
- `streamlit`: For building the web app
- `requests`: For making API calls
- `openai`: For AI model integrations
- `python-dotenv`: For managing environment variables
- `apache-airflow-providers-apache-kafka`: Provider for Airflow to use Kafka
- `apache-airflow-providers-snowflake`: Provider for Airflow to use Snowflake
---

## Future Enhancements

- Add more tables
- Create production ready tables - RAW tables, WIP tables, PUB tables on AWS or Azure to load the data as it is and tranform it prior to pushing to Final tables in Snowflake.
- Use Spark to perform data transformation.
- Create another table for reporting.
- Enhance UI with dynamic visualizations.

---

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

---

## Acknowledgments

- [Streamlit](https://streamlit.io/)
- [Hugging Face](https://huggingface.co/)
- [Airflow](https://airflow.apache.org/)
- [Snowflake](https://www.snowflake.com/en/)
- [Kafka](https://kafka.apache.org/)
- [Freepik](https://www.freepik.com/) for background images


---
Feel free to contribute by submitting issues or pull requests!
