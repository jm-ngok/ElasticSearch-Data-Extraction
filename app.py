import streamlit as st
from elasticsearch import Elasticsearch
import pandas as pd
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get Elasticsearch credentials from .env
ES_HOST = os.getenv("ES_HOST")
ES_USERNAME = os.getenv("ES_USER")
ES_PASSWORD = os.getenv("ES_PASSWORD")

# Streamlit UI
st.title("Elasticsearch Data Extractor")

# Validate credentials
if not ES_HOST or not ES_USERNAME or not ES_PASSWORD:
    st.error("Elasticsearch credentials are missing. Please check your .env file.")
else:
    # Connect to Elasticsearch
    try:
        es = Elasticsearch([ES_HOST], basic_auth=(ES_USERNAME, ES_PASSWORD))

        # Button to fetch indices
        if st.button("Fetch Indices"):
            try:
                indices = list(es.indices.get_alias().keys())  # Get list of indices
                if indices:
                    st.session_state["indices"] = indices
                    st.success(f"Found {len(indices)} indices.")
                else:
                    st.warning("No indices found.")
            except Exception as e:
                st.error(f"Error fetching indices: {e}")

        # Show dropdown if indices are available
        if "indices" in st.session_state:
            index_name = st.selectbox("Select Index", st.session_state["indices"])
        else:
            index_name = None

        if index_name:
            fetch_button = st.button("Fetch Data")

            if fetch_button:
                try:
                    # Query to fetch all data
                    query = {"query": {"match_all": {}}, "size": 1000}

                    response = es.search(index=index_name, body=query, scroll="2m")
                    hits = response["hits"]["hits"]

                    # Convert data to DataFrame
                    data = [hit["_source"] for hit in hits]
                    df = pd.DataFrame(data)

                    # Display data
                    st.write(df)

                    # Download CSV button
                    csv = df.to_csv(index=False).encode()
                    st.download_button("Download CSV", csv, "elasticsearch_data.csv", "text/csv")

                except Exception as e:
                    st.error(f"Error fetching data: {e}")

    except Exception as e:
        st.error(f"Error connecting to Elasticsearch: {e}")
