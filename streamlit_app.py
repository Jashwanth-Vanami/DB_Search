import streamlit as st
import os
import pandas as pd
from dotenv import load_dotenv
from SchemaGenerator import SchemaGenerator
from dbsearch import DBSearch

load_dotenv()

st.title("SQL Query Generator and Executor")

# Currently only MySQL is implemented.
db_type = st.selectbox("Select Database Type", ["mysql"])
user_prompt_text = st.text_area("Enter your query prompt:")

if st.button("Generate and Execute Query"):
    try:
        st.info("Starting DBSearch...")
        db_search = DBSearch(db_type)
        result = db_search.run(user_prompt_text)
        
        st.subheader("Generated SQL Query:")
        st.code(result["generated_query"], language="sql")
        
        if result["latency"] is not None and result["usage"] is not None:
            tokens = result["usage"].get("total_tokens", 0)
            cost = tokens / 1000 * 0.002  # example cost calculation
            st.write(f"**Latency:** {result['latency']:.2f} seconds, **Cost:** ${cost:.5f} ({tokens} tokens)")
        else:
            st.write("**Latency:** N/A")
        
        st.subheader("Query Results:")
        if result["rows"]:
            df = pd.DataFrame(result["rows"], columns=result["columns"])
            st.write(df)
        else:
            st.info("The query executed successfully but returned no results.")
    except Exception as e:
        st.error(f"An error occurred: {e}")