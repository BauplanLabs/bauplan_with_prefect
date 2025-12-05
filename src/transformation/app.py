import streamlit as st
import bauplan
import polars as pl

# Initialize Bauplan client
client = bauplan.Client()
user_name = client.info().user.username
branch_name = f"{user_name}.taxi_zones_prefect"

st.title("NYC Taxi Trip Miles by Borough")
st.write(f"Querying data from the `my_child` table in branch '{branch_name}' using Bauplan")



# Query the my_child table
with st.spinner("Fetching data from Bauplan..."):
    try:
        result = client.query(
            query="""
                SELECT Borough, SUM(trip_miles) as total_miles
                FROM my_child
                WHERE Borough IS NOT NULL
                GROUP BY Borough
                ORDER BY total_miles DESC
            """,
            ref=branch_name,
        )

        # Convert to Polars DataFrame for easier manipulation
        df = pl.from_arrow(result)

        st.success(f"Fetched {len(df)} boroughs from branch '{branch_name}'")

        # Display the data
        st.subheader("Total Miles per Borough")
        st.dataframe(df)

        # Create a bar chart
        st.subheader("Miles by Borough")
        st.bar_chart(df, x="Borough", y="total_miles")

    except Exception as e:
        st.error(f"Error querying Bauplan: {str(e)}")
        st.info("Make sure the pipeline has run and the my_child table exists")
