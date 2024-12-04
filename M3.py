import pandas as pd
import streamlit as st
from pymongo import MongoClient

# MongoDB connection
client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string
db = client["Movies-DB"]  # Replace with your database name
collection = db["Movies-C"]  # Replace with your collection name

# MongoDB Queries for Top 10 Emerging Stars and Genre Trends
pipeline_actors = [
    {
        "$project": {
            "cast": [
                {"name": "$actor_1_name", "facebook_likes": "$actor_1_facebook_likes"},
                {"name": "$actor_2_name", "facebook_likes": "$actor_2_facebook_likes"},
                {"name": "$actor_3_name", "facebook_likes": "$actor_3_facebook_likes"}
            ],
            "rating": "$imdb_score"
        }
    },
    {"$unwind": "$cast"},
    {
        "$group": {
            "_id": "$cast.name",
            "average_rating": {"$avg": "$rating"},
            "total_movies": {"$sum": 1},
            "total_likes": {"$sum": {"$ifNull": ["$cast.facebook_likes", 0]}}
        }
    },
    {
        "$match": {
            "average_rating": {"$gte": 7.0},
            "total_movies": {"$gte": 2}
        }
    },
    {"$sort": {"total_likes": -1}},
    {"$limit": 10}
]

pipeline_genres = [
    {
        "$project": {
            "title_year": 1,
            "genres": {"$split": ["$genres", "|"]}
        }
    },
    {"$unwind": "$genres"},
    {
        "$group": {
            "_id": {"genre": "$genres", "year": "$title_year"},
            "movie_count": {"$sum": 1}
        }
    },
    {"$sort": {"_id.year": 1, "movie_count": -1}}
]

# MongoDB query to filter movies based on selected conditions
def build_filter_query(imdb_min, year, genre, min_budget, max_budget):
    query = {}
    
    # Filter by IMDb score
    if imdb_min:
        query["imdb_score"] = {"$gte": imdb_min}
    
    # Filter by year
    if year:
        query["title_year"] = year
    
    # Filter by genre (using regular expression for partial matches)
    if genre:
        query["genres"] = {"$regex": genre, "$options": "i"}
    
    # Filter by budget range
    if min_budget and max_budget:
        query["budget"] = {"$gte": min_budget, "$lte": max_budget}
    
    return query

# Function to Add Movie to the Database
def add_movie():
    st.subheader("Add a New Movie")
    
    title = st.text_input("Movie Title")
    imdb_score = st.number_input("IMDb Score", 0.0, 10.0, 7.0)
    budget = st.number_input("Budget ($)", 0, 1000000000, 50000000)
    genre = st.text_input("Genre (comma separated for multiple genres)")
    actors = st.text_area("Actors (comma separated)")
    
    if st.button("Add Movie", key="add_movie_button"):
        if title and imdb_score and budget and genre and actors:
            movie_data = {
                "movie_title": title,
                "imdb_score": imdb_score,
                "budget": budget,
                "genres": genre.split(","),
                "actor_1_name": actors.split(",")[0] if len(actors.split(",")) > 0 else "",
                "actor_2_name": actors.split(",")[1] if len(actors.split(",")) > 1 else "",
                "actor_3_name": actors.split(",")[2] if len(actors.split(",")) > 2 else ""
            }
            collection.insert_one(movie_data)
            st.success(f"Movie '{title}' added successfully!")

# Function to Update Movie in the Database
def update_movie():
    st.subheader("Update Movie Details")
    
    movie_id = st.text_input("Enter Movie Title or ID to Update")
    field_to_update = st.selectbox("Select Field to Update", ["IMDb Score", "Budget", "Genres"])
    
    new_value = None
    if field_to_update == "IMDb Score":
        new_value = st.number_input("New IMDb Score", 0.0, 10.0, 7.0)
    elif field_to_update == "Budget":
        new_value = st.number_input("New Budget ($)", 0, 1000000000, 50000000)
    elif field_to_update == "Genres":
        new_value = st.text_input("New Genre(s) (comma separated)")
    
    if st.button("Update Movie", key="update_movie_button"):
        if movie_id and new_value:
            query = {"movie_title": movie_id} if not movie_id.isdigit() else {"_id": int(movie_id)}
            update_data = {f"${field_to_update.lower().replace(' ', '_')}": new_value}
            result = collection.update_one(query, {"$set": update_data})
            if result.matched_count > 0:
                st.success(f"Movie '{movie_id}' updated successfully!")
            else:
                st.warning("No movie found with that title or ID.")

# Function to Delete Movie from the Database
def delete_movie():
    st.subheader("Delete Movie")
    
    movie_id = st.text_input("Enter Movie Title or ID to Delete")
    
    if st.button("Delete Movie", key="delete_movie_button"):
        if movie_id:
            query = {"movie_title": movie_id} if not movie_id.isdigit() else {"_id": int(movie_id)}
            result = collection.delete_one(query)
            if result.deleted_count > 0:
                st.success(f"Movie '{movie_id}' deleted successfully!")
            else:
                st.warning("No movie found with that title or ID.")

# Streamlit App Layout
st.title("Movie Insights Dashboard")

# Create Tabs for User and Developer
tab = st.radio("Select a tab", ["Movie Insights", "Developer Tab"])

if tab == "Movie Insights":
    # Add Filter Section (Sidebar)
    st.sidebar.header("Filter Movies")
    imdb_min = st.sidebar.slider("IMDb Score", 0.0, 10.0, 7.5)
    year = st.sidebar.number_input("Year", min_value=1900, max_value=2023, value=2009)
    genre = st.sidebar.text_input("Genre", "")
    min_budget = st.sidebar.number_input("Min Budget (in $)", min_value=0, value=50000000)
    max_budget = st.sidebar.number_input("Max Budget (in $)", min_value=0, value=200000000)

    # Build the query based on the selected filters
    query = build_filter_query(imdb_min, year, genre, min_budget, max_budget)

    # Execute query for filtered movies
    filtered_movies = list(collection.find(query, {"_id": 0, "movie_title": 1, "imdb_score": 1, "title_year": 1, "genres": 1, "budget": 1}))

    # Display filtered movie results
    if filtered_movies:
        st.subheader("Filtered Movies")
        filtered_movies_df = pd.DataFrame(filtered_movies)
        st.dataframe(filtered_movies_df)
    else:
        st.write("No movies found matching the selected filters.")

    # Buttons for navigation (using buttons for separate sections)
    st.sidebar.header("Sections")
    show_actors = st.sidebar.button("Top 10 Emerging Stars", key="actors_button")
    show_genres = st.sidebar.button("Genre Trends Over Time", key="genres_button")

    if show_actors:
        # Execute aggregation query for Top 10 Emerging Stars
        results_actors = list(collection.aggregate(pipeline_actors))

        # Process results and display
        if results_actors:
            data_actors = pd.DataFrame(results_actors)
            data_actors.rename(columns={
                "_id": "Actor Name", "average_rating": "Average Rating",
                "total_movies": "Total Movies", "total_likes": "Total Likes"
            }, inplace=True)
            st.subheader("Top 10 Emerging Stars")
            st.dataframe(data_actors)  # Display data table
            st.bar_chart(data_actors.set_index("Actor Name")["Total Likes"])
        else:
            st.write("No data available for Top 10 Emerging Stars.")

    elif show_genres:
        # Execute aggregation query for Genre Trends Over Time
        results_genres = list(collection.aggregate(pipeline_genres))

        # Process results and display
        if results_genres:
            data_genres = pd.DataFrame(results_genres)
            data_genres["Genre"] = data_genres["_id"].apply(lambda x: x["genre"])
            data_genres["Year"] = data_genres["_id"].apply(lambda x: x["year"])
            data_genres.rename(columns={"movie_count": "Movie Count"}, inplace=True)
            data_genres.drop("_id", axis=1, inplace=True)
            st.subheader("Genre Trends Over Time")
            st.dataframe(data_genres)  # Display data table
            genre_trends_pivot = data_genres.pivot(index="Year", columns="Genre", values="Movie Count").fillna(0)
            st.bar_chart(genre_trends_pivot)
        else:
            st.write("No data available for Genre Trends.")

elif tab == "Developer Tab":
    # Developer Tab for CRUD operations
    st.sidebar.header("CRUD Operations")
    show_add = st.sidebar.button("Add Movie", key="add_button")
    show_update = st.sidebar.button("Update Movie", key="update_button")
    show_delete = st.sidebar.button("Delete Movie", key="delete_button")

    if show_add:
        add_movie()  # Call the function to add a movie

    elif show_update:
        update_movie()  # Call the function to update movie details

    elif show_delete:
        delete_movie()  # Call the function to delete a movie