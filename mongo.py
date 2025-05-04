import streamlit as st
import pymongo
import pandas as pd
import json
from pymongo import MongoClient
from bson import json_util
import base64
import re

# Set page configuration
st.set_page_config(
    page_title="MongoDB Explorer",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add custom CSS
st.markdown("""
    <style>
    .main {
        padding: 1rem;
    }
    .stApp {
        max-width: 1200px;
        margin: 0 auto;
    }
    h1, h2, h3 {
        color: #0066cc;
    }
    .mongo-header {
        background-color: #00684A;
        color: white;
        padding: 1rem;
        border-radius: 5px;
        margin-bottom: 1rem;
    }
    .stats-container {
        background-color: #f5f5f5;
        padding: 1rem;
        border-radius: 5px;
        margin-bottom: 1rem;
    }
    </style>
    """, unsafe_allow_html=True)

# Header
st.markdown("<div class='mongo-header'><h1>MongoDB Explorer</h1></div>", unsafe_allow_html=True)

# Connection function
@st.cache_resource
def get_mongo_client(connection_string):
    try:
        return MongoClient(connection_string)
    except Exception as e:
        st.error(f"Connection error: {e}")
        return None

# Connection details in sidebar
with st.sidebar:
    st.header("MongoDB Connection")
    connection_method = st.radio("Connection Method", ["Connection String", "Individual Parameters"])
    
    if connection_method == "Connection String":
        connection_string = st.text_input("Connection String", "mongodb://localhost:27017/", type="password")
        use_connection_string = True
    else:
        host = st.text_input("Host", "localhost")
        port = st.number_input("Port", value=27017, min_value=1, max_value=65535)
        username = st.text_input("Username (optional)")
        password = st.text_input("Password (optional)", type="password")
        auth_source = st.text_input("Auth Source (optional)", "admin")
        
        # Build connection string
        if username and password:
            connection_string = f"mongodb://{username}:{password}@{host}:{port}/?authSource={auth_source}"
        else:
            connection_string = f"mongodb://{host}:{port}/"
        use_connection_string = False
    
    connect_button = st.button("Connect")

# Initialize session state
if 'client' not in st.session_state:
    st.session_state.client = None
if 'db_selected' not in st.session_state:
    st.session_state.db_selected = None
if 'collection_selected' not in st.session_state:
    st.session_state.collection_selected = None
if 'query_results' not in st.session_state:
    st.session_state.query_results = None
if 'pagination_page' not in st.session_state:
    st.session_state.pagination_page = 1
if 'items_per_page' not in st.session_state:
    st.session_state.items_per_page = 10

# Connect to MongoDB
if connect_button:
    st.session_state.client = get_mongo_client(connection_string)
    if st.session_state.client:
        st.sidebar.success("Connected successfully!")
    else:
        st.sidebar.error("Connection failed.")

# Main interface - only show if connected
if st.session_state.client:
    # Database selection
    try:
        database_list = st.session_state.client.list_database_names()
        database_list = [db for db in database_list if db not in ['admin', 'local', 'config']]
        
        with st.sidebar:
            st.header("Database Selection")
            selected_db = st.selectbox("Select Database", database_list)
            
            if selected_db:
                st.session_state.db_selected = selected_db
                db = st.session_state.client[selected_db]
                collection_list = db.list_collection_names()
                
                st.header("Collection Selection")
                selected_collection = st.selectbox("Select Collection", collection_list)
                
                if selected_collection:
                    st.session_state.collection_selected = selected_collection
                    
                    # Define pagination controls
                    st.header("View Options")
                    st.session_state.items_per_page = st.slider("Items per page", 5, 100, 10)
                    
                    # Reset pagination when collection changes
                    if 'last_collection' not in st.session_state or st.session_state.last_collection != selected_collection:
                        st.session_state.pagination_page = 1
                        st.session_state.last_collection = selected_collection
    
        # Main area
        if st.session_state.db_selected and st.session_state.collection_selected:
            db = st.session_state.client[st.session_state.db_selected]
            collection = db[st.session_state.collection_selected]
            
            # Collection statistics
            st.header(f"Collection: {st.session_state.collection_selected}")
            
            # Stats in a nice container
            with st.expander("Collection Statistics", expanded=True):
                col1, col2, col3 = st.columns(3)
                
                doc_count = collection.count_documents({})
                col1.metric("Total Documents", doc_count)
                
                # Calculate storage size (approximate)
                stats = db.command("collstats", st.session_state.collection_selected)
                size_mb = round(stats.get("size", 0) / (1024 * 1024), 2)
                col2.metric("Collection Size", f"{size_mb} MB")
                
                # Get a sample document to count fields
                sample_doc = collection.find_one()
                if sample_doc:
                    field_count = len(sample_doc.keys()) - 1 if '_id' in sample_doc else len(sample_doc.keys())
                    col3.metric("Fields", field_count)
            
            # Query interface
            st.subheader("Query Documents")
            
            # Create tabs for different operations
            tab1, tab2, tab3, tab4 = st.tabs(["View Data", "Search", "Field Analysis", "Export"])
            
            with tab1:
                # Simple view of documents with pagination
                skip = (st.session_state.pagination_page - 1) * st.session_state.items_per_page
                cursor = collection.find().skip(skip).limit(st.session_state.items_per_page)
                
                # Convert to DataFrame
                docs = list(cursor)
                if docs:
                    # Get all unique keys from all documents
                    all_keys = set()
                    for doc in docs:
                        all_keys.update(doc.keys())
                    
                    # Create a DataFrame with all fields
                    df = pd.DataFrame(docs)
                    
                    # Convert ObjectId to strings
                    for col in df.columns:
                        if col == '_id':
                            df[col] = df[col].astype(str)
                    
                    # Show DataFrame
                    st.dataframe(df)
                    
                    # Pagination controls
                    col1, col2, col3 = st.columns([1, 3, 1])
                    with col1:
                        if st.button("Previous Page") and st.session_state.pagination_page > 1:
                            st.session_state.pagination_page -= 1
                            st.rerun()
                    
                    with col2:
                        total_pages = (doc_count + st.session_state.items_per_page - 1) // st.session_state.items_per_page
                        st.write(f"Page {st.session_state.pagination_page} of {total_pages}")
                    
                    with col3:
                        if st.button("Next Page"):
                            st.session_state.pagination_page += 1
                            st.rerun()
                else:
                    st.info("No documents found in this collection.")
            
            with tab2:
                st.subheader("Search and Filter")
                
                # Get all fields for the collection
                sample_doc = collection.find_one()
                if sample_doc:
                    fields = list(sample_doc.keys())
                    
                    # Create search interface
                    search_field = st.selectbox("Search field", fields)
                    search_type = st.radio("Search type", ["Exact match", "Contains", "Regex", "Greater than", "Less than"])
                    search_value = st.text_input("Search value")
                    
                    if st.button("Search"):
                        if search_value:
                            if search_type == "Exact match":
                                query = {search_field: search_value}
                            elif search_type == "Contains":
                                # For string fields
                                query = {search_field: {"$regex": search_value, "$options": "i"}}
                            elif search_type == "Regex":
                                try:
                                    query = {search_field: {"$regex": search_value}}
                                except re.error:
                                    st.error("Invalid regex pattern")
                                    query = {}
                            elif search_type == "Greater than":
                                try:
                                    # Try to convert to number if it looks like one
                                    if search_value.replace(".", "", 1).isdigit():
                                        val = float(search_value)
                                        query = {search_field: {"$gt": val}}
                                    else:
                                        query = {search_field: {"$gt": search_value}}
                                except:
                                    query = {search_field: {"$gt": search_value}}
                            elif search_type == "Less than":
                                try:
                                    # Try to convert to number if it looks like one
                                    if search_value.replace(".", "", 1).isdigit():
                                        val = float(search_value)
                                        query = {search_field: {"$lt": val}}
                                    else:
                                        query = {search_field: {"$lt": search_value}}
                                except:
                                    query = {search_field: {"$lt": search_value}}
                            
                            # Execute search
                            search_results = list(collection.find(query).limit(100))
                            
                            if search_results:
                                # Convert to DataFrame
                                search_df = pd.DataFrame(search_results)
                                
                                # Convert ObjectId to strings
                                for col in search_df.columns:
                                    if col == '_id':
                                        search_df[col] = search_df[col].astype(str)
                                
                                st.dataframe(search_df)
                                st.info(f"Found {len(search_results)} documents (showing max 100)")
                            else:
                                st.info("No matching documents found.")
                        else:
                            st.warning("Please enter a search value")
                else:
                    st.info("No documents found to extract fields from.")
            
            with tab3:
                st.subheader("Field Analysis")
                
                # Get all fields and their data types
                sample_docs = list(collection.find().limit(10))
                if sample_docs:
                    # Gather all fields from the sample
                    all_fields = set()
                    for doc in sample_docs:
                        all_fields.update(doc.keys())
                    
                    all_fields = sorted(list(all_fields))
                    
                    # Select field for analysis
                    field_to_analyze = st.selectbox("Select field to analyze", all_fields)
                    
                    if st.button("Analyze Field"):
                        # Determine if the field exists in all documents
                        doc_count = collection.count_documents({})
                        field_count = collection.count_documents({field_to_analyze: {"$exists": True}})
                        coverage = (field_count / doc_count) * 100 if doc_count > 0 else 0
                        
                        # Get unique values count
                        unique_values = len(collection.distinct(field_to_analyze))

                        # Show information about the field
                        col1, col2, col3 = st.columns(3)
                        col1.metric("Field Coverage", f"{coverage:.1f}%")
                        col2.metric("Documents with Field", field_count)
                        col3.metric("Unique Values", unique_values)
                        
                        # Sample values
                        with st.expander("Sample Values"):
                            sample_values = collection.distinct(field_to_analyze, {})[:20]  # Limit to 20 values
                            for i, value in enumerate(sample_values):
                                st.text(f"{i+1}. {value}")
                else:
                    st.info("No documents found to analyze.")
            
            with tab4:
                st.subheader("Export Data")
                
                # Options for export
                export_option = st.radio(
                    "Export options", 
                    ["All documents", 
                     "Current view", 
                     "Selected fields", 
                     "Single field (all values)", 
                     "Single field (unique values only)",
                     "Multiple fields (custom selection)"]
                )
                
                sample_doc = collection.find_one()
                if sample_doc:
                    all_fields = sorted(list(sample_doc.keys()))
                    
                    # For selected fields option
                    if export_option == "Selected fields":
                        selected_fields = st.multiselect("Select fields to export", all_fields)
                    
                    # For single field options
                    elif export_option in ["Single field (all values)", "Single field (unique values only)"]:
                        single_field = st.selectbox("Select field", all_fields)
                    
                    # For multiple fields custom selection
                    elif export_option == "Multiple fields (custom selection)":
                        custom_fields = st.multiselect("Select fields to export", all_fields)
                        include_ids = st.checkbox("Include document _id", value=False)
                        if not include_ids and "_id" in custom_fields:
                            custom_fields.remove("_id")
                else:
                    st.info("No documents found to extract fields from.")
                    single_field = None
                    selected_fields = []
                    custom_fields = []
                
                if st.button("Generate Export"):
                    # Process based on export option
                    if export_option == "All documents":
                        docs_to_export = list(collection.find({}))
                        export_type = "all documents"
                        
                    elif export_option == "Current view":
                        skip = (st.session_state.pagination_page - 1) * st.session_state.items_per_page
                        docs_to_export = list(collection.find().skip(skip).limit(st.session_state.items_per_page))
                        export_type = "current view"
                        
                    elif export_option == "Selected fields":
                        if selected_fields:
                            projection = {field: 1 for field in selected_fields}
                            docs_to_export = list(collection.find({}, projection))
                            export_type = "selected fields"
                        else:
                            docs_to_export = []
                            st.warning("Please select at least one field to export.")
                            export_type = None
                    
                    elif export_option == "Single field (all values)":
                        if single_field:
                            # Extract only the specified field from all documents
                            docs_to_export = []
                            for doc in collection.find({}):
                                if single_field in doc:
                                    docs_to_export.append({single_field: doc[single_field]})
                            export_type = f"single field '{single_field}' (all values)"
                        else:
                            docs_to_export = []
                            st.warning("Please select a field to export.")
                            export_type = None
                    
                    elif export_option == "Single field (unique values only)":
                        if single_field:
                            # Get unique values for the selected field
                            unique_values = collection.distinct(single_field)
                            docs_to_export = [{single_field: value} for value in unique_values]
                            export_type = f"unique values of field '{single_field}'"
                        else:
                            docs_to_export = []
                            st.warning("Please select a field to export.")
                            export_type = None
                    
                    elif export_option == "Multiple fields (custom selection)":
                        if custom_fields:
                            # Create projection for selected fields
                            projection = {field: 1 for field in custom_fields}
                            if include_ids:
                                projection["_id"] = 1
                            else:
                                projection["_id"] = 0
                            
                            docs_to_export = list(collection.find({}, projection))
                            export_type = "custom selected fields"
                        else:
                            docs_to_export = []
                            st.warning("Please select at least one field to export.")
                            export_type = None
                    
                    # Generate download if we have data
                    if docs_to_export:
                        # Convert to properly formatted JSON
                        json_str = json_util.dumps(docs_to_export, indent=2)
                        
                        # Create download link
                        b64 = base64.b64encode(json_str.encode()).decode()
                        
                        # Create appropriate filename
                        if export_option == "Single field (all values)":
                            filename = f"{st.session_state.db_selected}_{st.session_state.collection_selected}_{single_field}.json"
                        elif export_option == "Single field (unique values only)":
                            filename = f"{st.session_state.db_selected}_{st.session_state.collection_selected}_{single_field}_unique.json"
                        elif export_option == "Multiple fields (custom selection)":
                            fields_str = "_".join(custom_fields[:3])  # Limit filename length
                            if len(custom_fields) > 3:
                                fields_str += "_etc"
                            filename = f"{st.session_state.db_selected}_{st.session_state.collection_selected}_{fields_str}.json"
                        else:
                            filename = f"{st.session_state.db_selected}_{st.session_state.collection_selected}.json"
                        
                        href = f'<a href="data:file/json;base64,{b64}" download="{filename}" class="download-button">Download JSON File</a>'
                        st.markdown(f"""
                        <style>
                        .download-button {{
                            display: inline-block;
                            padding: 0.5rem 1rem;
                            background-color: #4CAF50;
                            color: white;
                            text-decoration: none;
                            border-radius: 4px;
                            font-weight: bold;
                            margin: 1rem 0;
                        }}
                        </style>
                        {href}
                        """, unsafe_allow_html=True)
                        
                        # Preview
                        with st.expander("Preview (first 5 documents)"):
                            preview_json = json_util.dumps(docs_to_export[:5], indent=2)
                            st.code(preview_json, language="json")
                        
                        # Stats
                        st.success(f"Export prepared with {len(docs_to_export)} documents for {export_type}.")
    
    except Exception as e:
        st.error(f"Error: {e}")
else:
    st.info("Please connect to MongoDB using the connection details in the sidebar.")
    
    # Example usage instructions
    with st.expander("How to use this app"):
        st.markdown("""
        ## MongoDB Explorer Instructions
        
        1. **Connect to MongoDB**:
           - Enter your MongoDB connection string or individual parameters in the sidebar
           - Click "Connect"
        
        2. **Navigate your data**:
           - Select a database from the dropdown
           - Select a collection to view
        
        3. **Explore your data**:
           - View documents with pagination
           - Search and filter by field values
           - Analyze field properties and statistics
           - Export data in JSON format
        
        This app does not support modifying or deleting data for safety reasons.
        """)