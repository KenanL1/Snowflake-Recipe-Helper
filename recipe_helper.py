import streamlit as st  # Import python packages
from snowflake.snowpark import Session
from snowflake.core import Root
from dotenv import load_dotenv

import os
import pandas as pd
import re

# Load environment variables
load_dotenv()

pd.set_option("max_colwidth", None)

# Default Values
NUM_CHUNKS = 3  # Num-chunks provided as context.

# service parameters
CORTEX_SEARCH_DATABASE = st.secrets['SNOWFLAKE_DATABASE']
CORTEX_SEARCH_SCHEMA = st.secrets['SNOWFLAKE_SCHEMA']
CORTEX_SEARCH_SERVICE = st.secrets['SNOWFLAKE_NUTRITION_SERVICE']
CORTEX_SEARCH_FOOD_SERVICE = st.secrets['SNOWFLAKE_FOOD_LIST_SERVICE']
CONNECTION_PARAMETERS = {
    "account": st.secrets["SNOWFLAKE_ACCOUNT"],
    "user": st.secrets["SNOWFLAKE_USER"],
    "password": st.secrets["SNOWFLAKE_PASSWORD"],
    "role": "ACCOUNTADMIN",
    "database": CORTEX_SEARCH_DATABASE,
    "warehouse": st.secrets['SNOWFLAKE_WAREHOUSE'],
    "schema": CORTEX_SEARCH_SCHEMA
}

# columns to query in the service
COLUMNS = [
    "chunk",
    "file_name",
    "page_number"
]

# Connect to Snowpark python
session = Session.builder.configs(CONNECTION_PARAMETERS).create()
root = Root(session)

# Get the cortex search services
svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_SERVICE]
food_svc = root.databases[CORTEX_SEARCH_DATABASE].schemas[CORTEX_SEARCH_SCHEMA].cortex_search_services[CORTEX_SEARCH_FOOD_SERVICE]

# Config options for testing
# def config_options():

#     st.sidebar.selectbox('Select your model:', (
#         'mistral-large2',
#         'mistral-7b',
#     ), key="model_name")

#     st.sidebar.expander("Session State").write(st.session_state)


def get_similar_chunks_search_service(query):
    """Retrieve chunks from SNOWFLAKE_NUTRITION_SERVICE"""

    print(query)
    response = svc.search(query, COLUMNS, limit=NUM_CHUNKS)
    # st.sidebar.json(response.json())
    return response.json()


def get_matching_food_search_service(query):
    """Retrieve chunks from SNOWFLAKE_FOOD_LIST_SERVICE"""

    response = food_svc.search(query, ["description"], limit=5)
    # st.sidebar.json(response.json())
    return response.json()


def create_recipe_prompt(myquestion):
    """Prompt to retrieve recipes from docs"""

    prompt_context = get_similar_chunks_search_service(myquestion)
    prompt = f"""
    Based on the context, if nutrient information about the recipe cannot be found then only list out the ingredients for the recipe in the question
    else provide all information about the recipe.

    Answer should have the following format if has nutrition infromation:
    ## Recipe Name
    ## Prep Time
    ## Servings
    ## Nutrition Facts
    ## Ingredients
    ## Directions
    Else
    ##Ingredients
        
       <context>          
       {prompt_context}
       </context>
       <question>  
       {myquestion}
       </question>
       Answer: 
       """

    # st.sidebar.markdown(prompt)
    # json_data = json.loads(prompt_context)

    return prompt_context, prompt


def create_food_prompt(myquestion):
    """Prompt to retrieve matching foods"""

    prompt_context = get_matching_food_search_service(myquestion)

    prompt = f'''
    List the provided ingredients that have a corresponding match in the <ingredients> and <context>.
    Each ingredient in <ingredients> should ONLY have AT MOST ONE match. 
    If there are multiple matches display ONLY the first match.
    If there is not match return NO MATCH
    Display the matching ingredient in bullet-points,
    use the EXACT description/name from the <context>. 
    Exclude the ingredients if it does not have a match.
    Do not add any additional notes or information.

    If there is ONLY 1 ingredient (apple) in <ingredients> ONLY 1 ingredient should be shown

       <context>          
       {prompt_context}
       </context>
       <ingredients>  
       {myquestion}
       </ingredients>
       Answer: 
       '''

    # st.sidebar.markdown(prompt)
    # json_data = json.loads(prompt_context)

    return prompt


def create_nutrient_prompt(ingredients, nutrient, recipe_context):
    """Prompt to display recipe with calculated nutrient value"""

    prompt = f'''
    Using the <ingredients> and <nutrient> and <context> data below. 
    
    Fill in the information using the following format below:
    ## Recipe Name
    ### Prep Time
    ### Servings
    ### Nutrition Facts Calculated
    ### Ingredients
    ### Directions

    For Nutrition Facts
    Use the information provided to calculate the nutrient facts for this recipe.
    Nutrient data is for 100g of ingredient, multiply or divide if recipe ask for different amount.
    Add nutrient data together at the end
    Only use information from nutrient data, do not make stuff up.
    List the ingredients that do not have nutrient value.
    Exclude the section if there is no data about it

       <context>
        {recipe_context}
       </context>
       <nutrient>          
       {nutrient.to_dict(orient='records')}
       </nutrient>
       <ingredients>  
       {ingredients}
       </ingredients>
       Answer: 
       '''

    # st.sidebar.markdown(prompt)
    # json_data = json.loads(nutrient)

    return prompt


def generate_nutrient_data(text):
    """Get nutrient data using SQL"""

    sql_query = f"""
    SELECT DISTINCT food.description, nut.nutrient_name, nut.amount as amount, unit_name
    from NUTRITION_DB.NUTRITION_SCHEMA.FOODS as food
    INNER JOIN NUTRITION_DB.NUTRITION_SCHEMA.FOOD_NUTRIENTS as nut
    ON food.fdcid = nut.fdcid
    WHERE description IN ('{text}')
        AND NUTRIENT_NAME IN ('Energy (Atwater Specific Factors)', 'Energy', 'Cholesterol', 'Protein', 'Carbohydrate, by difference', 'Sugars, Total', 'Total lipid (fat)', 'Fiber, total dietary')
    ORDER BY food.description
    """
    res = session.sql(sql_query).collect()
    # st.markdown(res)

    df = pd.DataFrame(res)
    # st.table(df)
    return df


def complete(prompt):
    """Snowflake Cortex for generation"""

    cmd = """
            select snowflake.cortex.complete(?, ?) as response
          """
    df_response = session.sql(
        cmd, params=["mistral-large2", prompt]).collect()
    # print(df_response)
    return df_response


def main():
    st.title(f":chopsticks: Nutritional Recipe Helper :bacon:")
    # config_options()
    question = st.text_input(
        "Enter question", placeholder="Search for a Recipe", label_visibility="collapsed")

    if question:
        # Use RAG to get the recipe for a food
        recipe_context, prompt = create_recipe_prompt(question)
        response = complete(prompt)
        res_text = response[0].RESPONSE
        # Check if response contains nutrient information
        pattern = r'(?i)(nutrition|carbohydrate|calories|protein|carbohydrates|vitamins|minerals|fiber)'
        pattern_match = bool(re.search(pattern, res_text))
        # Display response if nutritent info included
        if pattern_match:
            st.markdown(res_text)
        # If no nutritent info continue workflow to get nutritent info of each ingredient and calculate
        # total nutritent fact
        else:
            ingredient_list = []
            # st.markdown(res_text)
            # Parse response to get ingredient list
            food_list = [line.replace('*', "").replace("-", "").strip()
                         for line in res_text.split("\n") if line.strip()]

            print(food_list)
            # Use RAG to check if theres a similar ingredient in SQL food table
            for food in food_list:
                response = complete(create_food_prompt(food))
                res_text = response[0].RESPONSE
                if "NO MATCH" in res_text:
                    continue
                ingredient_list.append(res_text)
                # st.markdown(food)
                # st.markdown(res_text)
            nutrient_data = generate_nutrient_data(
                "','".join(ingredient_list).replace(" - ", "").strip())
            # generate response with new nutrient info
            response = complete(create_nutrient_prompt(
                food_list, nutrient_data, recipe_context))
            st.markdown(response[0].RESPONSE)


if __name__ == "__main__":
    main()
