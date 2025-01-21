# Recipe Helper
A Streamlit app powered by Snowflake and Mistral, designed to simplify meal planning and provide nutritional information.

## Description
Are you tired of flipping through cookbooks to find the perfect recipe, only to realize it lacks nutritional information? Recipe Helper solves this problem by allowing you to load your cookbooks, search for recipes, and view nutritional information based on ingredients used.

## Technology

- **Cortex Search:** Utilizes a RAG engine with cookbook text data for semantic search and contextualized responses.
- **Mistral Large 2:** Leverages Snowflake Cortex for generation.
- **Streamlit:** Provides a simple and intuitive frontend for displaying responses.

## Deploying

**1. Configure Snowflake Environment**

Create a .env file with the following Snowflake configuration:
```
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=
SNOWFLAKE_NUTRITION_SERVICE=
SNOWFLAKE_FOOD_LIST_SERVICE=
```

**2. Create Food and Nutrient Tables**

Run the following command to create tables using food data from foundationDownload.json:
```bash
py .\setup\ETL.py
```

**3. Create CORTEX SEARCH SERVICE**

Run the following command to create the service using docs in the docs folder and available foods in the foods data table:
```bash
py .\setup\ingess.py
```
**4. Start the app**

Run the following command to start the app:
```bash
streamlit run .\recipe_helper.py
```

## Data Sources
- Food data obtained from [U.S. Department of Agriculture](https://fdc.nal.usda.gov/)

- PDF docs:
    - [One-PotMealsCookbook-June2020.pdf](https://www.nutrition.va.gov/docs/UpdatedPatientEd/One-PotMealsCookbook-June2020.pdf)
    - [DGA_2020-2025_StartSimple_withMyPlate_English_color.pdf](https://www.dietaryguidelines.gov/sites/default/files/2021-03/DGA_2020-2025_StartSimple_withMyPlate_English_color.pdf)
    - [Easy_recipes_no_nutrient.pdf](https://www.bu.edu/geneva/files/2010/08/Easy_recipes.pdf)
