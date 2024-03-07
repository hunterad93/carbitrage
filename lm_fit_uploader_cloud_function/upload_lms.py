from google.cloud import bigquery
import pandas as pd
import statsmodels.api as sm


def lm_fit_uploader(request): #simply wrapped everything in a function to use with google cloud function
    # Initialize a BigQuery client
    client = bigquery.Client()

    # Query data from GBQ view lm_fit_vw
    # This view uses last 6 months of listings, filters out rows with any nulls in columns being queries, 
    # maps 'condition' in listing from 6 categories to 0/1, filters to only include makes/models with over 100 listings,
    # then filters out outliers by price - anything below 0.05 quantile or above 0.95 quantile. Exact query can be viewed in GBQ.

    query = """
        SELECT make, model, year, log_odometer, price, condition_mapped, time_posted FROM `car-buying-272019.car_buying.lm_fit_vw`
    """
    query_job = client.query(query)
    lm_fit_vw = query_job.to_dataframe()

    # Convert 'year', 'odometer', and 'condition_mapped' to float64, which is compatible with statsmodels
    lm_fit_vw['year'] = lm_fit_vw['year'].astype('float64')
    lm_fit_vw['log_odometer'] = lm_fit_vw['log_odometer'].astype('float64')
    lm_fit_vw['condition_mapped'] = lm_fit_vw['condition_mapped'].astype('float64')


    # Generate a list of unique make/model combinations from lm_fit_vw
    unique_make_model = lm_fit_vw[['make', 'model']].drop_duplicates()
    make_model_list = list(unique_make_model.itertuples(index=False, name=None))

    # Initialize an empty list to store the results
    results = []

    for make, model in make_model_list:
        temp_df = lm_fit_vw[(lm_fit_vw['make'] == make) & (lm_fit_vw['model'] == model)]
        
        # Ensure there's no missing values in the columns of interest
        temp_df = temp_df.dropna(subset=['log_odometer', 'year', 'price', 'condition_mapped'])
        predictors = ['log_odometer', 'condition_mapped']
        if temp_df['year'].nunique() > 1:
            predictors.append('year')  # Add 'year' if more than one unique value, if a make/model only has 1 year there will be a bug.
        
        X = temp_df[predictors]
        y = temp_df['price']
        
        try:
            X = sm.add_constant(X)  # Add a constant term for the intercept
            model_fit = sm.OLS(y, X).fit()
            
            # Extract the model parameters
            intercept = model_fit.params.get('const')
            miles_coeff = model_fit.params.get('log_odometer', 0)  # Default to 0 if not present
            condition_coeff = model_fit.params.get('condition_mapped', 0)  # Extract condition coefficient
            year_coeff = model_fit.params.get('year', 0)  # Default to 0 if 'year' was not included
            
            # Store results including the sample size
            results.append({
                'make': make, 
                'model': model, 
                'intercept': intercept, 
                'miles_coeff': miles_coeff, 
                'condition_coeff': condition_coeff,
                'year_coeff': year_coeff, 
                'r_squared': model_fit.rsquared, #including r_squared and sample size to add some uncertainty estimate in dashboard
                'sample_size': len(temp_df)
            })
        except Exception as e:
            print(f"Error fitting model for {make} {model}: {e}")

    # Convert the list of dictionaries to a DataFrame
    lm_lookup_table = pd.DataFrame(results)

    # Define the schema
    schema = [
        bigquery.SchemaField("make", "STRING"),
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("intercept", "FLOAT"),
        bigquery.SchemaField("miles_coeff", "FLOAT"),
        bigquery.SchemaField("condition_coeff", "FLOAT"),
        bigquery.SchemaField("year_coeff", "FLOAT"),
        bigquery.SchemaField("r_squared", "FLOAT"),
        bigquery.SchemaField("sample_size", "INTEGER"),
    ]

    # Specify the job configuration to overwrite the table and set the schema
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    # Upload model_results to a GBQ table, overwriting any existing data
    table_id = 'car-buying-272019.car_buying.lm_lookup_table'
    job = client.load_table_from_dataframe(lm_lookup_table, table_id, job_config=job_config)

    # Wait for the job to complete
    job.result()

    return {"message": f"uploaded and overwrote data in {table_id}"}
