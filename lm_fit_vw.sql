WITH Filtered AS ( --filtering to appropriate columns in rows where no important nulls
    SELECT 
        make, 
        model, 
        year, 
        log(odometer+1) as log_odometer, --avoiding errors on log(0)
        price, 
        -- Apply condition mapping, lm testing found this only slightly reduced r-squared but it should prevent overfitting
        CASE 
            WHEN condition IN ('new', 'like new', 'excellent') THEN 1
            WHEN condition IN ('good', 'fair', 'salvage') THEN 0
            ELSE NULL -- Handle unexpected condition values by setting them to NULL
        END AS condition_mapped,
        time_posted
    FROM `car-buying-272019.car_buying.processed_listing_pages`
    WHERE make IS NOT NULL 
      AND model IS NOT NULL 
      AND year IS NOT NULL 
      AND odometer IS NOT NULL 
      AND price IS NOT NULL
      AND condition IS NOT NULL
      -- Ensure time_posted is within the last 6 months
      AND time_posted >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 * 30 DAY)
),

ValidMakesModels AS ( --creates a list of car make/models where there is a decent bit of data
    SELECT make, model
    FROM Filtered
    GROUP BY make, model
    HAVING COUNT(*) >= 100
),

ValidMakesModelsListings AS ( --uses the ValidMakesModels to select rows from Filtered
    SELECT F.*
    FROM Filtered F
    INNER JOIN ValidMakesModels V ON F.make = V.make AND F.model = V.model
),

PriceQuantiles AS ( --define acceptable prices for each make model year combo - avoiding outliers when fitting
    SELECT 
        make,
        model,
        year,
        APPROX_QUANTILES(price, 100)[OFFSET(5)] AS low_quantile,
        APPROX_QUANTILES(price, 100)[OFFSET(95)] AS high_quantile
    FROM ValidMakesModelsListings
    GROUP BY make, model, year
)

SELECT 
    PF.make, 
    PF.model, 
    PF.year, 
    PF.log_odometer, 
    PF.price, 
    PF.condition_mapped, 
    PF.time_posted
FROM ValidMakesModelsListings PF
INNER JOIN PriceQuantiles PQ ON PF.make = PQ.make AND PF.model = PQ.model AND PF.year = PQ.year
WHERE PF.price > PQ.low_quantile AND PF.price < PQ.high_quantile