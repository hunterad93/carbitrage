SELECT
  listings.url,
  state_lookup.state,
  listings.location,
  listings.time_posted,
  listings.name,
  listings.make,
  listings.model,
  listings.year,
  listings.odometer,
  listings.title,
  listings.paint,
  listings.drive,
  listings.cylinders,
  listings.fuel,
  listings.type,
  listings.transmission,
  listings.condition,
  listings.price,
  listings.title_text,
  listings.latitude,
  listings.longitude,
  lms.intercept + lms.miles_coeff * (LOG(listings.odometer + 0.1)) + /* prevent log of 0 */
  lms.condition_coeff *
  CASE
    WHEN listings.condition IN ('new', 'like new', 'excellent') THEN 1
    WHEN listings.condition IN ('good','fair', 'salvage') THEN 0
  ELSE
  NULL
END
  + lms.year_coeff * (listings.year) AS predicted_price,
  lms.r_squared,
  lms.sample_size
FROM
  `car-buying-272019.car_buying.processed_listing_pages` AS listings
JOIN `car-buying-272019.car_buying.state_lookup_table` AS state_lookup
ON state_lookup.region = listings.location 
LEFT JOIN
  `car-buying-272019.car_buying.lm_lookup_table` AS lms
ON
  listings.make = lms.make
  AND listings.model = lms.model
WHERE
  listings.time_posted >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 45 DAY)
  AND listings.price < 75000
  AND listings.price > 250
  AND listings.odometer < 300000
  AND listings.odometer > 0
  AND year >= EXTRACT(YEAR
  FROM
    CURRENT_DATE()) - 50
  AND year <= EXTRACT(YEAR
  FROM
    CURRENT_DATE()) + 1
ORDER BY
  listings.time_posted DESC;