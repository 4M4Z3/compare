-- Check the schema of your GenCast table
SELECT 
  column_name,
  data_type,
  is_nullable
FROM 
  `your-project.weathernext_gen_fo.INFORMATION_SCHEMA.COLUMNS`
WHERE 
  table_name = '126478713_1_0'
ORDER BY 
  ordinal_position;

-- Get a sample of the data to understand the structure
SELECT *
FROM `your-project.weathernext_gen_fo.126478713_1_0`
LIMIT 10; 