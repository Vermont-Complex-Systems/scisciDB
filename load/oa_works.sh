[jstonge1@vacc-login4 scisciDB]$ ls /users/j/s/jstonge1/data/scisciDB/openalex/data/works/ | head -20
manifest                                                                                             
parse_manifest.json                                                                                  
updated_date=2016-06-24                                                                              
updated_date=2016-07-22                                                                              
updated_date=2016-08-23                                                                              
updated_date=2016-09-16                                                                              
updated_date=2016-09-23                                                                              
updated_date=2016-09-30                                                                              
updated_date=2016-10-07                                                                              
updated_date=2016-10-14                                                                              
updated_date=2016-10-28                                                                              
updated_date=2016-11-11                                                                              
updated_date=2016-11-30                                                                              
updated_date=2016-12-08                                                                              
updated_date=2016-12-16                                                                              
updated_date=2017-01-06                                                                              
updated_date=2017-01-13                                                                              
updated_date=2017-01-26                                                                              
updated_date=2017-02-03                                                                              
updated_date=2017-02-10                                                                              

but also this used to work

#!/bin/bash
set -e

echo "Starting oa_works export to partitioned parquet..."

duckdb << 'EOF'
SET temp_directory = '.tmp/'; 
SET memory_limit='110GB';
SET threads=32;
SET max_temp_directory_size = '200GB';
SET preserve_insertion_order=false;
SET enable_progress_bar = true;

COPY (
  SELECT *
  FROM read_parquet('/users/j/s/jstonge1/data/scisciDB/openalex/data/works/**/*.parquet')
)
TO '/netfiles/compethicslab/scisciDB/oa_works'
(FORMAT parquet, PARTITION_BY publication_year, OVERWRITE);

EOF

echo "Export complete!"

