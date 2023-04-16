# Errata for *Analytics Optimization with Columnstore Indexes in Microsoft SQL Server*

On **page 93** [code]:
 
This join misses clause on partition_number: LEFT JOIN sys.internal_partitions ON internal_partitions.object_id = tables.object_id should be: LEFT JOIN sys.internal_partitions ON internal_partitions.object_id = tables.object_id AND internal_partitions.partition_number = column_store_row_groups.partition_number.

***

On **page xx** [Summary of error]:
 
Details of error here. Highlight key pieces in **bold**.

***
