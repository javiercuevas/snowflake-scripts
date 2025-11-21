SELECT *
FROM TABLE(
    INFER_SCHEMA(
        LOCATION => '@WKSP_DTS.RAW_PI.STG_PI/pi/landing/dataset_01/ADLS_STSUP_GenSum5mData_20251121132005.parquet',
        FILE_FORMAT => 'parquet_format'
    )
);
