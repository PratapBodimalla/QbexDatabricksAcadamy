# Import a File and Create table and visualize

```python
dbutils.fs.cp("https://github.com/PratapBodimalla/qbex-adf-student-docs/raw/main/Datasets/Data_files/ipl_files/single_files/csv/ipl_matches.csv", "/Volumes/ws_qbex_dev/default/meenestham")
```

```sql
LIST "/Volumes/ws_qbex_dev/default/meenestham"
```

```python
df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/Volumes/ws_qbex_dev/default/meenestham/ipl_matches.csv")
```

```python
display(df)
```

```python
df.write.mode("overwrite").saveAsTable("ws_qbex_dev.default.ipl_match_data")
```

## Create a vizualization on the table