val query = """select sch.name SchemaName
  |, obj.name TableName
  |, cols.name ColumnName
  |, t.name TypeName
  |, case when cols.is_nullable = 0 then 'No' else 'Yes' end Nullable
  |, CAST(ep.value AS varchar(999)) Description
  |from sys.extended_properties ep
  |join sys.objects obj
  |on ep.major_id = obj.object_id
  |join sys.schemas sch
  |on obj.schema_id = sch.schema_id
  |join sys.columns cols
  |on obj.object_id = cols.object_id
  |and ep.minor_id = cols.column_id
  |join sys.types t
  |on cols.user_type_id = t.user_type_id
  |where ep.class_desc = 'OBJECT_OR_COLUMN'
  |and sch.name not in ('dbo')""".stripMargin
val df = spark.read.format("jdbc").
  option("url",  "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks;trustServerCertificate=true;integratedsecurity=true").
  option("query", query).
  option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
  load().
  orderBy($"SchemaName".asc, $"TableName".asc, $"ColumnName".asc).
  repartition(1)
df.write.format("csv").
  mode("overwrite").
  save(".\\csv\\AdventureWorksOltpSchemaDescription.csv")