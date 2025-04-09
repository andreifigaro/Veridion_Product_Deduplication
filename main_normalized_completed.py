# importarea librariilor pe care le voi folosi
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType, ArrayType
import pandas as pd
import re

# definirea functiei de normalizare a sirurilor cu scopul de a le standardiza
def normalize_string(text):
    if text is None:
        return ''
    text = text.lower()
    text = re.sub(r'[^a-z0-9\s]', '', text) 
    text = re.sub(r'\s+', ' ', text).strip()
    return text

# crearea sesiunii Spark si incarcarea documentului Parquet
spark = SparkSession.builder.appName("VeridionChallenge").getOrCreate()
df = spark.read.parquet(r"C:\Users\traia\Downloads\veridion_product_deduplication_challenge.snappy.parquet")
df.printSchema()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

# definirea functiei de distanta levenshtein pentru siruri similare
def normalized_levenshtein(s1: str, s2: str) -> float:
    if s1 == s2:
        return 0.0
    if not s1 or not s2:
        return 1.0

    len_s1, len_s2 = len(s1), len(s2)
    dp = [[0] * (len_s2 + 1) for _ in range(len_s1 + 1)]

    for i in range(len_s1 + 1):
        dp[i][0] = i
    for j in range(len_s2 + 1):
        dp[0][j] = j

    for i in range(1, len_s1 + 1):
        for j in range(1, len_s2 + 1):
            cost = 0 if s1[i - 1] == s2[j - 1] else 1
            dp[i][j] = min(
                dp[i - 1][j] + 1,    # stergere
                dp[i][j - 1] + 1,    # inserare
                dp[i - 1][j - 1] + cost  # Substituire
            )

    distance = dp[len_s1][len_s2]
    normalized = distance / max(len_s1, len_s2)
    return normalized

# definirea functiei de similaritate dintre siruri 
def similarity(a, b):
    return 1.0 - normalized_levenshtein(a, b)

# definirea functiilor ce ma vor ajuta la identificarea tipurilor de coloane
def is_array_type(field_type):
    return isinstance(field_type, ArrayType)

def is_struct_array(field_type):
    return isinstance(field_type, ArrayType) and isinstance(field_type.elementType, StructType)

original_schema = df.schema

# definirea UDF-ului de deduplicare folosind Pandas
@pandas_udf(original_schema, PandasUDFType.GROUPED_MAP)
def deduplicate_group(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.reset_index(drop=True)
    used = set()
    result_rows = []

    for i, row in pdf.iterrows():
        if i in used:
            continue
        cluster_indices = [i]
        name_i = normalize_string(row['product_name'])
        for j in range(i + 1, len(pdf)):
            if j in used:
                continue
            name_j = normalize_string(pdf.at[j, 'product_name'])
            if similarity(name_i, name_j) > 0.6:
                cluster_indices.append(j)
                used.add(j)
        used.add(i)

        # Cluster DataFrame
        cluster_df = pdf.loc[cluster_indices]

        # declararea unui sir nou cu rolul de a retine valorile comasate
        new_row = {}

        for field in original_schema:
            col = field.name
            col_type = field.dataType

            if is_array_type(col_type):
                # simplificarea si deduplicarea sirurilor
                combined = cluster_df[col].dropna().tolist()
                flat = []
                for item in combined:
                    if item is not None:
                        flat.extend(item)
                # Deduplicarea propriu zisa (doar pentru obiectele hashable)
                if is_struct_array(col_type):
                    seen = set()
                    deduped = []
                    for entry in flat:
                        frozen = tuple(sorted(entry.items())) if isinstance(entry, dict) else tuple(entry)
                        if frozen not in seen:
                            seen.add(frozen)
                            deduped.append(entry)
                    new_row[col] = deduped
                else:
                    new_row[col] = list(pd.Series(flat).dropna().unique())
            else:
                # Foloseste valoarea primei linii pentru campurile scalare
                new_row[col] = cluster_df.iloc[0][col]

        result_rows.append(new_row)

    return pd.DataFrame(result_rows)

similarity_udf = F.udf(similarity, FloatType())

# Repartitionează DataFrame-ul înainte de deduplicare (ajustând numărul de partitii pentru date mari)
df = df.repartition(100)  # Adjust the number based on your available resources

# aplicarea deduplicării folosind groupBy
deduped_df = (
    df.groupby("root_domain")
      .apply(deduplicate_group)
)

print(deduped_df.count())  # Check the count of the deduplicated DataFrame
deduped_df.show(5, False)

# aplicarea finala a deduplicarii, previzualizarea rezultatelor si exportul livrabilelor in Parquet si CSV
# df = df.withColumn("global_group", F.lit(1))

# deduplicated_df = df.groupBy("global_group").apply(deduplicate_group).drop("global_group")

# deduplicated_df.show(truncate=False)

deduped_df.write.mode("overwrite").parquet("Veridion_Deduped_Products.parquet")

#deduplicated_df.toPandas().to_csv(r"C:\\Users\\traia\\Downloads\\Veridion_Deduped_Product.csv", index=False)
