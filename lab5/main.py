from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, ArrayType
import re
import nltk
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from pyspark.ml.feature import HashingTF, IDF
from pyspark.ml.clustering import KMeans

# Загрузка необходимых ресурсов NLTK
nltk.download('punkt')
nltk.download('punkt_tab')
nltk.download('wordnet')

# Создание сессии Spark
spark = SparkSession.builder \
    .appName("TestSpark") \
    .config("spark.driver.host", "localhost") \
    .config("spark.security.manager", "false") \
    .config("spark.ui.port", "4040") \
    .getOrCreate()

# Загрузка данных из нескольких текстовых файлов
file_paths = [
    "./text/1.txt",
    "./text/2.txt",
    "./text/3.txt",
    "./text/4.txt",
    "./text/5.txt",
    "./text/6.txt",
    "./text/7.txt"
]

# Чтение всех файлов в один DataFrame
df = spark.read.text(file_paths)

# Инициализация лемматизатора
lemmatizer = WordNetLemmatizer()

# Функция очистки текста
def clean_text(text):
    text = re.sub(r'\W+', ' ', text)
    return text.lower()

# Функция токенизации
def tokenize(text):
    return word_tokenize(text)

# Функция лемматизации
def lemmatize(tokens):
    return [lemmatizer.lemmatize(token) for token in tokens]

# Регистрация UDF
clean_text_udf = udf(clean_text, StringType())
tokenize_udf = udf(tokenize, ArrayType(StringType()))  # Change to return ArrayType
lemmatize_udf = udf(lemmatize, ArrayType(StringType()))  # Change to return ArrayType

# Применение UDF-функций к DataFrame
df_cleaned = df.withColumn("cleaned_text", clean_text_udf(df.value))
df_tokenized = df_cleaned.withColumn("tokenized_text", tokenize_udf(df_cleaned.cleaned_text))
df_lemmatized = df_tokenized.withColumn("lemmatized_text", lemmatize_udf(df_tokenized.tokenized_text))

# Сохранение оригинальных текстов
final_df = df_lemmatized.select("value", "cleaned_text", "tokenized_text", "lemmatized_text")

# Преобразование текстов в векторы
hashingTF = HashingTF(inputCol="lemmatized_text", outputCol="rawFeatures")
featurizedData = hashingTF.transform(final_df)

idf = IDF(inputCol="rawFeatures", outputCol="features")
idfModel = idf.fit(featurizedData)
rescaledData = idfModel.transform(featurizedData)

# Кластеризация с помощью KMeans
kmeans = KMeans(k=5, seed=1)
model = kmeans.fit(rescaledData)

# Применение модели к данным
predictions = model.transform(rescaledData)

# Сохранение результатов кластеризации в DataFrame
result_df = predictions.select("value", "cleaned_text", "tokenized_text", "lemmatized_text", "prediction")
result_df.show()

# Завершение сессии Spark
spark.stop()
import time
while(True):
    print("hello")
    time.sleep(10)