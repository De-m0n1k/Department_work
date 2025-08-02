import xml.etree.ElementTree as ET
import sys
import os
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import col


#Чтение из параметров программы, обработка ошибки отсутствия переданного файла (основного)
arguments = sys.argv
if(len(arguments) != 2 and len(arguments) != 3): raise KeyError
candle_width = 300000
candle_date_from = 19000101
candle_date_to = 20200101
candle_time_from = 1000
candle_time_to = 1800
path_to_csv = arguments[1]
if(len(arguments) == 3):
    config_path = arguments[2]
    tree = ET.ElementTree(file=config_path)
    root = tree.getroot()
    cfg = {}
    for child in root.findall('property'):
        name = child.find('name')
        val = child.find('value')
        if name is not None and val is not None:
            cfg[name.text] = val.text
    candle_width = int(cfg.get("candle.width", 300000))
    candle_date_from = int(cfg.get("candle.date.from", 19000101))
    candle_date_to = int(cfg.get("candle.date.to", 20200101))
    candle_time_from = int(cfg.get("candle.time.from", 1000))
    candle_time_to = int(cfg.get("candle.time.to", 1800))


#Создание сессии + вспомогательные функции
spark = SparkSession.builder.master("local[*]").appName("Jap_Candles").getOrCreate()

def time_to_msec(time):
    hr, mn, sec, msec = time[:2], time[2:4], time[4:6], time[6:]
    return ((int(hr) * 60 + int(mn)) * 60 + int(sec)) * 1000 + int(msec)


def msec_to_time(msec):
    hr = str(msec // 3600000)
    mn = str(msec // 60000 % 60)
    sec = str(msec // 1000 % 60)
    msecs = str(msec % 1000)
    if len(hr) == 1: hr = '0' + hr
    if len(mn) == 1: mn = '0' + mn
    if len(sec) == 1: sec = '0' + sec
    if len(msecs) == 1: msecs = '00' + msecs
    if len(msecs) == 2: msecs = '0' + msecs
    return hr + mn + sec + msecs


#Основная функция для Map'а
def mapper(row):
    instr, moment, price = row
    date, time = str(moment)[:8], str(moment)[8:]
    msec_from = time_to_msec(time) - time_to_msec(str(candle_time_from) + "00000")
    rounded_time = msec_to_time((msec_from // candle_width) * candle_width + time_to_msec(str(candle_time_from) + "00000"))
    return Row("SYMBOL", "CANDLE_START", "MOMENT", "PRICE")(instr, date+rounded_time, moment, price)


#Функция для вывода OHLC по списку цен
def ohlc(row):
    row = list(map(lambda elem: int(elem[1] * 10) / 10 if int(elem[1] * 100 % 10) < 5 else int(elem[1] * 10 + 1) / 10, sorted(list(row), key=lambda row: row[0])))
    return (row[0], max(row), min(row), row[-1])


#Считывание DF из файла + сортировка по MOMENT и ID_DEAL (чтобы соответствовать условию про ID_DEAL) + приведение к удобному виду и фильтрация по входным данным из config.xml
df = spark.read.csv(path=path_to_csv, sep=',', header=True, inferSchema=True).orderBy(["MOMENT", "ID_DEAL"], ascending=[True,True])
df_new = df.select(col("#SYMBOL"), col("MOMENT"), col("PRICE_DEAL"))
df_new_with_datetime = df_new.withColumn("DATE", (col("MOMENT") / (10 ** 9)).cast("int")).withColumn("TIME", (col("MOMENT") / (10 ** 5) % (10 ** 4)).cast("int"))
df_filtered = df_new_with_datetime.filter((col("DATE") >= candle_date_from) & (col("DATE") < candle_date_to) & (col("TIME") >= candle_time_from) & (col("TIME") < candle_time_to)).select(col("#SYMBOL"), col("MOMENT"), col("PRICE_DEAL"))


#Перевод из вида Row(SYMBOL, CANDLE_START, MOMENT, PRICE) сначала к RDD со строками вида ((SYMBOL, CANDLE_START), (MOMENT, PRICE)), \
# затем группирую по Key, значит, по свечам, по каждой свече составляю ее вид OHLC, и еще раз группирую по ключу, а значит, по названию инструмента
rdd_key_value = df_filtered.rdd.map(mapper).map(lambda row: ((row[0], row[1]), (row[2], row[3]))).groupByKey().mapValues(ohlc).map(lambda row: (row[0][0], (row[0][1], row[1][0], row[1][1], row[1][2], row[1][3]))).groupByKey()


#Collect + запись в файлы
#Не могу сделать более эффективно (т.к. в map'е нельзя сохранять строки в файлы из-за того, что он "ленивый")
for row in rdd_key_value.collect():
    instr, candles = row[0], sorted(list(row[1]), key=lambda elem: elem[0])
    path_to_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "instruments", instr.lower()+".csv")
    if not(os.path.exists(os.path.join(os.path.dirname(os.path.abspath(__file__)), "instruments"))): os.mkdir(os.path.join(os.path.dirname(os.path.abspath(__file__)), "instruments"))
    file = open(path_to_file, "w+", encoding="utf-8")
    for line in candles:
        fin_line = ",".join([instr, str(line[0]), str(line[1]), str(line[2]), str(line[3]), str(line[4])])
        file.write(fin_line + '\n')
