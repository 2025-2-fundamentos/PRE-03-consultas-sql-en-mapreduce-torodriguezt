"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

import os
import shutil
from homework.mapreduce import mapreduce  # type: ignore

# -------------------------------------------------------------------
# Columnas en el dataset:
# total_bill, tip, sex, smoker, day, time, size
# -------------------------------------------------------------------


# -------------------------------------------------------------------
# QUERY 1:
# SELECT *, tip/total_bill as tip_rate
# FROM tips;
# -------------------------------------------------------------------
def mapper_query_1(sequence):
    """Mapper Query 1"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip() + ",tip_rate"))
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result


def reducer_query_1(sequence):
    """Reducer Query 1"""
    return sequence


# -------------------------------------------------------------------
# QUERY 2:
# SELECT *
# FROM tips
# WHERE time = 'Dinner';
# -------------------------------------------------------------------
def mapper_query_2(sequence):
    """Mapper Query 2"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner":
                result.append((index, row.strip()))
    return result


def reducer_query_2(sequence):
    """Reducer Query 2"""
    return sequence


# -------------------------------------------------------------------
# QUERY 3:
# SELECT *
# FROM tips
# WHERE time = 'Dinner' AND tip > 5.00;
# -------------------------------------------------------------------
def mapper_query_3(sequence):
    """Mapper Query 3"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if row_values[5] == "Dinner" and float(row_values[1]) > 5.00:
                result.append((index, row.strip()))
    return result


def reducer_query_3(sequence):
    """Reducer Query 3"""
    return sequence


# -------------------------------------------------------------------
# QUERY 4:
# SELECT *
# FROM tips
# WHERE size >= 5 OR total_bill > 45;
# -------------------------------------------------------------------
def mapper_query_4(sequence):
    """Mapper Query 4"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            if int(row_values[6]) >= 5 or float(row_values[0]) > 45:
                result.append((index, row.strip()))
    return result


def reducer_query_4(sequence):
    """Reducer Query 4"""
    return sequence


# -------------------------------------------------------------------
# QUERY 5:
# SELECT sex, count(*)
# FROM tips
# GROUP BY sex;
# -------------------------------------------------------------------
def mapper_query_5(sequence):
    """Mapper Query 5"""
    result = []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result


def reducer_query_5(sequence):
    """Reducer Query 5"""
    counter = {}
    for key, value in sequence:
        counter[key] = counter.get(key, 0) + value
    return list(counter.items())


# -------------------------------------------------------------------
# Funciones auxiliares
# -------------------------------------------------------------------
def clean_and_run(output_folder, mapper_fn, reducer_fn):
    """Borra carpeta de salida si existe y ejecuta mapreduce"""
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)
    mapreduce(
        input_folder="files/input/",
        output_folder=output_folder,
        mapper_fn=mapper_fn,
        reducer_fn=reducer_fn,
    )


# -------------------------------------------------------------------
# ORQUESTADOR
# -------------------------------------------------------------------
def run():
    """Orquestador de queries"""
    clean_and_run("files/query_1", mapper_query_1, reducer_query_1)
    clean_and_run("files/query_2", mapper_query_2, reducer_query_2)
    clean_and_run("files/query_3", mapper_query_3, reducer_query_3)
    clean_and_run("files/query_4", mapper_query_4, reducer_query_4)
    clean_and_run("files/query_5", mapper_query_5, reducer_query_5)


if __name__ == "__main__":
    run()
