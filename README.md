# Data Generator

## Overview
**Data Generator** — это проект для генерации данных и записи их в формате Parquet и JSON в хранилище Ozone. Он упрощает создание, структурирование и сохранение больших объемов данных для тестирования и анализа.

## Features
- Генерация данных на основе заранее заданной структуры (`DataBundle`). +
- Поддержка формата **Parquet** для компактного хранения и совместимости с аналитическими инструментами. +
- Поддержка формата JSON +
- Поддержка планировки загрузки данных в Apache Ozone -
- Интеграция с Ozone для записи данных непосредственно в распределенное хранилище. +
- Использование Avro-схемы для описания структуры данных. +
- Поддержка сжатия данных с использованием алгоритма **Snappy**. +

## Technologies Used
- **Java**
- **Spring Framework**
- **Apache Avro**
- **Apache Parquet**
- **Apache Ozone**
- **Hadoop**

## Requirements
- Java 11+
- Apache Ozone cluster (для записи данных).
- Gradle (для управления зависимостями).

## Installation
   ```bash
   git clone git@github.com:pinkyMan79/dw-datagen-service.git
   cd data-generator
   ```