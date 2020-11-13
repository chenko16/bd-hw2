# bd-hw2

Для сборки проекта в системе должен быть установлен maven. Для сборки необходимо в корневой директории проекта (где лежит файл pom.xml) выполнить команду:  
`mvn clean package`
	
Jar файл приложения должен появится в директории `target` модуля `spark-rdd`.    

Для генерации тестовых данных используйте скрипт в корне проекта `./generate_data.sh`. Для записи данных в kafka необходимо поместить входной файл рядом с `kafka-producer-1.0.jar`, который после сборки должен появится в директории `target` модуля `kafka-producer`.  

Для запуска приложения необходимо предварительно установить `spark-3.0.1`. Запуск приложения осуществляется с помощью следующей команды:  
`sudo ${SPARK_HOME}/bin/spark-submit --class ru.mephi.chenko.spark.SparkApplication --master yarn --deploy-mode cluster --jars hdfs://localhost:9000/user/root/libs/* hdfs://localhost:9000/user/root/spark-rdd-1.0.jar 5m`  

Предварительно необходимо загрузить в hdfs зависимости и сам jar файл приложения.
