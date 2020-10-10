package ru.mephi.chenko.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import ru.mephi.chenko.dto.MetricDto;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Service
public class CVSService {

    private static final Logger logger = LoggerFactory.getLogger(CVSService.class);

    private String[] HEADERS = { "id", "time", "value"};

    public List<MetricDto> parseCsv(File csvFile) {
        List<MetricDto> metricList = new ArrayList<>();

        try {
            Reader in = new FileReader(csvFile);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withHeader(HEADERS)
                    .parse(in);

            for (CSVRecord record : records) {
                Long id = Long.parseLong(record.get("id").trim());
                // Умножаю на 1000, т.к. в файле хранится количество секунд с 1970, а Date принимает количество миллисекунд
                Date time = new Date(Long.parseLong(record.get("time").trim()) * 1000);
                Integer value = Integer.parseInt(record.get("value").trim());
                MetricDto metric = new MetricDto(id, time, value);
                metricList.add(metric);
            }
        } catch (IOException ex) {
            logger.error(ex.getMessage(), ex);
        }

        return metricList;
    }
}
