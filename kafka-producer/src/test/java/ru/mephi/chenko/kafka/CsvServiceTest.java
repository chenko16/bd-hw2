package ru.mephi.chenko.kafka;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;
import ru.mephi.chenko.kafka.dto.MetricDto;
import ru.mephi.chenko.kafka.service.CSVService;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@RunWith(SpringRunner.class)
@ActiveProfiles("test")
public class CsvServiceTest {

    @Autowired
    private CSVService csvService;

    @Test
    public void testCsvParser() throws IOException {
        MetricDto firstMetric = new MetricDto(1L, new Date(1602438632000L), 50);
        MetricDto secondMetric = new MetricDto(2L, new Date(1602438632000L), 60);

        List<MetricDto> expectedResultList = new ArrayList<>();
        expectedResultList.add(firstMetric);
        expectedResultList.add(secondMetric);


        File testFile = File.createTempFile("test_log", ".csv");
        testFile.deleteOnExit();

        BufferedWriter writer = new BufferedWriter(new FileWriter(testFile, true));
        // id, timestamp, value
        writer.append("1, 1602438632000, 50\n");
        writer.append("2, 1602438632000, 60\n");
        writer.close();

        List<MetricDto> metricList = csvService.parseCsv(testFile);

        assertEquals(expectedResultList.size(), metricList.size());
        assertTrue(metricList.containsAll(expectedResultList));
    }
}
