package org.deng.kafka_streaming.utils;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractExcel {
    public static Map<Integer, List<String>> read_excel(String path) throws IOException {
        FileInputStream file = new FileInputStream(path);
        Workbook workbook = new XSSFWorkbook(file);
        Sheet sheet = workbook.getSheetAt(0);

        Map<Integer, List<String>> data = new HashMap<>();
        
        for (int i = 0; i < sheet.getLastRowNum(); i++) {
            if (i == 0) {
                continue;
            }
            List<String> row = new ArrayList<>();
            for (int j = 0; j < sheet.getRow(i).getLastCellNum(); j++) {
                if (j == 0) {
                    LocalDate dateValue = LocalDate.from(sheet.getRow(i).getCell(j).getLocalDateTimeCellValue());
                    // get timestamp and parse it to string
                    long timestamp = dateValue.toEpochDay();
                    row.add(String.valueOf(timestamp));
                    continue;
                } else if (j == 2) {
                    LocalDateTime timeValue = sheet.getRow(i).getCell(j).getLocalDateTimeCellValue();
                    int hour = timeValue.getHour();
                    row.add(String.valueOf(hour));
                    continue;
                }
                switch (sheet.getRow(i).getCell(j).getCellType()) {
                    case STRING:
                        row.add(sheet.getRow(i).getCell(j).getStringCellValue());
                        break;
                    case NUMERIC:
                        row.add(String.valueOf(sheet.getRow(i).getCell(j).getNumericCellValue()));
                        break;
                    case BOOLEAN:
                        row.add(String.valueOf(sheet.getRow(i).getCell(j).getBooleanCellValue()));
                        break;
                    case FORMULA:
                        row.add(String.valueOf(sheet.getRow(i).getCell(j).getCellFormula()));
                        break;
                    default:
                        row.add("");
                }
            }
            data.put(i - 1, row);
        }
        
        return data;
    }
}
