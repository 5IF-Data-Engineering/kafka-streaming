package org.deng.kafka_streaming.utils;

import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.deng.kafka_streaming.model.BusDelay;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExtractExcel {
    public static Map<Integer, BusDelay> read_excel(String path) throws IOException {
        FileInputStream file = new FileInputStream(path);
        Workbook workbook = new XSSFWorkbook(file);
        Sheet sheet = workbook.getSheetAt(0);

        Map<Integer, BusDelay> data = new HashMap<>();
        for (int i = 0; i < sheet.getLastRowNum(); i++) {
            if (i == 0) {
                continue;
            }
            BusDelay busDelay = new BusDelay();
            for (int j = 0; j < sheet.getRow(i).getLastCellNum(); j++) {
                if (j == 0) {
                    LocalDate dateValue = LocalDate.from(sheet.getRow(i).getCell(j).getLocalDateTimeCellValue());
                    busDelay.setYear(dateValue.getYear());
                    busDelay.setMonth(dateValue.getMonthValue());
                    busDelay.setDayOfMonth(dateValue.getDayOfMonth());
                } else if (j == 1) {
                    int routeValue = (int) sheet.getRow(i).getCell(j).getNumericCellValue();
                    busDelay.setRoute(routeValue);
                } else if (j == 2) {
                    LocalDateTime timeValue = sheet.getRow(i).getCell(j).getLocalDateTimeCellValue();
                    int hour = timeValue.getHour();
                    busDelay.setHour(hour);
                } else if (j == 3) {
                    String dayValue = sheet.getRow(i).getCell(j).getStringCellValue();
                    busDelay.setDay(dayValue);
                    if (dayValue.equals("Saturday") || dayValue.equals("Sunday")) {
                        busDelay.setDayType("weekend");
                    } else {
                        busDelay.setDayType("weekday");
                    }

                } else if (j == 4) {
                    String locationValue = sheet.getRow(i).getCell(j).getStringCellValue();
                    busDelay.setLocation(locationValue);
                } else if (j == 5) {
                    String incidentValue = sheet.getRow(i).getCell(j).getStringCellValue();
                    busDelay.setIncident(incidentValue);
                } else if (j == 6) {
                    float delayValue = (float) sheet.getRow(i).getCell(j).getNumericCellValue();
                    busDelay.setDelay(delayValue);
                } else if (j == 7) {
                    float gapValue = (float) sheet.getRow(i).getCell(j).getNumericCellValue();
                    busDelay.setGap(gapValue);
                } else if (j == 8) {
                    String directionValue = sheet.getRow(i).getCell(j).getStringCellValue();
                    busDelay.setDirection(directionValue);
                } else if (j == 9) {
                    int vehicleValue = (int) sheet.getRow(i).getCell(j).getNumericCellValue();
                    busDelay.setVehicle(vehicleValue);
                }
            }
            data.put(i - 1, busDelay);
        }
        
        return data;
    }
}
