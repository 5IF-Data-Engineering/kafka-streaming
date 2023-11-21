package org.deng.kafka_streaming.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class BusDelay {
    public int year;
    public int month;
    public int dayOfMonth;
    public String dayType; // weekday, weekend
    public int hour;
    public int route;
    public String day;
    public String location;
    public String incident;
    public float delay;
    public float gap;
    public String direction;
    public int vehicle;
}
