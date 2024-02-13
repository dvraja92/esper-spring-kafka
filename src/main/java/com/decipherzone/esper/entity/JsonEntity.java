package com.decipherzone.esper.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;

import java.util.Date;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class JsonEntity {

    private Date currentDate;

    private String data;

    private String topic;
}
