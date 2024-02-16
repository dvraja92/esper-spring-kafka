package com.contextsuite.esper.events;

import lombok.*;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
@SuperBuilder(toBuilder = true)
public class PersonEvent {

    protected Integer age;
    protected String name;
}
