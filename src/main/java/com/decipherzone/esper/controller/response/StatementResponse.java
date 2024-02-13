package com.decipherzone.esper.controller.response;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class StatementResponse {

    private String name;
    private String deploymentId;
    private Boolean isDestroyed;

}
