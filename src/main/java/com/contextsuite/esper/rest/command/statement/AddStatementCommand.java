package com.contextsuite.esper.rest.command.statement;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@Builder
@RequiredArgsConstructor
@AllArgsConstructor
public class AddStatementCommand {
    private String name;
    private String statement;
}
