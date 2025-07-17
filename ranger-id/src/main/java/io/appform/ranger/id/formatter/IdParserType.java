package io.appform.ranger.id.formatter;

import lombok.Getter;

@Getter
public enum IdParserType {
    DEFAULT (0);

    private final int value;

    IdParserType(final int value) {
        this.value = value;
    }
}
