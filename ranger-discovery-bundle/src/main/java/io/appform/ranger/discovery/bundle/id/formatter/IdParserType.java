package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.Getter;

@Getter
public enum IdParserType {
    DEFAULT (00),
    SUFFIX (01);

    private final int value;

    IdParserType(final int value) {
        this.value = value;
    }
}
