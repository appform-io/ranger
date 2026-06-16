package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.joda.time.DateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FormattedId {
    private String id;
    private DateTime dateTime;
    private int exponent;
    private long time;
}
