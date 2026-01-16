package io.appform.ranger.discovery.bundle.id;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.AllArgsConstructor;
import lombok.Builder;

import java.util.Date;

/**
 * A representation of an ID
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class InternalId {
    private String id;
    private Date generatedDate;
    private int node;
    private int exponent;
    private long time;
    private String prefix;
    private String suffix;
}
