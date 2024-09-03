package io.appform.ranger.discovery.bundle.id;

import lombok.val;
import org.joda.time.DateTime;


public class IdUtils {
    public static DateTime getDateTimeFromSeconds(long seconds) {
        // Convert seconds to milliSeconds
        val millis = seconds * 1000L;
        // Get DateTime object from milliSeconds
        return new DateTime(millis);
    }
}
