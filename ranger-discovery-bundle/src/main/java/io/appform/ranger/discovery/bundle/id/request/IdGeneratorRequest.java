package io.appform.ranger.discovery.bundle.id.request;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.appform.ranger.discovery.bundle.id.IdGeneratorType;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorators;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.Data;
import lombok.val;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Data
public class IdGeneratorRequest {
    final String prefix;
    final String suffix;
    final boolean includeBase36;
    final boolean includeRandomNonce;
    final int idGenerators;
    
    private IdGeneratorRequest(IdGeneratorRequestBuilder builder) {
        this.prefix = builder.prefix;
        this.suffix = builder.suffix;
        this.includeBase36 = builder.includeBase36;
        this.includeRandomNonce = builder.includeRandomNonce;
        this.idGenerators = builder.idGenerationType;
    }
    
    public static IdGeneratorRequestBuilder builder() {
        return new IdGeneratorRequestBuilder();
    }
    
    public static class IdGeneratorRequestBuilder {
        private String prefix;
        private String suffix;
        private boolean includeBase36;
        private boolean includeRandomNonce;
        private IdFormatter idFormatter;
        private final List<IdDecorator> idDecorators = new ArrayList<>();
        private int idGenerationType;
        
        public IdGeneratorRequestBuilder withPrefix(final String prefix) {
            validateIdPrefix(prefix);
            this.prefix = prefix;
            return this;
        }
        
        public IdGeneratorRequestBuilder withSuffix(final String suffix) {
            validateIdSuffix(prefix);
            this.suffix = suffix;
            return this;
        }
        
        public IdGeneratorRequestBuilder includeBase36() {
            this.includeBase36 = true;
            idDecorators.add(IdDecorators.base36());
            return this;
        }
        
        public IdGeneratorRequestBuilder includeRandomNonce() {
            validateIdFormatterSelection(idFormatter);
            this.includeRandomNonce = true;
            idFormatter = IdFormatters.randomNonce();
            return this;
        }
        
        public IdGeneratorRequest build() {
            this.idGenerationType = IdGeneratorType.findValue(
                    idFormatter, idDecorators)
                    .orElseThrow(() -> new RuntimeException("Invalid combination of formatter and decorators"));
            return new IdGeneratorRequest(this);
        }
    }
    
    private static void validateIdPrefix(final String namespace) {
        if (Strings.isNullOrEmpty(namespace)) {
            return;
        }
        val idRegex = "^[a-zA-Z]+$";
        Preconditions.checkArgument(
                namespace.matches(idRegex),
                "Prefix does not match the required regex: " + idRegex);
    }
    
    private static void validateIdSuffix(final String suffix) {
        if (Strings.isNullOrEmpty(suffix)) {
            return;
        }
        val idRegex = "^[a-zA-Z0-9]+$";
        Preconditions.checkArgument(
                suffix.matches(idRegex),
                "Suffix does not match the required regex: " + idRegex);
    }
    
    private static void validateIdFormatterSelection(final IdFormatter idFormatter) {
        Preconditions.checkArgument(
                Objects.isNull(idFormatter),
                "Only one IdFormatter can be selected per request" + idFormatter);
    }
}
