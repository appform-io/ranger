/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.appform.ranger.discovery.bundle.id;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import io.appform.ranger.discovery.bundle.id.v2.formatter.IdParsersV2;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@SuppressWarnings("unused")
@Slf4j
@UtilityClass
public class IdGeneratorV2 {
    private static final IdGeneratorBase baseGenerator = new IdGeneratorBase();
    
    public static void initialize() {
        baseGenerator.setNodeId(NodeUtils.getNode());
    }

    public static synchronized void cleanUp() {
        baseGenerator.cleanUp();
    }

    public static synchronized void initialize(
            List<IdValidationConstraint> globalConstraints,
            Map<String, List<IdValidationConstraint>> domainSpecificConstraints) {
        initialize();
        if(null != globalConstraints && !globalConstraints.isEmpty() ) {
            baseGenerator.registerGlobalConstraints(globalConstraints);
        }

        if (null != domainSpecificConstraints) {
            domainSpecificConstraints.forEach(baseGenerator::registerDomainSpecificConstraints);
        }
    }

    public static void registerDomain(Domain domain) {
        baseGenerator.registerDomain(domain);
    }

    public static synchronized void registerGlobalConstraints(IdValidationConstraint... constraints) {
        registerGlobalConstraints(ImmutableList.copyOf(constraints));
    }

    public static synchronized void registerGlobalConstraints(List<IdValidationConstraint> constraints) {
        baseGenerator.registerGlobalConstraints(constraints);
    }

    public static synchronized void registerDomainSpecificConstraints(
            final String domain,
            final IdValidationConstraint... validationConstraints) {
        registerDomainSpecificConstraints(domain, ImmutableList.copyOf(validationConstraints));
    }

    public static synchronized void registerDomainSpecificConstraints(
            String domain,
            List<IdValidationConstraint> validationConstraints) {
        baseGenerator.registerDomainSpecificConstraints(domain, validationConstraints);
    }

    /**
     * Generate id with given prefix
     *
     * @param prefix String prefix with will be used to blindly merge
     * @param suffix String suffix with will be appended at end
     * @param idGenerationFormatters Formatter to use for id generation
     * @return Generated Id
     */
    public static Id generate(final String prefix,
                              final String suffix,
                              final int idGenerationFormatters) {
        validateIdRegex(prefix);
        return baseGenerator.generate(prefix, suffix, idGenerationFormatters);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix String prefix
     * @param suffix String suffix
     * @param domain Domain for constraint selection
     * @param idGenerationFormatters Formatter to use for id generation
     * @return Return generated id or empty if it was impossible to satisfy constraints and generate
     */
    public static Optional<Id> generateWithConstraints(final String prefix,
                                                       final String suffix,
                                                       final @NonNull String domain,
                                                       final int idGenerationFormatters) {
        return generateWithConstraints(prefix, suffix, domain, true, idGenerationFormatters);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix     String prefix
     * @param suffix     String suffix
     * @param domain     Domain for constraint selection
     * @param idGenerationFormatters Formatter to use for id generation
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(final String prefix,
                                                       final String suffix,
                                                       final @NonNull String domain,
                                                       final boolean skipGlobal,
                                                       final int idGenerationFormatters) {
        validateIdRegex(prefix);
        return baseGenerator.generateWithConstraints(prefix, suffix, domain, skipGlobal, idGenerationFormatters);
    }
    
    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param suffix        String suffix
     * @param inConstraints Constraints that need to be validated.
     * @param idGenerationFormatters Formatter to use for id generation
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(
            final String prefix,
            final String suffix,
            final List<IdValidationConstraint> inConstraints,
            final int idGenerationFormatters) {
        return generateWithConstraints(prefix, suffix, inConstraints, false, idGenerationFormatters);
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return Id if it could be generated
     */
    public static Optional<Id> parse(final String idString) {
        return IdParsersV2.parse(idString);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param suffix        String suffix
     * @param inConstraints Constraints that need to be validate.
     * @param skipGlobal    Skip global constrains and use only passed ones
     * @param idGenerationFormatters Formatter to use for id generation
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(
            final String prefix,
            final String suffix,
            final List<IdValidationConstraint> inConstraints,
            final boolean skipGlobal,
            final int idGenerationFormatters) {
        return generate(IdGenerationRequest.builder()
                                .prefix(prefix)
                                .suffix(suffix)
                                .constraints(inConstraints)
                                .skipGlobal(skipGlobal)
                                .idGenerationFormatters(idGenerationFormatters)
                                .build());
    }

    public static Optional<Id> generate(final IdGenerationRequest request) {
        validateIdRegex(request.getPrefix());
        return baseGenerator.generateWithConstraints(request);
    }
    
    private void validateIdRegex(final String namespace) {
        val idRegex = "^[a-zA-Z]+$";
        Preconditions.checkArgument(
                namespace.matches(idRegex),
                "Prefix does not match the required regex: " + idRegex);
    }
}
