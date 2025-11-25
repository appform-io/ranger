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

import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.nonce.RandomNonceGenerator;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

/**
 * Id generation
 */
@SuppressWarnings("unused")
@Slf4j
@UtilityClass
public class IdGenerator {
    private static final IdGeneratorBase baseGenerator = new IdGeneratorBase(IdFormatters.original(), new RandomNonceGenerator(),
            Domain.DEFAULT(DomainVersion.V1));

    public static void initialize() {
        baseGenerator.setNodeId(NodeUtils.getNode());
    }

    public static synchronized void cleanUp() {
        baseGenerator.cleanUp();
    }

    public static synchronized void initialize(
            List<IdValidationConstraint> globalConstraints,
            Map<String, List<IdValidationConstraint>> domainSpecificConstraints) {
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
            String domain,
            IdValidationConstraint... validationConstraints) {
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
     * @return Generated Id
     */
    public static Id generate(String prefix) {
        return baseGenerator.generate(prefix);
    }

    public static Id generate(
            final String prefix,
            final IdFormatter idFormatter) {
        return baseGenerator.generate(prefix, idFormatter);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix String prefix
     * @param domain Domain for constraint selection
     * @return Return generated id or empty if it was impossible to satisfy constraints and generate
     */
    public static Optional<Id> generateWithConstraints(String prefix, @NonNull String domain) {
        return generateWithConstraints(prefix, domain, true);
    }

    /**
     * Generate id that mathces all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix     String prefix
     * @param domain     Domain for constraint selection
     * @param skipGlobal Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(String prefix, @NonNull String domain, boolean skipGlobal) {
        return baseGenerator.generateWithConstraints(prefix, domain, skipGlobal);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validated.
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(
            String prefix,
            final List<IdValidationConstraint> inConstraints) {
        return generateWithConstraints(prefix, inConstraints, false);
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return Id if it could be generated
     */
    public static Optional<Id> parse(final String idString) {
        return IdParsers.parse(idString);
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix        String prefix
     * @param inConstraints Constraints that need to be validate.
     * @param skipGlobal    Skip global constrains and use only passed ones
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(
            String prefix,
            final List<IdValidationConstraint> inConstraints,
            boolean skipGlobal) {
        return generate(IdGenerationRequest.builder()
                                .prefix(prefix)
                                .constraints(inConstraints)
                                .skipGlobal(skipGlobal)
                                .idFormatter(IdFormatters.original())
                                .build());
    }

    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param prefix     String prefix
     * @param skipGlobal Skip global constrains and use only passed ones
     * @param domain     Domain
     * @return Id if it could be generated
     */
    private static Optional<Id> generateWithConstraints(
            String prefix,
            final Domain domain,
            boolean skipGlobal) {
        return generate(IdGenerationRequest.builder()
                                .prefix(prefix)
                                .constraints(domain.getConstraints())
                                .skipGlobal(skipGlobal)
                                .domain(domain.getDomain())
                                .idFormatter(domain.getIdFormatter())
                                .build());
    }

    public static Optional<Id> generate(final IdGenerationRequest request) {
        return baseGenerator.generateWithConstraints(request);
    }

}
