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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.generator.IdProvider;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsersV2;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInternalRequest;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequestV2;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
import lombok.NonNull;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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
     * @param request Request containing prefix, suffix and other formatting details
     * @return Generated Id
     */
    public static Id generate(final IdGenerationRequestV2 request) {
        val internalRequest = getRequest(request);
        return baseGenerator.getId(getIdFromIdInfo(internalRequest));
    }
    
    /**
     * Generate id that matches all passed constraints.
     * NOTE: There are performance implications for this.
     * The evaluation of constraints will take it's toll on id generation rates. Tun rests to check speed.
     *
     * @param request Request containing prefix, suffix and other formatting details
     * @return Id if it could be generated
     */
    public static Optional<Id> generateWithConstraints(final IdGenerationRequestV2 request) {
        val internalRequest = getRequest(request);
        return baseGenerator.generateWithConstraints(internalRequest, IdGeneratorV2::getIdFromIdInfo)
                .map(baseGenerator::getId);
    }
    
    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return Id if it could be generated
     */
    public static Optional<Id> parse(final String idString) {
        val parsedId = IdParsersV2.parse(idString).orElse(null);
        return Optional.ofNullable(baseGenerator.getId(parsedId));
    }
    
    private InternalId getIdFromIdInfo(final IdGenerationInternalRequest request) {
        val domain = request.getDomain() != null ? baseGenerator.getRegisteredDomains().getOrDefault(request.getDomain(), Domain.DEFAULT) : Domain.DEFAULT;
        val idGenerationInput = IdGenerationInput.builder()
                .domain(domain)
                .build();
        
        // FormattedId based on nonce result
        val formattedId = request.getIdFormatter().format(baseGenerator.getNodeId(), idGenerationInput);
        // Applying decorators on formattedId
        String decoratedId = formattedId.getId();
        for (IdDecorator idDecorator: request.getIdDecorators()) {
            decoratedId = idDecorator.decorate(decoratedId);
        }
        
        val id = String.format("%s%02d%s%s", request.getPrefix(), request.getIdGenerationValue(), decoratedId, request.getSuffix());
        return baseGenerator.getIdFromIdInfo(id, formattedId);
    }
    
    private IdGenerationInternalRequest getRequest(final IdGenerationRequestV2 request) {
        return IdGenerationInternalRequest.builder()
                .prefix(request.getPrefix())
                .constraints(request.getConstraints())
                .skipGlobal(request.isSkipGlobal())
                .idFormatter(request.getIdFormatter())
                .domain(request.getDomain())
                .suffix(request.getSuffix())
                .idFormatter(request.getIdFormatter())
                .idDecorators(request.getIdDecorators())
                .idGenerationValue(request.getIdGenerationValue())
                .build();
    }
}
