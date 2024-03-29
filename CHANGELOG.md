# Changelog

All notable changes to this project will be documented in this file.

## [1.0-RC15] (2023-12-06)

- Added service discovery bundle usage to readme https://github.com/appform-io/ranger/pull/30
- Move Ranger Discovery Bundle and library version upgrades https://github.com/appform-io/ranger/pull/29
    - Moved discovery bundle from https://github.com/appform-io/dropwizard-service-discovery
    - Upgraded to dropwizard version 2.1.10 : BOM update.
    - Upgraded to java version 17 with release in 11
    - Upgraded to junit 5 and fixed the necessary tests including wiremock versions and tests.
    - Upgraded the curator framework version
    - Removed com.fasterxml and introduced dropwizard-jackson for the jackson bindings.
    - Removed logback dependency
    - Fixed sonar after the java version upgrade

## [1.0-RC14] (2023-11-13)

- Fixes reset of service node list on outage, and return older service list https://github.com/appform-io/ranger/pull/27

## [1.0-RC13] (2023-11-02)

- Handling NodeDataSource refresh failures gracefully https://github.com/appform-io/ranger/pull/25

## [1.0-RC12] (2023-07-28)

- Dynamic service registration https://github.com/appform-io/ranger/pull/22

## [1.0-RC10] (2023-03-21)

- Introduced a portScheme on ServiceNode, so multiple types of protocols could be supported. Was assumed to be HTTP
  instead. https://github.com/appform-io/ranger/pull/20 