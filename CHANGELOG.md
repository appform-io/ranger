# Changelog
All notable changes to this project will be documented in this file.

## [1.0-RC17]
- Domain specific collision checker in id generator

## [1.0-RC16]
- Fixed hierarchical environment aware shard selector  : If a service  is deployed with environment :  env.x.y.z then it should be able to discover other services present in environment -  [env , env.x , env.x.y, env.x.y.z ]

## [1.0-RC15]
- Moved discovery bundle from https://github.com/appform-io/dropwizard-service-discovery.
- Updated to dropwizard version 2.1.10 : BOM update.
- Upgraded to java version 17 with release in 11
- Removed com.fasterxml and introduced dropwizard-jackson for the jackson bindings.
- Upgraded to junit 5 and fixed the necessary tests including wiremock versions and tests.
- Upgraded the curator framework version
- Done sonar fixes after the java version upgrade

## [1.0-RC14]
- When the Collection of Services is empty the monitor should gracefully return [PR](https://github.com/appform-io/ranger/pull/27)

## [1.0-RC13]
- Handling NodeDataSource refresh failures gracefully

## [1.0-RC12]
- Dynamic service registration

## [1.0-RC11]
- Introduced a portScheme on ServiceNode, so multiple types of protocols could be supported. Was assumed to be HTTP instead. 