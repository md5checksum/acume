INCLUDE(${CMAKE_CURRENT_SOURCE_DIR}/GMSmoduleversion.cmake)

if(PROD_FEATURE_DEV_ENV)
    set(CMAKE_INSTALL_PREFIX
	   ${PLATFORM_RPM_COMPONENTS}/acume-base)
    set(CMAKE_PHOENIX_ACUME_INSTALL_PREFIX
        ${PLATFORM_RPM_COMPONENTS}/acume-base-phoenix)
execute_process(COMMAND mkdir -p ${CMAKE_INSTALL_PREFIX})
else()
    set(CMAKE_PHOENIX_ACUME_INSTALL_PREFIX
        ${CMAKE_INSTALL_PREFIX})
endif()


add_subdirectory(war)

add_custom_target(acume
                  ALL
                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
                  COMMAND mvn clean install -DskipTests=true
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

install(DIRECTORY war/src/main/tomcat DESTINATION ${CMAKE_INSTALL_PREFIX}/opt/tms/acume)

add_custom_target(genjaxbforacume
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                  COMMAND xjc -p com.guavus.acume.cache.gen -d cache/src/main/java/ cache/src/main/resources/cubedefinition.xsd)

add_dependencies(acume genjaxbforacume)


install(CODE "execute_process(COMMAND chmod +x ${CMAKE_INSTALL_PREFIX}/opt/tms/acume/tomcat/bin/startup.sh)")
install(CODE "execute_process(COMMAND chmod +x ${CMAKE_INSTALL_PREFIX}/opt/tms/acume/tomcat/bin/getUdfJarPaths.sh)")

install(CODE "execute_process(COMMAND mkdir -p ${CMAKE_PHOENIX_ACUME_INSTALL_PREFIX}/modules/gms_${GMS_MODULE_VERSION}/config-mgmt/webtool/configure/gms/Profiles/Default/)")
install(DIRECTORY Profiles/Default/ DESTINATION ${CMAKE_PHOENIX_ACUME_INSTALL_PREFIX}/modules/gms_${GMS_MODULE_VERSION}/config-mgmt/webtool/configure/gms/Profiles/Default/ FILES_MATCHING PATTERN "*.xml")


add_custom_target(acume_base_rpm
                  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}
                  COMMAND make-rpm.sh
                  -s ${CMAKE_CURRENT_SOURCE_DIR}/acume-base.spec
                  -r ${CMAKE_INSTALL_PREFIX}
                  -o ${PLATFORM_RPM_REFLEX_DIR}
                  -c
${PROD_OUTPUT_ROOT}/product-guavus-x86_64/build/release/build_version.sh
                  VERBATIM)

add_custom_target(acume_base_phoenix_rpm
                  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}
                  COMMAND make-rpm.sh
                  -s ${CMAKE_CURRENT_SOURCE_DIR}/acume-base-phoenix.spec
                  -r ${CMAKE_PHOENIX_ACUME_INSTALL_PREFIX}
                  -o ${PLATFORM_RPM_REFLEX_DIR}
                  -c
${PROD_OUTPUT_ROOT}/product-guavus-x86_64/build/release/build_version.sh
                  VERBATIM)


add_dependencies(rpm acume_base_rpm)
add_dependencies(rpm acume_base_phoenix_rpm)
