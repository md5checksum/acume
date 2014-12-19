add_subdirectory(cache)
add_subdirectory(war)
add_subdirectory(core)

add_custom_target(acume
                  ALL
                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
                  COMMAND mvn clean install -DskipTests=true 
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
install(DIRECTORY war/target/acume-war-${SUPER_NAME}${SUPER_VERSION} DESTINATION opt/tms/java)
install(DIRECTORY war/src/main/tomcat DESTINATION opt/tms/acume)

install(CODE "execute_process(COMMAND chmod +x ${CMAKE_INSTALL_PREFIX}/opt/tms/acume/tomcat/bin/startup.sh)")
install(CODE "execute_process(COMMAND mkdir -p modules/gms_${GMS_MODULE_VERSION}/config-mgmt/webtool/configure/gms/Profiles/Default/)")
install(DIRECTORY Profiles/Default/ DESTINATION modules/gms_${GMS_MODULE_VERSION}/config-mgmt/webtool/configure/gms/Profiles/Default/ FILES_MATCHING PATTERN "*.xml")
