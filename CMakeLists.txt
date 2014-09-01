INCLUDE(${CMAKE_SOURCE_DIR}/version.cmake)

add_custom_target(IntelligentCache
                  ALL
                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
                  COMMAND mvn clean install -DskipTests=true 
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

install(FILES IntelligentCache/target/IntelligentCache-${SUPER_NAME}${SUPER_VERSION}-jar-with-guava-hive-dependencies.jar DESTINATION opt/tms/java)
install(CODE "execute_process(COMMAND unzip -u -d ${ATLAS_DEPLOY_PATH}/IntelligentCache-${PROJECT_VERSION} ${WAR_PATH}/IntelligentCache-${PROJECT_VERSION}.war)")

set(REPORTS_LOCATION ${CMAKE_INSTALL_PREFIX}/opt/tms/rge-${PROJECT_VERSION}/WEB-INF/classes)
set(LIB_LOCATION ${CMAKE_INSTALL_PREFIX}/opt/tms/rge-${PROJECT_VERSION}/WEB-INF/lib)
set(FONTS_LOCATION ${CMAKE_INSTALL_PREFIX}/usr/java/default/lib/fonts)

