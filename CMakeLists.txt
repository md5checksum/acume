INCLUDE(${CMAKE_SOURCE_DIR}/version.cmake)

add_subdirectory(cache)
add_subdirectory(war)
add_subdirectory(core)

add_custom_target(acume
                  ALL
                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
                  COMMAND mvn clean install -DskipTests=true 
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
#install(FILES war/target/acume-war-${SUPER_NAME}${SUPER_VERSION} DESTINATION opt/tms/java)
#install(CODE "execute_process(COMMAND unzip -u -d opt/tms/java/acume-war-${PROJECT_VERSION} ${WAR_PATH}/acume-war-${PROJECT_VERSION}.war)")
install(CODE "execute_process(COMMAND unzip -u -d ${ATLAS_DEPLOY_PATH}/acume-war-${PROJECT_VERSION} ${WAR_PATH}/acume-war-${PROJECT_VERSION}.war)")
