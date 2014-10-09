#INCLUDE(${CMAKE_SOURCE_DIR}/version.cmake)
#
#add_subdirectory(cache)
#add_subdirectory(core)
#
#execute_process(COMMAND mkdir -p ${CMAKE_INSTALL_PREFIX}/opt/tms/acume/lib))
#execute_process(COMMAND mkdir -p ${CMAKE_INSTALL_PREFIX}/opt/tms/acume/classes))
#add_custom_target(Acume
#                  ALL
#                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
#                  COMMAND mvn clean install -DskipTests=true 
#                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
#
##install(FILES Acume/target/Acume-${SUPER_NAME}${SUPER_VERSION}-jar-with-guava-hive-dependencies.jar DESTINATION opt/tms/java)
#install(CODE "execute_process(COMMAND unzip -u -d ${ATLAS_DEPLOY_PATH}/Acume-${PROJECT_VERSION} ${WAR_PATH}/Acume-${PROJECT_VERSION}.war)")
