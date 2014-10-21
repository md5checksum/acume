INCLUDE(${CMAKE_SOURCE_DIR}/version.cmake)

add_subdirectory(cache)
add_subdirectory(war)
add_subdirectory(core)

add_custom_target(acume
                  ALL
                  COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
                  COMMAND mvn clean install -DskipTests=true 
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
install(FILES war/target/war-${SUPER_NAME}${SUPER_VERSION}.war DESTINATION opt/tms/java)
