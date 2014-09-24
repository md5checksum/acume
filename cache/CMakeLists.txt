add_custom_target(Acume
                  ALL
						COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
						COMMAND mvn clean install -DskipTests=true 
						WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
install(FILES target/Acume-${PROJECT_VERSION}-jar-with-dependencies.jar DESTINATION /opt/tms/acume/lib)
