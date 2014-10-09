add_custom_target(genjaxbclasses
                  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
						                  COMMAND xjc -p com.guavus.acume.cache.gen -d src/main/java/ src/main/resources/cubedefinition.xsd)

add_custom_target(Acume
                  ALL
						COMMAND ./pom.py ${SUPER_NAME}${SUPER_VERSION}
						COMMAND mvn clean install -DskipTests=true 
						WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})

add_dependencies(Acume genjaxbclasses)

install(FILES target/Acume-${PROJECT_VERSION}-jar-with-dependencies.jar DESTINATION /opt/tms/acume/lib)
