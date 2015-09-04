if(PROD_FEATURE_DEV_ENV)
    set(CMAKE_INSTALL_PREFIX
        ${PLATFORM_RPM_COMPONENTS}/acume-solution)
execute_process(COMMAND mkdir -p ${CMAKE_INSTALL_PREFIX})
endif()

install(DIRECTORY target/acume-war-${SUPER_NAME}${SUPER_VERSION} DESTINATION ${CMAKE_INSTALL_PREFIX}/opt/tms/java)


add_custom_target(acume_solution_rpm
                  WORKING_DIRECTORY ${CMAKE_INSTALL_PREFIX}
                  COMMAND make-rpm.sh
                  -s ${CMAKE_CURRENT_SOURCE_DIR}/acume-solution.spec
                  -r ${CMAKE_INSTALL_PREFIX}
                  -o ${PLATFORM_RPM_REFLEX_DIR}
                  -c
${PROD_OUTPUT_ROOT}/product-guavus-x86_64/build/release/build_version.sh
                  VERBATIM)

add_dependencies(rpm acume_solution_rpm)
