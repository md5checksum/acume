execute_process(COMMAND cat ${CMAKE_CURRENT_SOURCE_DIR}/../gms/.MODULE_VERSION
                COMMAND tr "\n" " "
                COMMAND sed -e "s/ //"
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                OUTPUT_VARIABLE GMS_MODULE_VERSION)

