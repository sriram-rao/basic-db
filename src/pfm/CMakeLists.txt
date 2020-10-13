add_library(pfm pfm.cc Record.cpp ../include/Record.h Slot.cpp ../include/Slot.h)
add_dependencies(pfm googlelog)
target_link_libraries(pfm glog)